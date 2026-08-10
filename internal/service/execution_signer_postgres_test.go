// Integration test against a real Postgres -- skipped unless
// ATOS_TEST_DATABASE_URL is set. Run with:
//
//	ATOS_TEST_DATABASE_URL="postgres://user@localhost:5432/atos_test?sslmode=disable" go test ./internal/service/... -run TestExecutionSignerService_TwoReplicasRecoverSameStuckOperation
package service_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

// blockingSignerCore lets a test force genuine overlap between two
// concurrent signer operations instead of relying on raw goroutine
// scheduling luck (against a fast mock core with no real network, the
// first of two "concurrent" calls routinely runs to completion before the
// second even starts, which would make TestExecutionSignerService_
// ConcurrentRotationsOnDifferentIdempotencyKeysDoNotBothSucceed pass for
// the wrong reason -- two sequential rotations, not two racing ones).
// AuthorizeExecutionSigner blocks the FIRST time it is called (after
// driveRotate has already durably advanced the operation past
// intent_persisted to new_authorization_pending -- a non-terminal
// checkpoint) until the test closes proceed, giving the test a deterministic
// window in which a second, independent operation attempt is guaranteed
// to still see the first as in flight.
type blockingSignerCore struct {
	toscore.Core
	// blockOnExecutionSignerID names the ONE execution_signer_id whose
	// AuthorizeExecutionSigner call should block -- not simply "the first
	// call ever", since the test's own setup (authorizing the ORIGINAL
	// signer before the race begins) also goes through this same method
	// and must not be blocked, or the test deadlocks before the race it
	// means to set up ever starts.
	blockOnExecutionSignerID string
	authorizeStarted         chan struct{}
	proceed                  chan struct{}
	blockOnce                sync.Once
}

func (c *blockingSignerCore) AuthorizeExecutionSigner(ctx context.Context, req toscore.AuthorizeExecutionSignerRequest) (toscore.ExecutionSignerAuthorization, bool, error) {
	if req.ExecutionSignerID == c.blockOnExecutionSignerID {
		c.blockOnce.Do(func() {
			close(c.authorizeStarted)
			<-c.proceed
		})
	}
	return c.Core.AuthorizeExecutionSigner(ctx, req)
}

// TestExecutionSignerService_ConcurrentRotationsOnDifferentIdempotencyKeysDoNotBothSucceed
// is the exact P0 scenario a review of this PR flagged: two Rotate calls
// for the SAME capability, using DIFFERENT idempotency keys (so neither is
// a retry of the other -- resumeOrConflict never applies), with genuine
// overlap. Both would read old signer A as "current" before either has
// persisted anything that would change it; without serialization at the
// (capability_id, capability_version) level -- specifically, without
// rejecting a second open while any non-terminal operation already exists
// for that capability version, since a lock around the read-then-open
// step ALONE only prevents them reading at the exact same instant, not in
// quick succession before the first reaches Completed -- both could
// independently authorize a new signer and complete, leaving two
// valid-at-tos-protocol signers (B and C) with only one ever visible
// through CurrentSigner: the other permanently orphaned, unnamed and
// unrevokable.
// TestExecutionSignerService_ConcurrentAuthorizationsOnDifferentIdempotencyKeysDoNotBothSucceed
// is Authorize's counterpart to the Rotate test below: Authorize is not
// exempt from capability-scoped serialization just because it never reads
// CurrentSigner going in -- two concurrent Authorize calls on different
// idempotency keys for a capability with NO current signer yet could
// previously both open and complete independently, leaving two
// valid-at-tos-protocol signers (B and C) with only one ever visible
// through CurrentSigner. Uses the same blockingSignerCore-forced-overlap
// technique as the Rotate test for the same reason: a fast mock core
// makes two "concurrent" calls routinely run sequentially by accident.
func TestExecutionSignerService_ConcurrentAuthorizationsOnDifferentIdempotencyKeysDoNotBothSucceed(t *testing.T) {
	url := os.Getenv("ATOS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx := context.Background()
	sA, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sA.Close()
	sB, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sB.Close()

	providerID := "prov_sigop_concurrentauth_" + uuid.NewString()
	capabilities := service.NewCapabilityService(sA)
	blocking := &blockingSignerCore{
		Core: toscoremock.NewContractFixture(sA), blockOnExecutionSignerID: "signer-auth-B",
		authorizeStarted: make(chan struct{}), proceed: make(chan struct{}),
	}
	signersA := service.NewExecutionSignerService(sA, blocking, capabilities)
	signersB := service.NewExecutionSignerService(sB, blocking, capabilities)

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Concurrent Authorize Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-" + providerID,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	authorizeAs := func(signers *service.ExecutionSignerService, signerID, idempotencyKey string) (domain.ExecutionSignerOperation, error) {
		return signers.Authorize(ctx, service.AuthorizeSignerInput{
			ProviderID: providerID, CapabilityID: cap.ID,
			ExecutionSignerID: signerID, SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
			ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
			IdempotencyKey: idempotencyKey,
		})
	}

	var wg sync.WaitGroup
	var firstOp domain.ExecutionSignerOperation
	var firstErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		firstOp, firstErr = authorizeAs(signersA, "signer-auth-B", "authz-to-b")
	}()

	select {
	case <-blocking.authorizeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first authorize to reach AuthorizeExecutionSigner")
	}

	secondOp, secondErr := authorizeAs(signersB, "signer-auth-C", "authz-to-c")
	close(blocking.proceed)
	wg.Wait()

	if firstErr != nil {
		t.Fatalf("first authorize (which was already in flight) unexpectedly failed: %v", firstErr)
	}
	if firstOp.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("first authorize checkpoint = %s, want completed", firstOp.Checkpoint)
	}
	if secondErr == nil {
		t.Fatalf("expected the second authorize to be rejected while the first was still in flight, got %+v", secondOp)
	}
	var domainErr *domain.Error
	if !errors.As(secondErr, &domainErr) || domainErr.Code != domain.ErrSignerOperationInProgress {
		t.Fatalf("second authorize error = %v, want domain.ErrSignerOperationInProgress", secondErr)
	}

	_, signerID, found, err := signersA.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatalf("CurrentSigner: %v", err)
	}
	if !found || signerID != "signer-auth-B" {
		t.Fatalf("current signer = %q found=%v, want signer-auth-B (the authorize that was already in flight)", signerID, found)
	}

	// The rejected second authorize's target must never have reached
	// tos-protocol at all.
	_, loserFound, err := blocking.Core.ResolveExecutionSignerAuthorization(ctx, providerID, cap.ID, cap.Version, "signer-auth-C", time.Now().UTC())
	if err != nil {
		t.Fatalf("ResolveExecutionSignerAuthorization: %v", err)
	}
	if loserFound {
		t.Fatal("the rejected authorize's signer-auth-C was authorized at tos-protocol despite being rejected before opening -- orphaned signer")
	}
}

func TestExecutionSignerService_ConcurrentRotationsOnDifferentIdempotencyKeysDoNotBothSucceed(t *testing.T) {
	url := os.Getenv("ATOS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx := context.Background()
	sA, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sA.Close()
	sB, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sB.Close()

	providerID := "prov_sigop_concurrentrotate_" + uuid.NewString()
	capabilities := service.NewCapabilityService(sA)
	blocking := &blockingSignerCore{
		Core: toscoremock.NewContractFixture(sA), blockOnExecutionSignerID: "signer-new-B",
		authorizeStarted: make(chan struct{}), proceed: make(chan struct{}),
	}
	signersA := service.NewExecutionSignerService(sA, blocking, capabilities)
	signersB := service.NewExecutionSignerService(sB, blocking, capabilities)

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Concurrent Rotate Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-" + providerID,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := signersA.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: providerID, CapabilityID: cap.ID,
		ExecutionSignerID: "signer-old-concurrentrotate", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-old-concurrentrotate",
	}); err != nil {
		t.Fatalf("initial authorize: %v", err)
	}

	rotateTo := func(signers *service.ExecutionSignerService, newSignerID, idempotencyKey string) (domain.ExecutionSignerOperation, error) {
		return signers.Rotate(ctx, service.RotateSignerInput{
			ProviderID: providerID, CapabilityID: cap.ID,
			NewExecutionSignerID: newSignerID, NewSignerPublicKey: testSignerKey(t), NewSignatureAlgorithm: "ed25519",
			NewValidFrom: time.Now().UTC().Add(-time.Minute), NewValidUntil: time.Now().UTC().Add(24 * time.Hour),
			RevocationReasonCode: "test", IdempotencyKey: idempotencyKey,
		})
	}

	var wg sync.WaitGroup
	var firstOp domain.ExecutionSignerOperation
	var firstErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		firstOp, firstErr = rotateTo(signersA, "signer-new-B", "rotate-to-b")
	}()

	// Wait until the first rotation has durably advanced past
	// intent_persisted (a non-terminal checkpoint) and is blocked inside
	// its AuthorizeExecutionSigner call -- guaranteed overlap, not a race
	// against goroutine scheduling.
	select {
	case <-blocking.authorizeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first rotation to reach AuthorizeExecutionSigner")
	}

	secondOp, secondErr := rotateTo(signersB, "signer-new-C", "rotate-to-c")
	close(blocking.proceed)
	wg.Wait()

	if firstErr != nil {
		t.Fatalf("first rotation (which was already in flight) unexpectedly failed: %v", firstErr)
	}
	if firstOp.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("first rotation checkpoint = %s, want completed", firstOp.Checkpoint)
	}
	if secondErr == nil {
		t.Fatalf("expected the second rotation to be rejected while the first was still in flight, got %+v", secondOp)
	}
	var domainErr *domain.Error
	if !errors.As(secondErr, &domainErr) || domainErr.Code != domain.ErrSignerOperationInProgress {
		t.Fatalf("second rotation error = %v, want domain.ErrSignerOperationInProgress", secondErr)
	}
	if !domainErr.Retryable {
		t.Fatal("ErrSignerOperationInProgress must be retryable -- the caller should retry once the in-flight operation completes")
	}

	_, signerID, found, err := signersA.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatalf("CurrentSigner: %v", err)
	}
	if !found || signerID != "signer-new-B" {
		t.Fatalf("current signer = %q found=%v, want signer-new-B (the rotation that was already in flight)", signerID, found)
	}

	// signer-new-C must not exist as an orphaned, unnamed authorization at
	// tos-protocol either -- the rejected rotation must never have reached
	// the point of calling AuthorizeExecutionSigner for it at all.
	_, loserFound, err := blocking.Core.ResolveExecutionSignerAuthorization(ctx, providerID, cap.ID, cap.Version, "signer-new-C", time.Now().UTC())
	if err != nil {
		t.Fatalf("ResolveExecutionSignerAuthorization: %v", err)
	}
	if loserFound {
		t.Fatal("the rejected rotation's signer-new-C was authorized at tos-protocol despite being rejected before opening -- orphaned signer")
	}

	// The retryable promise must be real: retrying the rejected rotation
	// after the first one completed must now succeed.
	retryOp, err := rotateTo(signersB, "signer-new-C", "rotate-to-c-retry")
	if err != nil {
		t.Fatalf("retry after the in-flight rotation completed: %v", err)
	}
	if retryOp.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("retry checkpoint = %s, want completed", retryOp.Checkpoint)
	}
}

// TestExecutionSignerService_TwoReplicasRecoverSameStuckOperation is the
// §20/§28 multi-replica correctness requirement applied to the
// reconciler: two independent ATOS replicas (separate postgres.Store
// connections to the SAME database, exactly like two real API server
// processes) racing to drive forward the SAME stuck rotation must
// converge to exactly one completed outcome -- no split-brain signer
// projection, no duplicate remote mutation.
//
// The remote authority (toscore.Core) is deliberately ONE shared
// instance, modeling the single real tos-protocol backend both replicas
// would actually talk to; only the store connections are independent,
// modeling each replica's own connection pool to the same physical
// database.
func TestExecutionSignerService_TwoReplicasRecoverSameStuckOperation(t *testing.T) {
	url := os.Getenv("ATOS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx := context.Background()
	sA, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sA.Close()
	sB, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sB.Close()

	providerID := "prov_sigop_tworeplica_" + uuid.NewString()
	capabilitiesA := service.NewCapabilityService(sA)
	fake := &flakySignerCore{Core: toscoremock.NewContractFixture(sA)}
	signersA := service.NewExecutionSignerService(sA, fake, capabilitiesA)
	signersB := service.NewExecutionSignerService(sB, fake, capabilitiesA)

	cap, err := capabilitiesA.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Two Replica Signer Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-" + providerID,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := signersA.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: providerID, CapabilityID: cap.ID,
		ExecutionSignerID: "signer-old-tworeplica", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-old-tworeplica",
	}); err != nil {
		t.Fatalf("initial authorize: %v", err)
	}

	// Force the rotation to get stuck Reconciling on the FIRST attempt.
	fake.authorizeFailuresLeft = 1
	fake.authorizeFailureRetryable = true
	rotateIn := service.RotateSignerInput{
		ProviderID: providerID, CapabilityID: cap.ID,
		NewExecutionSignerID: "signer-new-tworeplica", NewSignerPublicKey: testSignerKey(t), NewSignatureAlgorithm: "ed25519",
		NewValidFrom: time.Now().UTC().Add(-time.Minute), NewValidUntil: time.Now().UTC().Add(24 * time.Hour),
		RevocationReasonCode: "test", IdempotencyKey: "rotate-tworeplica",
	}
	if _, err := signersA.Rotate(ctx, rotateIn); err == nil {
		t.Fatal("expected the first rotate attempt to fail ambiguously")
	}

	// Both "replicas" now race to reconcile the same stuck operation
	// concurrently.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs <- signersA.ReconcileStaleOperations(ctx, time.Now().UTC().Add(time.Hour), 1000)
	}()
	go func() {
		defer wg.Done()
		errs <- signersB.ReconcileStaleOperations(ctx, time.Now().UTC().Add(time.Hour), 1000)
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent reconcile: %v", err)
		}
	}

	opA, foundA, err := signersA.Status(ctx, cap.ID)
	if err != nil || !foundA {
		t.Fatalf("Status via replica A: found=%v err=%v", foundA, err)
	}
	if opA.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("checkpoint via replica A = %s, want completed", opA.Checkpoint)
	}
	_, signerIDA, foundCurrentA, err := signersA.CurrentSigner(ctx, cap.ID)
	if err != nil || !foundCurrentA || signerIDA != "signer-new-tworeplica" {
		t.Fatalf("current signer via replica A = %q found=%v err=%v, want signer-new-tworeplica", signerIDA, foundCurrentA, err)
	}
	_, signerIDB, foundCurrentB, err := signersB.CurrentSigner(ctx, cap.ID)
	if err != nil || !foundCurrentB || signerIDB != "signer-new-tworeplica" {
		t.Fatalf("current signer via replica B = %q found=%v err=%v, want signer-new-tworeplica (both replicas must observe the same converged state)", signerIDB, foundCurrentB, err)
	}
}
