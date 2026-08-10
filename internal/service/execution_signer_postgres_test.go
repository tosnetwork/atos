// Integration test against a real Postgres -- skipped unless
// ATOS_TEST_DATABASE_URL is set. Run with:
//
//	ATOS_TEST_DATABASE_URL="postgres://user@localhost:5432/atos_test?sslmode=disable" go test ./internal/service/... -run TestExecutionSignerService_TwoReplicasRecoverSameStuckOperation
package service_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

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
