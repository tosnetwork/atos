package service_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// flakySignerCore wraps a real toscore.Core (the stateful mock), letting
// tests inject a bounded number of ambiguous ("network") or definitive
// failures into AuthorizeExecutionSigner/RevokeExecutionSigner before
// falling through to the real implementation -- this is how the
// crash-recovery tests below simulate "the RPC call's outcome was
// uncertain" without a real network.
type flakySignerCore struct {
	toscore.Core
	mu                        sync.Mutex
	authorizeFailuresLeft     int
	authorizeFailureRetryable bool
	revokeFailuresLeft        int
	revokeFailureRetryable    bool
	authorizeCalls            int
	revokeCalls               int
}

func (f *flakySignerCore) AuthorizeExecutionSigner(ctx context.Context, req toscore.AuthorizeExecutionSignerRequest) (toscore.ExecutionSignerAuthorization, bool, error) {
	f.mu.Lock()
	f.authorizeCalls++
	if f.authorizeFailuresLeft > 0 {
		f.authorizeFailuresLeft--
		retryable := f.authorizeFailureRetryable
		f.mu.Unlock()
		return toscore.ExecutionSignerAuthorization{}, false, domain.NewError(domain.ErrNetworkUnavailable, "simulated network failure", retryable)
	}
	f.mu.Unlock()
	return f.Core.AuthorizeExecutionSigner(ctx, req)
}

func (f *flakySignerCore) RevokeExecutionSigner(ctx context.Context, req toscore.RevokeExecutionSignerRequest) (toscore.ExecutionSignerAuthorization, bool, error) {
	f.mu.Lock()
	f.revokeCalls++
	if f.revokeFailuresLeft > 0 {
		f.revokeFailuresLeft--
		retryable := f.revokeFailureRetryable
		f.mu.Unlock()
		return toscore.ExecutionSignerAuthorization{}, false, domain.NewError(domain.ErrNetworkUnavailable, "simulated network failure", retryable)
	}
	f.mu.Unlock()
	return f.Core.RevokeExecutionSigner(ctx, req)
}

func testSignerKey(t *testing.T) []byte {
	t.Helper()
	return []byte("0123456789abcdef0123456789abcdef") // 33 bytes -- length doesn't matter to the mock, only to the real tos-protocol RPC path
}

func registerSignerTestCapability(t *testing.T, capabilities *service.CapabilityService, providerID string, modes ...domain.TrustMode) domain.Capability {
	t.Helper()
	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Signer Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: modes,
		IdempotencyKey:      "register-" + providerID,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return cap
}

func TestExecutionSignerAuthorize_GoldenPath(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_golden", domain.TrustModeManaged, domain.TrustModeVerified)

	now := time.Now().UTC()
	op, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_golden", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-golden", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(24 * time.Hour),
		IdempotencyKey: "authz-golden-1",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if op.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("checkpoint = %s, want completed", op.Checkpoint)
	}
	if op.NewAuthorizationRef == "" {
		t.Fatal("expected a non-empty authorization ref once completed")
	}

	authorization, found, err := core.ResolveExecutionSignerAuthorization(ctx, "agt_sig_golden", cap.ID, cap.Version, "signer-golden", now)
	if err != nil {
		t.Fatal(err)
	}
	if !found || authorization.Revoked {
		t.Fatalf("expected the signer resolvable and not revoked, got found=%v authorization=%+v", found, authorization)
	}
}

// TestExecutionSignerAuthorize_RejectsSecondSignerWhileOneCurrent proves
// Authorize is only for the FIRST signer: once a capability has a current
// signer, a second Authorize (a genuinely new idempotency_key, a
// different execution_signer_id) must be rejected rather than silently
// creating a second, independently-valid-at-tos-protocol signer that
// CurrentSigner never selects -- Rotate is the only path that replaces an
// existing signer.
func TestExecutionSignerAuthorize_RejectsSecondSignerWhileOneCurrent(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_second", domain.TrustModeManaged)

	now := time.Now().UTC()
	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_second", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-second-first", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(24 * time.Hour),
		IdempotencyKey: "authz-second-1",
	}); err != nil {
		t.Fatalf("first Authorize: %v", err)
	}

	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_second", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-second-second", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(24 * time.Hour),
		IdempotencyKey: "authz-second-2",
	}); err == nil {
		t.Fatal("expected a second Authorize (different idempotency_key, different signer) to be rejected while a signer is already current")
	}

	_, signerID, found, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || signerID != "signer-second-first" {
		t.Fatalf("current signer = %q found=%v, want signer-second-first (unaffected by the rejected second Authorize)", signerID, found)
	}
}

// TestExecutionSignerAuthorize_ExplicitValidityChangeConflicts proves the
// validity comparison in resumeOrConflict is a partial exception, not a
// blanket one: when the caller EXPLICITLY supplies a different validity
// window under the SAME idempotency_key, that must conflict like any
// other changed field -- omitting the field (letting the transport layer
// default it) is the only case allowed to differ across delivery attempts.
func TestExecutionSignerAuthorize_ExplicitValidityChangeConflicts(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_validity", domain.TrustModeManaged)

	now := time.Now().UTC()
	first := service.AuthorizeSignerInput{
		ProviderID: "agt_sig_validity", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-validity", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(24 * time.Hour),
		ValidFromExplicit: true, ValidUntilExplicit: true,
		IdempotencyKey: "authz-validity-1",
	}
	if _, err := signers.Authorize(ctx, first); err != nil {
		t.Fatalf("first Authorize: %v", err)
	}

	changed := first
	changed.ValidUntil = now.Add(48 * time.Hour) // explicitly different from the persisted window
	if _, err := signers.Authorize(ctx, changed); err == nil {
		t.Fatal("expected an explicit valid_until change under the same idempotency_key to conflict")
	}
}

func TestExecutionSignerAuthorize_IdempotentReplayDoesNotCallRPCTwice(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	fake := &flakySignerCore{Core: toscoremock.NewContractFixture(st)}
	signers := service.NewExecutionSignerService(st, fake, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_replay", domain.TrustModeManaged, domain.TrustModeVerified)

	in := service.AuthorizeSignerInput{
		ProviderID: "agt_sig_replay", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-replay", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-replay-1",
	}
	first, err := signers.Authorize(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := signers.Authorize(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned a different operation: %s vs %s", second.ID, first.ID)
	}
	if fake.authorizeCalls != 1 {
		t.Fatalf("AuthorizeExecutionSigner RPC calls = %d, want exactly 1 (replay must not re-call)", fake.authorizeCalls)
	}
}

func TestExecutionSignerAuthorize_RejectsNonOwningProvider(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_owner", domain.TrustModeManaged, domain.TrustModeVerified)

	_, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_impostor", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-x", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "authz-owner-1",
	})
	if err == nil {
		t.Fatal("expected a permission error authorizing against a capability owned by a different provider")
	}
}

func TestExecutionSignerRevoke_GoldenPath(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_revoke", domain.TrustModeManaged)

	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_revoke", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-revoke", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-revoke-1",
	}); err != nil {
		t.Fatal(err)
	}

	op, err := signers.Revoke(ctx, service.RevokeSignerInput{
		ProviderID: "agt_sig_revoke", CapabilityID: cap.ID, ReasonCode: "rotation",
		IdempotencyKey: "revoke-revoke-1",
	})
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if op.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("checkpoint = %s, want completed", op.Checkpoint)
	}

	// toscore.Core.ResolveExecutionSignerAuthorization reports a revoked
	// signer as found=false (not "found and Revoked=true") -- it answers
	// "is this signer currently authorized", not "does a record exist".
	_, found, err := core.ResolveExecutionSignerAuthorization(ctx, "agt_sig_revoke", cap.ID, cap.Version, "signer-revoke", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected the revoked signer to no longer resolve as currently authorized")
	}

	_, _, found2, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if found2 {
		t.Fatal("CurrentSigner must report none after a completed revoke")
	}
}

// TestExecutionSignerRevoke_ReplayAfterCompletionReturnsSameOperation
// proves a retry with the same idempotency_key, made AFTER the original
// Revoke already completed, resumes/returns the original operation
// instead of failing. Before this fix, Revoke read CurrentSigner before
// checking for an existing operation -- and a completed revoke leaves no
// current signer, so the retry hit "no currently authorized execution
// signer to revoke" (NotFound) before it ever got a chance to recognize
// its own idempotency_key.
func TestExecutionSignerRevoke_ReplayAfterCompletionReturnsSameOperation(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_revoke_replay", domain.TrustModeManaged)

	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_revoke_replay", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-revoke-replay", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-revoke-replay-1",
	}); err != nil {
		t.Fatal(err)
	}

	revokeIn := service.RevokeSignerInput{
		ProviderID: "agt_sig_revoke_replay", CapabilityID: cap.ID, ReasonCode: "rotation",
		IdempotencyKey: "revoke-replay-1",
	}
	first, err := signers.Revoke(ctx, revokeIn)
	if err != nil {
		t.Fatalf("first Revoke: %v", err)
	}
	if first.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("checkpoint = %s, want completed", first.Checkpoint)
	}

	second, err := signers.Revoke(ctx, revokeIn)
	if err != nil {
		t.Fatalf("replayed Revoke after completion: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned a different operation: %s vs %s", second.ID, first.ID)
	}
	if second.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("replay checkpoint = %s, want completed", second.Checkpoint)
	}
}

func TestExecutionSignerRevoke_RefusedWhileStrongerModeActive(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	fake := &flakySignerCore{Core: toscoremock.NewContractFixture(st)}
	signers := service.NewExecutionSignerService(st, fake, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_active", domain.TrustModeManaged, domain.TrustModeVerified)

	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_active", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-active", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-active-1",
	}); err != nil {
		t.Fatal(err)
	}

	// Force Verified active directly -- isolates the revoke-safety check
	// under test from the full activation pipeline already covered
	// elsewhere.
	forced, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry := forced.ModeSupport.Entry(domain.TrustModeVerified)
	entry.Status = domain.ModeSupportActive
	forced.ModeSupport[domain.TrustModeVerified] = entry
	forced.SupportedTrustModes = forced.ModeSupport.ActiveModes()
	if err := st.Put(ctx, forced); err != nil {
		t.Fatal(err)
	}

	_, err = signers.Revoke(ctx, service.RevokeSignerInput{
		ProviderID: "agt_sig_active", CapabilityID: cap.ID, ReasonCode: "test",
		IdempotencyKey: "revoke-active-1",
	})
	if err == nil {
		t.Fatal("expected Revoke to be refused while verified is active")
	}
	if fake.revokeCalls != 0 {
		t.Fatalf("RevokeExecutionSigner RPC calls = %d, want 0 (refused before ever calling out)", fake.revokeCalls)
	}
}

func TestExecutionSignerRevoke_NotFoundWhenNoCurrentSigner(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_none", domain.TrustModeManaged)

	_, err := signers.Revoke(ctx, service.RevokeSignerInput{
		ProviderID: "agt_sig_none", CapabilityID: cap.ID, ReasonCode: "test",
		IdempotencyKey: "revoke-none-1",
	})
	if err == nil {
		t.Fatal("expected an error revoking when no signer has ever been authorized")
	}
}

func TestExecutionSignerRotate_GoldenPath(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_rotate", domain.TrustModeManaged)

	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_rotate", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-old", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-rotate-1",
	}); err != nil {
		t.Fatal(err)
	}

	op, err := signers.Rotate(ctx, service.RotateSignerInput{
		ProviderID: "agt_sig_rotate", CapabilityID: cap.ID,
		NewExecutionSignerID: "signer-new", NewSignerPublicKey: testSignerKey(t), NewSignatureAlgorithm: "ed25519",
		NewValidFrom: time.Now().UTC().Add(-time.Minute), NewValidUntil: time.Now().UTC().Add(24 * time.Hour),
		RevocationReasonCode: "rotation", IdempotencyKey: "rotate-rotate-1",
	})
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if op.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("checkpoint = %s, want completed", op.Checkpoint)
	}

	now := time.Now().UTC()
	// See the equivalent comment in TestExecutionSignerRevoke_GoldenPath:
	// a revoked signer resolves as found=false, not found=true+Revoked=true.
	_, oldFound, err := core.ResolveExecutionSignerAuthorization(ctx, "agt_sig_rotate", cap.ID, cap.Version, "signer-old", now)
	if err != nil {
		t.Fatal(err)
	}
	if oldFound {
		t.Fatal("expected the old signer to no longer resolve as currently authorized after a completed rotation")
	}
	newAuth, found, err := core.ResolveExecutionSignerAuthorization(ctx, "agt_sig_rotate", cap.ID, cap.Version, "signer-new", now)
	if err != nil {
		t.Fatal(err)
	}
	if !found || newAuth.Revoked {
		t.Fatalf("expected the new signer authorized and not revoked, got found=%v authorization=%+v", found, newAuth)
	}

	_, signerID, foundCurrent, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !foundCurrent || signerID != "signer-new" {
		t.Fatalf("current signer = %q found=%v, want signer-new", signerID, foundCurrent)
	}
}

// TestExecutionSignerRotate_ReplayAfterCompletionReturnsSameOperation
// proves a retry with the same idempotency_key, made AFTER the original
// Rotate already completed, resumes/returns the original operation instead
// of failing. Before this fix, Rotate read CurrentSigner before checking
// for an existing operation -- and a completed rotation makes the NEW
// signer current, so the retry built a fresh candidate operation whose
// OldAuthorizationID/OldExecutionSignerID were the NEW signer (not the
// original old signer the first call actually persisted), which
// OpenSignerOperation's content-hash comparison then rejected as a
// genuine conflict.
func TestExecutionSignerRotate_ReplayAfterCompletionReturnsSameOperation(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_rotate_replay", domain.TrustModeManaged)

	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_rotate_replay", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-old-replay", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-rotate-replay-1",
	}); err != nil {
		t.Fatal(err)
	}

	rotateIn := service.RotateSignerInput{
		ProviderID: "agt_sig_rotate_replay", CapabilityID: cap.ID,
		NewExecutionSignerID: "signer-new-replay", NewSignerPublicKey: testSignerKey(t), NewSignatureAlgorithm: "ed25519",
		NewValidFrom: time.Now().UTC().Add(-time.Minute), NewValidUntil: time.Now().UTC().Add(24 * time.Hour),
		RevocationReasonCode: "rotation", IdempotencyKey: "rotate-replay-1",
	}
	first, err := signers.Rotate(ctx, rotateIn)
	if err != nil {
		t.Fatalf("first Rotate: %v", err)
	}
	if first.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("checkpoint = %s, want completed", first.Checkpoint)
	}

	second, err := signers.Rotate(ctx, rotateIn)
	if err != nil {
		t.Fatalf("replayed Rotate after completion: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned a different operation: %s vs %s", second.ID, first.ID)
	}
	if second.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("replay checkpoint = %s, want completed", second.Checkpoint)
	}

	_, signerID, foundCurrent, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !foundCurrent || signerID != "signer-new-replay" {
		t.Fatalf("current signer after replay = %q found=%v, want signer-new-replay", signerID, foundCurrent)
	}
}

// TestExecutionSignerRotate_OldSignerStaysCurrentThroughAmbiguousAuthorizeFailure
// is the crash-safety centerpiece: an ambiguous failure authorizing the
// NEW signer must never revoke the OLD signer, must leave the operation
// Reconciling, and must leave the OLD signer as the durably-observable
// current signer -- then a retry with the same idempotency key (as if the
// caller or the reconciler tried again) converges to a fully completed
// rotation without ever calling RevokeExecutionSigner before
// AuthorizeExecutionSigner actually succeeded.
func TestExecutionSignerRotate_OldSignerStaysCurrentThroughAmbiguousAuthorizeFailure(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	fake := &flakySignerCore{Core: toscoremock.NewContractFixture(st)}
	signers := service.NewExecutionSignerService(st, fake, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_crash", domain.TrustModeManaged)

	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_crash", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-old-crash", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-crash-1",
	}); err != nil {
		t.Fatal(err)
	}

	fake.authorizeFailuresLeft = 1
	fake.authorizeFailureRetryable = true
	rotateIn := service.RotateSignerInput{
		ProviderID: "agt_sig_crash", CapabilityID: cap.ID,
		NewExecutionSignerID: "signer-new-crash", NewSignerPublicKey: testSignerKey(t), NewSignatureAlgorithm: "ed25519",
		NewValidFrom: time.Now().UTC().Add(-time.Minute), NewValidUntil: time.Now().UTC().Add(24 * time.Hour),
		RevocationReasonCode: "rotation", IdempotencyKey: "rotate-crash-1",
	}
	_, err := signers.Rotate(ctx, rotateIn)
	if err == nil {
		t.Fatal("expected a retryable error from the ambiguous authorize failure")
	}
	de, ok := err.(*domain.Error)
	if !ok || !de.Retryable {
		t.Fatalf("expected a retryable domain.Error, got %v", err)
	}

	stuck, found, err := signers.Status(ctx, cap.ID)
	if err != nil || !found {
		t.Fatalf("Status: found=%v err=%v", found, err)
	}
	if stuck.Checkpoint != domain.CheckpointReconciling {
		t.Fatalf("checkpoint = %s, want reconciling", stuck.Checkpoint)
	}
	if fake.revokeCalls != 0 {
		t.Fatalf("RevokeExecutionSigner must never be called before the new signer is authorized, got %d calls", fake.revokeCalls)
	}

	now := time.Now().UTC()
	oldStillCurrent, found, err := fake.Core.ResolveExecutionSignerAuthorization(ctx, "agt_sig_crash", cap.ID, cap.Version, "signer-old-crash", now)
	if err != nil {
		t.Fatal(err)
	}
	if !found || oldStillCurrent.Revoked {
		t.Fatal("the old signer must remain authorized (not revoked) while the rotation is stuck reconciling")
	}
	_, signerID, foundCurrent, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !foundCurrent || signerID != "signer-old-crash" {
		t.Fatalf("current signer while reconciling = %q found=%v, want signer-old-crash (still authoritative)", signerID, foundCurrent)
	}

	// Retry with the same idempotency key -- same caller behavior a real
	// retry-after-timeout would exhibit. This time nothing fails.
	completed, err := signers.Rotate(ctx, rotateIn)
	if err != nil {
		t.Fatalf("retried Rotate: %v", err)
	}
	if completed.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("checkpoint after retry = %s, want completed", completed.Checkpoint)
	}
	_, signerID, foundCurrent, err = signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !foundCurrent || signerID != "signer-new-crash" {
		t.Fatalf("current signer after completed retry = %q found=%v, want signer-new-crash", signerID, foundCurrent)
	}
}

// TestExecutionSignerService_ReconcileStaleOperations_ResumesReconcilingAuthorize
// proves the reconciler sweep -- not a caller-driven retry -- also
// converges a stuck operation, exercising RunReconciler's underlying
// ReconcileStaleOperations directly.
func TestExecutionSignerService_ReconcileStaleOperations_ResumesReconcilingAuthorize(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	fake := &flakySignerCore{Core: toscoremock.NewContractFixture(st)}
	signers := service.NewExecutionSignerService(st, fake, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_reconcile", domain.TrustModeManaged)

	fake.authorizeFailuresLeft = 1
	fake.authorizeFailureRetryable = true
	in := service.AuthorizeSignerInput{
		ProviderID: "agt_sig_reconcile", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-reconcile", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-reconcile-1",
	}
	if _, err := signers.Authorize(ctx, in); err == nil {
		t.Fatal("expected the first attempt to fail ambiguously")
	}
	stuck, found, err := signers.Status(ctx, cap.ID)
	if err != nil || !found || stuck.Checkpoint != domain.CheckpointReconciling {
		t.Fatalf("precondition: expected a reconciling operation, got found=%v op=%+v err=%v", found, stuck, err)
	}

	// No more injected failures -- the reconciler's own retry succeeds.
	if err := signers.ReconcileStaleOperations(ctx, time.Now().UTC().Add(time.Second), 10); err != nil {
		t.Fatalf("ReconcileStaleOperations: %v", err)
	}

	resolved, found, err := signers.Status(ctx, cap.ID)
	if err != nil || !found {
		t.Fatalf("Status: found=%v err=%v", found, err)
	}
	if resolved.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("checkpoint after reconciler sweep = %s, want completed", resolved.Checkpoint)
	}
}

// TestExecutionSignerAuthorize_DefinitiveRejectionDoesNotReconcile proves
// a definitive (non-retryable) failure leaves the operation at its
// pre-call checkpoint rather than marking it Reconciling -- Reconciling
// is reserved for genuinely uncertain outcomes, not "the request was
// rejected".
func TestExecutionSignerAuthorize_DefinitiveRejectionDoesNotReconcile(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	fake := &flakySignerCore{Core: toscoremock.NewContractFixture(st)}
	signers := service.NewExecutionSignerService(st, fake, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_definitive", domain.TrustModeManaged)

	fake.authorizeFailuresLeft = 1
	fake.authorizeFailureRetryable = false
	_, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_definitive", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-definitive", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-definitive-1",
	})
	if err == nil {
		t.Fatal("expected the definitive rejection to surface as an error")
	}
	op, found, err := signers.Status(ctx, cap.ID)
	if err != nil || !found {
		t.Fatalf("Status: found=%v err=%v", found, err)
	}
	if op.Checkpoint != domain.CheckpointNewAuthorizationPending {
		t.Fatalf("checkpoint = %s, want new_authorization_pending (unchanged by a definitive rejection)", op.Checkpoint)
	}
	if op.FailureReason == "" {
		t.Fatal("expected FailureReason to record the definitive rejection for operator visibility")
	}
}
