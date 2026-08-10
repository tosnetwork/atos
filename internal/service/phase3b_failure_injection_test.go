package service_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestFailClosed_AllReadinessEvidenceGreenStillDoesNotActivate is the
// phase3b provider-trust-readiness prompt's explicit final failure-
// injection requirement: healthy transport + passed certification +
// genuinely authorized execution signer, ALL true simultaneously, still
// must not put verified/native into supported_trust_modes without a
// granting ActivationAuthority. This is the strongest version of the
// invariant §7.2.0/§7.2.1 exist to protect -- earlier tests
// (TestCertificationOpen_CombinedReadinessSignalsStillDoNotActivate in
// certification_test.go) proved it without a real signer authorization in
// the mix; this one adds that missing dimension using the real
// ExecutionSignerService built in this phase.
func TestFailClosed_AllReadinessEvidenceGreenStillDoesNotActivate(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)
	certifications := service.NewCertificationService(st, capabilities, resolver)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_failclosed_all_green", srv.URL, []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified})

	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_failclosed_all_green", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-failclosed-all-green",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_failclosed_all_green", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-failclosed", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-failclosed-all-green",
	}); err != nil {
		t.Fatal(err)
	}

	// Confirm the test actually assembled every green signal it claims to
	// -- a false pass here (evidence not really green) would make the
	// assertion below meaningless.
	view, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	verified := view.Readiness[domain.TrustModeVerified]
	if !verified.TransportHealthy || !verified.HealthFresh || !verified.Certified {
		t.Fatalf("test setup invalid: health/certification not actually green: %+v", verified)
	}
	_, _, signerFound, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !signerFound {
		t.Fatal("test setup invalid: signer authorization not actually current")
	}

	// The mode-support pipeline only advances requested->pending on
	// evidence (already exercised); it never advances pending->active
	// without an explicit EvaluateActivation call through an authority.
	// Confirm status is at most pending, never active, before even
	// evaluating authority.
	afterEvidence, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterEvidence.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("verified must not be active merely from recording readiness evidence, before any activation-authority evaluation ever runs")
	}

	// Now explicitly evaluate against the production fail-closed
	// authority -- the one and only authority any real deployment uses.
	granted, reasonCode, err := capabilities.EvaluateActivation(ctx, service.FailClosedActivationAuthority{}, cap.ID, domain.TrustModeVerified)
	if err != nil {
		t.Fatal(err)
	}
	if granted {
		t.Fatal("FailClosedActivationAuthority must never grant, regardless of how much readiness evidence exists")
	}
	if reasonCode != domain.ActivationAuthorityUnavailable {
		t.Fatalf("reason code = %q, want %q", reasonCode, domain.ActivationAuthorityUnavailable)
	}

	final, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("verified must still not be active after a denied activation evaluation")
	}
	for _, mode := range final.SupportedTrustModes {
		if mode == domain.TrustModeVerified {
			t.Fatalf("verified must not appear in supported_trust_modes: %v", final.SupportedTrustModes)
		}
	}
}

// TestExecutionSignerAuthorize_RemoteCommitSucceedsButResponseIsLost
// distinguishes "the remote call itself failed" (already covered
// elsewhere) from the specifically named §28 case: the remote side
// durably committed the authorization, but ATOS's own process never saw
// the success response (simulated here by letting the underlying stateful
// mock actually apply the mutation, then discarding the result and
// returning an error as if the response were lost). A subsequent retry
// with the same idempotency key must discover the already-applied remote
// state (via AuthorizeExecutionSigner's own AuthorizationID idempotency)
// and converge to Completed -- never create a second remote authorization
// and never get stuck believing nothing happened.
func TestExecutionSignerAuthorize_RemoteCommitSucceedsButResponseIsLost(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	real := toscoremock.NewContractFixture(st)
	lossy := &responseLossSignerCore{Core: real, dropAuthorizeResponses: 1}
	signers := service.NewExecutionSignerService(st, lossy, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_lost_authz", domain.TrustModeManaged)

	in := service.AuthorizeSignerInput{
		ProviderID: "agt_sig_lost_authz", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-lost-authz", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-lost-response",
	}
	if _, err := signers.Authorize(ctx, in); err == nil {
		t.Fatal("expected the first attempt to report a retryable error (its response was dropped)")
	}
	if lossy.realAuthorizeCalls != 1 {
		t.Fatalf("expected exactly one real remote AuthorizeExecutionSigner call so far, got %d", lossy.realAuthorizeCalls)
	}

	completed, err := signers.Authorize(ctx, in)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if completed.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("checkpoint after retry = %s, want completed", completed.Checkpoint)
	}
	// The retry's own AuthorizeExecutionSigner call (via driveAuthorize
	// resuming from Reconciling) hits the real mock's idempotent-replay
	// path (same AuthorizationID, same fields) rather than creating a
	// second authorization -- confirmed by the mock's own idempotency
	// invariant, exercised for real here, not re-asserted separately.
	_, signerID, found, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || signerID != "signer-lost-authz" {
		t.Fatalf("current signer = %q found=%v, want signer-lost-authz", signerID, found)
	}
}

// TestExecutionSignerRevoke_RemoteCommitSucceedsButResponseIsLost mirrors
// the authorize case above for revocation.
func TestExecutionSignerRevoke_RemoteCommitSucceedsButResponseIsLost(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	real := toscoremock.NewContractFixture(st)
	lossy := &responseLossSignerCore{Core: real}
	signers := service.NewExecutionSignerService(st, lossy, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_lost_revoke", domain.TrustModeManaged)

	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_lost_revoke", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-lost-revoke", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-for-lost-revoke",
	}); err != nil {
		t.Fatal(err)
	}

	lossy.dropRevokeResponses = 1
	revokeIn := service.RevokeSignerInput{
		ProviderID: "agt_sig_lost_revoke", CapabilityID: cap.ID, ReasonCode: "test",
		IdempotencyKey: "revoke-lost-response",
	}
	if _, err := signers.Revoke(ctx, revokeIn); err == nil {
		t.Fatal("expected the first revoke attempt to report a retryable error (its response was dropped)")
	}
	// The old signer must remain resolvable (not locally revoked) purely
	// because a response was lost -- §17's explicit rule.
	_, _, foundStillCurrent, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !foundStillCurrent {
		t.Fatal("signer must still be considered current while the revoke's outcome is uncertain (reconciling), not prematurely revoked")
	}

	completed, err := signers.Revoke(ctx, revokeIn)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if completed.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("checkpoint after retry = %s, want completed", completed.Checkpoint)
	}
	_, _, foundAfter, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if foundAfter {
		t.Fatal("expected no current signer once the revoke genuinely completes")
	}
}

// responseLossSignerCore simulates "the remote call durably committed but
// this process never observed the success response" -- distinct from
// flakySignerCore's "the call never reached / was rejected by the
// remote": here the REAL underlying call is made and its state change
// genuinely applies, and only the caller-visible response is discarded.
type responseLossSignerCore struct {
	toscore.Core
	dropAuthorizeResponses int
	dropRevokeResponses    int
	realAuthorizeCalls     int
	realRevokeCalls        int
}

func (r *responseLossSignerCore) AuthorizeExecutionSigner(ctx context.Context, req toscore.AuthorizeExecutionSignerRequest) (toscore.ExecutionSignerAuthorization, bool, error) {
	r.realAuthorizeCalls++
	authorization, created, err := r.Core.AuthorizeExecutionSigner(ctx, req)
	if err != nil {
		return authorization, created, err
	}
	if r.dropAuthorizeResponses > 0 {
		r.dropAuthorizeResponses--
		return toscore.ExecutionSignerAuthorization{}, false, domain.NewError(domain.ErrNetworkUnavailable, "simulated lost response after remote commit", true)
	}
	return authorization, created, nil
}

func (r *responseLossSignerCore) RevokeExecutionSigner(ctx context.Context, req toscore.RevokeExecutionSignerRequest) (toscore.ExecutionSignerAuthorization, bool, error) {
	r.realRevokeCalls++
	authorization, revoked, err := r.Core.RevokeExecutionSigner(ctx, req)
	if err != nil {
		return authorization, revoked, err
	}
	if r.dropRevokeResponses > 0 {
		r.dropRevokeResponses--
		return toscore.ExecutionSignerAuthorization{}, false, domain.NewError(domain.ErrNetworkUnavailable, "simulated lost response after remote commit", true)
	}
	return authorization, revoked, nil
}

// TestExecutionSignerService_RepeatedReconcilerSweepIsIdempotent proves
// running the reconciler's sweep multiple times in a row over an already-
// converged (or empty) set of operations is harmless -- no duplicate
// remote calls, no error, no state change.
func TestExecutionSignerService_RepeatedReconcilerSweepIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	fake := &flakySignerCore{Core: toscoremock.NewContractFixture(st)}
	signers := service.NewExecutionSignerService(st, fake, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_repeat_reconcile", domain.TrustModeManaged)

	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_repeat_reconcile", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-repeat", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-repeat-reconcile",
	}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := signers.ReconcileStaleOperations(ctx, time.Now().UTC().Add(time.Hour), 100); err != nil {
			t.Fatalf("sweep %d: %v", i, err)
		}
	}
	if fake.authorizeCalls != 1 {
		t.Fatalf("AuthorizeExecutionSigner calls after repeated reconciler sweeps over an already-completed operation = %d, want 1 (unchanged)", fake.authorizeCalls)
	}
}

// TestExecutionSignerAuthorize_ServiceLevelIdempotencyConflict proves the
// store-level idempotency-conflict guarantee (already covered directly
// against the store in execution_signer_operation_test.go) is correctly
// surfaced through the service's own public Authorize method: the same
// idempotency key with different signer content must fail, not silently
// pick one.
func TestExecutionSignerAuthorize_ServiceLevelIdempotencyConflict(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "agt_sig_svc_conflict", domain.TrustModeManaged)

	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_svc_conflict", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-conflict-a", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-svc-conflict",
	}); err != nil {
		t.Fatal(err)
	}
	_, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_svc_conflict", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-conflict-b", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-svc-conflict",
	})
	de, ok := err.(*domain.Error)
	if !ok || de.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}
