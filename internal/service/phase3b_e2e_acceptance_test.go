// Integration test against a real Postgres -- skipped unless
// ATOS_TEST_DATABASE_URL is set. Run with:
//
//	ATOS_TEST_DATABASE_URL="postgres://user@localhost:5432/atos_test?sslmode=disable" go test ./internal/service/... -run TestPhase3B_EndToEndProviderTrustReadinessAcceptance
package service_test

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

// TestPhase3B_EndToEndProviderTrustReadinessAcceptance is the phase3B
// provider-trust-readiness prompt's §29 required lifecycle acceptance
// scenario, run against a real Postgres database. Each numbered step
// below corresponds directly to the prompt's own 18-item scenario.
//
// "restart ATOS" (steps 13 and 16) is simulated the same way this
// codebase's other crash-recovery tests already do it (see
// mutation_test.go in tos-protocol for the identical convention): fresh
// service instances constructed against the SAME durable Postgres store
// -- a real process restart differs from this only in that the Go
// runtime's in-memory state is discarded, which these services never
// relied on for correctness to begin with (that is the entire point of
// the durable checkpoint journal this phase built).
//
// The remote trust authority (toscore.Core) is a stateful mock wrapped
// for deterministic fault injection, not a live tos-protocol process --
// tos-protocol's own signer RPCs were independently audited and given
// dedicated test coverage in a separate PR (tosnetwork/tos-protocol#15);
// mocks "remain appropriate for deterministic fault injection" per the
// prompt's own §29 closing note, and standing up a live ConnectRPC
// tos-protocol server inside this test would duplicate that coverage
// without adding to it.
func TestPhase3B_EndToEndProviderTrustReadinessAcceptance(t *testing.T) {
	url := os.Getenv("ATOS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx := context.Background()
	st, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	httpSrv := httptest.NewServer(certifiableHTTPHandler())
	defer httpSrv.Close()
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: httpSrv.Client()}))

	providerID := "prov_e2e_" + uuid.NewString()
	capabilities := service.NewCapabilityService(st)

	// Steps 1+3 (collapsed into one registration call): provider owns
	// Capability C version N, with Managed requested (and immediately
	// active per Managed's unconditional-activation rule) and Verified
	// requested alongside it. The HTTP binding's EligibleTrustModes
	// covers both from the start -- a provider setting up its transport
	// ahead of formally requesting a stronger mode is the realistic
	// registration shape; the narrative's sequencing is preserved in the
	// assertions below, not in an artificial two-call registration dance.
	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Phase 3B E2E Capability", Description: "acceptance scenario",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: httpSrv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified}},
		},
		IdempotencyKey: "register-e2e-" + providerID,
	})
	if err != nil {
		t.Fatalf("step 1/3 Register: %v", err)
	}
	capabilityVersionN := cap.Version

	// Step 2: Managed is active.
	if !cap.ModeSupport.Active(domain.TrustModeManaged) {
		t.Fatalf("step 2: managed = %+v, want active", cap.ModeSupport.Entry(domain.TrustModeManaged))
	}
	// Step 4: Verified becomes requested/pending, NOT active -- freshly
	// requested with zero readiness evidence recorded yet, it starts at
	// `requested` specifically (§7.2.0), which is what "not yet pending"
	// means before any evidence cycle has run.
	if cap.ModeSupport.Entry(domain.TrustModeVerified).Status != domain.ModeSupportRequested {
		t.Fatalf("step 4: verified status = %s, want requested", cap.ModeSupport.Entry(domain.TrustModeVerified).Status)
	}
	if cap.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("step 4: verified must not be active")
	}

	health := service.NewHealthService(st, capabilities, resolver)
	certifications := service.NewCertificationService(st, capabilities, resolver)

	// Step 5: provider binding is healthy. This also drives the §7.2.0
	// requested->pending readiness-pipeline trigger.
	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatalf("step 5 CheckCapability: %v", err)
	}
	afterHealth, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterHealth.ModeSupport.Entry(domain.TrustModeVerified).Status != domain.ModeSupportPending {
		t.Fatalf("step 5: verified status after health check = %s, want pending", afterHealth.ModeSupport.Entry(domain.TrustModeVerified).Status)
	}

	// Step 6: exact version/binding passes sandbox certification.
	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: providerID, CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-e2e-" + providerID,
	}); err != nil {
		t.Fatalf("step 6 Open certification: %v", err)
	}

	fake := &flakySignerCore{Core: toscoremock.NewContractFixture(st)}
	signers := service.NewExecutionSignerService(st, fake, capabilities)

	// Step 7: provider authorizes execution signer S1.
	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: providerID, CapabilityID: cap.ID,
		ExecutionSignerID: "S1", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-e2e-s1",
	}); err != nil {
		t.Fatalf("step 7 Authorize: %v", err)
	}
	// Step 8: signer authorization is confirmed.
	_, currentSignerID, found, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || currentSignerID != "S1" {
		t.Fatalf("step 8: current signer = %q found=%v, want S1", currentSignerID, found)
	}

	// Step 9: public readiness shows all relevant evidence green.
	readiness, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, signers, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	verified := readiness.Readiness[domain.TrustModeVerified]
	if !verified.TransportHealthy || !verified.HealthFresh || !verified.Certified || !verified.SignerAuthorized {
		t.Fatalf("step 9: readiness = %+v, want every evidence dimension green", verified)
	}

	// Step 10: Verified remains NOT active because the Phase 4 activation
	// authority is unavailable -- the production fail-closed authority is
	// the ONLY one wired into any real deployment.
	granted, reasonCode, err := capabilities.EvaluateActivation(ctx, service.FailClosedActivationAuthority{}, "prn_e2e_admin", cap.ID, domain.TrustModeVerified, "eval-e2e-step10")
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != domain.ActivationAuthorityUnavailable {
		t.Fatalf("step 10: granted=%v reasonCode=%q, want denied/ACTIVATION_AUTHORITY_UNAVAILABLE", granted, reasonCode)
	}
	afterEvaluate, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterEvaluate.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("step 10: verified must still not be active")
	}

	// Steps 11+12: rotate S1 -> S2, injecting a crash (ambiguous, lost
	// response) during the rotation's new-signer authorization step.
	fake.authorizeFailuresLeft = 1
	fake.authorizeFailureRetryable = true
	rotateIn := service.RotateSignerInput{
		ProviderID: providerID, CapabilityID: cap.ID,
		NewExecutionSignerID: "S2", NewSignerPublicKey: testSignerKey(t), NewSignatureAlgorithm: "ed25519",
		NewValidFrom: time.Now().UTC().Add(-time.Minute), NewValidUntil: time.Now().UTC().Add(24 * time.Hour),
		RevocationReasonCode: "scheduled rotation", IdempotencyKey: "rotate-e2e-s1-s2",
	}
	if _, err := signers.Rotate(ctx, rotateIn); err == nil {
		t.Fatal("steps 11/12: expected the injected crash to surface as a retryable error")
	}
	stuck, found, err := signers.Status(ctx, cap.ID)
	if err != nil || !found {
		t.Fatalf("Status: found=%v err=%v", found, err)
	}
	if stuck.Checkpoint != domain.CheckpointReconciling {
		t.Fatalf("steps 11/12: checkpoint = %s, want reconciling", stuck.Checkpoint)
	}
	// Old signer S1 must remain authoritative mid-crash.
	_, midCrashSignerID, midCrashFound, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !midCrashFound || midCrashSignerID != "S1" {
		t.Fatalf("steps 11/12: current signer mid-crash = %q found=%v, want S1 (still authoritative)", midCrashSignerID, midCrashFound)
	}

	// Step 13: restart ATOS.
	capabilitiesAfterRestart1 := service.NewCapabilityService(st)
	signersAfterRestart1 := service.NewExecutionSignerService(st, fake, capabilitiesAfterRestart1)

	// Step 14: reconciliation converges to S2 according to the frozen
	// rotation semantics.
	if err := signersAfterRestart1.ReconcileStaleOperations(ctx, time.Now().UTC().Add(time.Hour), 1000); err != nil {
		t.Fatalf("step 14 ReconcileStaleOperations: %v", err)
	}
	converged, found, err := signersAfterRestart1.Status(ctx, cap.ID)
	if err != nil || !found {
		t.Fatalf("Status after reconcile: found=%v err=%v", found, err)
	}
	if converged.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("step 14: checkpoint after reconciliation = %s, want completed", converged.Checkpoint)
	}
	_, convergedSignerID, convergedFound, err := signersAfterRestart1.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !convergedFound || convergedSignerID != "S2" {
		t.Fatalf("step 14: current signer after reconciliation = %q found=%v, want S2", convergedSignerID, convergedFound)
	}

	// Step 15: revoke S2, injecting a lost response on the revoke RPC.
	fake.revokeFailuresLeft = 1
	fake.revokeFailureRetryable = true
	revokeIn := service.RevokeSignerInput{
		ProviderID: providerID, CapabilityID: cap.ID, ReasonCode: "decommissioning",
		IdempotencyKey: "revoke-e2e-s2",
	}
	if _, err := signersAfterRestart1.Revoke(ctx, revokeIn); err == nil {
		t.Fatal("step 15: expected the injected lost response to surface as a retryable error")
	}

	// Step 16: restart again.
	capabilitiesAfterRestart2 := service.NewCapabilityService(st)
	signersAfterRestart2 := service.NewExecutionSignerService(st, fake, capabilitiesAfterRestart2)

	// Step 17: reconciliation confirms final revocation state.
	if err := signersAfterRestart2.ReconcileStaleOperations(ctx, time.Now().UTC().Add(time.Hour), 1000); err != nil {
		t.Fatalf("step 17 ReconcileStaleOperations: %v", err)
	}
	finalOp, found, err := signersAfterRestart2.Status(ctx, cap.ID)
	if err != nil || !found {
		t.Fatalf("Status after final reconcile: found=%v err=%v", found, err)
	}
	if finalOp.Checkpoint != domain.CheckpointCompleted || finalOp.Type != domain.SignerOperationRevoke {
		t.Fatalf("step 17: final operation = %+v, want a completed revoke", finalOp)
	}
	_, _, foundAfterRevoke, err := signersAfterRestart2.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if foundAfterRevoke {
		t.Fatal("step 17: expected no current signer after the confirmed revocation")
	}

	// Step 18: at no point did provider-controlled evidence activate
	// Verified or Native -- final check across the whole scenario.
	final, err := capabilitiesAfterRestart2.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Version != capabilityVersionN {
		t.Fatalf("test invariant violated: capability version changed during the scenario (%s -> %s), invalidating step 18's comparison", capabilityVersionN, final.Version)
	}
	if final.ModeSupport.Active(domain.TrustModeVerified) || final.ModeSupport.Active(domain.TrustModeNative) {
		t.Fatalf("step 18: verified/native must never have become active: %+v", final.ModeSupport)
	}
	for _, mode := range final.SupportedTrustModes {
		if mode == domain.TrustModeVerified || mode == domain.TrustModeNative {
			t.Fatalf("step 18: supported_trust_modes must never include verified/native: %v", final.SupportedTrustModes)
		}
	}
}
