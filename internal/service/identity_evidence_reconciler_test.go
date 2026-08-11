package service_test

import (
	"context"
	"testing"
	"time"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func TestIdentityEvidenceReconciler_SuspendsAfterIdentityRevoked(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	capabilities.WithManifestAnchor(core)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	identities := service.NewIdentityBindingService(st, core)
	authority := service.NewTOSBackedActivationAuthority(core, st, signers)

	providerID := "prn_reconciler_revoke"
	cap := registerSignerTestCapability(t, capabilities, providerID, domain.TrustModeManaged, domain.TrustModeVerified)
	core.SeedAgentIdentity("agt_" + providerID)
	if _, err := identities.Bind(ctx, service.BindIdentityInput{
		PrincipalID: providerID, AgentID: "agt_" + providerID, IdempotencyKey: "bind-" + providerID,
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: providerID, CapabilityID: cap.ID,
		ExecutionSignerID: "signer-" + providerID, SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(24 * time.Hour),
		IdempotencyKey: "authz-" + providerID,
	}); err != nil {
		t.Fatal(err)
	}

	// Force the mode to pending (the real readiness pipeline is out of
	// scope for this test) and activate it through the real EvaluateActivation
	// path, exactly like the REST/service layer would.
	entry := cap.ModeSupport.Entry(domain.TrustModeVerified)
	entry.Status = domain.ModeSupportPending
	cap.ModeSupport[domain.TrustModeVerified] = entry
	if err := st.Put(ctx, cap); err != nil {
		t.Fatal(err)
	}
	granted, reasonCode, err := capabilities.EvaluateActivation(ctx, authority, "operator-1", cap.ID, domain.TrustModeVerified, "eval-reconciler-1")
	if err != nil {
		t.Fatalf("EvaluateActivation: %v", err)
	}
	if !granted {
		t.Fatalf("EvaluateActivation did not grant: reasonCode=%q", reasonCode)
	}
	activated, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if activated.ModeSupport.Entry(domain.TrustModeVerified).Status != domain.ModeSupportActive {
		t.Fatalf("precondition failed: verified is %s, want active", activated.ModeSupport.Entry(domain.TrustModeVerified).Status)
	}

	// A sweep with nothing changed must be a no-op: still active.
	reconciler := service.NewIdentityEvidenceReconciler(capabilities, authority)
	if err := reconciler.SweepVerified(ctx, 100); err != nil {
		t.Fatal(err)
	}
	stillActive, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stillActive.ModeSupport.Entry(domain.TrustModeVerified).Status != domain.ModeSupportActive {
		t.Fatalf("sweep with no changes suspended a healthy capability: %s", stillActive.ModeSupport.Entry(domain.TrustModeVerified).Status)
	}

	// Now revoke the provider's identity binding entirely OUT-OF-BAND
	// (never through EvaluateActivation) -- the reconciler, not an admin,
	// must be the one to notice.
	if _, err := core.RevokePrincipalBinding(ctx, "atos-gateway", "revoke-"+providerID, providerID, "test-revocation"); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.SweepVerified(ctx, 100); err != nil {
		t.Fatal(err)
	}
	suspended, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	entryAfter := suspended.ModeSupport.Entry(domain.TrustModeVerified)
	if entryAfter.Status != domain.ModeSupportSuspended {
		t.Fatalf("verified status = %s after identity revocation, want suspended", entryAfter.Status)
	}
	if entryAfter.Reason != service.ReasonProviderIdentityRevoked {
		t.Fatalf("suspension reason = %q, want %q", entryAfter.Reason, service.ReasonProviderIdentityRevoked)
	}
	for _, mode := range suspended.SupportedTrustModes {
		if mode == domain.TrustModeVerified {
			t.Fatal("supported_trust_modes still lists verified after suspension")
		}
	}
}

func TestIdentityEvidenceReconciler_NilSafe(t *testing.T) {
	var r *service.IdentityEvidenceReconciler
	if err := r.SweepVerified(context.Background(), 10); err != nil {
		t.Fatalf("nil reconciler should no-op, got: %v", err)
	}
}
