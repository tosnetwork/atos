package service

import (
	"context"
	"testing"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestCapabilityUpdate_SuspendsActiveVerifiedOnVersionBump proves a
// terms-changing Update (which bumps the version) suspends an already-Active
// Verified mode rather than silently carrying "active" over to a new
// version whose manifest/schemas/pricing were never evaluated by
// ActivationAuthority.Evaluate -- otherwise SupportedTrustModes (what
// quote/invoke paths gate on) would keep advertising verified for content
// that was never actually checked.
func TestCapabilityUpdate_SuspendsActiveVerifiedOnVersionBump(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	capabilities.WithManifestAnchor(core)

	in := testRegisterInput("agt_active_bump", domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}})
	in.RequestedTrustModes = []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified}
	original, err := capabilities.Register(ctx, in)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Simulate this capability having already gone through the full
	// readiness/EvaluateActivation flow and reached Active for verified,
	// without re-driving that whole flow here (out of scope for this test).
	if _, err := st.UpdateCapability(ctx, original.ID, func(current domain.Capability, exists bool) (domain.Capability, error) {
		current.ModeSupport = current.ModeSupport.AdvanceToPending(domain.TrustModeVerified).Activate(domain.TrustModeVerified)
		current.SupportedTrustModes = current.ModeSupport.ActiveModes()
		return current, nil
	}); err != nil {
		t.Fatalf("test setup: %v", err)
	}
	preUpdate, err := capabilities.Get(ctx, original.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !preUpdate.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("test setup failed: verified should be active before the update")
	}

	updated, err := capabilities.Update(ctx, original.ID, "agt_active_bump", map[string]any{
		"pricing": map[string]any{"model": "fixed", "price_hint": map[string]any{"amount": "2.00", "currency": "USD"}},
	}, "update-suspend-active-1")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Version == original.Version {
		t.Fatalf("precondition failed: pricing change should have bumped the version (still %s)", updated.Version)
	}
	if updated.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("verified must NOT still be active for a version that was never evaluated")
	}
	if updated.ModeSupport.Entry(domain.TrustModeVerified).Status != domain.ModeSupportSuspended {
		t.Fatalf("verified must be suspended (not e.g. reset to unsupported), got status=%s", updated.ModeSupport.Entry(domain.TrustModeVerified).Status)
	}
	for _, mode := range updated.SupportedTrustModes {
		if mode == domain.TrustModeVerified {
			t.Fatal("SupportedTrustModes (what quote/invoke paths gate on) must not include verified for the un-evaluated new version")
		}
	}
}
