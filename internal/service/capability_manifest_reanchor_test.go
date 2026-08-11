package service

import (
	"context"
	"testing"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestCapabilityUpdate_ReanchorsManifestOnVersionBump proves Update's
// re-anchoring gap is closed: a terms-changing PATCH bumps the version
// (existing behavior) AND actually anchors the NEW version's manifest
// through the configured core, not only computing a local digest that is
// never anchored anywhere (the Phase 4A gap this test closes).
func TestCapabilityUpdate_ReanchorsManifestOnVersionBump(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	capabilities.WithManifestAnchor(core)

	in := testRegisterInput("agt_reanchor", domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}})
	in.RequestedTrustModes = []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified}
	original, err := capabilities.Register(ctx, in)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	verifiedBefore, _, err := core.VerifyCapabilityOwnership(ctx, original.ID, "agt_reanchor", original.Version, original.ManifestCommitment)
	if err != nil {
		t.Fatal(err)
	}
	if !verifiedBefore {
		t.Fatal("precondition failed: original version should already be anchored by Register")
	}
	if original.Ownership.Status != domain.OwnershipAnchored || original.Ownership.Commitment == "" {
		t.Fatalf("Register did not update the public Ownership projection: %+v", original.Ownership)
	}

	updated, err := capabilities.Update(ctx, original.ID, "agt_reanchor", map[string]any{
		"pricing": map[string]any{"model": "fixed", "price_hint": map[string]any{"amount": "2.00", "currency": "USD"}},
	}, "update-reanchor-1")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Version == original.Version {
		t.Fatalf("precondition failed: terms change should have bumped the version (still %s)", updated.Version)
	}
	if updated.ManifestCommitment == original.ManifestCommitment {
		t.Fatal("precondition failed: manifest commitment should differ after a version bump")
	}

	verifiedAfter, reasonCode, err := core.VerifyCapabilityOwnership(ctx, updated.ID, "agt_reanchor", updated.Version, updated.ManifestCommitment)
	if err != nil {
		t.Fatal(err)
	}
	if !verifiedAfter {
		t.Fatalf("new version's manifest was not anchored by Update: reasonCode=%q", reasonCode)
	}
	if updated.Ownership.Status != domain.OwnershipAnchored || updated.Ownership.Commitment == "" {
		t.Fatalf("Update did not refresh the public Ownership projection for the new version: %+v", updated.Ownership)
	}
	if updated.Ownership.Commitment == original.Ownership.Commitment {
		t.Fatal("new version's ownership commitment must differ from the old version's")
	}

	// The OLD version's manifest remains separately anchored and valid --
	// re-anchoring the new version must not retroactively invalidate or
	// overwrite history for Quotes/Jobs still referencing the old version.
	verifiedOld, _, err := core.VerifyCapabilityOwnership(ctx, original.ID, "agt_reanchor", original.Version, original.ManifestCommitment)
	if err != nil {
		t.Fatal(err)
	}
	if !verifiedOld {
		t.Fatal("old version's manifest anchor was disturbed by re-anchoring the new version")
	}
}

// TestCapabilityUpdate_NoTermsChangeDoesNotReanchor proves a non-terms
// PATCH (no version bump) does not attempt to re-anchor -- the manifest
// commitment is unchanged, so anchorManifestIfRequested's idempotent
// replay path is exercised, not a wasted new commitment.
func TestCapabilityUpdate_NoTermsChangeDoesNotReanchor(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	capabilities.WithManifestAnchor(core)

	in := testRegisterInput("agt_reanchor_noop", domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}})
	in.RequestedTrustModes = []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified}
	original, err := capabilities.Register(ctx, in)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	updated, err := capabilities.Update(ctx, original.ID, "agt_reanchor_noop", map[string]any{
		"name": "Renamed, no terms change",
	}, "update-reanchor-noop-1")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Version != original.Version || updated.ManifestCommitment != original.ManifestCommitment {
		t.Fatalf("a non-terms PATCH must not bump version/manifest: before=%s/%s after=%s/%s",
			original.Version, original.ManifestCommitment, updated.Version, updated.ManifestCommitment)
	}
}

// TestCapabilityUpdate_ResubmittingIdenticalSchemaDoesNotBumpVersion proves
// input_schema/output_schema/bindings are gated on the value actually
// differing, not merely on the key being present in the patch (like
// pricing's own samePricing check already was) -- a client that always
// resends the full current object (a common, legitimate REST PATCH
// pattern) must not bump the version, re-anchor the manifest, or (since a
// version bump now also suspends an already-Active stronger mode) suspend
// Verified on every resubmission of UNCHANGED content.
func TestCapabilityUpdate_ResubmittingIdenticalSchemaDoesNotBumpVersion(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	capabilities.WithManifestAnchor(core)

	in := testRegisterInput("agt_resubmit_identical", domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}})
	in.RequestedTrustModes = []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified}
	original, err := capabilities.Register(ctx, in)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	// Simulate this capability having already reached Active for verified
	// (see the parallel test in capability_update_suspends_active_mode_test.go).
	if _, err := st.UpdateCapability(ctx, original.ID, func(current domain.Capability, exists bool) (domain.Capability, error) {
		current.ModeSupport = current.ModeSupport.AdvanceToPending(domain.TrustModeVerified).Activate(domain.TrustModeVerified)
		current.SupportedTrustModes = current.ModeSupport.ActiveModes()
		return current, nil
	}); err != nil {
		t.Fatalf("test setup: %v", err)
	}

	updated, err := capabilities.Update(ctx, original.ID, "agt_resubmit_identical", map[string]any{
		"input_schema": map[string]any{"type": "object"}, // identical to what Register already set
	}, "update-resubmit-identical-1")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Version != original.Version {
		t.Fatalf("resubmitting an UNCHANGED input_schema must not bump the version: before=%s after=%s", original.Version, updated.Version)
	}
	if !updated.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("resubmitting UNCHANGED content must not suspend an already-Active mode")
	}
}
