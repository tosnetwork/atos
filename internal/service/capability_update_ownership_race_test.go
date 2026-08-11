package service

import (
	"context"
	"testing"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// concurrentSupersedeCore wraps a real toscoremock.Core, but on the FIRST
// CommitCapabilityManifest call also lands an entirely separate, later
// Update for the SAME capability directly against the store -- reproducing
// (deterministically, without real goroutines) the exact interleaving
// where a concurrent write supersedes the row to a NEWER version between
// Update's first CAS write and its second (Ownership-anchoring) CAS write.
type concurrentSupersedeCore struct {
	*toscoremock.Core
	capabilities *CapabilityService
	capabilityID string
	fired        bool
}

func (c *concurrentSupersedeCore) CommitCapabilityManifest(ctx context.Context, capability domain.Capability) (string, error) {
	ref, err := c.Core.CommitCapabilityManifest(ctx, capability)
	if err != nil {
		return ref, err
	}
	if !c.fired {
		c.fired = true
		// A pricing change (a terms change) bumps the version, reproducing
		// the exact scenario this test targets.
		if _, err := c.capabilities.Update(ctx, c.capabilityID, capability.ProviderID, map[string]any{
			"pricing": map[string]any{"model": "fixed", "price_hint": map[string]any{"amount": "9.99", "currency": "USD"}},
		}, "concurrent-supersede-1"); err != nil {
			panic("concurrent supersede setup failed: " + err.Error())
		}
	}
	return ref, err
}

// TestCapabilityUpdate_ConcurrentSupersedeReturnsOwnCallersResult proves
// Update returns the CALLING request's own successful result even when a
// concurrent Update supersedes the row to a NEWER version between the
// version-bump CAS write and the Ownership-anchoring CAS write -- not the
// OTHER caller's data, which UpdateCapability's closure correctly declines
// to overwrite but previously still leaked into the return value via blind
// reassignment.
func TestCapabilityUpdate_ConcurrentSupersedeReturnsOwnCallersResult(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := NewCapabilityService(st)
	baseCore := toscoremock.NewContractFixture(st)
	baseCore.SetNetwork("tos-devnet")

	in := testRegisterInput("agt_race", domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}})
	in.RequestedTrustModes = []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified}
	original, err := capabilities.Register(ctx, in)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	racingCore := &concurrentSupersedeCore{Core: baseCore, capabilities: capabilities, capabilityID: original.ID}
	capabilities.WithManifestAnchor(racingCore)

	// This caller's own patch bumps the version via output_schema, distinct
	// from the concurrent writer's pricing change above, so the two are
	// independently identifiable in the assertions below.
	updated, err := capabilities.Update(ctx, original.ID, "agt_race", map[string]any{
		"output_schema": map[string]any{"type": "object", "properties": map[string]any{"caller_a_field": map[string]any{"type": "string"}}},
	}, "caller-a-update")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !racingCore.fired {
		t.Fatal("test setup failed: concurrent supersede never fired")
	}

	// This caller's OWN patch (output_schema) must be reflected -- not the
	// concurrent writer's pricing change.
	if _, ok := updated.OutputSchema["properties"].(map[string]any)["caller_a_field"]; !ok {
		t.Fatalf("Update must return this caller's OWN result: got output_schema %+v", updated.OutputSchema)
	}
	if updated.Pricing.PriceHint.Amount == "9.99" {
		t.Fatal("Update returned the CONCURRENT writer's pricing instead of this caller's own result")
	}

	// The concurrent writer's own change is still durably persisted --
	// this fix must not have discarded or corrupted it.
	final, err := capabilities.Get(ctx, original.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Pricing.PriceHint.Amount != "9.99" {
		t.Fatalf("concurrent writer's change must still be the durable final state: %+v", final.Pricing)
	}
}
