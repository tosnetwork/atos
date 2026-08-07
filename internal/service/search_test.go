package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

func mustRegister(t *testing.T, h harness, in service.RegisterCapabilityInput) domain.Capability {
	t.Helper()
	c, err := h.capabilities.Register(context.Background(), in)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return c
}

// TestSearchRanksHigherTrustAbove guards docs/CAPABILITIES.md's ranking
// formula: at equal text relevance, higher provider trust must rank
// first.
func TestSearchRanksHigherTrustAbove(t *testing.T) {
	ctx := context.Background()
	h := newHarness()

	low := mustRegister(t, h, service.RegisterCapabilityInput{
		ProviderID: "agt_low", Name: "Widget Analyzer", Description: "analyzes widgets",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
	})
	high := mustRegister(t, h, service.RegisterCapabilityInput{
		ProviderID: "agt_high", Name: "Widget Analyzer", Description: "analyzes widgets",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
	})
	// Trust is normally set by tos-core, not at registration — bump it
	// directly through the store to simulate an established, trusted
	// provider for this test.
	bumpTrust(t, h, high.ID, 0.95)
	bumpTrust(t, h, low.ID, 0.1)

	results, err := h.capabilities.Search(ctx, service.SearchInput{Query: "widget"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) < 2 {
		t.Fatalf("expected at least 2 results, got %d", len(results))
	}
	if results[0].ID != high.ID {
		t.Errorf("top result = %s (%s), want the higher-trust capability %s", results[0].ID, results[0].Name, high.ID)
	}
}

// TestSearchMaxPriceFilterExcludes guards the hard-filter half: a
// capability priced above max_price must never appear, regardless of how
// well it otherwise ranks.
func TestSearchMaxPriceFilterExcludes(t *testing.T) {
	ctx := context.Background()
	h := newHarness()

	cheap := mustRegister(t, h, service.RegisterCapabilityInput{
		ProviderID: "agt_cheap", Name: "Budget Task Runner", Description: "cheap",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
	})
	mustRegister(t, h, service.RegisterCapabilityInput{
		ProviderID: "agt_pricey", Name: "Premium Task Runner", Description: "expensive",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "50.00", Currency: "USD"}},
	})

	maxPrice := domain.Money{Amount: "5.00", Currency: "USD"}
	results, err := h.capabilities.Search(ctx, service.SearchInput{
		Query:   "task runner",
		Filters: service.SearchFilters{MaxPrice: &maxPrice},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	for _, r := range results {
		if r.ID != cheap.ID {
			t.Errorf("result %s (%s) exceeds max_price 5.00, should have been filtered out", r.ID, r.Name)
		}
	}
	if len(results) != 1 || results[0].ID != cheap.ID {
		t.Errorf("expected exactly the cheap capability, got %d results", len(results))
	}
}

// TestSearchMinTrustScoreFilterExcludes guards the min_trust_score filter.
func TestSearchMinTrustScoreFilterExcludes(t *testing.T) {
	ctx := context.Background()
	h := newHarness()

	trusted := mustRegister(t, h, service.RegisterCapabilityInput{
		ProviderID: "agt_trusted", Name: "Trusted Service", Description: "reliable",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
	})
	untrusted := mustRegister(t, h, service.RegisterCapabilityInput{
		ProviderID: "agt_untrusted", Name: "Untrusted Service", Description: "unproven",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
	})
	bumpTrust(t, h, trusted.ID, 0.9)
	bumpTrust(t, h, untrusted.ID, 0.05)

	minTrust := 0.5
	results, err := h.capabilities.Search(ctx, service.SearchInput{
		Query:   "service",
		Filters: service.SearchFilters{MinTrustScore: &minTrust},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	for _, r := range results {
		if r.ID == untrusted.ID {
			t.Errorf("untrusted capability %s should have been excluded by min_trust_score", untrusted.ID)
		}
	}
	if len(results) != 1 || results[0].ID != trusted.ID {
		t.Errorf("expected exactly the trusted capability, got %d results", len(results))
	}
}

// TestSearchDeliveryModeFilterExcludes guards the delivery_modes filter.
func TestSearchDeliveryModeFilterExcludes(t *testing.T) {
	ctx := context.Background()
	h := newHarness()

	instant := mustRegister(t, h, service.RegisterCapabilityInput{
		ProviderID: "agt_instant", Name: "Fast Lookup", Description: "instant",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
	})
	mustRegister(t, h, service.RegisterCapabilityInput{
		ProviderID: "agt_async", Name: "Slow Lookup", Description: "async",
		DeliveryMode: domain.DeliveryAsync,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
	})

	results, err := h.capabilities.Search(ctx, service.SearchInput{
		Query:   "lookup",
		Filters: service.SearchFilters{DeliveryModes: []domain.DeliveryMode{domain.DeliveryInstant}},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 || results[0].ID != instant.ID {
		t.Errorf("expected exactly the instant capability, got %d results", len(results))
	}
}

// bumpTrust reaches into the store directly to simulate tos-core having
// updated a capability's trust score — there is no public API to set
// trust arbitrarily (by design: trust is earned, not self-declared).
func bumpTrust(t *testing.T, h harness, capabilityID string, score float64) {
	t.Helper()
	ctx := context.Background()
	c, err := h.capabilities.Get(ctx, capabilityID)
	if err != nil {
		t.Fatal(err)
	}
	c.Trust.Score = score
	c.UpdatedAt = time.Now().UTC()
	if err := h.store().Put(ctx, c); err != nil {
		t.Fatal(err)
	}
}
