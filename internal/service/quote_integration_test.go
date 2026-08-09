package service_test

import (
	"context"
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

func registerMeteredCapability(t *testing.T, h harness, providerID string, rates *domain.MeteredRates) domain.Capability {
	t.Helper()
	cap, err := h.capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Metered Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{
			Model: domain.PricingMetered, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"},
			MeteredRates: rates,
		},
		IdempotencyKey: "register-" + providerID,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return cap
}

// TestQuoteCreate_FreezesFullPricingContractIntoTermsHash proves the real
// QuoteService.Create wiring actually feeds PricingModel/Price/MeteredRates
// into TermsHash, not just a pure reimplementation of the hash function.
func TestQuoteCreate_FreezesFullPricingContractIntoTermsHash(t *testing.T) {
	h := newHarness()
	metered := registerMeteredCapability(t, h, "agt_terms_metered", &domain.MeteredRates{PerOutputToken: "0.02"})
	fixed := registerCapability(t, h, "agt_terms_fixed", "1.00")

	quoteMetered := createQuote(t, h, metered.ID)
	quoteFixed := createQuote(t, h, fixed.ID)

	if quoteMetered.TermsHash == "" || quoteFixed.TermsHash == "" {
		t.Fatal("expected non-empty terms_hash on both quotes")
	}
	if quoteMetered.TermsHash == quoteFixed.TermsHash {
		t.Fatalf("metered and fixed-price quotes produced the same terms_hash: %s", quoteMetered.TermsHash)
	}
}

// TestQuoteCreate_DifferentMeteredRatesProduceDifferentTermsHash proves two
// capabilities that differ only in their per-dimension metered rate (same
// price_hint, same total_max) still produce different Quote.TermsHash
// values -- the specific gap this fix closes.
func TestQuoteCreate_DifferentMeteredRatesProduceDifferentTermsHash(t *testing.T) {
	h := newHarness()
	cheap := registerMeteredCapability(t, h, "agt_terms_cheap", &domain.MeteredRates{PerOutputToken: "0.01"})
	expensive := registerMeteredCapability(t, h, "agt_terms_expensive", &domain.MeteredRates{PerOutputToken: "0.05"})

	quoteCheap := createQuote(t, h, cheap.ID)
	quoteExpensive := createQuote(t, h, expensive.ID)

	if quoteCheap.Price.TotalMax != quoteExpensive.Price.TotalMax {
		t.Fatalf("test setup: expected identical total_max, got %s vs %s", quoteCheap.Price.TotalMax, quoteExpensive.Price.TotalMax)
	}
	if quoteCheap.TermsHash == quoteExpensive.TermsHash {
		t.Fatalf("quotes with different metered rates but the same total_max produced the same terms_hash: %s", quoteCheap.TermsHash)
	}
}
