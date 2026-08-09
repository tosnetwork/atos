package service

import (
	"context"
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// testRegisterInput builds a RegisterCapabilityInput for providerID. Callers
// in this file always pass a distinct providerID per registration, which
// keeps the derived idempotency key distinct too.
func testRegisterInput(providerID string, pricing domain.Pricing) RegisterCapabilityInput {
	return RegisterCapabilityInput{
		ProviderID:     providerID,
		Name:           "Test Capability",
		Description:    "for tests",
		DeliveryMode:   domain.DeliveryInstant,
		InputSchema:    map[string]any{"type": "object"},
		OutputSchema:   map[string]any{"type": "object"},
		Pricing:        pricing,
		IdempotencyKey: "register-" + providerID,
	}
}

// Register must reject an invalid MeteredRate up front, before the
// capability is ever stored -- not defer discovery to settlement time,
// where a Job would already have debited and escrowed funds against it.
func TestCapabilityService_RegisterRejectsInvalidMeteredRate(t *testing.T) {
	svc := NewCapabilityService(memory.New())
	cases := []struct {
		name  string
		rates domain.MeteredRates
	}{
		{"negative", domain.MeteredRates{PerOutputToken: "-1"}},
		{"non-numeric", domain.MeteredRates{PerOutputToken: "abc"}},
		{"excess-precision", domain.MeteredRates{PerOutputToken: "0.0000000001"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pricing := domain.Pricing{
				Model: domain.PricingMetered, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"},
				MeteredRates: &c.rates,
			}
			in := testRegisterInput("agt_provider_"+c.name, pricing)
			if _, err := svc.Register(context.Background(), in); err == nil {
				t.Fatalf("expected Register to reject %+v", c.rates)
			}
		})
	}
}

// A valid MeteredRate must still register successfully.
func TestCapabilityService_RegisterAcceptsValidMeteredRate(t *testing.T) {
	svc := NewCapabilityService(memory.New())
	pricing := domain.Pricing{
		Model: domain.PricingMetered, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"},
		MeteredRates: &domain.MeteredRates{PerOutputToken: "0.000001"},
	}
	if _, err := svc.Register(context.Background(), testRegisterInput("agt_provider_valid", pricing)); err != nil {
		t.Fatalf("Register: %v", err)
	}
}

// Update must reject a patch that sets an invalid MeteredRate the same way
// Register does.
func TestCapabilityService_UpdateRejectsInvalidMeteredRate(t *testing.T) {
	svc := NewCapabilityService(memory.New())
	pricing := domain.Pricing{
		Model: domain.PricingMetered, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"},
		MeteredRates: &domain.MeteredRates{PerOutputToken: "0.000001"},
	}
	cap, err := svc.Register(context.Background(), testRegisterInput("agt_provider_upd", pricing))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	patch := map[string]any{
		"pricing": map[string]any{
			"model":      "metered",
			"price_hint": map[string]any{"amount": "1.00", "currency": "USD"},
			"metered_rates": map[string]any{
				"per_output_token": "-1",
			},
		},
	}
	if _, err := svc.Update(context.Background(), cap.ID, "agt_provider_upd", patch, "update-bad-rate"); err == nil {
		t.Fatal("expected Update to reject an invalid metered rate")
	}
}

// A provider PATCH that only changes MeteredRates (model/unit/price_hint
// unchanged) must not be silently dropped: it must bump the Capability's
// version, change its manifest commitment, and be visible to a Quote
// created afterward -- while a Quote created BEFORE the update keeps its
// already-frozen (old) rate untouched.
func TestCapabilityService_UpdateRateOnlyChangeIsNotSilentlyIgnored(t *testing.T) {
	st := memory.New()
	capabilities := NewCapabilityService(st)
	quotes := NewQuoteService(st)

	pricing := domain.Pricing{
		Model: domain.PricingMetered, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"},
		MeteredRates: &domain.MeteredRates{PerOutputToken: "0.000001"},
	}
	cap, err := capabilities.Register(context.Background(), testRegisterInput("agt_provider_rateonly", pricing))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	originalVersion := cap.Version
	originalCommitment := cap.ManifestCommitment

	oldQuote, err := quotes.Create(context.Background(), CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote (before update): %v", err)
	}
	if oldQuote.MeteredRates == nil || oldQuote.MeteredRates.PerOutputToken != "0.000001" {
		t.Fatalf("old quote should have frozen the original rate, got %+v", oldQuote.MeteredRates)
	}

	patch := map[string]any{
		"pricing": map[string]any{
			"model":      "metered",
			"price_hint": map[string]any{"amount": "1.00", "currency": "USD"},
			"metered_rates": map[string]any{
				"per_output_token": "0.000002",
			},
		},
	}
	updated, err := capabilities.Update(context.Background(), cap.ID, "agt_provider_rateonly", patch, "update-rate-only")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Version == originalVersion {
		t.Errorf("rate-only pricing update did not bump the capability version (stayed %q)", updated.Version)
	}
	if updated.ManifestCommitment == originalCommitment {
		t.Error("rate-only pricing update did not change the manifest commitment")
	}
	if updated.Pricing.MeteredRates == nil || updated.Pricing.MeteredRates.PerOutputToken != "0.000002" {
		t.Fatalf("capability pricing did not actually update, got %+v", updated.Pricing.MeteredRates)
	}

	newQuote, err := quotes.Create(context.Background(), CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote (after update): %v", err)
	}
	if newQuote.MeteredRates == nil || newQuote.MeteredRates.PerOutputToken != "0.000002" {
		t.Fatalf("new quote should freeze the updated rate, got %+v", newQuote.MeteredRates)
	}
	if newQuote.TermsHash == oldQuote.TermsHash {
		t.Error("old and new quote must not share a TermsHash once the frozen rate differs")
	}

	// The Quote created before the update must still carry its original
	// frozen rate -- re-fetching it must never reinterpret it against the
	// capability's now-current live pricing.
	reloadedOldQuote, err := quotes.Get(context.Background(), oldQuote.ID)
	if err != nil {
		t.Fatalf("Get old quote: %v", err)
	}
	if reloadedOldQuote.MeteredRates == nil || reloadedOldQuote.MeteredRates.PerOutputToken != "0.000001" {
		t.Fatalf("old quote's frozen rate must not change after the capability was updated, got %+v", reloadedOldQuote.MeteredRates)
	}
}

// samePricing must treat two Pricing values with identical
// Model/Unit/PriceHint but different MeteredRates as different.
func TestSamePricing_ComparesMeteredRates(t *testing.T) {
	base := domain.Pricing{
		Model: domain.PricingMetered, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"},
		MeteredRates: &domain.MeteredRates{PerOutputToken: "0.000001"},
	}
	same := base
	sameRates := *base.MeteredRates
	same.MeteredRates = &sameRates
	if !samePricing(base, same) {
		t.Error("identical MeteredRates (different pointer, same value) should compare equal")
	}

	different := base
	differentRates := domain.MeteredRates{PerOutputToken: "0.000002"}
	different.MeteredRates = &differentRates
	if samePricing(base, different) {
		t.Error("different MeteredRates should not compare equal")
	}

	nilRates := base
	nilRates.MeteredRates = nil
	if samePricing(base, nilRates) {
		t.Error("nil vs non-nil MeteredRates should not compare equal")
	}
	if !samePricing(nilRates, nilRates) {
		t.Error("nil MeteredRates should compare equal to itself")
	}
}
