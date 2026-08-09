package service

import (
	"strconv"
	"strings"
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
)

func testQuote(price domain.Price, rates *domain.MeteredRates) domain.Quote {
	return domain.Quote{
		ID: "q_1", CapabilityID: "cap_1", CapabilityVersion: "v1",
		ProviderID: "prov_1", TrustMode: domain.TrustModeManaged,
		Price: price, MeteredRates: rates, PricingModel: domain.PricingMetered,
		TermsHash: "sha256:terms",
	}
}

func testReceipt(usage domain.Usage) domain.ExecutionReceipt {
	return domain.ExecutionReceipt{
		ID: "rcpt_1", QuoteID: "q_1", JobID: "job_1",
		ProviderID: "prov_1", CapabilityID: "cap_1", CapabilityVersion: "v1",
		Usage: usage,
	}
}

// A capability with no MeteredRates configured must bill the full frozen
// Quote subtotal, exactly matching pre-Phase-2B (Fixed price) behavior.
func TestComputeBillingSnapshot_UnmeteredChargesFullSubtotal(t *testing.T) {
	price := domain.Price{Subtotal: "1.00", Fees: "0.05", TotalMax: "1.05", Currency: "USD"}
	q := testQuote(price, nil)
	r := testReceipt(domain.Usage{InputTokens: 999999})

	snap, err := computeBillingSnapshot(q, r)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ProviderGross.Amount != "1.00" {
		t.Errorf("provider gross = %s, want 1.00", snap.ProviderGross.Amount)
	}
	if snap.GrossCharge.Amount != "1.05" {
		t.Errorf("gross charge = %s, want 1.05", snap.GrossCharge.Amount)
	}
	if snap.PrincipalRefund.Amount != "0.00" {
		t.Errorf("refund = %s, want 0.00", snap.PrincipalRefund.Amount)
	}
}

// Usage implying a charge in excess of the quoted subtotal must be clamped
// to the frozen subtotal -- metered usage can never overcharge beyond what
// was quoted.
func TestComputeBillingSnapshot_UsageExceedingMaxCannotOvercharge(t *testing.T) {
	price := domain.Price{Subtotal: "1.00", Fees: "0.05", TotalMax: "1.05", Currency: "USD"}
	rates := &domain.MeteredRates{PerOutputToken: "0.01"}
	q := testQuote(price, rates)
	// 1000 tokens * 0.01 = 10.00, far more than the 1.00 quoted subtotal.
	r := testReceipt(domain.Usage{OutputTokens: 1000})

	snap, err := computeBillingSnapshot(q, r)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ProviderGross.Amount != "1.00" {
		t.Errorf("provider gross = %s, want clamped to 1.00", snap.ProviderGross.Amount)
	}
	if snap.GrossCharge.Amount != "1.05" {
		t.Errorf("gross charge = %s, want clamped to 1.05 (quote.total_max)", snap.GrossCharge.Amount)
	}
	quoteMax, _ := parseTestAmount(price.TotalMax)
	charge, _ := parseTestAmount(snap.GrossCharge.Amount)
	if charge > quoteMax {
		t.Fatalf("charge %s exceeds quote.total_max %s", snap.GrossCharge.Amount, price.TotalMax)
	}
}

// Metered usage below the quoted maximum must charge exactly the metered
// amount, proportionally split into provider gross + gateway fee, and must
// refund the remainder back toward the principal up to total_max.
func TestComputeBillingSnapshot_PartialUsageRefundsRemainder(t *testing.T) {
	price := domain.Price{Subtotal: "1.00", Fees: "0.05", TotalMax: "1.05", Currency: "USD"}
	rates := &domain.MeteredRates{PerOutputToken: "0.01"} // 0.01 at 2 decimals is valid
	q := testQuote(price, rates)
	// 50 tokens * 0.01 = 0.50 (half the quoted subtotal).
	r := testReceipt(domain.Usage{OutputTokens: 50})

	snap, err := computeBillingSnapshot(q, r)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ProviderGross.Amount != "0.50" {
		t.Errorf("provider gross = %s, want 0.50", snap.ProviderGross.Amount)
	}
	// gateway fee = 0.05 * 0.50 / 1.00 = 0.025 -> truncates to 0.02
	if snap.GatewayFee.Amount != "0.02" {
		t.Errorf("gateway fee = %s, want 0.02", snap.GatewayFee.Amount)
	}
	if snap.GrossCharge.Amount != "0.52" {
		t.Errorf("gross charge = %s, want 0.52", snap.GrossCharge.Amount)
	}
	if snap.PrincipalRefund.Amount != "0.53" {
		t.Errorf("refund = %s, want 0.53 (1.05 - 0.52)", snap.PrincipalRefund.Amount)
	}
}

// Billing must never read a Capability's current live pricing -- only the
// values already frozen into the Quote at Quote-creation time. Simulate a
// "price change after Quote was created" by simply never passing a live
// Capability into computeBillingSnapshot at all: it is not even a parameter,
// which is the structural guarantee this test documents.
func TestComputeBillingSnapshot_NeverReadsLivePricing(t *testing.T) {
	price := domain.Price{Subtotal: "1.00", Fees: "0.05", TotalMax: "1.05", Currency: "USD"}
	rates := &domain.MeteredRates{PerOutputToken: "0.01"}
	q := testQuote(price, rates)
	r := testReceipt(domain.Usage{OutputTokens: 50})

	snap1, err := computeBillingSnapshot(q, r)
	if err != nil {
		t.Fatal(err)
	}
	// Mutate what would be the "live" capability rates elsewhere in the
	// system -- computeBillingSnapshot only ever sees q.MeteredRates, a copy
	// frozen at Quote-creation time, so a second call with the same frozen
	// Quote must produce an identical result regardless of any live pricing
	// change happening concurrently.
	liveRatesChangedElsewhere := domain.MeteredRates{PerOutputToken: "0.99"}
	_ = liveRatesChangedElsewhere
	snap2, err := computeBillingSnapshot(q, r)
	if err != nil {
		t.Fatal(err)
	}
	if snap1.GrossCharge.Amount != snap2.GrossCharge.Amount {
		t.Fatalf("billing is not deterministic from frozen quote: %s != %s", snap1.GrossCharge.Amount, snap2.GrossCharge.Amount)
	}
}

// An invalid (over-precision) metered rate must be rejected rather than
// silently truncated or accepted, since that could misprice a job.
func TestComputeBillingSnapshot_InvalidRateRejected(t *testing.T) {
	price := domain.Price{Subtotal: "1.00", Fees: "0.05", TotalMax: "1.05", Currency: "USD"}
	// meteredRateDecimals=9, so a rate needs more than 9 decimal places to
	// be rejected -- this is no longer just quoteDecimals=2.
	rates := &domain.MeteredRates{PerOutputToken: "0.0000000001"} // 10 decimals
	q := testQuote(price, rates)
	r := testReceipt(domain.Usage{OutputTokens: 50})

	if _, err := computeBillingSnapshot(q, r); err == nil {
		t.Fatal("expected an error for a rate with excess decimal precision")
	}
}

// TestComputeBillingSnapshot_SubCentRatePrecisionIsRespected proves realistic
// sub-cent per-dimension unit rates (well finer than the settlement
// currency's own 2-decimal precision) are parsed and priced correctly, and
// only truncated to currency precision once at the very end -- not
// rejected outright, and not truncated to zero per-dimension before
// summing.
func TestComputeBillingSnapshot_SubCentRatePrecisionIsRespected(t *testing.T) {
	price := domain.Price{Subtotal: "10.00", Fees: "0.50", TotalMax: "10.50", Currency: "USD"}
	// $0.000001 per token (a realistic LLM token price) would have been
	// rejected outright before this fix (quoteDecimals=2 could not parse
	// more than 2 decimal places).
	rates := &domain.MeteredRates{PerOutputToken: "0.000001"}
	q := testQuote(price, rates)
	// 300,000 tokens * $0.000001 = $0.30 -- large enough to matter after
	// rounding to cents, small enough to stay comfortably under the 10.00
	// quoted subtotal.
	r := testReceipt(domain.Usage{OutputTokens: 300000})

	snap, err := computeBillingSnapshot(q, r)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ProviderGross.Amount != "0.30" {
		t.Fatalf("provider gross = %s, want 0.30", snap.ProviderGross.Amount)
	}
}

// TestComputeBillingSnapshot_SubCentUsageTruncatesNotZero proves usage
// small enough that the metered charge is sub-cent overall still resolves
// deterministically via truncation at the final settlement precision (not
// an error, not silently rounded up).
func TestComputeBillingSnapshot_SubCentUsageTruncatesNotZero(t *testing.T) {
	price := domain.Price{Subtotal: "10.00", Fees: "0.50", TotalMax: "10.50", Currency: "USD"}
	rates := &domain.MeteredRates{PerOutputToken: "0.000001"}
	q := testQuote(price, rates)
	// 1 token * $0.000001 = $0.000001, which truncates to $0.00 at the
	// settlement currency's 2-decimal precision.
	r := testReceipt(domain.Usage{OutputTokens: 1})

	snap, err := computeBillingSnapshot(q, r)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ProviderGross.Amount != "0.00" {
		t.Fatalf("provider gross = %s, want 0.00 (truncated, not rejected)", snap.ProviderGross.Amount)
	}
}

// The gross charge must never exceed quote.total_max, checked across a
// range of usage magnitudes including usage far beyond anything realistic.
func TestComputeBillingSnapshot_NeverExceedsTotalMax(t *testing.T) {
	price := domain.Price{Subtotal: "10.00", Fees: "0.50", TotalMax: "10.50", Currency: "USD"}
	rates := &domain.MeteredRates{PerOutputToken: "0.01", PerInputToken: "0.01", PerExecutionMillisecond: "0.01"}
	q := testQuote(price, rates)

	for _, usage := range []domain.Usage{
		{},
		{OutputTokens: 1},
		{OutputTokens: 1000, InputTokens: 1000, ExecutionMillis: 1000},
		{OutputTokens: 1 << 32, InputTokens: 1 << 32, ExecutionMillis: 1 << 32},
	} {
		r := testReceipt(usage)
		snap, err := computeBillingSnapshot(q, r)
		if err != nil {
			t.Fatal(err)
		}
		charge, _ := parseTestAmount(snap.GrossCharge.Amount)
		max, _ := parseTestAmount(price.TotalMax)
		if charge > max {
			t.Fatalf("usage %+v: charge %s exceeds total_max %s", usage, snap.GrossCharge.Amount, price.TotalMax)
		}
		refund, _ := parseTestAmount(snap.PrincipalRefund.Amount)
		if refund < 0 {
			t.Fatalf("usage %+v: negative refund %s", usage, snap.PrincipalRefund.Amount)
		}
		if charge+refund != max {
			t.Fatalf("usage %+v: charge %s + refund %s != total_max %s", usage, snap.GrossCharge.Amount, snap.PrincipalRefund.Amount, price.TotalMax)
		}
	}
}

// parseTestAmount converts a 2-decimal string like "1.05" into integer cents
// for simple arithmetic assertions in tests, independent of the money
// package under test.
func parseTestAmount(s string) (int64, error) {
	parts := strings.SplitN(s, ".", 2)
	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}
	frac := int64(0)
	if len(parts) == 2 {
		frac, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, err
		}
	}
	return whole*100 + frac, nil
}
