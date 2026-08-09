package service

import (
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
)

// fixedTermsHashInputs holds every non-pricing termsHash input constant so
// these tests isolate exactly the pricing contract's contribution to the
// commitment -- calling QuoteService.Create twice and comparing hashes
// would be flaky, since ExpiresAt/ExecutionDeadline are real wall-clock
// timestamps that differ between any two calls regardless of pricing.
func fixedTermsHashInputs(pricingModel, subtotal, fees, totalMax string, meteredRates *domain.MeteredRates) string {
	return termsHash(
		"cap_1", "1.0.0", "prov_1", "prn_1",
		"managed", "",
		pricingModel, subtotal, fees, totalMax, "USD",
		hashCommitment(meteredRates),
		"atos_managed", "managed_balance",
		"2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z",
		"dp_hash", "", "", "input_commit",
	)
}

// TestTermsHash_CommitsFullPricingContract proves that Quote.TermsHash
// changes whenever any part of the frozen pricing contract changes -- the
// pricing model, the subtotal/fee split (even holding total_max constant),
// or any individual metered rate -- and stays deterministic for identical
// terms. Before this fix, two Quotes could share a TermsHash while
// differing in how a Job would actually be charged.
func TestTermsHash_CommitsFullPricingContract(t *testing.T) {
	baselineRates := &domain.MeteredRates{PerOutputToken: "0.01"}
	baseline := fixedTermsHashInputs("metered", "9.00", "1.00", "10.00", baselineRates)

	cases := map[string]string{
		"changed pricing model":                      fixedTermsHashInputs("fixed", "9.00", "1.00", "10.00", baselineRates),
		"changed subtotal/fee split, same total_max": fixedTermsHashInputs("metered", "8.00", "2.00", "10.00", baselineRates),
		"changed metered rate":                       fixedTermsHashInputs("metered", "9.00", "1.00", "10.00", &domain.MeteredRates{PerOutputToken: "0.05"}),
		"added a second metered dimension":           fixedTermsHashInputs("metered", "9.00", "1.00", "10.00", &domain.MeteredRates{PerOutputToken: "0.01", PerInputToken: "0.01"}),
		"removed metered rates entirely":             fixedTermsHashInputs("metered", "9.00", "1.00", "10.00", nil),
	}
	for name, got := range cases {
		if got == baseline {
			t.Errorf("%s: TermsHash unchanged (%s) even though the pricing contract differs from the baseline", name, got)
		}
	}

	identical := fixedTermsHashInputs("metered", "9.00", "1.00", "10.00", &domain.MeteredRates{PerOutputToken: "0.01"})
	if identical != baseline {
		t.Errorf("identical pricing terms produced different hashes: %s vs %s", identical, baseline)
	}
}
