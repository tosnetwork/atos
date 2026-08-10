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

// fixedTermsHashInputsWithBinding is fixedTermsHashInputs's binding/schema
// analog, holding every other termsHash input constant.
func fixedTermsHashInputsWithBinding(binding *domain.CapabilityBinding, inputSchema, outputSchema map[string]any) string {
	return termsHash(
		"cap_1", "1.0.0", "prov_1", "prn_1",
		"managed", "",
		"fixed", "9.00", "1.00", "10.00", "USD",
		hashCommitment((*domain.MeteredRates)(nil)),
		"atos_managed", "managed_balance",
		"2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z",
		"dp_hash", "", "", "input_commit",
		hashCommitment(binding), hashCommitment(inputSchema), hashCommitment(outputSchema),
	)
}

// TestTermsHash_CommitsFrozenBindingAndSchema proves TermsHash changes
// whenever the frozen Binding/InputSchema/OutputSchema differ -- these are
// exactly the execution semantics Job creation later freezes from the
// Quote (see domain.Quote.Binding's doc comment), so two Quotes must not
// share a TermsHash while differing in what a Job would actually execute
// against.
func TestTermsHash_CommitsFrozenBindingAndSchema(t *testing.T) {
	bindingA := &domain.CapabilityBinding{Transport: domain.AdapterHTTP, EndpointRef: "https://provider-a.example.com"}
	bindingB := &domain.CapabilityBinding{Transport: domain.AdapterHTTP, EndpointRef: "https://provider-b.example.com"}
	inputSchema := map[string]any{"type": "object"}
	outputSchema := map[string]any{"type": "object"}

	baseline := fixedTermsHashInputsWithBinding(bindingA, inputSchema, outputSchema)

	cases := map[string]string{
		"changed binding endpoint": fixedTermsHashInputsWithBinding(bindingB, inputSchema, outputSchema),
		"removed binding entirely": fixedTermsHashInputsWithBinding(nil, inputSchema, outputSchema),
		"changed input schema":     fixedTermsHashInputsWithBinding(bindingA, map[string]any{"type": "string"}, outputSchema),
		"changed output schema":    fixedTermsHashInputsWithBinding(bindingA, inputSchema, map[string]any{"type": "string"}),
	}
	for name, got := range cases {
		if got == baseline {
			t.Errorf("%s: TermsHash unchanged (%s) even though the frozen binding/schema differs from the baseline", name, got)
		}
	}

	identical := fixedTermsHashInputsWithBinding(&domain.CapabilityBinding{Transport: domain.AdapterHTTP, EndpointRef: "https://provider-a.example.com"}, inputSchema, outputSchema)
	if identical != baseline {
		t.Errorf("identical binding/schema terms produced different hashes: %s vs %s", identical, baseline)
	}
}
