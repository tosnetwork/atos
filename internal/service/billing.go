package service

import (
	"fmt"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
)

// computeBillingSnapshot deterministically derives the metered billing
// result for one Job from its frozen Quote terms and a verified Execution
// Receipt's usage. It is a pure function of its two arguments -- given the
// same frozen Quote and the same verified Receipt, it always returns the
// identical result -- and it never reads a Capability's current live
// pricing configuration.
//
// Capabilities without MeteredRates (quote.MeteredRates == nil, which is
// every capability priced Fixed/Free/PerUse/Negotiated, and any
// Metered/PerUnit capability with no rates configured) bill the full frozen
// Quote subtotal, exactly matching Phase 0/1 behavior. Metered billing only
// ever narrows the provider's charge toward the verified usage; it can
// never exceed what the frozen Quote already committed to.
//
// Which of those two billing modes applies is gated on quote.PricingModel,
// not merely on whether quote.MeteredRates happens to be set: a Quote whose
// PricingModel does not itself permit metered billing (Free/Fixed/PerUse/
// Negotiated) but which somehow still carries non-empty MeteredRates is
// necessarily a Quote frozen before validatePricing existed, or otherwise
// corrupted -- charging it by usage anyway would silently violate the
// pricing contract the Quote committed to and short the provider relative
// to what was actually quoted, so that case fails closed instead. An empty
// PricingModel (a Quote frozen before that field was recorded, i.e. before
// Phase 2B) is treated the same as a non-metered model for backward
// compatibility with Jobs already in flight when this check shipped.
func computeBillingSnapshot(quote domain.Quote, receipt domain.ExecutionReceipt) (domain.BillingSnapshot, error) {
	currency := quote.Price.Currency
	decimals := quoteDecimals
	if quote.TrustMode == domain.TrustModeVerified {
		if quote.AssetDecimals != verifiedTOSDecimals || currency != "TOS" {
			return domain.BillingSnapshot{}, domain.NewError(domain.ErrSettlementFailed, "verified Quote has invalid TOS decimal contract", false)
		}
		decimals = verifiedTOSDecimals
	}
	subtotal, err := money.Parse(quote.Price.Subtotal, currency, decimals)
	if err != nil {
		return domain.BillingSnapshot{}, domain.NewError(domain.ErrSettlementFailed, "quote has an invalid subtotal: "+err.Error(), false)
	}
	fees, err := money.Parse(quote.Price.Fees, currency, decimals)
	if err != nil {
		return domain.BillingSnapshot{}, domain.NewError(domain.ErrSettlementFailed, "quote has an invalid fee: "+err.Error(), false)
	}
	totalMax, err := money.Parse(quote.Price.TotalMax, currency, decimals)
	if err != nil {
		return domain.BillingSnapshot{}, domain.NewError(domain.ErrSettlementFailed, "quote has an invalid total_max: "+err.Error(), false)
	}

	providerGross := subtotal
	switch quote.PricingModel {
	case domain.PricingMetered, domain.PricingPerUnit:
		if quote.MeteredRates != nil {
			metered, err := meterUsage(*quote.MeteredRates, receipt.Usage, currency, decimals)
			if err != nil {
				return domain.BillingSnapshot{}, err
			}
			// Metered usage can never charge more than the frozen subtotal
			// it was quoted against, no matter how usage compares to
			// whatever the provider originally estimated when pricing the
			// capability.
			providerGross = metered.Min(subtotal)
		}
	case domain.PricingFree, domain.PricingFixed, domain.PricingPerUse, domain.PricingNegotiated, "":
		if hasNonEmptyMeteredRates(quote.MeteredRates) {
			return domain.BillingSnapshot{}, domain.NewError(domain.ErrSettlementFailed, "quote pricing_model does not permit metered_rates", false)
		}
	default:
		return domain.BillingSnapshot{}, domain.NewError(domain.ErrSettlementFailed, "quote has an unknown pricing_model", false)
	}

	gatewayFee := fees
	switch {
	case subtotal.IsPositive():
		gatewayFee, err = fees.MulDiv(providerGross, subtotal)
		if err != nil {
			return domain.BillingSnapshot{}, err
		}
	case !providerGross.IsPositive():
		gatewayFee = money.Zero(currency, decimals)
	}

	grossCharge, err := providerGross.Add(gatewayFee)
	if err != nil {
		return domain.BillingSnapshot{}, err
	}
	// Defense in depth: the proportional split above already guarantees
	// grossCharge <= totalMax by construction whenever Subtotal+Fees=TotalMax
	// holds, but a charge must never exceed what was quoted even if that
	// invariant were ever violated upstream.
	grossCharge = grossCharge.Min(totalMax)
	refund, err := totalMax.Sub(grossCharge)
	if err != nil {
		return domain.BillingSnapshot{}, err
	}

	return domain.BillingSnapshot{
		JobID: receipt.JobID, QuoteID: quote.ID, ReceiptID: receipt.ID,
		ProviderID: quote.ProviderID, CapabilityID: quote.CapabilityID, CapabilityVersion: quote.CapabilityVersion,
		// UsageCommitment records the verified Execution Receipt's own proof
		// -chain usage commitment (set by the tos-protocol adapter from the
		// signed receipt, or by the mock provider in tests) -- not a fresh
		// local rehash of Usage -- so it audits what was actually verified,
		// not a value ATOS recomputed for itself after the fact.
		TrustMode: quote.TrustMode, Usage: receipt.Usage, UsageCommitment: receipt.UsageCommitment,
		PricingModel: quote.PricingModel, PricingTermsHash: quote.TermsHash,
		GrossCharge:     domain.Money{Amount: grossCharge.String(), Currency: currency},
		ProviderGross:   domain.Money{Amount: providerGross.String(), Currency: currency},
		GatewayFee:      domain.Money{Amount: gatewayFee.String(), Currency: currency},
		PrincipalRefund: domain.Money{Amount: refund.String(), Currency: currency},
		CalculatedAt:    time.Now().UTC(),
	}, nil
}

// meteredRateDecimals is the internal precision unit rates are parsed and
// summed at -- deliberately much finer than quoteDecimals, since realistic
// per-token/per-byte/per-millisecond prices are routinely sub-cent (e.g.
// $0.000001/token). Rounding to the settlement currency's actual precision
// (quoteDecimals) happens exactly once, at the very end of meterUsage, via
// Rescale -- never per-dimension, which would compound truncation error
// across dimensions before the final clamp against the frozen subtotal.
const meteredRateDecimals = 9

// validatePricing enforces the full pricing contract for a Capability's
// Pricing, called at registration/update -- and again, on the Capability's
// currently stored Pricing, as defense in depth right before it gets frozen
// into a new Quote -- rather than only being discovered for the first time
// inside computeBillingSnapshot at settlement, by which point a Job has
// already been debited and escrowed:
//
//  1. Model must be one of the known PricingModel values.
//  2. Only PricingMetered/PricingPerUnit may carry non-empty MeteredRates;
//     a Free/Fixed/PerUse/Negotiated capability with a stray configured
//     rate would otherwise be silently billed by usage instead of the
//     price it actually declared (see computeBillingSnapshot).
//  3. Every configured MeteredRate must parse at meteredRateDecimals
//     precision (rejects negative, non-numeric, or over-precise rates).
func validatePricing(pricing domain.Pricing) error {
	switch pricing.Model {
	case domain.PricingFree, domain.PricingFixed, domain.PricingPerUse, domain.PricingNegotiated:
		if hasNonEmptyMeteredRates(pricing.MeteredRates) {
			return fmt.Errorf("pricing_model %q does not support metered_rates", pricing.Model)
		}
		return nil
	case domain.PricingMetered, domain.PricingPerUnit:
		return validateMeteredRateValues(pricing.MeteredRates, pricing.PriceHint.Currency)
	default:
		return fmt.Errorf("invalid pricing_model %q", pricing.Model)
	}
}

// hasNonEmptyMeteredRates reports whether rates configures at least one
// dimension, treating a nil pointer and an all-empty-string struct the
// same way (neither contributes to metered billing -- see
// domain.MeteredRates's doc comment).
func hasNonEmptyMeteredRates(rates *domain.MeteredRates) bool {
	if rates == nil {
		return false
	}
	return rates.PerInputByte != "" || rates.PerOutputByte != "" || rates.PerInputToken != "" ||
		rates.PerOutputToken != "" || rates.PerExecutionMillisecond != ""
}

// validateMeteredRateValues eagerly parses every configured MeteredRate at
// meteredRateDecimals precision so a malformed rate (negative, non-numeric,
// or more precise than meteredRateDecimals allows) is rejected up front
// rather than only being discovered inside meterUsage at settlement, where
// a permanently invalid frozen rate would fail identically on every
// reconciliation retry forever.
func validateMeteredRateValues(rates *domain.MeteredRates, currency string) error {
	if rates == nil {
		return nil
	}
	dimensions := []struct {
		field string
		rate  string
	}{
		{"per_input_byte", rates.PerInputByte},
		{"per_output_byte", rates.PerOutputByte},
		{"per_input_token", rates.PerInputToken},
		{"per_output_token", rates.PerOutputToken},
		{"per_execution_millisecond", rates.PerExecutionMillisecond},
	}
	for _, d := range dimensions {
		if d.rate == "" {
			continue
		}
		if _, err := money.Parse(d.rate, currency, meteredRateDecimals); err != nil {
			return fmt.Errorf("metered_rates.%s: %w", d.field, err)
		}
	}
	return nil
}

// meterUsage sums each configured per-dimension rate times its
// corresponding verified usage count. A dimension with no configured rate
// (empty string) does not contribute.
func meterUsage(rates domain.MeteredRates, usage domain.Usage, currency string, settlementDecimals ...int) (money.Amount, error) {
	total := money.Zero(currency, meteredRateDecimals)
	dimensions := []struct {
		rate  string
		count uint64
	}{
		{rates.PerInputByte, usage.InputBytes},
		{rates.PerOutputByte, usage.OutputBytes},
		{rates.PerInputToken, usage.InputTokens},
		{rates.PerOutputToken, usage.OutputTokens},
		{rates.PerExecutionMillisecond, usage.ExecutionMillis},
	}
	for _, d := range dimensions {
		if d.rate == "" {
			continue
		}
		rate, err := money.Parse(d.rate, currency, meteredRateDecimals)
		if err != nil {
			return money.Amount{}, domain.NewError(domain.ErrSettlementFailed, "quote has an invalid metered rate: "+err.Error(), false)
		}
		sum, err := total.Add(rate.MulUint64(d.count))
		if err != nil {
			return money.Amount{}, err
		}
		total = sum
	}
	target := quoteDecimals
	if len(settlementDecimals) > 0 {
		target = settlementDecimals[0]
	}
	return total.Rescale(target), nil
}
