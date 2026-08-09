package service

import (
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
func computeBillingSnapshot(quote domain.Quote, receipt domain.ExecutionReceipt) (domain.BillingSnapshot, error) {
	currency := quote.Price.Currency
	subtotal, err := money.Parse(quote.Price.Subtotal, currency, quoteDecimals)
	if err != nil {
		return domain.BillingSnapshot{}, domain.NewError(domain.ErrSettlementFailed, "quote has an invalid subtotal: "+err.Error(), false)
	}
	fees, err := money.Parse(quote.Price.Fees, currency, quoteDecimals)
	if err != nil {
		return domain.BillingSnapshot{}, domain.NewError(domain.ErrSettlementFailed, "quote has an invalid fee: "+err.Error(), false)
	}
	totalMax, err := money.Parse(quote.Price.TotalMax, currency, quoteDecimals)
	if err != nil {
		return domain.BillingSnapshot{}, domain.NewError(domain.ErrSettlementFailed, "quote has an invalid total_max: "+err.Error(), false)
	}

	providerGross := subtotal
	if quote.MeteredRates != nil {
		metered, err := meterUsage(*quote.MeteredRates, receipt.Usage, currency)
		if err != nil {
			return domain.BillingSnapshot{}, err
		}
		// Metered usage can never charge more than the frozen subtotal it
		// was quoted against, no matter how usage compares to whatever the
		// provider originally estimated when pricing the capability.
		providerGross = metered.Min(subtotal)
	}

	gatewayFee := fees
	switch {
	case subtotal.IsPositive():
		gatewayFee, err = fees.MulDiv(providerGross, subtotal)
		if err != nil {
			return domain.BillingSnapshot{}, err
		}
	case !providerGross.IsPositive():
		gatewayFee = money.Zero(currency, quoteDecimals)
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
		TrustMode: quote.TrustMode, Usage: receipt.Usage, UsageCommitment: hashCommitment(receipt.Usage),
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

// meterUsage sums each configured per-dimension rate times its
// corresponding verified usage count. A dimension with no configured rate
// (empty string) does not contribute.
func meterUsage(rates domain.MeteredRates, usage domain.Usage, currency string) (money.Amount, error) {
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
	return total.Rescale(quoteDecimals), nil
}
