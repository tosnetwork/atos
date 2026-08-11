package domain

import "time"

// MeteredRates defines optional per-dimension unit prices for a Capability
// priced with PricingMetered or PricingPerUnit. Amounts are decimal strings
// in the capability's pricing currency, parsed at a much finer internal
// precision than the currency's own settlement precision (see
// internal/service/billing.go's meteredRateDecimals) so realistic sub-cent
// per-token/per-byte rates (e.g. "0.000001") are accepted; only the final
// summed charge is truncated to the currency's actual precision. A
// zero/absent rate means that dimension does not contribute to metered
// billing. Capabilities without
// MeteredRates (PricingFixed/PricingFree/PricingPerUse/PricingNegotiated,
// or a metered capability that simply has none configured) are billed the
// full frozen Quote subtotal, exactly matching Phase 0/1 behavior.
//
// MeteredRates is frozen into the Quote at Quote-creation time
// (Quote.MeteredRates) precisely so that billing at settlement time never
// has to -- and never may -- re-read the Capability's current live pricing
// configuration to reinterpret an old Job.
type MeteredRates struct {
	PerInputByte            string `json:"per_input_byte,omitempty"`
	PerOutputByte           string `json:"per_output_byte,omitempty"`
	PerInputToken           string `json:"per_input_token,omitempty"`
	PerOutputToken          string `json:"per_output_token,omitempty"`
	PerExecutionMillisecond string `json:"per_execution_millisecond,omitempty"`
}

// BillingSnapshot is the durable, auditable record of one Job's metered
// billing calculation: a deterministic function of the frozen Quote terms
// and the verified Execution Receipt usage. Because its inputs are
// themselves immutable and already durable, recomputing and re-saving the
// identical snapshot for the same Job is always a safe no-op.
type BillingSnapshot struct {
	JobID             string       `json:"job_id"`
	QuoteID           string       `json:"quote_id"`
	ReceiptID         string       `json:"receipt_id"`
	ProviderID        string       `json:"provider_id"`
	CapabilityID      string       `json:"capability_id"`
	CapabilityVersion string       `json:"capability_version"`
	TrustMode         TrustMode    `json:"trust_mode"`
	Usage             Usage        `json:"usage"`
	UsageCommitment   string       `json:"usage_commitment,omitempty"`
	PricingModel      PricingModel `json:"pricing_model"`
	PricingTermsHash  string       `json:"pricing_terms_hash"`
	// GrossCharge is the final amount actually charged to the principal
	// (0 <= GrossCharge <= Quote.Price.TotalMax). ProviderGross is the
	// portion of GrossCharge attributable to the provider's metered price;
	// GatewayFee is the remaining portion attributable to ATOS's frozen
	// Quote.Price.Fees, scaled proportionally with ProviderGross so
	// ProviderGross+GatewayFee never exceeds TotalMax. PrincipalRefund is
	// TotalMax-GrossCharge.
	GrossCharge     Money     `json:"gross_charge"`
	ProviderGross   Money     `json:"provider_gross"`
	GatewayFee      Money     `json:"gateway_fee"`
	PrincipalRefund Money     `json:"principal_refund"`
	CalculatedAt    time.Time `json:"calculated_at"`
}

// EarningStatus is the provider earnings lifecycle. frozen/released/reversed
// are reserved for Phase 2C's dispute workflow and are modeled here only so
// that phase does not require a schema migration; no Phase 2B code path
// drives a Phase 2B-created earning into any of those three states.
type EarningStatus string

const (
	EarningMaturing      EarningStatus = "maturing"
	EarningAvailable     EarningStatus = "available"
	EarningPayoutPending EarningStatus = "payout_pending"
	EarningPaid          EarningStatus = "paid"
	EarningFrozen        EarningStatus = "frozen"
	EarningReleased      EarningStatus = "released"
	EarningReversed      EarningStatus = "reversed"
)

func (s EarningStatus) Terminal() bool {
	return s == EarningPaid || s == EarningReversed
}

// ProviderEarning is one provider's durable claim on a single finalized,
// valid settlement, uniquely bound to it via SettlementID (enforced by a
// database uniqueness constraint, not an application-level check), and the
// idempotent external-payout state machine for actually paying it out.
// PayoutIdempotencyKey/PayoutFailureReason are internal recovery
// checkpoints, deliberately excluded from the public JSON contract.
type ProviderEarning struct {
	ID                string        `json:"earning_id"`
	ProviderID        string        `json:"provider_id"`
	JobID             string        `json:"job_id"`
	QuoteID           string        `json:"quote_id"`
	ReceiptID         string        `json:"receipt_id"`
	SettlementID      string        `json:"settlement_id"`
	CapabilityID      string        `json:"capability_id"`
	CapabilityVersion string        `json:"capability_version"`
	GrossAmount       Money         `json:"gross_amount"`
	GatewayFee        Money         `json:"gateway_fee"`
	NetAmount         Money         `json:"net_amount"`
	Status            EarningStatus `json:"status"`
	CreatedAt         time.Time     `json:"created_at"`
	MaturesAt         time.Time     `json:"matures_at"`
	AvailableAt       *time.Time    `json:"available_at,omitempty"`
	PayoutRequestedAt *time.Time    `json:"payout_requested_at,omitempty"`
	PayoutReference   string        `json:"payout_reference,omitempty"`
	PaidAt            *time.Time    `json:"paid_at,omitempty"`

	PayoutIdempotencyKey string    `json:"-"`
	PayoutAttempts       int       `json:"-"`
	PayoutLastAttemptAt  time.Time `json:"-"`
	PayoutFailureReason  string    `json:"-"`
	PayoutGeneration     int       `json:"-"`

	// DisputeHoldID is the ID of the Dispute currently holding this earning
	// out of the payout pipeline, or empty if none. It is set (in the same
	// transaction as the dispute's own checkpoint) when a Dispute is
	// opened against an earning that was mid-payout (PayoutPending) at
	// open time -- the payout attempt already in flight is allowed to
	// resolve (Query/recover to Paid, or fail to Available), but once
	// this hold is set, no *new* payout intent may begin for as long as it
	// remains set, even if the earning is transiently Available again
	// (e.g. the in-flight attempt was rejected). It is cleared when the
	// Dispute reaches a terminal economic outcome. Deliberately excluded
	// from the public JSON contract and from earningContentHash (a
	// lifecycle field, not identity/economic).
	DisputeHoldID string `json:"-"`
}
