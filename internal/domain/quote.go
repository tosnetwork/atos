package domain

import "time"

type Price struct {
	Subtotal string `json:"subtotal"`
	Fees     string `json:"fees"`
	TotalMax string `json:"total_max"`
	Currency string `json:"currency"`
}

// Quote is the immutable boundary between discovery and financially
// committing execution. requested_trust_mode records caller intent while
// trust_mode is always concrete.
type Quote struct {
	ID                        string               `json:"quote_id"`
	CapabilityID              string               `json:"capability_id"`
	CapabilityVersion         string               `json:"capability_version"`
	ProviderID                string               `json:"provider_id"`
	PrincipalID               string               `json:"principal_id,omitempty"`
	RequestedTrustMode        RequestedTrustMode   `json:"requested_trust_mode"`
	TrustMode                 TrustMode            `json:"trust_mode"`
	ProofProfile              ProofProfile         `json:"proof_profile,omitempty"`
	Price                     Price                `json:"price"`
	Settlement                SettlementDescriptor `json:"settlement"`
	Proof                     ProofDescriptor      `json:"proof"`
	ExpiresAt                 time.Time            `json:"expires_at"`
	RequiresConfirmation      bool                 `json:"requires_confirmation"`
	TermsHash                 string               `json:"terms_hash"`
	DisputePolicyHash         string               `json:"dispute_policy_hash,omitempty"`
	ServiceQuoteID            string               `json:"service_quote_id,omitempty"`
	UnderlyingServiceQuoteRef string               `json:"underlying_service_quote_ref,omitempty"`
	InputSummaryCommitment    string               `json:"input_summary_commitment,omitempty"`
	ExecutionDeadline         time.Time            `json:"execution_deadline,omitempty"`
	CreatedAt                 time.Time            `json:"-"`
	// MeteredRates is frozen from the Capability's pricing at Quote-creation
	// time (nil if the Capability has none configured). Settlement billing
	// (internal/service/billing.go) reads only this frozen copy, never the
	// Capability's current live pricing -- see domain.MeteredRates.
	MeteredRates *MeteredRates `json:"metered_rates,omitempty"`
	// PricingModel is likewise frozen from the Capability's pricing at
	// Quote-creation time. Along with MeteredRates and Price, it is part of
	// the frozen pricing contract committed into TermsHash (see
	// internal/service/quote.go's Create) -- not merely recorded for audit
	// trail -- so two Quotes cannot share a TermsHash while differing in
	// how a Job would actually be charged.
	PricingModel PricingModel `json:"pricing_model,omitempty"`
	// Binding, InputSchema and OutputSchema are frozen from the Capability
	// at Quote-creation time via SelectBinding, exactly like MeteredRates/
	// PricingModel above. Job creation (internal/service/job.go's submit)
	// MUST use these frozen values, never re-derive them from the
	// Capability's live state -- an already-issued Quote/Job MUST continue
	// to use its frozen version/binding semantics after a later Capability
	// update (atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.1.0's binding-
	// freeze rule). Binding is nil when the Capability has no transport
	// binding at all (the ordinary tos-native/human path), not as a signal
	// that this Quote predates this field.
	Binding      *CapabilityBinding `json:"binding,omitempty"`
	InputSchema  map[string]any     `json:"input_schema,omitempty"`
	OutputSchema map[string]any     `json:"output_schema,omitempty"`
}

func (q Quote) Expired(now time.Time) bool {
	return !now.Before(q.ExpiresAt)
}
