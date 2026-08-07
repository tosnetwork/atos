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
	UnderlyingServiceQuoteRef string               `json:"underlying_service_quote_ref,omitempty"`
	CreatedAt                 time.Time            `json:"-"`
}

func (q Quote) Expired(now time.Time) bool {
	return !now.Before(q.ExpiresAt)
}
