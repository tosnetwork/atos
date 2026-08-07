package domain

import "time"

type EscrowStatus string

const (
	EscrowReserved EscrowStatus = "reserved"
	EscrowSettled  EscrowStatus = "settled"
	EscrowReleased EscrowStatus = "released"
	EscrowDisputed EscrowStatus = "disputed"
)

func (s EscrowStatus) Terminal() bool {
	return s == EscrowSettled || s == EscrowReleased
}

type Escrow struct {
	ID              string               `json:"escrow_id"`
	QuoteID         string               `json:"quote_id"`
	JobID           string               `json:"job_id"`
	PrincipalID     string               `json:"principal_id"`
	ProviderID      string               `json:"provider_id"`
	CapabilityID    string               `json:"capability_id"`
	CapabilityVersion string             `json:"capability_version"`
	TrustMode       TrustMode            `json:"trust_mode"`
	ProofProfile    ProofProfile         `json:"proof_profile,omitempty"`
	Settlement      SettlementDescriptor `json:"settlement"`
	Reserved        Money                `json:"reserved"`
	Status          EscrowStatus         `json:"status"`
	NetworkProofRef string               `json:"network_proof_ref,omitempty"`
	CreatedAt       time.Time            `json:"created_at"`
	ExpiresAt       time.Time            `json:"expires_at"`
	SettledAt       *time.Time           `json:"settled_at,omitempty"`
}

type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}
