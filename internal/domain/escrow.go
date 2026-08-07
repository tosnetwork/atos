package domain

import "time"

// EscrowStatus implements the state machine from docs/SETTLEMENT.md's
// Escrow section: reserved is the only entry state, settled/released are
// terminal, disputed is a temporary hold on either path.
type EscrowStatus string

const (
	EscrowReserved EscrowStatus = "reserved"
	EscrowSettled  EscrowStatus = "settled"
	EscrowReleased EscrowStatus = "released"
	EscrowDisputed EscrowStatus = "disputed"
)

// Terminal reports whether the escrow can no longer change state.
func (s EscrowStatus) Terminal() bool {
	return s == EscrowSettled || s == EscrowReleased
}

type Escrow struct {
	ID           string       `json:"escrow_id"`
	QuoteID      string       `json:"quote_id"`
	PrincipalID  string       `json:"principal_id"`
	ProviderID   string       `json:"provider_id"`
	CapabilityID string       `json:"capability_id"`
	Reserved     Money        `json:"reserved"`
	Status       EscrowStatus `json:"status"`
	CreatedAt    time.Time    `json:"created_at"`
	ExpiresAt    time.Time    `json:"expires_at"`
	SettledAt    *time.Time   `json:"settled_at,omitempty"`
}

// Money is the wire-shape counterpart of money.Amount — kept separate so
// domain types stay serialization-friendly without importing big.Int
// formatting concerns into every call site.
type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}
