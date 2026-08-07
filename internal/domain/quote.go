package domain

import "time"

type Price struct {
	Subtotal string `json:"subtotal"`
	Fees     string `json:"fees"`
	TotalMax string `json:"total_max"`
	Currency string `json:"currency"`
}

// Quote is immutable and time-limited (docs/SETTLEMENT.md "Quote Object").
// Once issued it must never be mutated — a re-quote is a new Quote with a
// new ID.
type Quote struct {
	ID                   string    `json:"quote_id"`
	CapabilityID         string    `json:"capability_id"`
	CapabilityVersion    string    `json:"capability_version"`
	Price                Price     `json:"price"`
	ExpiresAt            time.Time `json:"expires_at"`
	RequiresConfirmation bool      `json:"requires_confirmation"`
	TermsHash            string    `json:"terms_hash"`
	CreatedAt            time.Time `json:"-"`
}

// Expired reports whether the quote can no longer be committed against.
func (q Quote) Expired(now time.Time) bool {
	return !now.Before(q.ExpiresAt)
}
