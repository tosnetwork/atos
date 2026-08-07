package domain

import "time"

// ReceiptStatus mirrors the escrow's terminal state plus dispute outcomes
// (docs/SETTLEMENT.md "Receipt").
type ReceiptStatus string

const (
	ReceiptSettled              ReceiptStatus = "settled"
	ReceiptReleased             ReceiptStatus = "released"
	ReceiptDisputed             ReceiptStatus = "disputed"
	ReceiptSettledAfterDispute  ReceiptStatus = "settled_after_dispute"
	ReceiptReleasedAfterDispute ReceiptStatus = "released_after_dispute"
)

type Receipt struct {
	ID        string        `json:"receipt_id"`
	QuoteID   string        `json:"quote_id"`
	EscrowID  string        `json:"escrow_id"`
	JobID     string        `json:"job_id"`
	Charged   Money         `json:"charged"`
	Refunded  Money         `json:"refunded"`
	Status    ReceiptStatus `json:"status"`
	CreatedAt time.Time     `json:"created_at"`
}

// ExecutionReceipt is what tos-ai signs after running a job — the input to
// tos-core.VerifyExecutionReceipt. It is intentionally distinct from the
// client-facing Receipt above: this one carries execution facts, the other
// carries billing facts.
type ExecutionReceipt struct {
	JobID        string    `json:"job_id"`
	ProviderID   string    `json:"provider_id"`
	CapabilityID string    `json:"capability_id"`
	InputHash    string    `json:"input_hash"`
	OutputHash   string    `json:"output_hash"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	Cost         Money     `json:"cost"`
	Signature    string    `json:"signature"`
}
