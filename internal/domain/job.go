package domain

import "time"

type JobState string

const (
	JobSubmitted     JobState = "submitted"
	JobWorking       JobState = "working"
	JobInputRequired JobState = "input_required"
	JobCanceling     JobState = "canceling"
	JobReconciling   JobState = "reconciling"
	JobCompleted     JobState = "completed"
	JobFailed        JobState = "failed"
	JobCanceled      JobState = "canceled"
	JobRejected      JobState = "rejected"
)

type EconomicState string

const (
	EconomicNone              EconomicState = ""
	EconomicDebited           EconomicState = "debited"
	EconomicEscrowPending     EconomicState = "escrow_pending"
	EconomicEscrowReserved    EconomicState = "escrow_reserved"
	EconomicSettlementPending EconomicState = "settlement_pending"
	EconomicReleasePending    EconomicState = "release_pending"
	EconomicSettled           EconomicState = "settled"
	EconomicReleased          EconomicState = "released"
)

func (s JobState) Terminal() bool {
	switch s {
	case JobCompleted, JobFailed, JobCanceled, JobRejected:
		return true
	default:
		return false
	}
}

type ConfirmationStatus string

const (
	ConfirmationPending  ConfirmationStatus = "pending"
	ConfirmationApproved ConfirmationStatus = "approved"
	ConfirmationDenied   ConfirmationStatus = "denied"
	ConfirmationExpired  ConfirmationStatus = "expired"
	ConfirmationConsumed ConfirmationStatus = "consumed"
)

// SpendConfirmation is a server-issued, single-use approval challenge. Its
// binding hash commits to the principal, Quote contract, request input and
// idempotency identity; a caller cannot authorize a paid operation by merely
// resubmitting a boolean flag.
type SpendConfirmation struct {
	ID          string             `json:"confirmation_id"`
	UserCode    string             `json:"user_code"`
	Status      ConfirmationStatus `json:"status"`
	Maximum     Money              `json:"maximum"`
	BindingHash string             `json:"-"`
	CreatedAt   time.Time          `json:"created_at"`
	ExpiresAt   time.Time          `json:"expires_at"`
	DecidedAt   *time.Time         `json:"decided_at,omitempty"`
	ConsumedAt  *time.Time         `json:"consumed_at,omitempty"`
}

type Artifact struct {
	ID                string         `json:"artifact_id"`
	Name              string         `json:"name,omitempty"`
	MimeType          string         `json:"mime_type,omitempty"`
	Content           map[string]any `json:"content,omitempty"`
	ContentCommitment string         `json:"content_commitment,omitempty"`
}

type Job struct {
	ID                     string             `json:"job_id"`
	InvocationID           string             `json:"invocation_id,omitempty"`
	CapabilityID           string             `json:"capability_id"`
	CapabilityVersion      string             `json:"capability_version"`
	ProviderID             string             `json:"provider_id"`
	QuoteID                string             `json:"quote_id"`
	ServiceQuoteID         string             `json:"service_quote_id,omitempty"`
	EscrowID               string             `json:"escrow_id"`
	PrincipalID            string             `json:"-"`
	TrustMode              TrustMode          `json:"trust_mode"`
	ProofProfile           ProofProfile       `json:"proof_profile,omitempty"`
	ProofStatus            ProofStatus        `json:"proof_status"`
	State                  JobState           `json:"state"`
	Input                  map[string]any     `json:"-"`
	Output                 map[string]any     `json:"output,omitempty"`
	Artifacts              []Artifact         `json:"artifacts,omitempty"`
	Confirmation           *SpendConfirmation `json:"confirmation,omitempty"`
	IdempotencyKey         string             `json:"-"`
	FailureReason          string             `json:"failure_reason,omitempty"`
	ErrorCode              ErrorCode          `json:"error_code,omitempty"`
	EconomicState          EconomicState      `json:"-"`
	ReconciliationRequired bool               `json:"reconciliation_required,omitempty"`
	PendingCredit          *Money             `json:"-"`
	ReconciliationTarget   JobState           `json:"-"`
	CreatedAt              time.Time          `json:"created_at"`
	UpdatedAt              time.Time          `json:"-"`
	CompletedAt            *time.Time         `json:"completed_at,omitempty"`
	EstimatedCompletionAt  *time.Time         `json:"estimated_completion_at,omitempty"`
	ExecutionDeadline      time.Time          `json:"execution_deadline,omitempty"`
}
