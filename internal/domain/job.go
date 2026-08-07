package domain

import "time"

type JobState string

const (
	JobSubmitted     JobState = "submitted"
	JobWorking       JobState = "working"
	JobInputRequired JobState = "input_required"
	JobCompleted     JobState = "completed"
	JobFailed        JobState = "failed"
	JobCanceled      JobState = "canceled"
	JobRejected      JobState = "rejected"
)

func (s JobState) Terminal() bool {
	switch s {
	case JobCompleted, JobFailed, JobCanceled, JobRejected:
		return true
	default:
		return false
	}
}

type Artifact struct {
	ID                string         `json:"artifact_id"`
	Name              string         `json:"name,omitempty"`
	MimeType          string         `json:"mime_type,omitempty"`
	Content           map[string]any `json:"content,omitempty"`
	ContentCommitment string         `json:"content_commitment,omitempty"`
}

type Job struct {
	ID                    string         `json:"job_id"`
	InvocationID          string         `json:"invocation_id,omitempty"`
	CapabilityID          string         `json:"capability_id"`
	CapabilityVersion     string         `json:"capability_version"`
	ProviderID            string         `json:"provider_id"`
	QuoteID               string         `json:"quote_id"`
	EscrowID              string         `json:"escrow_id"`
	PrincipalID           string         `json:"-"`
	TrustMode             TrustMode      `json:"trust_mode"`
	ProofProfile          ProofProfile   `json:"proof_profile,omitempty"`
	ProofStatus           ProofStatus    `json:"proof_status"`
	State                 JobState       `json:"state"`
	Input                 map[string]any `json:"-"`
	Output                map[string]any `json:"output,omitempty"`
	Artifacts             []Artifact     `json:"artifacts,omitempty"`
	IdempotencyKey        string         `json:"-"`
	FailureReason         string         `json:"failure_reason,omitempty"`
	ErrorCode             ErrorCode      `json:"error_code,omitempty"`
	CreatedAt             time.Time      `json:"created_at"`
	UpdatedAt             time.Time      `json:"-"`
	CompletedAt           *time.Time     `json:"completed_at,omitempty"`
	EstimatedCompletionAt *time.Time     `json:"estimated_completion_at,omitempty"`
}
