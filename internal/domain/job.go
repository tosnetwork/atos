package domain

import "time"

// JobState follows the canonical states from docs/MCP.md and the A2A
// mapping in docs/A2A.md: submitted -> working -> input_required -> working
// -> completed, with failed/canceled/rejected as terminal alternatives.
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
	ID       string         `json:"id"`
	Name     string         `json:"name,omitempty"`
	MimeType string         `json:"mime_type,omitempty"`
	Content  map[string]any `json:"content,omitempty"`
}

type Job struct {
	ID                    string         `json:"job_id"`
	CapabilityID          string         `json:"capability_id"`
	QuoteID               string         `json:"quote_id"`
	EscrowID              string         `json:"escrow_id"`
	PrincipalID           string         `json:"-"`
	State                 JobState       `json:"state"`
	Input                 map[string]any `json:"-"`
	Output                map[string]any `json:"output,omitempty"`
	Artifacts             []Artifact     `json:"artifacts,omitempty"`
	IdempotencyKey        string         `json:"-"`
	FailureReason         string         `json:"failure_reason,omitempty"`
	CreatedAt             time.Time      `json:"created_at"`
	UpdatedAt             time.Time      `json:"-"`
	EstimatedCompletionAt *time.Time     `json:"estimated_completion_at,omitempty"`
}
