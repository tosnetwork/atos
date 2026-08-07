// Package a2a implements the A2A profile from ~/atos-spec/docs/A2A.md: A2A
// Tasks map to ATOS Jobs, A2A Messages carry job interaction content, and
// ATOS-specific commercial fields (quote_id, idempotency_key, ...) ride in
// an A2A extension rather than modifying core A2A objects, per that doc's
// "Extensions" section.
package a2a

import (
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

// CommerceExtensionURI is the proposed extension URI from docs/A2A.md.
const CommerceExtensionURI = "https://atos.im/extensions/commerce/v1"

// TaskState is A2A's task state vocabulary. Note the hyphen in
// "input-required" — A2A convention, distinct from ATOS's internal
// "input_required" (see StateFromJob).
type TaskState string

const (
	TaskSubmitted     TaskState = "submitted"
	TaskWorking       TaskState = "working"
	TaskInputRequired TaskState = "input-required"
	TaskCompleted     TaskState = "completed"
	TaskFailed        TaskState = "failed"
	TaskCanceled      TaskState = "canceled"
	TaskRejected      TaskState = "rejected"
)

// StateFromJob implements the "State Mapping" table in docs/A2A.md.
func StateFromJob(s domain.JobState) TaskState {
	switch s {
	case domain.JobSubmitted:
		return TaskSubmitted
	case domain.JobWorking:
		return TaskWorking
	case domain.JobInputRequired:
		return TaskInputRequired
	case domain.JobCompleted:
		return TaskCompleted
	case domain.JobFailed:
		return TaskFailed
	case domain.JobCanceled:
		return TaskCanceled
	case domain.JobRejected:
		return TaskRejected
	default:
		return TaskWorking
	}
}

type TaskStatus struct {
	State     TaskState `json:"state"`
	Timestamp time.Time `json:"timestamp"`
}

// Artifact mirrors the A2A artifact shape closely enough for Phase 0 —
// ATOS's own domain.Artifact already carries the same essential fields.
type Artifact struct {
	ID      string         `json:"artifactId"`
	Name    string         `json:"name,omitempty"`
	Parts   []Part         `json:"parts,omitempty"`
	Content map[string]any `json:"content,omitempty"`
}

type Part struct {
	Kind string         `json:"kind"` // "text" | "data"
	Text string         `json:"text,omitempty"`
	Data map[string]any `json:"data,omitempty"`
}

// Task is the A2A-facing view of an ATOS Job.
type Task struct {
	ID        string     `json:"id"`
	ContextID string     `json:"contextId,omitempty"`
	Status    TaskStatus `json:"status"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
}

// TaskFromJob builds the A2A response object for a Job, per docs/A2A.md's
// "ATOS maps A2A Tasks to ATOS Jobs" mapping.
func TaskFromJob(j domain.Job) Task {
	artifacts := make([]Artifact, 0, len(j.Artifacts)+boolToInt(j.Output != nil))
	for _, a := range j.Artifacts {
		artifacts = append(artifacts, Artifact{ID: a.ID, Name: a.Name, Content: a.Content})
	}
	if j.Output != nil {
		artifacts = append(artifacts, Artifact{ID: j.ID + "-output", Name: "result", Content: j.Output})
	}
	return Task{
		ID:        j.ID,
		Status:    TaskStatus{State: StateFromJob(j.State), Timestamp: j.UpdatedAt},
		Artifacts: artifacts,
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Message is the A2A-facing request/response envelope carrying the ATOS
// commerce extension.
type Message struct {
	Role     string         `json:"role"`
	Parts    []Part         `json:"parts,omitempty"`
	TaskID   string         `json:"taskId,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// CommerceExtension is docs/A2A.md's "Extension fields" — quote_id, price,
// settlement_mode, trust and idempotency_key ride here, never wallet keys,
// chain RPC credentials or provider secrets (per that doc's explicit rule).
type CommerceExtension struct {
	CapabilityID   string `json:"capability_id"`
	QuoteID        string `json:"quote_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Confirmed      bool   `json:"confirmed"`
	Reason         string `json:"reason,omitempty"`
}

func (m Message) Commerce() CommerceExtension {
	raw, ok := m.Metadata[CommerceExtensionURI]
	if !ok {
		return CommerceExtension{}
	}
	ext, _ := raw.(map[string]any)
	get := func(k string) string {
		v, _ := ext[k].(string)
		return v
	}
	confirmed, _ := ext["confirmed"].(bool)
	return CommerceExtension{
		CapabilityID:   get("capability_id"),
		QuoteID:        get("quote_id"),
		IdempotencyKey: get("idempotency_key"),
		Confirmed:      confirmed,
		Reason:         get("reason"),
	}
}
