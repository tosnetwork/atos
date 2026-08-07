// Package a2a maps A2A Tasks and Messages onto the canonical ATOS Job and
// Quote contracts. ATOS commerce fields stay in a versioned extension rather
// than modifying core A2A objects.
package a2a

import (
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

const CommerceExtensionURI = "https://atos.im/extensions/commerce/v2"

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

type Artifact struct {
	ID      string         `json:"artifactId"`
	Name    string         `json:"name,omitempty"`
	Parts   []Part         `json:"parts,omitempty"`
	Content map[string]any `json:"content,omitempty"`
}

type Part struct {
	Kind string         `json:"kind"`
	Text string         `json:"text,omitempty"`
	Data map[string]any `json:"data,omitempty"`
}

type Task struct {
	ID        string         `json:"id"`
	ContextID string         `json:"contextId,omitempty"`
	Status    TaskStatus     `json:"status"`
	Artifacts []Artifact     `json:"artifacts,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

func TaskFromJob(j domain.Job) Task {
	artifacts := make([]Artifact, 0, len(j.Artifacts)+boolToInt(j.Output != nil))
	for _, a := range j.Artifacts {
		artifacts = append(artifacts, Artifact{ID: a.ID, Name: a.Name, Content: a.Content})
	}
	if j.Output != nil {
		artifacts = append(artifacts, Artifact{ID: j.ID + "-output", Name: "result", Content: j.Output})
	}
	return Task{
		ID:     j.ID,
		Status: TaskStatus{State: StateFromJob(j.State), Timestamp: j.UpdatedAt},
		Artifacts: artifacts,
		Metadata: map[string]any{
			CommerceExtensionURI: map[string]any{
				"quote_id": j.QuoteID,
				"capability_id": j.CapabilityID,
				"capability_version": j.CapabilityVersion,
				"provider_id": j.ProviderID,
				"trust_mode": j.TrustMode,
				"proof_profile": nullableProfile(j.ProofProfile),
				"proof_status": j.ProofStatus,
			},
		},
	}
}

func nullableProfile(profile domain.ProofProfile) any {
	if profile == "" {
		return nil
	}
	return profile
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

type Message struct {
	Role     string         `json:"role"`
	Parts    []Part         `json:"parts,omitempty"`
	TaskID   string         `json:"taskId,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

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
		CapabilityID: get("capability_id"), QuoteID: get("quote_id"),
		IdempotencyKey: get("idempotency_key"), Confirmed: confirmed,
		Reason: get("reason"),
	}
}
