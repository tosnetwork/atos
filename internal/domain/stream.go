package domain

import "time"

// JobEventType mirrors tos-protocol's atos.tos.v1.JobEventType so ATOS's
// durable journal and the upstream StreamJob RPC stay directly comparable.
type JobEventType string

const (
	JobEventState         JobEventType = "state"
	JobEventOutputChunk   JobEventType = "output_chunk"
	JobEventInputRequired JobEventType = "input_required"
	JobEventProofStatus   JobEventType = "proof_status"
	JobEventTerminal      JobEventType = "terminal"
)

// JobEvent is one durable, ordered entry in a Job's resumable stream
// journal. Sequence is the append order within a Job; Offset/StreamDigest
// only apply to output bytes and are cumulative across every OutputChunk
// event emitted so far (matching tos-protocol's StreamJobRequest/JobEvent
// resume fields: next_sequence, next_offset, expected_stream_digest).
type JobEvent struct {
	JobID            string       `json:"job_id"`
	Sequence         uint64       `json:"sequence"`
	EventType        JobEventType `json:"event_type"`
	State            JobState     `json:"state,omitempty"`
	Chunk            []byte       `json:"chunk,omitempty"`
	Offset           uint64       `json:"offset,omitempty"`
	TotalOutputBytes uint64       `json:"total_output_bytes,omitempty"`
	StreamDigest     string       `json:"stream_digest,omitempty"`
	Terminal         bool         `json:"terminal,omitempty"`
	Usage            *Usage       `json:"usage,omitempty"`
	ProofStatus      *ProofStatus `json:"proof_status,omitempty"`
	ErrorCode        ErrorCode    `json:"error_code,omitempty"`
	CreatedAt        time.Time    `json:"created_at"`
}

// JobStreamCursor is the durable resume point for a Job's stream journal.
// It is maintained transactionally alongside every JobEvent append so a
// resuming client (or a restarted ATOS process) never has to replay the
// full event history just to find where it left off.
type JobStreamCursor struct {
	JobID        string `json:"job_id"`
	NextSequence uint64 `json:"next_sequence"`
	NextOffset   uint64 `json:"next_offset"`
	StreamDigest string `json:"stream_digest,omitempty"`
	Terminal     bool   `json:"terminal"`
}
