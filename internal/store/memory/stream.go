package memory

import (
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// maxJobStreamChunkBytes mirrors the bound enforced by the Postgres store.
const maxJobStreamChunkBytes = 256 << 10

// streamDigestState is the marshaled resumable sha256 hasher state for one
// Job's cumulative output-chunk digest, mirroring the Postgres store's
// digest_state column so both backends validate identically.
type streamDigestState []byte

func eventContentHash(e domain.JobEvent) string {
	encoded, _ := json.Marshal(struct {
		EventType        domain.JobEventType `json:"event_type"`
		State            domain.JobState     `json:"state"`
		Chunk            []byte              `json:"chunk"`
		Offset           uint64              `json:"offset"`
		TotalOutputBytes uint64              `json:"total_output_bytes"`
		StreamDigest     string              `json:"stream_digest"`
		Terminal         bool                `json:"terminal"`
		Usage            *domain.Usage       `json:"usage"`
		ProofStatus      *domain.ProofStatus `json:"proof_status"`
		ErrorCode        domain.ErrorCode    `json:"error_code"`
	}{
		EventType: e.EventType, State: e.State, Chunk: e.Chunk, Offset: e.Offset,
		TotalOutputBytes: e.TotalOutputBytes, StreamDigest: e.StreamDigest,
		Terminal: e.Terminal, Usage: e.Usage, ProofStatus: e.ProofStatus, ErrorCode: e.ErrorCode,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func (s *Store) AppendJobStreamEvent(ctx context.Context, event domain.JobEvent) error {
	if event.JobID == "" {
		return domain.NewError(domain.ErrValidationFailed, "job_id is required", false)
	}
	if len(event.Chunk) > maxJobStreamChunkBytes {
		return domain.NewError(domain.ErrStreamChunkTooLarge, fmt.Sprintf("chunk exceeds %d bytes", maxJobStreamChunkBytes), false)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cursor := s.streamCursors[event.JobID]

	if event.Sequence < cursor.NextSequence {
		events := s.streamEvents[event.JobID]
		idx := int(event.Sequence)
		if idx < 0 || idx >= len(events) {
			return domain.NewError(domain.ErrStreamSequenceConflict, "stream sequence is behind the durable cursor but has no matching row", false)
		}
		if eventContentHash(events[idx]) != eventContentHash(event) {
			return domain.NewError(domain.ErrStreamSequenceConflict, "stream sequence already holds different content", false)
		}
		return nil
	}
	if event.Sequence > cursor.NextSequence {
		return store.ErrConflict
	}
	if cursor.Terminal {
		return domain.NewError(domain.ErrStreamTerminal, "job stream already recorded a terminal event", false)
	}

	stored := event
	nextOffset := cursor.NextOffset
	nextDigest := cursor.StreamDigest
	if event.EventType == domain.JobEventOutputChunk {
		if event.Offset != cursor.NextOffset {
			return domain.NewError(domain.ErrStreamOffsetInvalid, fmt.Sprintf("chunk offset %d does not match expected offset %d", event.Offset, cursor.NextOffset), false)
		}
		hasher := sha256.New()
		if state := s.streamDigests[event.JobID]; len(state) > 0 {
			unmarshaler, ok := hasher.(encoding.BinaryUnmarshaler)
			if !ok {
				return errors.New("memory: sha256 hasher does not support resumable state")
			}
			if err := unmarshaler.UnmarshalBinary(state); err != nil {
				return fmt.Errorf("memory: restore stream digest state: %w", err)
			}
		}
		hasher.Write(event.Chunk)
		computedDigest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
		if event.StreamDigest != "" && event.StreamDigest != computedDigest {
			return domain.NewError(domain.ErrStreamDigestInvalid, "cumulative stream digest does not match the recomputed digest", false)
		}
		marshaler, ok := hasher.(encoding.BinaryMarshaler)
		if !ok {
			return errors.New("memory: sha256 hasher does not support resumable state")
		}
		state, err := marshaler.MarshalBinary()
		if err != nil {
			return fmt.Errorf("memory: persist stream digest state: %w", err)
		}
		nextOffset = event.Offset + uint64(len(event.Chunk))
		nextDigest = computedDigest
		stored.Offset = event.Offset
		stored.StreamDigest = computedDigest
		s.streamDigests[event.JobID] = state
	}
	stored.Chunk = slices.Clone(stored.Chunk)
	s.streamEvents[event.JobID] = append(s.streamEvents[event.JobID], stored)
	s.streamCursors[event.JobID] = domain.JobStreamCursor{
		JobID: event.JobID, NextSequence: event.Sequence + 1, NextOffset: nextOffset,
		StreamDigest: nextDigest, Terminal: cursor.Terminal || event.Terminal,
	}
	return nil
}

func (s *Store) JobStreamEvents(ctx context.Context, jobID string, fromSequence uint64, limit int) ([]domain.JobEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.streamEvents[jobID]
	var out []domain.JobEvent
	for _, e := range events {
		if e.Sequence < fromSequence {
			continue
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Store) JobStreamCursor(ctx context.Context, jobID string) (domain.JobStreamCursor, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cursor, found := s.streamCursors[jobID]
	if !found {
		return domain.JobStreamCursor{JobID: jobID}, false, nil
	}
	return cursor, true, nil
}

var _ store.JobStream = (*Store)(nil)
