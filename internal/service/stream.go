package service

import (
	"context"
	"fmt"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// StreamService serves a Job's durable, resumable event journal. The
// journal is a materialized, append-only cache of the execution provider's
// canonical event sequence: EnsureIngested pulls from the provider (real
// tos-protocol StreamJob RPC, or an equivalent local replay for the mock
// backend) into the store, and Events reads back the durable result. It
// never mutates Job/Account/Escrow state — reading or resuming a stream
// leaves Phase 0/1 economic semantics untouched.
type StreamService struct {
	store    store.Store
	provider tosai.Provider
}

func NewStreamService(s store.Store, provider tosai.Provider) *StreamService {
	return &StreamService{store: s, provider: provider}
}

// EnsureIngested pulls the provider's canonical event sequence into the
// durable journal unless it is already fully ingested (cursor.Terminal).
//
// If nothing has been durably ingested for this Job yet, it always pulls at
// least the first snapshot, regardless of the Job's current state. After
// that first snapshot, while the Job's own ATOS-level record has not yet
// reached a terminal state, EnsureIngested is a no-op: re-pulling from a
// still-running provider would only ever re-observe the same non-terminal
// STATE snapshot, and every SSE poll calling EnsureIngested would otherwise
// append a redundant STATE event to the journal for no benefit. Once the
// Job becomes terminal, the next call resumes the provider pull at the
// durable cursor rather than restarting.
//
// The resume request only carries next_offset/expected_stream_digest once
// the durable cursor has actually advanced past at least one OutputChunk
// event (cursor.NextOffset > 0). This is deliberate, not an optimization
// detail: a provider's retained-output identity digest is only stable once
// real output exists. A Job observed only in a non-terminal STATE has no
// output yet, so persisting or replaying that early digest would make the
// Job's own later, legitimate transition to completed look like content
// substitution -- which is exactly why UpstreamRetainedDigest is captured
// (see the onEvent callback below) only from OutputChunk events, never from
// STATE.
//
// Either way, AppendJobStreamEvent's idempotent-replay semantics make
// re-ingesting any already-durable event a safe no-op, which is what lets
// EnsureIngested be called freely, from any process, without an in-process
// "already ingesting" guard.
func (s *StreamService) EnsureIngested(ctx context.Context, jobID string) error {
	cursor, found, err := s.store.JobStreamCursor(ctx, jobID)
	if err != nil {
		return err
	}
	if found && cursor.Terminal {
		return nil
	}
	if found {
		job, err := s.store.GetJob(ctx, jobID)
		if err != nil {
			return err
		}
		if !job.State.Terminal() {
			return nil
		}
	}
	streamer, ok := s.provider.(tosai.Streamer)
	if !ok {
		return nil
	}
	req := tosai.StreamJobEventsRequest{JobID: jobID}
	if found {
		req.NextSequence = cursor.NextSequence
		if cursor.NextOffset > 0 {
			req.NextOffset = cursor.NextOffset
			req.ExpectedStreamDigest = cursor.UpstreamDigest
		}
	}
	err = streamer.StreamJobEvents(ctx, req, func(event domain.JobEvent) error {
		if event.JobID != jobID {
			return domain.NewError(domain.ErrStreamJobBindingMismatch, fmt.Sprintf("execution provider returned an event bound to job %q while streaming job %q", event.JobID, jobID), false)
		}
		if event.EventType == domain.JobEventOutputChunk && event.UpstreamRetainedDigest != "" {
			if err := s.store.SetJobStreamUpstreamDigest(ctx, jobID, event.UpstreamRetainedDigest); err != nil {
				return err
			}
		}
		return s.store.AppendJobStreamEvent(ctx, event)
	})
	if err != nil {
		if de, ok := err.(*domain.Error); ok && de.Code == domain.ErrNotFound {
			// The provider has not accepted/started this Job yet; there is
			// nothing to ingest. The caller can retry once it has.
			return nil
		}
		return err
	}
	return nil
}

// Events authorizes the caller against the owning Job, ensures the journal
// reflects the provider's current knowledge, validates any client-claimed
// resume position against durable state, and returns events from
// fromSequence onward together with the current cursor.
func (s *StreamService) Events(
	ctx context.Context, jobID, principalID string,
	fromSequence, expectedOffset uint64, expectedDigest string, limit int,
) ([]domain.JobEvent, domain.JobStreamCursor, error) {
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, domain.JobStreamCursor{}, domain.NewError(domain.ErrNotFound, "job not found", false)
		}
		return nil, domain.JobStreamCursor{}, err
	}
	if job.PrincipalID != principalID {
		return nil, domain.JobStreamCursor{}, domain.NewError(domain.ErrPermissionDenied, "not the job's owning principal", false)
	}
	if err := s.EnsureIngested(ctx, jobID); err != nil {
		return nil, domain.JobStreamCursor{}, err
	}
	// Any resume past sequence 0 MUST prove it knows the exact durable
	// cumulative state it claims to be resuming from -- including the
	// (common) case where that state is legitimately offset 0 / no digest
	// yet. There is no "only validate if a non-zero value was supplied"
	// escape hatch: omitting next_offset/expected_stream_digest is not a
	// way to skip the check, it is simply a claim of offset=0/digest="".
	if fromSequence > 0 {
		if err := s.validateResumeCursor(ctx, jobID, fromSequence, expectedOffset, expectedDigest); err != nil {
			return nil, domain.JobStreamCursor{}, err
		}
	}
	events, err := s.store.JobStreamEvents(ctx, jobID, fromSequence, limit)
	if err != nil {
		return nil, domain.JobStreamCursor{}, err
	}
	cursor, _, err := s.store.JobStreamCursor(ctx, jobID)
	if err != nil {
		return nil, domain.JobStreamCursor{}, err
	}
	return events, cursor, nil
}

// validateResumeCursor rejects a resume request whose claimed offset/digest
// does not match the journal's actual cumulative state as of fromSequence.
// Cumulative offset/digest state only changes on OutputChunk events, so the
// "actual" state is derived from the most recent OutputChunk event before
// fromSequence (LastJobStreamChunkBefore), not from whichever event type
// happens to sit immediately before it -- a PROOF_STATUS or STATE event in
// between does not reset or invalidate the cumulative output progress
// already durably recorded. fromSequence is also bounded by the durable
// cursor: a caller cannot claim to resume from a point the journal has
// never reached.
func (s *StreamService) validateResumeCursor(ctx context.Context, jobID string, fromSequence, expectedOffset uint64, expectedDigest string) error {
	cursor, found, err := s.store.JobStreamCursor(ctx, jobID)
	if err != nil {
		return err
	}
	if !found || fromSequence > cursor.NextSequence {
		return domain.NewError(domain.ErrStreamCursorMismatch, "resume cursor is ahead of the durable stream", false)
	}
	last, chunkFound, err := s.store.LastJobStreamChunkBefore(ctx, jobID, fromSequence)
	if err != nil {
		return err
	}
	var actualOffset uint64
	var actualDigest string
	if chunkFound {
		actualOffset = last.Offset + uint64(len(last.Chunk))
		actualDigest = last.StreamDigest
	}
	if expectedOffset != actualOffset {
		return domain.NewError(domain.ErrStreamCursorMismatch, "resume offset does not match durable stream state", false)
	}
	if expectedDigest != actualDigest {
		return domain.NewError(domain.ErrStreamCursorMismatch, "resume digest does not match durable stream state", false)
	}
	return nil
}
