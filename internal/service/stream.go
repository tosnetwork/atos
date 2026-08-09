package service

import (
	"context"

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
// It always resumes the provider replay from the Job's own start (sequence
// 0): providers execute synchronously today, so a Job's canonical event
// sequence is only ever produced once, in full, and AppendJobStreamEvent's
// idempotent-replay semantics make re-ingesting already-durable events a
// safe no-op. This is what lets EnsureIngested be called freely, from any
// process, on every stream read without an in-process "already ingesting"
// guard.
func (s *StreamService) EnsureIngested(ctx context.Context, jobID string) error {
	cursor, found, err := s.store.JobStreamCursor(ctx, jobID)
	if err != nil {
		return err
	}
	if found && cursor.Terminal {
		return nil
	}
	streamer, ok := s.provider.(tosai.Streamer)
	if !ok {
		return nil
	}
	err = streamer.StreamJobEvents(ctx, tosai.StreamJobEventsRequest{JobID: jobID}, func(event domain.JobEvent) error {
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
	if fromSequence > 0 && (expectedOffset != 0 || expectedDigest != "") {
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
// does not match the durable event immediately before fromSequence. Without
// this, a caller could splice a fabricated cursor onto a real job_id and
// receive events without the server ever detecting the substitution.
func (s *StreamService) validateResumeCursor(ctx context.Context, jobID string, fromSequence, expectedOffset uint64, expectedDigest string) error {
	prior, err := s.store.JobStreamEvents(ctx, jobID, fromSequence-1, 1)
	if err != nil {
		return err
	}
	if len(prior) == 0 || prior[0].Sequence != fromSequence-1 {
		return domain.NewError(domain.ErrStreamCursorMismatch, "resume cursor does not match a durable event", false)
	}
	event := prior[0]
	if event.EventType != domain.JobEventOutputChunk {
		if expectedOffset != 0 || expectedDigest != "" {
			return domain.NewError(domain.ErrStreamCursorMismatch, "resume cursor claims output progress the durable stream does not have", false)
		}
		return nil
	}
	actualOffset := event.Offset + uint64(len(event.Chunk))
	if expectedOffset != 0 && expectedOffset != actualOffset {
		return domain.NewError(domain.ErrStreamCursorMismatch, "resume offset does not match durable stream state", false)
	}
	if expectedDigest != "" && expectedDigest != event.StreamDigest {
		return domain.NewError(domain.ErrStreamCursorMismatch, "resume digest does not match durable stream state", false)
	}
	return nil
}
