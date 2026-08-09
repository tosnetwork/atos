package memory_test

import (
	"context"
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestAppendJobStreamEventOutputChunkReplayIsIdempotent is the regression
// test for a bug where the memory store's replay-identity comparison used
// the *stored* event (already mutated with the recomputed StreamDigest)
// instead of the pristine incoming event, unlike the Postgres store's
// content_hash column (which is computed once on the incoming event).
// Since real callers (the mock and real tos-protocol adapters) always send
// OutputChunk events with an empty StreamDigest and let the store compute
// it, a legitimate identical replay of the exact same upstream event must
// be accepted as a no-op, not rejected as a sequence conflict.
func TestAppendJobStreamEventOutputChunkReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	jobID := "job_replay_idempotent"

	if err := s.AppendJobStreamEvent(ctx, domain.JobEvent{JobID: jobID, Sequence: 0, EventType: domain.JobEventState, State: domain.JobWorking}); err != nil {
		t.Fatal(err)
	}
	chunk := domain.JobEvent{
		JobID: jobID, Sequence: 1, EventType: domain.JobEventOutputChunk,
		State: domain.JobWorking, Chunk: []byte("abc"), Offset: 0, TotalOutputBytes: 3,
		// StreamDigest deliberately empty, matching every real adapter: the
		// store computes and owns the cumulative digest itself.
	}
	if err := s.AppendJobStreamEvent(ctx, chunk); err != nil {
		t.Fatal(err)
	}

	// Replaying the exact same upstream event (still with an empty
	// StreamDigest, exactly as originally sent) must be a silent no-op.
	if err := s.AppendJobStreamEvent(ctx, chunk); err != nil {
		t.Fatalf("idempotent replay of an identical OutputChunk event should not fail, got: %v", err)
	}
	if err := s.AppendJobStreamEvent(ctx, chunk); err != nil {
		t.Fatalf("a third identical replay should still be idempotent, got: %v", err)
	}

	events, err := s.JobStreamEvents(ctx, jobID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d durable events, want exactly 2 (no duplicate rows from the replays)", len(events))
	}
}
