package postgres_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

// streamDigest returns the cumulative sha256 digest of prior+chunk, matching
// what AppendJobStreamEvent independently recomputes for OutputChunk events.
func streamDigest(prior []byte, chunk []byte) (string, []byte) {
	h := sha256.New()
	h.Write(prior)
	h.Write(chunk)
	sum := h.Sum(nil)
	return "sha256:" + hex.EncodeToString(sum), append(append([]byte(nil), prior...), chunk...)
}

func stateEvent(jobID string, seq uint64) domain.JobEvent {
	return domain.JobEvent{JobID: jobID, Sequence: seq, EventType: domain.JobEventState, State: domain.JobWorking}
}

func chunkEvent(jobID string, seq, offset uint64, chunk []byte, digest string) domain.JobEvent {
	return domain.JobEvent{
		JobID: jobID, Sequence: seq, EventType: domain.JobEventOutputChunk,
		State: domain.JobWorking, Chunk: chunk, Offset: offset,
		TotalOutputBytes: offset + uint64(len(chunk)), StreamDigest: digest,
	}
}

func terminalEvent(jobID string, seq uint64) domain.JobEvent {
	return domain.JobEvent{JobID: jobID, Sequence: seq, EventType: domain.JobEventTerminal, State: domain.JobCompleted, Terminal: true}
}

func domainErrCode(t *testing.T, err error) domain.ErrorCode {
	t.Helper()
	de, ok := err.(*domain.Error)
	if !ok {
		t.Fatalf("expected *domain.Error, got %T: %v", err, err)
	}
	return de.Code
}

func TestAppendJobStreamEventHappyPathAndTerminal(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_happy_" + randSuffix()

	if err := s.AppendJobStreamEvent(ctx, stateEvent(jobID, 0)); err != nil {
		t.Fatal(err)
	}
	digest1, cum1 := streamDigest(nil, []byte("hello "))
	if err := s.AppendJobStreamEvent(ctx, chunkEvent(jobID, 1, 0, []byte("hello "), digest1)); err != nil {
		t.Fatal(err)
	}
	digest2, _ := streamDigest(cum1, []byte("world"))
	if err := s.AppendJobStreamEvent(ctx, chunkEvent(jobID, 2, 6, []byte("world"), digest2)); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendJobStreamEvent(ctx, terminalEvent(jobID, 3)); err != nil {
		t.Fatal(err)
	}

	events, err := s.JobStreamEvents(ctx, jobID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 {
		t.Fatalf("got %d events, want 4", len(events))
	}
	if events[1].Offset != 0 || string(events[1].Chunk) != "hello " {
		t.Fatalf("chunk 1 stored wrong: offset=%d chunk=%q", events[1].Offset, events[1].Chunk)
	}
	if events[2].Offset != 6 || string(events[2].Chunk) != "world" {
		t.Fatalf("chunk 2 stored wrong: offset=%d chunk=%q", events[2].Offset, events[2].Chunk)
	}

	cursor, found, err := s.JobStreamCursor(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || !cursor.Terminal || cursor.NextSequence != 4 || cursor.NextOffset != 11 || cursor.StreamDigest != digest2 {
		t.Fatalf("unexpected cursor: %+v found=%v", cursor, found)
	}
}

// TestAppendJobStreamEventDuplicateIsIdempotent proves re-appending the exact
// same event at a sequence already durable is a silent no-op, not an error
// and not a duplicate row.
func TestAppendJobStreamEventDuplicateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_dup_" + randSuffix()

	event := stateEvent(jobID, 0)
	if err := s.AppendJobStreamEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendJobStreamEvent(ctx, event); err != nil {
		t.Fatalf("duplicate identical append should be idempotent, got: %v", err)
	}
	if err := s.AppendJobStreamEvent(ctx, event); err != nil {
		t.Fatalf("third duplicate identical append should still be idempotent, got: %v", err)
	}
	events, err := s.JobStreamEvents(ctx, jobID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want exactly 1 (no duplicate rows)", len(events))
	}
}

// TestAppendJobStreamEventSequenceSubstitutionRejected proves a second,
// different event claiming an already-durable sequence number is rejected
// rather than silently overwriting history.
func TestAppendJobStreamEventSequenceSubstitutionRejected(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_seqsub_" + randSuffix()

	original := stateEvent(jobID, 0)
	if err := s.AppendJobStreamEvent(ctx, original); err != nil {
		t.Fatal(err)
	}
	substituted := stateEvent(jobID, 0)
	substituted.State = domain.JobFailed // different content, same sequence
	err := s.AppendJobStreamEvent(ctx, substituted)
	if err == nil {
		t.Fatal("expected an error for sequence substitution, got nil")
	}
	if code := domainErrCode(t, err); code != domain.ErrStreamSequenceConflict {
		t.Fatalf("got error code %q, want %q", code, domain.ErrStreamSequenceConflict)
	}
	events, err := s.JobStreamEvents(ctx, jobID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].State != domain.JobWorking {
		t.Fatalf("original event must remain unmodified, got %+v", events)
	}
}

// TestAppendJobStreamEventOffsetSubstitutionRejected proves a chunk claiming
// a wrong starting offset (not contiguous with what's already durable) is
// rejected instead of corrupting the byte-position invariant.
func TestAppendJobStreamEventOffsetSubstitutionRejected(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_offsub_" + randSuffix()

	if err := s.AppendJobStreamEvent(ctx, stateEvent(jobID, 0)); err != nil {
		t.Fatal(err)
	}
	digest, _ := streamDigest(nil, []byte("abc"))
	// Correct next offset is 0; claim 100 instead.
	bad := chunkEvent(jobID, 1, 100, []byte("abc"), digest)
	err := s.AppendJobStreamEvent(ctx, bad)
	if err == nil {
		t.Fatal("expected an error for offset substitution, got nil")
	}
	if code := domainErrCode(t, err); code != domain.ErrStreamOffsetInvalid {
		t.Fatalf("got error code %q, want %q", code, domain.ErrStreamOffsetInvalid)
	}
}

// TestAppendJobStreamEventDigestSubstitutionRejected proves a chunk with the
// correct offset but a fabricated cumulative digest is rejected: ATOS
// independently recomputes the digest rather than trusting the caller.
func TestAppendJobStreamEventDigestSubstitutionRejected(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_digestsub_" + randSuffix()

	if err := s.AppendJobStreamEvent(ctx, stateEvent(jobID, 0)); err != nil {
		t.Fatal(err)
	}
	bad := chunkEvent(jobID, 1, 0, []byte("abc"), "sha256:"+hex.EncodeToString(make([]byte, 32)))
	err := s.AppendJobStreamEvent(ctx, bad)
	if err == nil {
		t.Fatal("expected an error for digest substitution, got nil")
	}
	if code := domainErrCode(t, err); code != domain.ErrStreamDigestInvalid {
		t.Fatalf("got error code %q, want %q", code, domain.ErrStreamDigestInvalid)
	}
}

// TestAppendJobStreamEventOversizedChunkRejected proves a chunk larger than
// the configured bound is rejected before ever reaching the database.
func TestAppendJobStreamEventOversizedChunkRejected(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_oversized_" + randSuffix()

	oversized := make([]byte, 256<<10+1)
	digest, _ := streamDigest(nil, oversized)
	err := s.AppendJobStreamEvent(ctx, chunkEvent(jobID, 0, 0, oversized, digest))
	if err == nil {
		t.Fatal("expected an error for oversized chunk, got nil")
	}
	if code := domainErrCode(t, err); code != domain.ErrStreamChunkTooLarge {
		t.Fatalf("got error code %q, want %q", code, domain.ErrStreamChunkTooLarge)
	}
}

// TestAppendJobStreamEventRejectsAfterTerminal proves no event, of any kind,
// is accepted once a terminal event has been durably recorded.
func TestAppendJobStreamEventRejectsAfterTerminal(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_afterterm_" + randSuffix()

	if err := s.AppendJobStreamEvent(ctx, stateEvent(jobID, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendJobStreamEvent(ctx, terminalEvent(jobID, 1)); err != nil {
		t.Fatal(err)
	}
	err := s.AppendJobStreamEvent(ctx, stateEvent(jobID, 2))
	if err == nil {
		t.Fatal("expected an error appending after terminal, got nil")
	}
	if code := domainErrCode(t, err); code != domain.ErrStreamTerminal {
		t.Fatalf("got error code %q, want %q", code, domain.ErrStreamTerminal)
	}

	// Replaying the already-durable terminal event itself must still be a
	// harmless idempotent no-op, not an error.
	if err := s.AppendJobStreamEvent(ctx, terminalEvent(jobID, 1)); err != nil {
		t.Fatalf("idempotent replay of the durable terminal event should not fail, got: %v", err)
	}
}

// TestAppendJobStreamEventGappedSequenceRejected proves an out-of-order
// append (skipping ahead of the next expected sequence) is rejected.
func TestAppendJobStreamEventGappedSequenceRejected(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_gap_" + randSuffix()

	err := s.AppendJobStreamEvent(ctx, stateEvent(jobID, 5))
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("got %v, want store.ErrConflict", err)
	}
}

// TestAppendJobStreamEventProcessRestartResumesCursor proves the durable
// cursor survives a simulated ATOS process restart: opening a brand-new
// *postgres.Store against the same database sees exactly the state left by
// the previous process, with no in-memory carryover required.
func TestAppendJobStreamEventProcessRestartResumesCursor(t *testing.T) {
	ctx := context.Background()
	jobID := "job_stream_restart_" + randSuffix()

	first := openTestStore(t)
	if err := first.AppendJobStreamEvent(ctx, stateEvent(jobID, 0)); err != nil {
		t.Fatal(err)
	}
	digest1, cum1 := streamDigest(nil, []byte("part-1"))
	if err := first.AppendJobStreamEvent(ctx, chunkEvent(jobID, 1, 0, []byte("part-1"), digest1)); err != nil {
		t.Fatal(err)
	}

	// Simulate a process restart: a second, independent Store connection
	// (as cmd/api/main.go would create on a fresh process start) must see
	// exactly what the first process durably committed.
	second := openTestStore(t)
	cursor, found, err := second.JobStreamCursor(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || cursor.NextSequence != 2 || cursor.NextOffset != 6 || cursor.StreamDigest != digest1 {
		t.Fatalf("restarted process saw wrong cursor: %+v found=%v", cursor, found)
	}

	// Resume from the restarted process using the recovered cursor.
	digest2, _ := streamDigest(cum1, []byte("-part-2"))
	if err := second.AppendJobStreamEvent(ctx, chunkEvent(jobID, 2, 6, []byte("-part-2"), digest2)); err != nil {
		t.Fatal(err)
	}
	if err := second.AppendJobStreamEvent(ctx, terminalEvent(jobID, 3)); err != nil {
		t.Fatal(err)
	}

	events, err := first.JobStreamEvents(ctx, jobID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 {
		t.Fatalf("got %d events after cross-process resume, want 4", len(events))
	}
}

// TestAppendJobStreamEventConcurrentIngestersConverge proves that multiple
// ATOS processes (or goroutines using independent Store handles) racing to
// ingest and append the very same upstream event sequence for one Job
// converge on exactly one durable journal, with no duplicate rows and no
// corrupted cursor -- the PostgreSQL advisory transaction lock, not any
// process-local mutex, is what makes this safe.
func TestAppendJobStreamEventConcurrentIngestersConverge(t *testing.T) {
	ctx := context.Background()
	jobID := "job_stream_concurrent_" + randSuffix()

	const workers = 8
	const chunks = 5

	events := make([]domain.JobEvent, 0, chunks+2)
	events = append(events, stateEvent(jobID, 0))
	cumulative := []byte(nil)
	seq := uint64(1)
	offset := uint64(0)
	for i := 0; i < chunks; i++ {
		chunk := []byte{byte('a' + i)}
		var digest string
		digest, cumulative = streamDigest(cumulative, chunk)
		events = append(events, chunkEvent(jobID, seq, offset, chunk, digest))
		seq++
		offset++
	}
	events = append(events, terminalEvent(jobID, seq))

	// Each "replica" is an independent Store handle, standing in for a
	// separate ATOS process/connection pool racing to ingest the same Job.
	// They are opened up front on the test goroutine: testing.T methods are
	// not safe to call from the worker goroutines below.
	replicas := make([]*postgres.Store, workers)
	for w := range replicas {
		replicas[w] = openTestStore(t)
	}

	var wg sync.WaitGroup
	errsCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(replica *postgres.Store) {
			defer wg.Done()
			for _, event := range events {
				if err := replica.AppendJobStreamEvent(ctx, event); err != nil {
					de, ok := err.(*domain.Error)
					if ok && de.Code == domain.ErrStreamTerminal {
						// Another replica already finished ingesting; this
						// replica's remaining (already-durable) events lose
						// the race for the terminal check but the earlier
						// ones in this call must all have succeeded or been
						// idempotent no-ops.
						continue
					}
					errsCh <- err
					return
				}
			}
		}(replicas[w])
	}
	wg.Wait()
	close(errsCh)
	for err := range errsCh {
		t.Fatalf("concurrent ingestion produced an unexpected error: %v", err)
	}

	s := openTestStore(t)
	got, err := s.JobStreamEvents(ctx, jobID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(events) {
		t.Fatalf("got %d durable events, want exactly %d (no duplicates, no gaps)", len(got), len(events))
	}
	for i, e := range got {
		if e.Sequence != uint64(i) {
			t.Fatalf("event %d has sequence %d, journal is not contiguous", i, e.Sequence)
		}
	}
	cursor, found, err := s.JobStreamCursor(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || !cursor.Terminal || cursor.NextSequence != uint64(len(events)) {
		t.Fatalf("unexpected final cursor after concurrent ingestion: %+v found=%v", cursor, found)
	}
}

// TestAppendJobStreamEventRollbackLeavesNoPartialRow proves a rejected
// append (here: digest substitution) commits nothing at all -- neither the
// event row nor a cursor advance -- so a caller's retry with the correct
// event lands cleanly instead of colliding with a half-applied attempt.
func TestAppendJobStreamEventRollbackLeavesNoPartialRow(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_rollback_" + randSuffix()

	if err := s.AppendJobStreamEvent(ctx, stateEvent(jobID, 0)); err != nil {
		t.Fatal(err)
	}
	bad := chunkEvent(jobID, 1, 0, []byte("abc"), "sha256:"+hex.EncodeToString(make([]byte, 32)))
	if err := s.AppendJobStreamEvent(ctx, bad); err == nil {
		t.Fatal("expected the corrupt append to fail")
	}

	cursor, found, err := s.JobStreamCursor(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || cursor.NextSequence != 1 || cursor.NextOffset != 0 {
		t.Fatalf("failed append must not advance the cursor, got %+v", cursor)
	}

	// Retry with the correct digest now succeeds cleanly.
	digest, _ := streamDigest(nil, []byte("abc"))
	if err := s.AppendJobStreamEvent(ctx, chunkEvent(jobID, 1, 0, []byte("abc"), digest)); err != nil {
		t.Fatalf("retry with correct content should succeed, got: %v", err)
	}
	events, err := s.JobStreamEvents(ctx, jobID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events after retry, want 2 (no leftover partial row from the rejected attempt)", len(events))
	}
}

// TestSetJobStreamUpstreamDigestBeforeAnyEvent proves the upstream identity
// digest can be recorded before the cursor row exists at all, and that a
// subsequent AppendJobStreamEvent's ON CONFLICT upsert leaves it intact.
func TestSetJobStreamUpstreamDigestBeforeAnyEvent(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_digest_first_" + randSuffix()
	digest := "sha256:" + hex.EncodeToString(make([]byte, 32))

	if err := s.SetJobStreamUpstreamDigest(ctx, jobID, digest); err != nil {
		t.Fatal(err)
	}
	cursor, found, err := s.JobStreamCursor(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || cursor.UpstreamDigest != digest || cursor.NextSequence != 0 {
		t.Fatalf("unexpected cursor after setting the digest before any event: %+v found=%v", cursor, found)
	}

	if err := s.AppendJobStreamEvent(ctx, stateEvent(jobID, 0)); err != nil {
		t.Fatal(err)
	}
	cursor, _, err = s.JobStreamCursor(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.UpstreamDigest != digest || cursor.NextSequence != 1 {
		t.Fatalf("AppendJobStreamEvent's cursor upsert must not clobber upstream_digest, got %+v", cursor)
	}
}

// TestSetJobStreamUpstreamDigestIsIdempotentButRejectsChange proves the
// digest is a set-once value: repeating the same value is a harmless no-op,
// but a different value for the same Job -- which should never legitimately
// happen, since a Job's completed output is immutable -- is rejected as a
// provider-consistency error rather than silently overwritten.
func TestSetJobStreamUpstreamDigestIsIdempotentButRejectsChange(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_digest_once_" + randSuffix()
	digest := "sha256:" + hex.EncodeToString(make([]byte, 32))

	if err := s.SetJobStreamUpstreamDigest(ctx, jobID, digest); err != nil {
		t.Fatal(err)
	}
	if err := s.SetJobStreamUpstreamDigest(ctx, jobID, digest); err != nil {
		t.Fatalf("repeating the same digest should be idempotent, got: %v", err)
	}

	other := "sha256:" + hex.EncodeToString(bytes.Repeat([]byte{1}, 32))
	err := s.SetJobStreamUpstreamDigest(ctx, jobID, other)
	if err == nil {
		t.Fatal("expected an error when the observed digest changes for an existing job stream")
	}
	de, ok := err.(*domain.Error)
	if !ok || de.Code != domain.ErrProviderFailed {
		t.Fatalf("got %v, want provider_failed", err)
	}

	cursor, _, err := s.JobStreamCursor(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.UpstreamDigest != digest {
		t.Fatalf("rejected change must not have overwritten the original digest, got %q", cursor.UpstreamDigest)
	}
}

// TestLastJobStreamChunkBeforeSkipsIntermediateNonChunkEvents proves the
// cumulative offset/digest lookup finds the most recent OutputChunk event
// before a given sequence even when a non-chunk event (e.g. PROOF_STATUS)
// sits immediately before that sequence -- cumulative state is a property
// of the whole stream, not of whichever event type happens to be adjacent.
func TestLastJobStreamChunkBeforeSkipsIntermediateNonChunkEvents(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_stream_lastchunk_" + randSuffix()

	if err := s.AppendJobStreamEvent(ctx, stateEvent(jobID, 0)); err != nil {
		t.Fatal(err)
	}
	digest, _ := streamDigest(nil, []byte("abc"))
	if err := s.AppendJobStreamEvent(ctx, chunkEvent(jobID, 1, 0, []byte("abc"), digest)); err != nil {
		t.Fatal(err)
	}
	// A PROOF_STATUS event between the chunk and the resume point: it must
	// not reset or hide the cumulative chunk state.
	if err := s.AppendJobStreamEvent(ctx, domain.JobEvent{JobID: jobID, Sequence: 2, EventType: domain.JobEventProofStatus, State: domain.JobWorking}); err != nil {
		t.Fatal(err)
	}

	event, found, err := s.LastJobStreamChunkBefore(ctx, jobID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected to find the chunk event before sequence 3")
	}
	if event.Sequence != 1 || event.Offset != 0 || string(event.Chunk) != "abc" || event.StreamDigest != digest {
		t.Fatalf("unexpected chunk event: %+v", event)
	}

	// Before any chunk exists at all (sequence 1), nothing is found.
	_, found, err = s.LastJobStreamChunkBefore(ctx, jobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected no chunk event before sequence 1")
	}
}
