package service_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// newStreamHarness mirrors newHarness but also returns the StreamService and
// the mock tosai.Provider it ingests from.
func newStreamHarness() (harness, *service.StreamService) {
	st := memory.New()
	provider := tosaimock.New()
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)
	streams := service.NewStreamService(st, provider)
	return harness{capabilities: capabilities, quotes: quotes, accounts: accounts, jobs: jobs, st: st}, streams
}

func TestStreamServiceIngestsAndServesCompletedJob(t *testing.T) {
	ctx := context.Background()
	h, streams := newStreamHarness()
	cap := registerCapability(t, h, "agt_stream_provider", "1.00")
	quote := createQuote(t, h, cap.ID)
	result, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_stream_client", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "stream-ingest-1",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Job.State != domain.JobCompleted {
		t.Fatalf("job did not complete: %+v", result.Job)
	}

	events, cursor, err := streams.Events(ctx, result.Job.ID, "prn_stream_client", 0, 0, "", 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if !cursor.Terminal {
		t.Fatalf("cursor is not terminal after streaming a completed job: %+v", cursor)
	}
	if len(events) < 2 {
		t.Fatalf("got %d events, want at least STATE + TERMINAL", len(events))
	}
	if events[0].EventType != domain.JobEventState {
		t.Fatalf("first event = %q, want state", events[0].EventType)
	}
	last := events[len(events)-1]
	if last.EventType != domain.JobEventTerminal || !last.Terminal {
		t.Fatalf("last event = %+v, want terminal", last)
	}

	// A second call must be a pure read: EnsureIngested short-circuits once
	// the cursor is terminal, so re-reading returns the identical journal.
	againEvents, _, err := streams.Events(ctx, result.Job.ID, "prn_stream_client", 0, 0, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(againEvents) != len(events) {
		t.Fatalf("re-reading the stream returned %d events, want %d (re-ingestion should be a no-op)", len(againEvents), len(events))
	}
}

func TestStreamServiceRejectsWrongPrincipal(t *testing.T) {
	ctx := context.Background()
	h, streams := newStreamHarness()
	cap := registerCapability(t, h, "agt_stream_provider_perm", "1.00")
	quote := createQuote(t, h, cap.ID)
	result, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_owner", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "stream-perm-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = streams.Events(ctx, result.Job.ID, "prn_intruder", 0, 0, "", 0)
	if err == nil {
		t.Fatal("expected a permission error for a non-owning principal")
	}
	de, ok := err.(*domain.Error)
	if !ok || de.Code != domain.ErrPermissionDenied {
		t.Fatalf("got %v, want permission_denied", err)
	}
}

func TestStreamServiceRejectsUnknownJob(t *testing.T) {
	ctx := context.Background()
	_, streams := newStreamHarness()
	_, _, err := streams.Events(ctx, "job_does_not_exist", "prn_anyone", 0, 0, "", 0)
	if err == nil {
		t.Fatal("expected a not-found error")
	}
	de, ok := err.(*domain.Error)
	if !ok || de.Code != domain.ErrNotFound {
		t.Fatalf("got %v, want not_found", err)
	}
}

// TestStreamServiceResumeCursorMismatchRejected proves a resume request
// whose claimed offset/digest cannot possibly correspond to the durable
// event immediately before it (here: a STATE event, which carries no output
// progress at all) is rejected rather than silently accepted.
func TestStreamServiceResumeCursorMismatchRejected(t *testing.T) {
	ctx := context.Background()
	h, streams := newStreamHarness()
	cap := registerCapability(t, h, "agt_stream_provider_resume", "1.00")
	quote := createQuote(t, h, cap.ID)
	result, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_resume", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "stream-resume-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Ingest first so sequence 0 (STATE) is durable.
	if _, _, err := streams.Events(ctx, result.Job.ID, "prn_resume", 0, 0, "", 0); err != nil {
		t.Fatal(err)
	}
	_, _, err = streams.Events(ctx, result.Job.ID, "prn_resume", 1, 99999, "sha256:0000000000000000000000000000000000000000000000000000000000000000", 0)
	if err == nil {
		t.Fatal("expected a resume cursor mismatch error")
	}
	de, ok := err.(*domain.Error)
	if !ok || de.Code != domain.ErrStreamCursorMismatch {
		t.Fatalf("got %v, want stream_cursor_mismatch", err)
	}
}

// noopProvider implements tosai.Provider with methods StreamService never
// calls; only the embedded Streamer implementation matters for these tests.
type noopProvider struct{}

func (noopProvider) RegisterProvider(context.Context, string, domain.Capability) error { return nil }
func (noopProvider) GetProviderStatus(context.Context, string) (bool, error)           { return true, nil }
func (noopProvider) SubmitJob(context.Context, tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	return tosai.SubmitJobResult{}, nil
}
func (noopProvider) GetJob(context.Context, string) (tosai.SubmitJobResult, error) {
	return tosai.SubmitJobResult{}, nil
}
func (noopProvider) CancelJob(context.Context, string, string) error { return nil }
func (noopProvider) FetchResult(context.Context, string) (map[string]any, error) {
	return nil, nil
}
func (noopProvider) FetchReceipt(context.Context, string) (*domain.ExecutionReceipt, error) {
	return nil, nil
}

// interruptedThenResumableStreamer simulates a provider whose first
// StreamJobEvents call is cut off mid-transfer (after delivering STATE and
// one chunk) and whose second call must be asked to resume from exactly
// where the first left off -- proving EnsureIngested actually uses the
// provider's next_offset resume capability rather than always re-pulling
// the complete output from scratch.
type interruptedThenResumableStreamer struct {
	noopProvider
	calls    int
	lastReq  tosai.StreamJobEventsRequest
	identity string
}

func (s *interruptedThenResumableStreamer) StreamJobEvents(ctx context.Context, req tosai.StreamJobEventsRequest, onEvent func(domain.JobEvent) error) error {
	s.calls++
	s.lastReq = req
	chunk1, chunk2 := []byte("hello "), []byte("world!")
	sum := sha256.Sum256(append(append([]byte(nil), chunk1...), chunk2...))
	s.identity = "sha256:" + hex.EncodeToString(sum[:])

	if s.calls == 1 {
		if req.NextSequence != 0 || req.NextOffset != 0 || req.ExpectedStreamDigest != "" {
			return domain.NewError(domain.ErrValidationFailed, "expected a fresh-start request on the first call", false)
		}
		if err := onEvent(domain.JobEvent{JobID: req.JobID, Sequence: 0, EventType: domain.JobEventState, State: domain.JobWorking, UpstreamRetainedDigest: s.identity, CreatedAt: time.Now().UTC()}); err != nil {
			return err
		}
		if err := onEvent(domain.JobEvent{JobID: req.JobID, Sequence: 1, EventType: domain.JobEventOutputChunk, State: domain.JobWorking, Chunk: chunk1, Offset: 0, TotalOutputBytes: uint64(len(chunk1) + len(chunk2)), UpstreamRetainedDigest: s.identity, CreatedAt: time.Now().UTC()}); err != nil {
			return err
		}
		return domain.NewError(domain.ErrNetworkUnavailable, "simulated mid-transfer disconnect", true)
	}

	// Second call: must resume, not restart.
	if req.NextSequence != 2 || req.NextOffset != uint64(len(chunk1)) || req.ExpectedStreamDigest != s.identity {
		return domain.NewError(domain.ErrValidationFailed, "second call did not request a resume from the durable cursor", false)
	}
	if err := onEvent(domain.JobEvent{JobID: req.JobID, Sequence: 2, EventType: domain.JobEventOutputChunk, State: domain.JobCompleted, Chunk: chunk2, Offset: uint64(len(chunk1)), TotalOutputBytes: uint64(len(chunk1) + len(chunk2)), UpstreamRetainedDigest: s.identity, CreatedAt: time.Now().UTC()}); err != nil {
		return err
	}
	return onEvent(domain.JobEvent{JobID: req.JobID, Sequence: 3, EventType: domain.JobEventTerminal, State: domain.JobCompleted, Terminal: true, UpstreamRetainedDigest: s.identity, CreatedAt: time.Now().UTC()})
}

func TestStreamServiceEnsureIngestedResumesAfterInterruptionInsteadOfRestarting(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	now := time.Now().UTC()
	jobID := "job_resume_interrupted"
	if err := st.PutJob(ctx, domain.Job{
		ID: jobID, PrincipalID: "prn_resume_interrupted", State: domain.JobCompleted,
		Input: map[string]any{}, Artifacts: []domain.Artifact{}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	streamer := &interruptedThenResumableStreamer{}
	streams := service.NewStreamService(st, streamer)

	// First attempt: the provider disconnects after STATE + one chunk. The
	// error must propagate, not be swallowed.
	if _, _, err := streams.Events(ctx, jobID, "prn_resume_interrupted", 0, 0, "", 0); err == nil {
		t.Fatal("expected the simulated disconnect error to propagate")
	}
	if streamer.calls != 1 {
		t.Fatalf("streamer called %d times, want exactly 1 after the first (failed) attempt", streamer.calls)
	}
	cursor, found, err := st.JobStreamCursor(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || cursor.NextSequence != 2 || cursor.NextOffset != 6 || cursor.Terminal {
		t.Fatalf("unexpected cursor after the interrupted first attempt: %+v found=%v", cursor, found)
	}

	// Second attempt: must resume from exactly where the first left off.
	events, resumedCursor, err := streams.Events(ctx, jobID, "prn_resume_interrupted", 0, 0, "", 0)
	if err != nil {
		t.Fatalf("second attempt should succeed by resuming, got: %v", err)
	}
	if streamer.calls != 2 {
		t.Fatalf("streamer called %d times, want exactly 2", streamer.calls)
	}
	if streamer.lastReq.NextSequence != 2 || streamer.lastReq.NextOffset != 6 || streamer.lastReq.ExpectedStreamDigest == "" {
		t.Fatalf("second call did not carry the resume cursor: %+v", streamer.lastReq)
	}
	if !resumedCursor.Terminal {
		t.Fatalf("cursor is not terminal after the resumed ingestion completed: %+v", resumedCursor)
	}
	if len(events) != 4 {
		t.Fatalf("got %d events after resume, want 4 (STATE + 2 chunks + TERMINAL, no duplicates)", len(events))
	}
	var reassembled []byte
	for _, e := range events {
		if e.EventType == domain.JobEventOutputChunk {
			reassembled = append(reassembled, e.Chunk...)
		}
	}
	if string(reassembled) != "hello world!" {
		t.Fatalf("reassembled output = %q, want %q (no missing/duplicated bytes across the resume boundary)", reassembled, "hello world!")
	}
}
