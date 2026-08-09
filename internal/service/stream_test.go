package service_test

import (
	"context"
	"testing"

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
