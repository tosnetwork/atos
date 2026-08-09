package mock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/domain"
)

func TestStreamJobEventsSynthesizesStateChunksTerminal(t *testing.T) {
	ctx := context.Background()
	p := New()
	result, err := p.SubmitJob(ctx, tosai.SubmitJobRequest{
		JobID: "job_mock_stream_1", QuoteID: "q1", ServiceQuoteID: "sq1",
		EscrowID: "e1", PrincipalID: "prn1", CapabilityID: "cap1",
		CapabilityVersion: "1", ProviderID: "agt1", TrustMode: domain.TrustModeManaged,
		Input: map[string]any{"hello": "world"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.State != domain.JobCompleted {
		t.Fatalf("mock SubmitJob state = %q, want completed", result.State)
	}

	var events []domain.JobEvent
	err = p.StreamJobEvents(ctx, tosai.StreamJobEventsRequest{JobID: "job_mock_stream_1"}, func(e domain.JobEvent) error {
		events = append(events, e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 3 {
		t.Fatalf("got %d events, want at least STATE + chunk + TERMINAL", len(events))
	}
	if events[0].EventType != domain.JobEventState {
		t.Fatalf("first event = %q, want state", events[0].EventType)
	}
	last := events[len(events)-1]
	if last.EventType != domain.JobEventTerminal || !last.Terminal {
		t.Fatalf("last event = %+v, want terminal", last)
	}

	// Sequences must be contiguous starting at 0.
	for i, e := range events {
		if e.Sequence != uint64(i) {
			t.Fatalf("event %d has sequence %d, want %d", i, e.Sequence, i)
		}
	}

	// Every event -- STATE, each chunk, and TERMINAL alike -- must carry the
	// same UpstreamRetainedDigest: the real tos-protocol server's own
	// digestMessage(stored.Output) identity check, matching mock's real-RPC
	// counterpart exactly (see internal/adapters/tosprotocol/stream.go).
	outputBytes, err := json.Marshal(result.Output)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(outputBytes)
	wantIdentity := "sha256:" + hex.EncodeToString(sum[:])
	for _, e := range events {
		if e.UpstreamRetainedDigest != wantIdentity {
			t.Fatalf("event %+v has UpstreamRetainedDigest %q, want %q", e, e.UpstreamRetainedDigest, wantIdentity)
		}
		if e.StreamDigest != "" {
			t.Fatalf("event %+v unexpectedly set StreamDigest -- mock must not claim a trustworthy per-chunk cumulative digest, matching the real adapter", e)
		}
	}
}

// TestStreamJobEventsHonorsNextOffset proves a resumed pull (NextOffset > 0)
// re-sends the mandatory STATE event but skips already-delivered output
// bytes, matching tos-protocol's real StreamJob resume contract exactly.
func TestStreamJobEventsHonorsNextOffset(t *testing.T) {
	ctx := context.Background()
	p := New()
	// Large enough input (mock echoes it back verbatim) that the JSON output
	// spans multiple 256KB chunks, so resuming after the first chunk still
	// leaves further chunk data to verify.
	if _, err := p.SubmitJob(ctx, tosai.SubmitJobRequest{
		JobID: "job_mock_stream_2", QuoteID: "q1", ServiceQuoteID: "sq1",
		EscrowID: "e1", PrincipalID: "prn1", CapabilityID: "cap1",
		CapabilityVersion: "1", ProviderID: "agt1", TrustMode: domain.TrustModeManaged,
		Input: map[string]any{"payload": strings.Repeat("x", 300<<10)},
	}); err != nil {
		t.Fatal(err)
	}

	var all []domain.JobEvent
	if err := p.StreamJobEvents(ctx, tosai.StreamJobEventsRequest{JobID: "job_mock_stream_2"}, func(e domain.JobEvent) error {
		all = append(all, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(all) < 3 {
		t.Fatalf("need at least STATE + chunk + TERMINAL for this test, got %d", len(all))
	}
	firstChunk := all[1]
	identityDigest := all[0].UpstreamRetainedDigest
	resumeOffset := firstChunk.Offset + uint64(len(firstChunk.Chunk))

	var resumed []domain.JobEvent
	err := p.StreamJobEvents(ctx, tosai.StreamJobEventsRequest{
		JobID: "job_mock_stream_2", NextSequence: 5,
		NextOffset: resumeOffset, ExpectedStreamDigest: identityDigest,
	}, func(e domain.JobEvent) error {
		resumed = append(resumed, e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// STATE is always re-sent first, labeled at NextSequence; then only the
	// chunks starting at resumeOffset, then TERMINAL.
	if len(resumed) != len(all)-1 {
		t.Fatalf("resumed got %d events, want %d (STATE + everything from the second chunk onward)", len(resumed), len(all)-1)
	}
	if resumed[0].EventType != domain.JobEventState || resumed[0].Sequence != 5 {
		t.Fatalf("resumed first event = %+v, want STATE at sequence 5", resumed[0])
	}
	if resumed[1].EventType != domain.JobEventOutputChunk || resumed[1].Offset != resumeOffset {
		t.Fatalf("resumed second event = %+v, want the chunk starting at offset %d", resumed[1], resumeOffset)
	}
}

func TestStreamJobEventsRejectsOffsetBeyondRetainedOutput(t *testing.T) {
	ctx := context.Background()
	p := New()
	if _, err := p.SubmitJob(ctx, tosai.SubmitJobRequest{
		JobID: "job_mock_stream_3", QuoteID: "q1", ServiceQuoteID: "sq1",
		EscrowID: "e1", PrincipalID: "prn1", CapabilityID: "cap1",
		CapabilityVersion: "1", ProviderID: "agt1", TrustMode: domain.TrustModeManaged,
		Input: map[string]any{"n": 1},
	}); err != nil {
		t.Fatal(err)
	}
	err := p.StreamJobEvents(ctx, tosai.StreamJobEventsRequest{JobID: "job_mock_stream_3", NextOffset: 1 << 30}, func(domain.JobEvent) error { return nil })
	if err == nil {
		t.Fatal("expected an error for next_offset beyond retained output")
	}
	de, ok := err.(*domain.Error)
	if !ok || de.Code != domain.ErrStreamCursorMismatch {
		t.Fatalf("got %v, want stream_cursor_mismatch", err)
	}
}

func TestStreamJobEventsRejectsMismatchedResumeDigest(t *testing.T) {
	ctx := context.Background()
	p := New()
	if _, err := p.SubmitJob(ctx, tosai.SubmitJobRequest{
		JobID: "job_mock_stream_4", QuoteID: "q1", ServiceQuoteID: "sq1",
		EscrowID: "e1", PrincipalID: "prn1", CapabilityID: "cap1",
		CapabilityVersion: "1", ProviderID: "agt1", TrustMode: domain.TrustModeManaged,
		Input: map[string]any{"n": 1},
	}); err != nil {
		t.Fatal(err)
	}
	cases := []tosai.StreamJobEventsRequest{
		{JobID: "job_mock_stream_4", NextOffset: 1},
		{JobID: "job_mock_stream_4", NextOffset: 1, ExpectedStreamDigest: "sha256:" + hex.EncodeToString(make([]byte, 32))},
	}
	for i, req := range cases {
		err := p.StreamJobEvents(ctx, req, func(domain.JobEvent) error { return nil })
		if err == nil {
			t.Fatalf("case %d: expected an error", i)
		}
		de, ok := err.(*domain.Error)
		if !ok || de.Code != domain.ErrStreamCursorMismatch {
			t.Fatalf("case %d: got %v, want stream_cursor_mismatch", i, err)
		}
	}
}

func TestStreamJobEventsUnknownJobFails(t *testing.T) {
	p := New()
	err := p.StreamJobEvents(context.Background(), tosai.StreamJobEventsRequest{JobID: "does_not_exist"}, func(domain.JobEvent) error { return nil })
	if err == nil {
		t.Fatal("expected an error for an unknown job")
	}
	de, ok := err.(*domain.Error)
	if !ok || de.Code != domain.ErrNotFound {
		t.Fatalf("got %v, want not_found", err)
	}
}
