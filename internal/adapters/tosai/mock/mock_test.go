package mock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	// Reassembled output chunks, hashed cumulatively, must match each
	// event's own reported cumulative digest (proves offset/digest fields
	// are self-consistent, not just individually well-formed).
	hasher := sha256.New()
	for _, e := range events {
		if e.EventType != domain.JobEventOutputChunk {
			continue
		}
		hasher.Write(e.Chunk)
		want := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
		if e.StreamDigest != want {
			t.Fatalf("chunk at sequence %d has digest %q, want %q", e.Sequence, e.StreamDigest, want)
		}
	}
}

func TestStreamJobEventsHonorsNextSequence(t *testing.T) {
	ctx := context.Background()
	p := New()
	if _, err := p.SubmitJob(ctx, tosai.SubmitJobRequest{
		JobID: "job_mock_stream_2", QuoteID: "q1", ServiceQuoteID: "sq1",
		EscrowID: "e1", PrincipalID: "prn1", CapabilityID: "cap1",
		CapabilityVersion: "1", ProviderID: "agt1", TrustMode: domain.TrustModeManaged,
		Input: map[string]any{"n": 1},
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
	if len(all) < 2 {
		t.Fatalf("need at least 2 events for this test, got %d", len(all))
	}

	var resumed []domain.JobEvent
	if err := p.StreamJobEvents(ctx, tosai.StreamJobEventsRequest{JobID: "job_mock_stream_2", NextSequence: 1}, func(e domain.JobEvent) error {
		resumed = append(resumed, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(resumed) != len(all)-1 {
		t.Fatalf("resumed from sequence 1 got %d events, want %d", len(resumed), len(all)-1)
	}
	if resumed[0].Sequence != 1 {
		t.Fatalf("resumed stream's first event has sequence %d, want 1", resumed[0].Sequence)
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
