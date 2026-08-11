package postgres_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

func TestQuoteCommitmentOperationTwoPostgresReplicasConvergeByCallerKey(t *testing.T) {
	url := os.Getenv("ATOS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx := context.Background()
	a, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	suffix := randSuffix()
	now := time.Now().UTC().Add(-time.Minute)
	makeOp := func(id string) domain.QuoteCommitmentOperation {
		q := domain.Quote{ID: id, PrincipalID: "prn_quote_" + suffix, IdempotencyKey: "idem_" + suffix, IdempotencyRequestHash: "sha256:request"}
		return domain.QuoteCommitmentOperation{QuoteID: id, Quote: q, ContentHash: "sha256:" + id, Checkpoint: domain.QuoteCommitmentIntentPersisted, CreatedAt: now, UpdatedAt: now}
	}
	var creators atomic.Int32
	var wg sync.WaitGroup
	ids := make(chan string, 2)
	for i, st := range []*postgres.Store{a, b} {
		wg.Add(1)
		go func(index int, s *postgres.Store) {
			defer wg.Done()
			op, created, err := s.OpenQuoteCommitment(ctx, makeOp("q_"+suffix+string(rune('a'+index))))
			if err != nil {
				t.Errorf("open: %v", err)
				return
			}
			if created {
				creators.Add(1)
			}
			ids <- op.QuoteID
		}(i, st)
	}
	wg.Wait()
	close(ids)
	var canonical string
	for id := range ids {
		if canonical == "" {
			canonical = id
		} else if id != canonical {
			t.Fatalf("replicas returned different Quote IDs: %q and %q", canonical, id)
		}
	}
	if creators.Load() != 1 {
		t.Fatalf("creators=%d, want 1", creators.Load())
	}
	stale, err := b.StaleQuoteCommitmentOperations(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, op := range stale {
		found = found || op.QuoteID == canonical
	}
	if !found {
		t.Fatalf("canonical pending operation %q was not recoverable by stale scan", canonical)
	}
}
