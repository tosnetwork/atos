package postgres_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

func TestVerifiedQuoteSingleUseAcrossIndependentPostgresStores(t *testing.T) {
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
	quoteID := "quote_verified_once_" + suffix
	makeJob := func(id string) domain.Job {
		now := time.Now().UTC()
		return domain.Job{
			ID: id, CapabilityID: "cap_" + suffix, CapabilityVersion: "1.0.0",
			QuoteID: quoteID, PrincipalID: "principal_" + suffix,
			ProviderID: "provider_" + suffix, TrustMode: domain.TrustModeVerified,
			ProofProfile: domain.ProofProfileTOSVerifiedV1, State: domain.JobSubmitted,
			Input: map[string]any{}, Artifacts: []domain.Artifact{}, CreatedAt: now, UpdatedAt: now,
		}
	}

	start := make(chan struct{})
	var winners, conflicts atomic.Int32
	var wg sync.WaitGroup
	for i, handle := range []*postgres.Store{a, b} {
		wg.Add(1)
		go func(i int, handle *postgres.Store) {
			defer wg.Done()
			<-start
			err := handle.PutJob(ctx, makeJob("job_verified_once_"+suffix+string(rune('a'+i))))
			switch {
			case err == nil:
				winners.Add(1)
			case errors.Is(err, store.ErrConflict):
				conflicts.Add(1)
			default:
				t.Errorf("PutJob: %v", err)
			}
		}(i, handle)
	}
	close(start)
	wg.Wait()
	if winners.Load() != 1 || conflicts.Load() != 1 {
		t.Fatalf("winners=%d conflicts=%d; want exactly one of each", winners.Load(), conflicts.Load())
	}
}
