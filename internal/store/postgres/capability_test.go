package postgres_test

import (
	"context"
	"sync"
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
)

func TestUpdateCapability_ExistsFalseForUnknownID(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	id := "cap_pg_missing_" + randSuffix()
	var sawExists bool
	if _, err := s.UpdateCapability(ctx, id, func(c domain.Capability, exists bool) (domain.Capability, error) {
		sawExists = exists
		return domain.Capability{
			ID: id, ProviderID: "agt_pg_" + randSuffix(), Name: "new", Description: "d",
			DeliveryMode: domain.DeliveryInstant, InputSchema: map[string]any{"type": "object"},
			OutputSchema: map[string]any{"type": "object"}, Status: domain.CapabilityActive,
		}, nil
	}); err != nil {
		t.Fatalf("UpdateCapability: %v", err)
	}
	if sawExists {
		t.Fatal("exists = true for a capability never stored")
	}
	got, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "new" {
		t.Fatalf("Get after UpdateCapability = %+v, want Name=new", got)
	}
}

func TestUpdateCapability_SeesCurrentStateAndPersistsResult(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	id := "cap_pg_seen_" + randSuffix()
	if err := s.Put(ctx, domain.Capability{
		ID: id, ProviderID: "agt_pg_" + randSuffix(), Name: "before", Description: "d",
		DeliveryMode: domain.DeliveryInstant, InputSchema: map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object"}, Status: domain.CapabilityActive,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	updated, err := s.UpdateCapability(ctx, id, func(c domain.Capability, exists bool) (domain.Capability, error) {
		if !exists {
			t.Fatal("exists = false for a capability that was Put")
		}
		if c.Name != "before" {
			t.Fatalf("fn saw Name = %q, want before", c.Name)
		}
		c.Name = "after"
		return c, nil
	})
	if err != nil {
		t.Fatalf("UpdateCapability: %v", err)
	}
	if updated.Name != "after" {
		t.Fatalf("UpdateCapability returned Name = %q, want after", updated.Name)
	}
	got, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "after" {
		t.Fatalf("Get after UpdateCapability = %+v, want Name=after", got)
	}
}

func TestUpdateCapability_FnErrorAbortsWithoutPersisting(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	id := "cap_pg_abort_" + randSuffix()
	if err := s.Put(ctx, domain.Capability{
		ID: id, ProviderID: "agt_pg_" + randSuffix(), Name: "before", Description: "d",
		DeliveryMode: domain.DeliveryInstant, InputSchema: map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object"}, Status: domain.CapabilityActive,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.UpdateCapability(ctx, id, func(c domain.Capability, exists bool) (domain.Capability, error) {
		c.Name = "should not persist"
		return c, domain.NewError(domain.ErrValidationFailed, "rejected", false)
	}); err == nil {
		t.Fatal("expected an error from a rejecting fn")
	}
	got, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "before" {
		t.Fatalf("fn's error did not abort the write: %+v", got)
	}
}

// TestUpdateCapability_ConcurrentEvaluationsAgainstStaleVersionConverge is
// the store-layer proof behind CapabilityService.EvaluateActivation's
// two-phase CAS fix: N goroutines each capture the SAME "evaluated"
// version (simulating N callers whose authority.Evaluate ran concurrently,
// each unaware of the others), then race to atomically apply their result
// via UpdateCapability, re-checking the version inside the closure exactly
// like EvaluateActivation does. Real Postgres row locking (not the memory
// store's single mutex) must serialize these: exactly one write actually
// changes the version; every other goroutine must see the version has
// already moved and reject via its own re-check, never silently applying
// a decision made against state that has since changed. Run with -race.
func TestUpdateCapability_ConcurrentEvaluationsAgainstStaleVersionConverge(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	id := "cap_pg_race_" + randSuffix()
	if err := s.Put(ctx, domain.Capability{
		ID: id, ProviderID: "agt_pg_" + randSuffix(), Name: "n", Description: "d",
		Version: "1.0.0", DeliveryMode: domain.DeliveryInstant,
		InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Status: domain.CapabilityActive,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	evaluatedVersion := "1.0.0"

	const attempts = 12
	var wg sync.WaitGroup
	results := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.UpdateCapability(ctx, id, func(c domain.Capability, exists bool) (domain.Capability, error) {
				if !exists {
					return domain.Capability{}, domain.NewError(domain.ErrCapabilityUnavailable, "not found", false)
				}
				if c.Version != evaluatedVersion {
					return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "version changed during evaluation; retry", true)
				}
				c.Version = "1.1.0"
				return c, nil
			})
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)

	succeeded, rejected := 0, 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		default:
			derr, ok := err.(*domain.Error)
			if !ok || derr.Code != domain.ErrValidationFailed {
				t.Fatalf("unexpected non-domain or wrong-code error: %v", err)
			}
			rejected++
		}
	}
	if succeeded != 1 {
		t.Fatalf("succeeded = %d, want exactly 1 (every other racer must see the version already moved)", succeeded)
	}
	if succeeded+rejected != attempts {
		t.Fatalf("succeeded(%d)+rejected(%d) != attempts(%d)", succeeded, rejected, attempts)
	}

	final, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if final.Version != "1.1.0" {
		t.Fatalf("final version = %q, want 1.1.0", final.Version)
	}
}
