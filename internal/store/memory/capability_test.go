package memory

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
)

// TestCapability_ActiveByMode mirrors postgres_test.go's identically-named
// test exactly (same assertions, same edge cases) -- store.Capabilities has
// two independent implementations for testing convenience, and prior to
// this the limit<=0 edge case was only ever exercised against postgres,
// letting the two diverge (memory treats it as unbounded, an earlier
// postgres revision treated it as "return zero rows") without any test
// catching the mismatch. Both backends must now assert the identical
// behavior for identical input.
func TestCapability_ActiveByMode(t *testing.T) {
	ctx := context.Background()
	s := New()

	verified := domain.Capability{
		ID: "cap_active_verified", ProviderID: "agt_active",
		Name: "Active Verified", Description: "for ActiveByMode",
		Version: "1.0.0", DeliveryMode: domain.DeliveryInstant,
		InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		SupportedTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified},
		Status:              domain.CapabilityActive,
	}
	managedOnly := verified
	managedOnly.ID = "cap_active_managed"
	managedOnly.SupportedTrustModes = []domain.TrustMode{domain.TrustModeManaged}

	if err := s.Put(ctx, verified); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, managedOnly); err != nil {
		t.Fatal(err)
	}

	active, err := s.ActiveByMode(ctx, domain.TrustModeVerified, 100)
	if err != nil {
		t.Fatalf("ActiveByMode: %v", err)
	}
	if !containsCapabilityID(active, verified.ID) {
		t.Errorf("ActiveByMode(verified) did not return %s", verified.ID)
	}
	if containsCapabilityID(active, managedOnly.ID) {
		t.Errorf("ActiveByMode(verified) incorrectly returned managed-only capability %s", managedOnly.ID)
	}

	// limit<=0 means unbounded -- identical assertion to postgres_test.go's
	// TestCapability_ActiveByMode, proving both backends agree.
	unbounded, err := s.ActiveByMode(ctx, domain.TrustModeVerified, 0)
	if err != nil {
		t.Fatalf("ActiveByMode(limit=0): %v", err)
	}
	if !containsCapabilityID(unbounded, verified.ID) {
		t.Errorf("ActiveByMode(verified, limit=0) must be unbounded, not return zero rows: got %d results", len(unbounded))
	}
}

func containsCapabilityID(caps []domain.Capability, id string) bool {
	for _, c := range caps {
		if c.ID == id {
			return true
		}
	}
	return false
}

func TestUpdateCapability_ExistsFalseForUnknownID(t *testing.T) {
	ctx := context.Background()
	s := New()
	var sawExists bool
	if _, err := s.UpdateCapability(ctx, "cap_missing", func(c domain.Capability, exists bool) (domain.Capability, error) {
		sawExists = exists
		return domain.Capability{ID: "cap_missing", Name: "new"}, nil
	}); err != nil {
		t.Fatalf("UpdateCapability: %v", err)
	}
	if sawExists {
		t.Fatal("exists = true for a capability never stored")
	}
	got, err := s.Get(ctx, "cap_missing")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "new" {
		t.Fatalf("Get after UpdateCapability = %+v, want Name=new", got)
	}
}

func TestUpdateCapability_SeesCurrentStateAndPersistsResult(t *testing.T) {
	ctx := context.Background()
	s := New()
	if err := s.Put(ctx, domain.Capability{ID: "cap_seen", Name: "before"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	updated, err := s.UpdateCapability(ctx, "cap_seen", func(c domain.Capability, exists bool) (domain.Capability, error) {
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
	got, err := s.Get(ctx, "cap_seen")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "after" {
		t.Fatalf("Get after UpdateCapability = %+v, want Name=after", got)
	}
}

func TestUpdateCapability_FnErrorAbortsWithoutPersisting(t *testing.T) {
	ctx := context.Background()
	s := New()
	if err := s.Put(ctx, domain.Capability{ID: "cap_abort", Name: "before"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.UpdateCapability(ctx, "cap_abort", func(c domain.Capability, exists bool) (domain.Capability, error) {
		c.Name = "should not persist"
		return c, domain.NewError(domain.ErrValidationFailed, "rejected", false)
	}); err == nil {
		t.Fatal("expected an error from a rejecting fn")
	}
	got, err := s.Get(ctx, "cap_abort")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "before" {
		t.Fatalf("fn's error did not abort the write: %+v", got)
	}
}

// TestUpdateCapability_ConcurrentIncrementsNeverLost is the same class of
// proof TestUpdateAccountConcurrentDebitsNeverOverspend already gives
// UpdateAccount: N concurrent UpdateCapability callers each incrementing a
// counter stored in Description must all land -- a plain Get-then-Put pair
// (what CapabilityService's mutations used before this fix) would lose
// updates under real concurrency. Run with -race to also confirm no data
// race on the underlying map.
func TestUpdateCapability_ConcurrentIncrementsNeverLost(t *testing.T) {
	ctx := context.Background()
	s := New()
	if err := s.Put(ctx, domain.Capability{ID: "cap_counter", Description: "0"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	const attempts = 50
	var wg sync.WaitGroup
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer wg.Done()
			if _, err := s.UpdateCapability(ctx, "cap_counter", func(c domain.Capability, exists bool) (domain.Capability, error) {
				n, _ := strconv.Atoi(c.Description)
				c.Description = strconv.Itoa(n + 1)
				return c, nil
			}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	got, err := s.Get(ctx, "cap_counter")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Description != strconv.Itoa(attempts) {
		t.Fatalf("final counter = %q, want %d -- a concurrent update was lost", got.Description, attempts)
	}
}
