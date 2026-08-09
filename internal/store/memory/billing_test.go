package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

func testBillingSnapshot(jobID string) domain.BillingSnapshot {
	return domain.BillingSnapshot{
		JobID: jobID, QuoteID: "q_1", ReceiptID: "rcpt_1", ProviderID: "prov_1",
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0", TrustMode: domain.TrustModeManaged,
		GrossCharge: domain.Money{Amount: "0.52", Currency: "USD"}, ProviderGross: domain.Money{Amount: "0.50", Currency: "USD"},
		GatewayFee: domain.Money{Amount: "0.02", Currency: "USD"}, PrincipalRefund: domain.Money{Amount: "0.53", Currency: "USD"},
		CalculatedAt: time.Now().UTC(),
	}
}

func TestPutBillingSnapshotIdempotentRetry(t *testing.T) {
	ctx := context.Background()
	s := New()
	snap := testBillingSnapshot("job_1")

	first, created, err := s.PutBillingSnapshot(ctx, snap)
	if err != nil || !created {
		t.Fatalf("first put: created=%v err=%v", created, err)
	}
	// A later recomputation may have a different CalculatedAt but must
	// still count as identical economic content.
	retry := snap
	retry.CalculatedAt = snap.CalculatedAt.Add(time.Hour)
	second, created, err := s.PutBillingSnapshot(ctx, retry)
	if err != nil {
		t.Fatalf("retry put: %v", err)
	}
	if created {
		t.Fatal("retry with identical economic content should report created=false")
	}
	if second.GrossCharge != first.GrossCharge {
		t.Fatalf("retry returned different content: %+v vs %+v", second, first)
	}
}

func TestPutBillingSnapshotConflictingRecomputeIsRejected(t *testing.T) {
	ctx := context.Background()
	s := New()
	snap := testBillingSnapshot("job_conflict")
	if _, _, err := s.PutBillingSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}

	conflicting := snap
	conflicting.GrossCharge = domain.Money{Amount: "0.99", Currency: "USD"}
	_, _, err := s.PutBillingSnapshot(ctx, conflicting)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}

	got, err := s.BillingSnapshotByJob(ctx, "job_conflict")
	if err != nil {
		t.Fatal(err)
	}
	if got.GrossCharge.Amount != "0.52" {
		t.Fatalf("stored snapshot mutated by rejected conflict: %s", got.GrossCharge.Amount)
	}
}

func testMemEarning(providerID, jobID, settlementID string) domain.ProviderEarning {
	now := time.Now().UTC()
	return domain.ProviderEarning{
		ID: "earn_" + settlementID, ProviderID: providerID, JobID: jobID,
		QuoteID: "q_" + jobID, ReceiptID: "rcpt_" + jobID, SettlementID: settlementID,
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0",
		GrossAmount: domain.Money{Amount: "1.05", Currency: "USD"},
		GatewayFee:  domain.Money{Amount: "0.05", Currency: "USD"},
		NetAmount:   domain.Money{Amount: "1.00", Currency: "USD"},
		Status:      domain.EarningMaturing, CreatedAt: now, MaturesAt: now,
	}
}

func TestCreateEarningIdempotentRetryIgnoresLifecycleFields(t *testing.T) {
	ctx := context.Background()
	s := New()
	e := testMemEarning("prov_1", "job_1", "settle_1")
	first, created, err := s.CreateEarning(ctx, e)
	if err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}
	// A retry with a different Status/CreatedAt/MaturesAt (as a freshly
	// constructed earning object would have) must still be recognized as
	// the same economic earning.
	retry := e
	retry.Status = domain.EarningAvailable
	retry.CreatedAt = e.CreatedAt.Add(time.Hour)
	retry.MaturesAt = e.MaturesAt.Add(2 * time.Hour)
	second, created, err := s.CreateEarning(ctx, retry)
	if err != nil {
		t.Fatalf("retry create: %v", err)
	}
	if created {
		t.Fatal("retry with identical identity/economic fields should report created=false")
	}
	if second.ID != first.ID || second.Status != first.Status {
		t.Fatalf("retry should return the ORIGINAL stored earning unchanged: %+v", second)
	}
}

func TestCreateEarningConflictingContentIsRejected(t *testing.T) {
	ctx := context.Background()
	s := New()
	e := testMemEarning("prov_1", "job_1", "settle_conflict")
	if _, _, err := s.CreateEarning(ctx, e); err != nil {
		t.Fatal(err)
	}

	conflicting := e
	conflicting.NetAmount = domain.Money{Amount: "999.99", Currency: "USD"}
	_, _, err := s.CreateEarning(ctx, conflicting)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}

	got, err := s.EarningBySettlement(ctx, "settle_conflict")
	if err != nil {
		t.Fatal(err)
	}
	if got.NetAmount.Amount != "1.00" {
		t.Fatalf("stored earning mutated by rejected conflict: %s", got.NetAmount.Amount)
	}
}

// TestUpdateEarningAllowsLifecycleFieldChanges proves the normal payout
// state machine (status/timestamp transitions) still works through
// UpdateEarning once the economic-field invariant is enforced.
func TestUpdateEarningAllowsLifecycleFieldChanges(t *testing.T) {
	ctx := context.Background()
	s := New()
	e := testMemEarning("prov_1", "job_lifecycle", "settle_lifecycle")
	if _, _, err := s.CreateEarning(ctx, e); err != nil {
		t.Fatal(err)
	}

	updated, err := s.UpdateEarning(ctx, e.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		if !exists {
			t.Fatal("expected earning to exist")
		}
		current.Status = domain.EarningAvailable
		now := time.Now().UTC()
		current.AvailableAt = &now
		return current, nil
	})
	if err != nil {
		t.Fatalf("UpdateEarning (lifecycle-only): %v", err)
	}
	if updated.Status != domain.EarningAvailable {
		t.Fatalf("status = %s, want available", updated.Status)
	}
}

// TestUpdateEarningRejectsEconomicFieldChange proves a callback that
// mutates an earning's identity/economic fields (ProviderID, SettlementID,
// GrossAmount, GatewayFee, NetAmount, ...) -- whether by a bug in a Phase 2C
// dispute callback or any other caller -- is rejected instead of silently
// persisted, so those fields stay immutable for the life of the earning
// exactly as CreateEarning's own idempotency-conflict check already
// guarantees at creation time.
func TestUpdateEarningRejectsEconomicFieldChange(t *testing.T) {
	ctx := context.Background()
	s := New()
	e := testMemEarning("prov_1", "job_economic", "settle_economic")
	if _, _, err := s.CreateEarning(ctx, e); err != nil {
		t.Fatal(err)
	}

	_, err := s.UpdateEarning(ctx, e.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		if !exists {
			t.Fatal("expected earning to exist")
		}
		current.NetAmount = domain.Money{Amount: "999.99", Currency: "USD"}
		return current, nil
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}

	got, err := s.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.NetAmount.Amount != "1.00" {
		t.Fatalf("stored earning mutated by rejected economic-field update: %s", got.NetAmount.Amount)
	}
}

// TestUpdateEarningRejectsIDChange proves a callback that changes the
// earning's ID is rejected: ID is deliberately excluded from
// earningContentHash (so CreateEarning can recognize a replay under a
// different candidate ID as the same settlement), so it needs this
// separate check. Without it, persisting a changed ID would move the entry
// to a different map key while s.earnings[originalID] and
// s.earningsBySettlement both go stale/inconsistent.
func TestUpdateEarningRejectsIDChange(t *testing.T) {
	ctx := context.Background()
	s := New()
	e := testMemEarning("prov_1", "job_idchange", "settle_idchange")
	if _, _, err := s.CreateEarning(ctx, e); err != nil {
		t.Fatal(err)
	}

	_, err := s.UpdateEarning(ctx, e.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		if !exists {
			t.Fatal("expected earning to exist")
		}
		current.ID = "earn_different_id"
		return current, nil
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}

	// The original entry, and the store's internal indexes, must be
	// untouched by the rejected update.
	got, err := s.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatalf("original earning must still be retrievable by its original id: %v", err)
	}
	if got.ID != e.ID {
		t.Fatalf("stored earning id mutated by a rejected update: %s", got.ID)
	}
	bySettlement, err := s.EarningBySettlement(ctx, e.SettlementID)
	if err != nil {
		t.Fatalf("EarningBySettlement: %v", err)
	}
	if bySettlement.ID != e.ID {
		t.Fatalf("earningsBySettlement index corrupted: points at %s, want %s", bySettlement.ID, e.ID)
	}
	if _, err := s.GetEarning(ctx, "earn_different_id"); err == nil {
		t.Fatal("a rejected ID change must not have created an entry under the new id")
	}
}

// TestEarningsAvailableForPayoutExcludesDisputeHeldEarnings proves a held
// earning (Status=Available, DisputeHoldID set) is never returned as a
// payout candidate, even though its Status alone would qualify -- so a
// batch full of held earnings cannot head-of-line-block genuinely payable
// ones behind the reconciler's limit.
func TestEarningsAvailableForPayoutExcludesDisputeHeldEarnings(t *testing.T) {
	ctx := context.Background()
	s := New()
	held := testMemEarning("prov_1", "job_held", "settle_held")
	held.Status = domain.EarningAvailable
	held.DisputeHoldID = "dispute_1"
	if _, _, err := s.CreateEarning(ctx, held); err != nil {
		t.Fatal(err)
	}
	free := testMemEarning("prov_1", "job_free", "settle_free")
	free.Status = domain.EarningAvailable
	if _, _, err := s.CreateEarning(ctx, free); err != nil {
		t.Fatal(err)
	}

	candidates, err := s.EarningsAvailableForPayout(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates = %d, want exactly 1 (held earning excluded)", len(candidates))
	}
	if candidates[0].ID != free.ID {
		t.Fatalf("candidate = %s, want the un-held earning %s", candidates[0].ID, free.ID)
	}
}
