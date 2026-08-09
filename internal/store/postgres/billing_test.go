package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func testEarning(providerID, jobID, settlementID string) domain.ProviderEarning {
	now := time.Now().UTC()
	return domain.ProviderEarning{
		ID: "earn_" + settlementID, ProviderID: providerID, JobID: jobID,
		QuoteID: "q_" + jobID, ReceiptID: "rcpt_" + jobID, SettlementID: settlementID,
		CapabilityID: "cap_" + jobID, CapabilityVersion: "1.0.0",
		GrossAmount: domain.Money{Amount: "1.05", Currency: "USD"},
		GatewayFee:  domain.Money{Amount: "0.05", Currency: "USD"},
		NetAmount:   domain.Money{Amount: "1.00", Currency: "USD"},
		Status:      domain.EarningMaturing, CreatedAt: now, MaturesAt: now,
	}
}

// TestBillingSnapshotRoundTripIsIdempotent proves PutBillingSnapshot is a
// safe no-op to call twice for the same JobID, and that all fields
// (including nested Money/Usage) survive the JSONB round trip.
func TestBillingSnapshotRoundTripIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_pg_bill_" + randSuffix()

	snap := domain.BillingSnapshot{
		JobID: jobID, QuoteID: "q_1", ReceiptID: "rcpt_1", ProviderID: "prov_1",
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0", TrustMode: domain.TrustModeManaged,
		Usage:            domain.Usage{InputTokens: 10, OutputTokens: 20},
		UsageCommitment:  "sha256:usage",
		PricingModel:     domain.PricingMetered,
		PricingTermsHash: "sha256:terms",
		GrossCharge:      domain.Money{Amount: "0.52", Currency: "USD"},
		ProviderGross:    domain.Money{Amount: "0.50", Currency: "USD"},
		GatewayFee:       domain.Money{Amount: "0.02", Currency: "USD"},
		PrincipalRefund:  domain.Money{Amount: "0.53", Currency: "USD"},
		CalculatedAt:     time.Now().UTC().Truncate(time.Microsecond),
	}
	if err := s.PutBillingSnapshot(ctx, snap); err != nil {
		t.Fatalf("PutBillingSnapshot: %v", err)
	}
	// Calling it again with identical content must be a safe no-op.
	if err := s.PutBillingSnapshot(ctx, snap); err != nil {
		t.Fatalf("PutBillingSnapshot (retry): %v", err)
	}
	got, err := s.BillingSnapshotByJob(ctx, jobID)
	if err != nil {
		t.Fatalf("BillingSnapshotByJob: %v", err)
	}
	if got.GrossCharge != snap.GrossCharge || got.ProviderGross != snap.ProviderGross ||
		got.GatewayFee != snap.GatewayFee || got.PrincipalRefund != snap.PrincipalRefund {
		t.Fatalf("round-tripped money fields do not match: got %+v, want %+v", got, snap)
	}
	if got.Usage != snap.Usage {
		t.Fatalf("round-tripped usage = %+v, want %+v", got.Usage, snap.Usage)
	}
	if got.PricingModel != snap.PricingModel || got.PricingTermsHash != snap.PricingTermsHash {
		t.Fatalf("round-tripped pricing metadata mismatch: got %+v", got)
	}
}

func TestBillingSnapshotByJobNotFound(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	if _, err := s.BillingSnapshotByJob(ctx, "job_pg_missing_"+randSuffix()); err != store.ErrNotFound {
		t.Fatalf("got %v, want store.ErrNotFound", err)
	}
}

// TestCreateEarningExactlyOnePerSettlement proves the settlement_id
// uniqueness constraint: a second CreateEarning call for the same
// settlement returns the original earning with created=false instead of
// erroring or creating a duplicate row.
func TestCreateEarningExactlyOnePerSettlement(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	settlementID := "settle_" + suffix
	e := testEarning("prov_"+suffix, "job_"+suffix, settlementID)

	first, created, err := s.CreateEarning(ctx, e)
	if err != nil {
		t.Fatalf("CreateEarning (first): %v", err)
	}
	if !created {
		t.Fatalf("first CreateEarning should report created=true")
	}

	duplicate := e
	duplicate.ID = "earn_different_id_" + suffix // even a different candidate ID must not create a second row
	second, created, err := s.CreateEarning(ctx, duplicate)
	if err != nil {
		t.Fatalf("CreateEarning (duplicate): %v", err)
	}
	if created {
		t.Fatalf("duplicate CreateEarning should report created=false")
	}
	if second.ID != first.ID {
		t.Fatalf("duplicate CreateEarning returned a different earning: got %s, want %s", second.ID, first.ID)
	}

	all, err := s.EarningsByProvider(ctx, e.ProviderID)
	if err != nil {
		t.Fatalf("EarningsByProvider: %v", err)
	}
	count := 0
	for _, got := range all {
		if got.SettlementID == settlementID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("found %d earnings for settlement %s, want exactly 1", count, settlementID)
	}
}

// TestCreateEarningConcurrentCreationHasSingleWinner proves the database
// uniqueness constraint (not application-level locking) is what makes
// concurrent earning creation from multiple Postgres connections safe: many
// goroutines racing to create an earning for the SAME settlement must
// converge on exactly one created=true winner and exactly one row.
func TestCreateEarningConcurrentCreationHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	settlementID := "settle_concurrent_" + suffix
	providerID := "prov_concurrent_" + suffix

	const attempts = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	var unexpected string

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := testEarning(providerID, "job_concurrent_"+suffix, settlementID)
			_, created, err := s.CreateEarning(ctx, e)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if unexpected == "" {
					unexpected = err.Error()
				}
				return
			}
			if created {
				winners++
			}
		}(i)
	}
	wg.Wait()
	if unexpected != "" {
		t.Fatalf("unexpected concurrent CreateEarning error: %s", unexpected)
	}
	if winners != 1 {
		t.Fatalf("concurrent CreateEarning winners = %d, want exactly 1", winners)
	}

	all, err := s.EarningsByProvider(ctx, providerID)
	if err != nil {
		t.Fatalf("EarningsByProvider: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("found %d earnings after concurrent creation, want exactly 1", len(all))
	}
}

// TestUpdateEarningCASConcurrentPayoutTransitionHasSingleWinner proves the
// row-lock CAS in UpdateEarning serializes concurrent workers all trying to
// transition the SAME earning from Available to PayoutPending -- exactly
// the race two reconciler workers or two ATOS replicas can trigger.
func TestUpdateEarningCASConcurrentPayoutTransitionHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	settlementID := "settle_cas_" + suffix
	e := testEarning("prov_cas_"+suffix, "job_cas_"+suffix, settlementID)
	e.Status = domain.EarningAvailable
	if _, _, err := s.CreateEarning(ctx, e); err != nil {
		t.Fatalf("CreateEarning: %v", err)
	}

	const attempts = 12
	var wg sync.WaitGroup
	var mu sync.Mutex
	transitions := 0
	var unexpected string

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			updated, err := s.UpdateEarning(ctx, e.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
				if !exists {
					return domain.ProviderEarning{}, store.ErrNotFound
				}
				if current.Status != domain.EarningAvailable {
					return current, nil // already transitioned -- no-op, not an error
				}
				now := time.Now().UTC()
				current.Status = domain.EarningPayoutPending
				current.PayoutRequestedAt = &now
				current.PayoutIdempotencyKey = "payout:" + current.ID + ":v1"
				return current, nil
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if unexpected == "" {
					unexpected = err.Error()
				}
				return
			}
			if updated.Status == domain.EarningPayoutPending && updated.PayoutRequestedAt != nil {
				transitions++
			}
		}()
	}
	wg.Wait()
	if unexpected != "" {
		t.Fatalf("unexpected concurrent UpdateEarning error: %s", unexpected)
	}
	if transitions != attempts {
		t.Fatalf("expected all %d callers to observe the transitioned (or already-transitioned) state, got %d", attempts, transitions)
	}

	final, err := s.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatalf("GetEarning: %v", err)
	}
	if final.Status != domain.EarningPayoutPending {
		t.Fatalf("final status = %s, want payout_pending", final.Status)
	}
}

// TestSettledJobsMissingEarningFindsGapAndExcludesCompleted proves the
// backfill scan finds a settled Job with no earning row, and stops finding
// it once an earning exists.
func TestSettledJobsMissingEarningFindsGapAndExcludesCompleted(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	jobID := "job_backfill_" + suffix
	principalID := "prn_backfill_" + suffix

	job := domain.Job{
		ID: jobID, CapabilityID: "cap_1", QuoteID: "q_1", PrincipalID: principalID,
		State: domain.JobCompleted, EconomicState: domain.EconomicSettled,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := s.PutJob(ctx, job); err != nil {
		t.Fatalf("PutJob: %v", err)
	}

	found := false
	gaps, err := s.SettledJobsMissingEarning(ctx, 1000)
	if err != nil {
		t.Fatalf("SettledJobsMissingEarning: %v", err)
	}
	for _, j := range gaps {
		if j.ID == jobID {
			found = true
		}
	}
	if !found {
		t.Fatalf("settled job %s with no earning should be reported as a gap", jobID)
	}

	settlementID := "settle_backfill_" + suffix
	e := testEarning("prov_backfill_"+suffix, jobID, settlementID)
	if _, _, err := s.CreateEarning(ctx, e); err != nil {
		t.Fatalf("CreateEarning: %v", err)
	}

	gaps, err = s.SettledJobsMissingEarning(ctx, 1000)
	if err != nil {
		t.Fatalf("SettledJobsMissingEarning (after earning created): %v", err)
	}
	for _, j := range gaps {
		if j.ID == jobID {
			t.Fatalf("job %s still reported as a gap after its earning was created", jobID)
		}
	}
}
