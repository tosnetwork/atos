package postgres_test

import (
	"context"
	"errors"
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
	first, created, err := s.PutBillingSnapshot(ctx, snap)
	if err != nil {
		t.Fatalf("PutBillingSnapshot: %v", err)
	}
	if !created {
		t.Fatal("first PutBillingSnapshot should report created=true")
	}
	// Calling it again with identical content must be a safe no-op.
	second, created, err := s.PutBillingSnapshot(ctx, snap)
	if err != nil {
		t.Fatalf("PutBillingSnapshot (retry): %v", err)
	}
	if created {
		t.Fatal("retried PutBillingSnapshot with identical content should report created=false")
	}
	if second.GrossCharge != first.GrossCharge {
		t.Fatalf("retry returned different content: %+v vs %+v", second, first)
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

// TestPutBillingSnapshotConflictingRecomputeIsRejected proves that if a
// billing snapshot already exists for a JobID and a second call presents
// DIFFERENT economic content for the same job (e.g. a crash-recovery replay
// that recomputed a different charge), the store rejects it with
// ErrIdempotencyConflict instead of silently keeping the stale value.
func TestPutBillingSnapshotConflictingRecomputeIsRejected(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	jobID := "job_pg_conflict_" + randSuffix()

	original := domain.BillingSnapshot{
		JobID: jobID, QuoteID: "q_1", ReceiptID: "rcpt_1", ProviderID: "prov_1",
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0", TrustMode: domain.TrustModeManaged,
		GrossCharge: domain.Money{Amount: "0.52", Currency: "USD"}, ProviderGross: domain.Money{Amount: "0.50", Currency: "USD"},
		GatewayFee: domain.Money{Amount: "0.02", Currency: "USD"}, PrincipalRefund: domain.Money{Amount: "0.53", Currency: "USD"},
		CalculatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	if _, _, err := s.PutBillingSnapshot(ctx, original); err != nil {
		t.Fatalf("PutBillingSnapshot (original): %v", err)
	}

	conflicting := original
	conflicting.GrossCharge = domain.Money{Amount: "0.73", Currency: "USD"}
	conflicting.CalculatedAt = time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	_, _, err := s.PutBillingSnapshot(ctx, conflicting)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}

	// The original snapshot must remain untouched by the rejected conflict.
	got, err := s.BillingSnapshotByJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.GrossCharge.Amount != "0.52" {
		t.Fatalf("stored snapshot was mutated by a rejected conflict: gross_charge = %s, want 0.52", got.GrossCharge.Amount)
	}
}

// TestCreateEarningConflictingContentIsRejected proves that if an earning
// already exists for a settlement_id and a second call presents DIFFERENT
// identity/economic fields for the same settlement, the store rejects it
// with ErrIdempotencyConflict instead of silently returning the stale row.
func TestCreateEarningConflictingContentIsRejected(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	settlementID := "settle_conflict_" + suffix
	original := testEarning("prov_"+suffix, "job_"+suffix, settlementID)
	if _, _, err := s.CreateEarning(ctx, original); err != nil {
		t.Fatalf("CreateEarning (original): %v", err)
	}

	conflicting := original
	conflicting.NetAmount = domain.Money{Amount: "999.99", Currency: "USD"}
	_, _, err := s.CreateEarning(ctx, conflicting)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}

	got, err := s.EarningBySettlement(ctx, settlementID)
	if err != nil {
		t.Fatal(err)
	}
	if got.NetAmount.Amount != "1.00" {
		t.Fatalf("stored earning was mutated by a rejected conflict: net_amount = %s, want 1.00", got.NetAmount.Amount)
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

// TestUpdateEarningRejectsEconomicFieldChange proves UpdateEarning rejects a
// callback that mutates an earning's identity/economic fields (ProviderID,
// SettlementID, GrossAmount, GatewayFee, NetAmount, ...) instead of
// persisting it. upsertEarningSQL's ON CONFLICT already excludes the
// dedicated economic columns from its SET clause, but it does still refresh
// the JSONB payload column (which also carries those fields, for lifecycle
// data) -- without this invariant, a buggy callback could make a later
// scanEarning (which applies that payload on top of the dedicated columns)
// read back a different economic value than what CreateEarning originally
// committed.
func TestUpdateEarningRejectsEconomicFieldChange(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	settlementID := "settle_economic_" + suffix
	e := testEarning("prov_economic_"+suffix, "job_economic_"+suffix, settlementID)
	if _, _, err := s.CreateEarning(ctx, e); err != nil {
		t.Fatalf("CreateEarning: %v", err)
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
		t.Fatalf("stored earning mutated by a rejected economic-field update: net_amount = %s, want 1.00", got.NetAmount.Amount)
	}
}

// TestUpdateEarningRejectsIDChange proves a callback that changes the
// earning's ID is rejected: ID is deliberately excluded from
// earningContentHash (so CreateEarning can recognize a replay under a
// different candidate ID as the same settlement), so it needs this
// separate check. upsertEarningSQL's ON CONFLICT target is id itself, so
// without this check persisting a changed ID would insert/target an
// entirely different row than the one the transaction just locked with
// `SELECT ... WHERE id=$1 FOR UPDATE`.
func TestUpdateEarningRejectsIDChange(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	settlementID := "settle_idchange_" + suffix
	e := testEarning("prov_idchange_"+suffix, "job_idchange_"+suffix, settlementID)
	if _, _, err := s.CreateEarning(ctx, e); err != nil {
		t.Fatalf("CreateEarning: %v", err)
	}

	_, err := s.UpdateEarning(ctx, e.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		if !exists {
			t.Fatal("expected earning to exist")
		}
		current.ID = "earn_different_id_" + suffix
		return current, nil
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}

	got, err := s.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatalf("original earning must still be retrievable by its original id: %v", err)
	}
	if got.ID != e.ID {
		t.Fatalf("stored earning id mutated by a rejected update: %s", got.ID)
	}
	if _, err := s.GetEarning(ctx, "earn_different_id_"+suffix); err == nil {
		t.Fatal("a rejected ID change must not have created a row under the new id")
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

// TestEarningsAvailableForPayoutExcludesDisputeHeldEarnings proves a held
// earning (Status=Available, DisputeHoldID set) is never returned as a
// payout candidate against real Postgres, even though its Status alone
// would qualify and it satisfies idx_provider_earnings_available's old
// (status='available')-only predicate -- so a batch full of held earnings
// cannot head-of-line-block genuinely payable ones behind the
// reconciler's limit.
func TestEarningsAvailableForPayoutExcludesDisputeHeldEarnings(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()

	held := testEarning("prov_hold_"+suffix, "job_hold_"+suffix, "settle_hold_"+suffix)
	held.Status = domain.EarningAvailable
	if _, _, err := s.CreateEarning(ctx, held); err != nil {
		t.Fatalf("CreateEarning (held): %v", err)
	}
	if _, err := s.UpdateEarning(ctx, held.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		current.DisputeHoldID = "dispute_hold_" + suffix
		return current, nil
	}); err != nil {
		t.Fatalf("UpdateEarning (set hold): %v", err)
	}

	free := testEarning("prov_free_"+suffix, "job_free_"+suffix, "settle_free_"+suffix)
	free.Status = domain.EarningAvailable
	if _, _, err := s.CreateEarning(ctx, free); err != nil {
		t.Fatalf("CreateEarning (free): %v", err)
	}

	candidates, err := s.EarningsAvailableForPayout(ctx, 1000)
	if err != nil {
		t.Fatalf("EarningsAvailableForPayout: %v", err)
	}
	sawFree, sawHeld := false, false
	for _, e := range candidates {
		if e.ID == free.ID {
			sawFree = true
		}
		if e.ID == held.ID {
			sawHeld = true
		}
	}
	if !sawFree {
		t.Fatalf("un-held earning %s was not returned as a payout candidate", free.ID)
	}
	if sawHeld {
		t.Fatalf("held earning %s was incorrectly returned as a payout candidate", held.ID)
	}
}
