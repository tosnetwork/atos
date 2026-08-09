package postgres_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

func testDisputeEarning(providerID, jobID, settlementID string) domain.ProviderEarning {
	now := time.Now().UTC()
	return domain.ProviderEarning{
		ID: "earn_" + settlementID, ProviderID: providerID, JobID: jobID,
		QuoteID: "q_" + jobID, ReceiptID: "rcpt_" + jobID, SettlementID: settlementID,
		CapabilityID: "cap_" + jobID, CapabilityVersion: "1.0.0",
		GrossAmount: domain.Money{Amount: "1.05", Currency: "USD"},
		GatewayFee:  domain.Money{Amount: "0.05", Currency: "USD"},
		NetAmount:   domain.Money{Amount: "1.00", Currency: "USD"},
		Status:      domain.EarningAvailable, CreatedAt: now, MaturesAt: now,
	}
}

func testDisputeFor(jobID string, e domain.ProviderEarning) domain.Dispute {
	now := time.Now().UTC()
	return domain.Dispute{
		ID: "dispute_" + jobID, PrincipalID: "prn_" + jobID, ProviderID: e.ProviderID,
		JobID: jobID, QuoteID: e.QuoteID, CapabilityID: e.CapabilityID,
		ReceiptID: e.ReceiptID, SettlementID: e.SettlementID, EarningID: e.ID,
		ChargedAmount: e.GrossAmount, OriginalRefundAmount: domain.Money{Amount: "0.00", Currency: "USD"},
		Reason: "not delivered", ReviewStatus: domain.DisputeOpened,
		EconomicState: domain.DisputeEconomicFrozen, OpenedAt: now, UpdatedAt: now,
	}
}

func TestOpenDisputeFreezesEarningAtomically(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	jobID := "job_" + suffix
	earning := testDisputeEarning("prov_"+suffix, jobID, "settle_"+suffix)
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}

	dispute, nextEarning, created, err := s.OpenDispute(ctx, jobID, earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		if !exists {
			t.Fatal("expected earning to exist")
		}
		d := testDisputeFor(jobID, e)
		next := e
		next.Status = domain.EarningFrozen
		return d, next, nil
	})
	if err != nil {
		t.Fatalf("OpenDispute: %v", err)
	}
	if !created {
		t.Fatal("expected created=true")
	}
	if nextEarning.Status != domain.EarningFrozen {
		t.Fatalf("earning status = %s, want frozen", nextEarning.Status)
	}
	got, err := s.EarningByJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.EarningFrozen {
		t.Fatalf("stored earning status = %s, want frozen", got.Status)
	}
	storedDispute, err := s.DisputeByJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if storedDispute.ID != dispute.ID {
		t.Fatalf("dispute mismatch: %+v", storedDispute)
	}
}

func TestOpenDisputeIsIdempotentByJob(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	jobID := "job_" + suffix
	earning := testDisputeEarning("prov_"+suffix, jobID, "settle_"+suffix)
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}

	first, _, created, err := s.OpenDispute(ctx, jobID, earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		d := testDisputeFor(jobID, e)
		next := e
		next.Status = domain.EarningFrozen
		return d, next, nil
	})
	if err != nil || !created {
		t.Fatalf("first open: created=%v err=%v", created, err)
	}

	buildCalled := false
	second, _, created, err := s.OpenDispute(ctx, jobID, earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		buildCalled = true
		return domain.Dispute{}, domain.ProviderEarning{}, nil
	})
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if created {
		t.Fatal("second open should report created=false")
	}
	if buildCalled {
		t.Fatal("build must not be called when a dispute already exists for the job")
	}
	if second.ID != first.ID {
		t.Fatalf("second open returned a different dispute: %s vs %s", second.ID, first.ID)
	}
}

// TestOpenDisputeConcurrentOpenersConvergeToOne proves the UNIQUE(job_id)
// constraint plus the advisory transaction lock make "at most one dispute
// per Job" a real database guarantee under real concurrent connections,
// not merely a service-layer race that happens to work in tests.
func TestOpenDisputeConcurrentOpenersConvergeToOne(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	jobID := "job_concurrent_" + suffix
	earning := testDisputeEarning("prov_"+suffix, jobID, "settle_"+suffix)
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}

	const attempts = 16
	var wg sync.WaitGroup
	var creators int64
	ids := make(chan string, attempts)

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, _, created, err := s.OpenDispute(ctx, jobID, earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
				dd := testDisputeFor(jobID, e)
				next := e
				next.Status = domain.EarningFrozen
				return dd, next, nil
			})
			if err != nil {
				t.Errorf("OpenDispute: %v", err)
				return
			}
			if created {
				atomic.AddInt64(&creators, 1)
			}
			ids <- d.ID
		}()
	}
	wg.Wait()
	close(ids)
	if creators != 1 {
		t.Fatalf("creators = %d, want exactly 1", creators)
	}
	seen := make(map[string]bool)
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("observed %d distinct dispute ids, want exactly 1: %v", len(seen), seen)
	}

	all, err := s.DisputesByProvider(ctx, "prov_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("found %d dispute rows for provider, want exactly 1", len(all))
	}
}

// TestOpenDisputeRacesPayoutTransitionExactlyOneOutcome proves the
// documented invariant against real Postgres: racing OpenDispute against
// the earnings payout state machine's own Available->PayoutPending
// transition (both go through UpdateEarning/OpenDispute's row lock on the
// SAME provider_earnings row) can never produce "earning paid AND dispute
// claims funds are frozen" -- exactly one of the two wins the row lock
// first, and OpenDispute's build sees whichever status actually committed.
func TestOpenDisputeRacesPayoutTransitionExactlyOneOutcome(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	const attempts = 20
	for i := 0; i < attempts; i++ {
		// A fresh Job/earning per iteration -- avoids needing raw SQL
		// cleanup between iterations and keeps each race fully isolated.
		suffix := randSuffix()
		jobID := "job_race_" + suffix
		earning := testDisputeEarning("prov_"+suffix, jobID, "settle_"+suffix)
		if _, _, err := s.CreateEarning(ctx, earning); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		var payoutTransitioned int32
		var disputeFroze int32
		var disputePendingPayout int32

		wg.Add(2)
		go func() {
			defer wg.Done()
			// Simulates EarningsService.beginPayoutUnderLock's CAS.
			updated, err := s.UpdateEarning(ctx, earning.ID, func(e domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
				if !exists || e.Status != domain.EarningAvailable {
					return e, nil
				}
				e.Status = domain.EarningPayoutPending
				now := time.Now().UTC()
				e.PayoutRequestedAt = &now
				return e, nil
			})
			if err != nil {
				t.Errorf("payout transition: %v", err)
				return
			}
			if updated.Status == domain.EarningPayoutPending {
				atomic.AddInt32(&payoutTransitioned, 1)
			}
		}()
		go func() {
			defer wg.Done()
			d, _, _, err := s.OpenDispute(ctx, jobID, earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
				dd := testDisputeFor(jobID, e)
				next := e
				switch e.Status {
				case domain.EarningAvailable, domain.EarningMaturing:
					next.Status = domain.EarningFrozen
					dd.EconomicState = domain.DisputeEconomicFrozen
				case domain.EarningPayoutPending:
					dd.EconomicState = domain.DisputeEconomicPendingPayoutResolution
				default:
					dd.EconomicState = domain.DisputeEconomicPaid
				}
				return dd, next, nil
			})
			if err != nil {
				t.Errorf("OpenDispute: %v", err)
				return
			}
			switch d.EconomicState {
			case domain.DisputeEconomicFrozen:
				atomic.AddInt32(&disputeFroze, 1)
			case domain.DisputeEconomicPendingPayoutResolution:
				atomic.AddInt32(&disputePendingPayout, 1)
			}
		}()
		wg.Wait()

		// Exactly one legitimate outcome for this iteration: either the
		// dispute won the race and froze the earning before the payout
		// transition could apply (payoutTransitioned must then be 0,
		// since the earning was no longer Available for it to claim), or
		// the payout transition won and the dispute correctly deferred to
		// pending_payout_resolution instead of claiming frozen.
		finalEarning, err := s.EarningByJob(ctx, jobID)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case disputeFroze == 1 && payoutTransitioned == 0:
			if finalEarning.Status != domain.EarningFrozen {
				t.Fatalf("iteration %d: dispute froze but final earning status = %s, want frozen", i, finalEarning.Status)
			}
		case disputePendingPayout == 1 && payoutTransitioned == 1:
			if finalEarning.Status != domain.EarningPayoutPending {
				t.Fatalf("iteration %d: payout transitioned but final earning status = %s, want payout_pending", i, finalEarning.Status)
			}
		default:
			t.Fatalf("iteration %d: impossible/ambiguous outcome -- froze=%d pendingPayout=%d payoutTransitioned=%d final=%s",
				i, disputeFroze, disputePendingPayout, payoutTransitioned, finalEarning.Status)
		}
		if finalEarning.Status == domain.EarningPayoutPending && disputeFroze == 1 {
			t.Fatalf("iteration %d: IMPOSSIBLE STATE -- earning payout_pending/paid path reached AND dispute claims frozen", i)
		}
	}
}

func TestUpdateDisputeRejectsIdentityFieldChange(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	jobID := "job_immutable_" + suffix
	earning := testDisputeEarning("prov_"+suffix, jobID, "settle_"+suffix)
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}
	dispute, _, _, err := s.OpenDispute(ctx, jobID, earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		d := testDisputeFor(jobID, e)
		next := e
		next.Status = domain.EarningFrozen
		return d, next, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.UpdateDispute(ctx, dispute.ID, func(d domain.Dispute, exists bool) (domain.Dispute, error) {
		d.ChargedAmount = domain.Money{Amount: "999.99", Currency: "USD"}
		return d, nil
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}

	got, err := s.GetDispute(ctx, dispute.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ChargedAmount.Amount != "1.05" {
		t.Fatalf("stored dispute mutated by rejected update: %s", got.ChargedAmount.Amount)
	}
}

// TestResolveDisputeCreditsExactlyOnceConcurrently proves ResolveDispute's
// atomic three-way transaction (dispute + earning + account, row-locked
// together) converges to exactly one credit under real concurrent
// Postgres connections attempting the same principal-win resolution --
// the earning reversal and the account credit are never observable as two
// separate operations with a crash/race window between them.
func TestResolveDisputeCreditsExactlyOnceConcurrently(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	jobID := "job_refund_" + suffix
	principalID := "prn_" + suffix
	earning := testDisputeEarning("prov_"+suffix, jobID, "settle_"+suffix)
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}
	dispute, _, _, err := s.OpenDispute(ctx, jobID, earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		d := testDisputeFor(jobID, e)
		d.PrincipalID = principalID
		next := e
		next.Status = domain.EarningFrozen
		return d, next, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	const attempts = 10
	var wg sync.WaitGroup
	var credits int64
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, err := s.ResolveDispute(ctx, dispute.ID, principalID, domain.Account{PrincipalID: principalID},
				func(d domain.Dispute, e domain.ProviderEarning, eExists bool, a domain.Account, aExists bool) (domain.Dispute, domain.ProviderEarning, domain.Account, error) {
					if d.EconomicState == domain.DisputeEconomicRefunded {
						return d, e, a, nil
					}
					if !eExists || e.Status != domain.EarningFrozen {
						return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrSettlementFailed, "earning not frozen", false)
					}
					a.Balance = domain.Money{Amount: "1.05", Currency: "USD"}
					e.Status = domain.EarningReversed
					d.EconomicState = domain.DisputeEconomicRefunded
					d.ReviewStatus = domain.DisputeResolvedForPrincipal
					atomic.AddInt64(&credits, 1)
					return d, e, a, nil
				})
			if err != nil {
				t.Errorf("ResolveDispute: %v", err)
			}
		}()
	}
	wg.Wait()
	if credits != 1 {
		t.Fatalf("credits applied = %d, want exactly 1", credits)
	}
	final, err := s.GetAccount(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Balance.Amount != "1.05" {
		t.Fatalf("final balance = %s, want 1.05 (no double credit)", final.Balance.Amount)
	}
	finalEarning, err := s.EarningByJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if finalEarning.Status != domain.EarningReversed {
		t.Fatalf("final earning status = %s, want reversed", finalEarning.Status)
	}
}
