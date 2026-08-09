package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

func testDisputeEarning(jobID string) domain.ProviderEarning {
	now := time.Now().UTC()
	return domain.ProviderEarning{
		ID: "earn_" + jobID, ProviderID: "prov_1", JobID: jobID,
		QuoteID: "q_" + jobID, ReceiptID: "rcpt_" + jobID, SettlementID: "settle_" + jobID,
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0",
		GrossAmount: domain.Money{Amount: "1.05", Currency: "USD"},
		GatewayFee:  domain.Money{Amount: "0.05", Currency: "USD"},
		NetAmount:   domain.Money{Amount: "1.00", Currency: "USD"},
		Status:      domain.EarningAvailable, CreatedAt: now, MaturesAt: now,
	}
}

func testDisputeFor(jobID string, earning domain.ProviderEarning) domain.Dispute {
	now := time.Now().UTC()
	return domain.Dispute{
		ID: "dispute_" + jobID, PrincipalID: "prn_1", ProviderID: earning.ProviderID,
		JobID: jobID, QuoteID: earning.QuoteID, CapabilityID: earning.CapabilityID,
		ReceiptID: earning.ReceiptID, SettlementID: earning.SettlementID, EarningID: earning.ID,
		ChargedAmount: earning.GrossAmount, OriginalRefundAmount: domain.Money{Amount: "0.00", Currency: "USD"},
		Reason: "not delivered", ReviewStatus: domain.DisputeOpened,
		EconomicState: domain.DisputeEconomicFrozen, OpenedAt: now, UpdatedAt: now,
	}
}

func TestOpenDisputeFreezesEarningAtomically(t *testing.T) {
	ctx := context.Background()
	s := New()
	earning := testDisputeEarning("job_1")
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}

	dispute, nextEarning, created, err := s.OpenDispute(ctx, "job_1", earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		if !exists {
			t.Fatal("expected earning to exist")
		}
		d := testDisputeFor("job_1", e)
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
	got, err := s.EarningByJob(ctx, "job_1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.EarningFrozen {
		t.Fatalf("stored earning status = %s, want frozen", got.Status)
	}
	storedDispute, err := s.DisputeByJob(ctx, "job_1")
	if err != nil {
		t.Fatal(err)
	}
	if storedDispute.ID != dispute.ID {
		t.Fatalf("dispute mismatch: %+v", storedDispute)
	}
}

// TestOpenDisputeIsIdempotentByJob proves a second OpenDispute call for the
// same job_id returns the existing dispute rather than creating a second
// one or re-invoking build.
func TestOpenDisputeIsIdempotentByJob(t *testing.T) {
	ctx := context.Background()
	s := New()
	earning := testDisputeEarning("job_2")
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}

	first, _, created, err := s.OpenDispute(ctx, "job_2", earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		d := testDisputeFor("job_2", e)
		next := e
		next.Status = domain.EarningFrozen
		return d, next, nil
	})
	if err != nil || !created {
		t.Fatalf("first open: created=%v err=%v", created, err)
	}

	buildCalled := false
	second, _, created, err := s.OpenDispute(ctx, "job_2", earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
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

// TestOpenDisputeConcurrentOpenersConvergeToOne proves 8+ concurrent
// OpenDispute callers for the same job_id converge to exactly one created
// dispute, even against the memory store's single coarse mutex (matching
// what the Postgres UNIQUE(job_id) constraint + advisory lock guarantee
// under real concurrency).
func TestOpenDisputeConcurrentOpenersConvergeToOne(t *testing.T) {
	ctx := context.Background()
	s := New()
	earning := testDisputeEarning("job_concurrent")
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}

	const attempts = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	creators := 0
	ids := make(map[string]bool)

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, _, created, err := s.OpenDispute(ctx, "job_concurrent", earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
				dd := testDisputeFor("job_concurrent", e)
				next := e
				next.Status = domain.EarningFrozen
				return dd, next, nil
			})
			if err != nil {
				t.Errorf("OpenDispute: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if created {
				creators++
			}
			ids[d.ID] = true
		}()
	}
	wg.Wait()
	if creators != 1 {
		t.Fatalf("creators = %d, want exactly 1", creators)
	}
	if len(ids) != 1 {
		t.Fatalf("observed %d distinct dispute ids, want exactly 1: %v", len(ids), ids)
	}
}

func TestUpdateDisputeRejectsIdentityFieldChange(t *testing.T) {
	ctx := context.Background()
	s := New()
	earning := testDisputeEarning("job_3")
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}
	dispute, _, _, err := s.OpenDispute(ctx, "job_3", earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		d := testDisputeFor("job_3", e)
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

func TestUpdateDisputeAllowsLifecycleFieldChanges(t *testing.T) {
	ctx := context.Background()
	s := New()
	earning := testDisputeEarning("job_4")
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}
	dispute, _, _, err := s.OpenDispute(ctx, "job_4", earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		d := testDisputeFor("job_4", e)
		next := e
		next.Status = domain.EarningFrozen
		return d, next, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	updated, err := s.UpdateDispute(ctx, dispute.ID, func(d domain.Dispute, exists bool) (domain.Dispute, error) {
		d.ReviewStatus = domain.DisputeUnderReview
		d.ReviewerID = "rev_1"
		return d, nil
	})
	if err != nil {
		t.Fatalf("UpdateDispute (lifecycle-only): %v", err)
	}
	if updated.ReviewStatus != domain.DisputeUnderReview {
		t.Fatalf("status = %s, want under_review", updated.ReviewStatus)
	}
}

// TestUpdateDisputeAndEarningFreezesLate exercises UpdateDisputeAndEarning's
// real remaining use -- DisputeService.reconcilePendingPayout's "freeze
// late" branch: a dispute opened while the earning's payout was ambiguous
// (PendingPayoutResolution) later observes the earning settled back to
// Available (the payout attempt never moved funds), and freezes it then,
// committing both the earning transition and the dispute's updated
// checkpoint in one transaction.
func TestUpdateDisputeAndEarningFreezesLate(t *testing.T) {
	ctx := context.Background()
	s := New()
	earning := testDisputeEarning("job_5")
	earning.Status = domain.EarningPayoutPending
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}
	dispute, _, _, err := s.OpenDispute(ctx, "job_5", earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		d := testDisputeFor("job_5", e)
		d.EconomicState = domain.DisputeEconomicPendingPayoutResolution
		return d, e, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if dispute.EconomicState != domain.DisputeEconomicPendingPayoutResolution {
		t.Fatalf("economic state = %s, want pending_payout_resolution", dispute.EconomicState)
	}

	// The payout attempt resolves to "no funds moved" -- the earning is
	// back to Available. Simulate that directly (UpdateEarning is the
	// earnings reconciler's own primitive, exercised elsewhere).
	if _, err := s.UpdateEarning(ctx, earning.ID, func(e domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		e.Status = domain.EarningAvailable
		return e, nil
	}); err != nil {
		t.Fatal(err)
	}

	updatedDispute, updatedEarning, err := s.UpdateDisputeAndEarning(ctx, dispute.ID, func(d domain.Dispute, e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		if !exists {
			t.Fatal("expected earning to exist")
		}
		if e.Status != domain.EarningAvailable {
			t.Fatalf("earning status = %s, want available", e.Status)
		}
		e.Status = domain.EarningFrozen
		d.EconomicState = domain.DisputeEconomicFrozen
		return d, e, nil
	})
	if err != nil {
		t.Fatalf("UpdateDisputeAndEarning: %v", err)
	}
	if updatedDispute.EconomicState != domain.DisputeEconomicFrozen {
		t.Fatalf("economic state = %s, want frozen", updatedDispute.EconomicState)
	}
	if updatedEarning.Status != domain.EarningFrozen {
		t.Fatalf("earning status = %s, want frozen", updatedEarning.Status)
	}
	storedEarning, err := s.EarningByJob(ctx, "job_5")
	if err != nil {
		t.Fatal(err)
	}
	if storedEarning.Status != domain.EarningFrozen {
		t.Fatalf("stored earning status = %s, want frozen", storedEarning.Status)
	}
}

// TestResolveDisputeCreditsExactlyOnce proves ResolveDispute's atomic
// three-way transaction (dispute + earning + account) commits a
// principal-win's earning reversal and account credit together, and that
// a retry (e.g. a duplicate client resolve call, or reconciler-style
// replay) does not credit a second time.
func TestResolveDisputeCreditsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	s := New()
	earning := testDisputeEarning("job_6")
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}
	dispute, _, _, err := s.OpenDispute(ctx, "job_6", earning.SettlementID, func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		d := testDisputeFor("job_6", e)
		next := e
		next.Status = domain.EarningFrozen
		return d, next, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	resolve := func() (domain.Dispute, domain.ProviderEarning, domain.Account, error) {
		return s.ResolveDispute(ctx, dispute.ID, "prn_1", domain.Account{PrincipalID: "prn_1"},
			func(d domain.Dispute, e domain.ProviderEarning, eExists bool, a domain.Account, aExists bool) (domain.Dispute, domain.ProviderEarning, domain.Account, error) {
				if d.EconomicState == domain.DisputeEconomicRefunded {
					return d, e, a, nil
				}
				if !eExists || e.Status != domain.EarningFrozen {
					t.Fatalf("earning not frozen: exists=%v status=%s", eExists, e.Status)
				}
				a.Balance = domain.Money{Amount: "1.05", Currency: "USD"}
				e.Status = domain.EarningReversed
				d.EconomicState = domain.DisputeEconomicRefunded
				d.ReviewStatus = domain.DisputeResolvedForPrincipal
				return d, e, a, nil
			})
	}

	firstDispute, firstEarning, firstAccount, err := resolve()
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	if firstDispute.EconomicState != domain.DisputeEconomicRefunded {
		t.Fatalf("economic state = %s, want refunded", firstDispute.EconomicState)
	}
	if firstEarning.Status != domain.EarningReversed {
		t.Fatalf("earning status = %s, want reversed", firstEarning.Status)
	}
	if firstAccount.Balance.Amount != "1.05" {
		t.Fatalf("balance = %s, want 1.05", firstAccount.Balance.Amount)
	}

	// A retry must not credit again.
	secondDispute, _, secondAccount, err := resolve()
	if err != nil {
		t.Fatalf("second (idempotent) resolve: %v", err)
	}
	if secondDispute.EconomicState != domain.DisputeEconomicRefunded {
		t.Fatalf("economic state after replay = %s, want refunded", secondDispute.EconomicState)
	}
	if secondAccount.Balance.Amount != "1.05" {
		t.Fatalf("balance after replay resolve = %s, want unchanged 1.05 (no double credit)", secondAccount.Balance.Amount)
	}
}
