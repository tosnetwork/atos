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

	dispute, nextEarning, created, err := s.OpenDispute(ctx, "job_1", func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
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

	first, _, created, err := s.OpenDispute(ctx, "job_2", func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		d := testDisputeFor("job_2", e)
		next := e
		next.Status = domain.EarningFrozen
		return d, next, nil
	})
	if err != nil || !created {
		t.Fatalf("first open: created=%v err=%v", created, err)
	}

	buildCalled := false
	second, _, created, err := s.OpenDispute(ctx, "job_2", func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
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
			d, _, created, err := s.OpenDispute(ctx, "job_concurrent", func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
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
	dispute, _, _, err := s.OpenDispute(ctx, "job_3", func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
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
	dispute, _, _, err := s.OpenDispute(ctx, "job_4", func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
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

func TestUpdateDisputeAndEarningCommitsBothTogether(t *testing.T) {
	ctx := context.Background()
	s := New()
	earning := testDisputeEarning("job_5")
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}
	dispute, _, _, err := s.OpenDispute(ctx, "job_5", func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		d := testDisputeFor("job_5", e)
		next := e
		next.Status = domain.EarningFrozen
		return d, next, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	updatedDispute, updatedEarning, err := s.UpdateDisputeAndEarning(ctx, dispute.ID, func(d domain.Dispute, e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		if !exists {
			t.Fatal("expected earning to exist")
		}
		if e.Status != domain.EarningFrozen {
			t.Fatalf("earning status = %s, want frozen", e.Status)
		}
		e.Status = domain.EarningReversed
		d.EconomicState = domain.DisputeEconomicRefundPending
		return d, e, nil
	})
	if err != nil {
		t.Fatalf("UpdateDisputeAndEarning: %v", err)
	}
	if updatedDispute.EconomicState != domain.DisputeEconomicRefundPending {
		t.Fatalf("economic state = %s, want refund_pending", updatedDispute.EconomicState)
	}
	if updatedEarning.Status != domain.EarningReversed {
		t.Fatalf("earning status = %s, want reversed", updatedEarning.Status)
	}
	storedEarning, err := s.EarningByJob(ctx, "job_5")
	if err != nil {
		t.Fatal(err)
	}
	if storedEarning.Status != domain.EarningReversed {
		t.Fatalf("stored earning status = %s, want reversed", storedEarning.Status)
	}
}

func TestUpdateDisputeAndAccountCreditsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	s := New()
	earning := testDisputeEarning("job_6")
	if _, _, err := s.CreateEarning(ctx, earning); err != nil {
		t.Fatal(err)
	}
	dispute, _, _, err := s.OpenDispute(ctx, "job_6", func(e domain.ProviderEarning, exists bool) (domain.Dispute, domain.ProviderEarning, error) {
		d := testDisputeFor("job_6", e)
		d.EconomicState = domain.DisputeEconomicRefundPending
		next := e
		next.Status = domain.EarningReversed
		return d, next, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	credit := func() (domain.Dispute, domain.Account, error) {
		return s.UpdateDisputeAndAccount(ctx, dispute.ID, "prn_1", domain.Account{PrincipalID: "prn_1"}, func(d domain.Dispute, exists bool, a domain.Account, aExists bool) (domain.Dispute, domain.Account, error) {
			if d.EconomicState == domain.DisputeEconomicRefunded {
				return d, a, nil
			}
			a.Balance = domain.Money{Amount: "1.05", Currency: "USD"}
			d.EconomicState = domain.DisputeEconomicRefunded
			d.ReviewStatus = domain.DisputeResolvedForPrincipal
			return d, a, nil
		})
	}

	first, account, err := credit()
	if err != nil {
		t.Fatalf("first credit: %v", err)
	}
	if first.EconomicState != domain.DisputeEconomicRefunded {
		t.Fatalf("economic state = %s, want refunded", first.EconomicState)
	}
	if account.Balance.Amount != "1.05" {
		t.Fatalf("balance = %s, want 1.05", account.Balance.Amount)
	}

	// A retry (e.g. reconciler replay after a crash) must not credit again.
	second, account2, err := credit()
	if err != nil {
		t.Fatalf("second (idempotent) credit: %v", err)
	}
	if second.EconomicState != domain.DisputeEconomicRefunded {
		t.Fatalf("economic state after replay = %s, want refunded", second.EconomicState)
	}
	if account2.Balance.Amount != "1.05" {
		t.Fatalf("balance after replay credit = %s, want unchanged 1.05 (no double credit)", account2.Balance.Amount)
	}
}
