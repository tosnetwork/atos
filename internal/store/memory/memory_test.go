package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// TestUpdateAccountConcurrentDebitsNeverOverspend guards Fix #1 from the
// Codex review referenced in ~/atos/review.codex.md: a separate Get+Put
// let two concurrent debits both observe the pre-debit balance. Run with
// -race to also confirm no data race on the underlying map.
func TestUpdateAccountConcurrentDebitsNeverOverspend(t *testing.T) {
	ctx := context.Background()
	s := New()
	seed := domain.Account{
		PrincipalID: "prn_test",
		Balance:     domain.Money{Amount: "10", Currency: "USD"},
	}
	debitAmount := 1 // $1 per debit, in whole dollars for simplicity
	attempts := 30   // more attempts than the balance can cover (only 10 should succeed)

	var wg sync.WaitGroup
	var succeeded int32
	var mu sync.Mutex

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.UpdateAccount(ctx, "prn_test", seed, func(a domain.Account, exists bool) (domain.Account, error) {
				balance := mustParseUSD(t, a.Balance.Amount)
				if balance < debitAmount {
					return domain.Account{}, store.ErrConflict // insufficient funds
				}
				a.Balance.Amount = formatUSD(balance - debitAmount)
				return a, nil
			})
			if err == nil {
				mu.Lock()
				succeeded++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if succeeded != 10 {
		t.Errorf("expected exactly 10 successful $1 debits from a $10 balance, got %d", succeeded)
	}

	final, err := s.GetAccount(ctx, "prn_test")
	if err != nil {
		t.Fatal(err)
	}
	if final.Balance.Amount != "0" {
		t.Errorf("final balance = %q, want exactly drained to 0", final.Balance.Amount)
	}
}

func mustParseUSD(t *testing.T, s string) int {
	t.Helper()
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Fatalf("test helper only supports whole-dollar amounts, got %q", s)
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func formatUSD(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// TestUpdateJobClaimIsExclusive guards Fix #2: only one caller may win a
// compare-and-swap claim on a job, even under concurrent attempts — the
// mechanism JobService.claimForExecution relies on to prevent a
// confirmed-reissue race from double-executing and double-charging.
func TestUpdateJobClaimIsExclusive(t *testing.T) {
	ctx := context.Background()
	s := New()
	job := domain.Job{ID: "job_test", State: domain.JobSubmitted}
	if err := s.PutJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	const attempts = 20
	var wg sync.WaitGroup
	var wins int32
	var mu sync.Mutex

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.UpdateJob(ctx, "job_test", func(j domain.Job, exists bool) (domain.Job, error) {
				if !exists || j.State != domain.JobSubmitted {
					return domain.Job{}, store.ErrConflict
				}
				j.State = domain.JobWorking
				return j, nil
			})
			if err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if wins != 1 {
		t.Errorf("expected exactly 1 caller to win the claim, got %d", wins)
	}

	final, err := s.GetJob(ctx, "job_test")
	if err != nil {
		t.Fatal(err)
	}
	if final.State != domain.JobWorking {
		t.Errorf("final state = %q, want %q", final.State, domain.JobWorking)
	}
}

// TestIdempotencyLifecycle guards Fix #3: a reservation must report
// InProgress while unfinished (so a concurrent duplicate retries instead
// of crashing on an empty response_key), resolve to Completed once
// Finished, and be fully forgettable via Release so a corrected retry
// after a pre-commit failure is not permanently blocked.
func TestIdempotencyLifecycle(t *testing.T) {
	ctx := context.Background()
	s := New()

	rec, ok, err := s.Reserve(ctx, "prn_a", "key1", "hash1", time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("first Reserve should succeed")
	}
	if rec.Status != store.IdempotencyInProgress {
		t.Errorf("status = %q, want in_progress", rec.Status)
	}

	rec2, ok2, err := s.Reserve(ctx, "prn_a", "key1", "hash1", time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if ok2 {
		t.Fatal("second Reserve of the same in-progress key should not succeed")
	}
	if rec2.Status != store.IdempotencyInProgress {
		t.Errorf("replay status = %q, want in_progress", rec2.Status)
	}

	if err := s.Finish(ctx, "prn_a", "key1", "job_123"); err != nil {
		t.Fatal(err)
	}

	rec3, ok3, err := s.Reserve(ctx, "prn_a", "key1", "hash1", time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if ok3 {
		t.Fatal("Reserve after Finish should still report not-ok (already claimed)")
	}
	if rec3.Status != store.IdempotencyCompleted || rec3.ResponseKey != "job_123" {
		t.Errorf("got status=%q response_key=%q, want completed/job_123", rec3.Status, rec3.ResponseKey)
	}

	if err := s.Release(ctx, "prn_a", "key1"); err != nil {
		t.Fatal(err)
	}
	_, ok4, err := s.Reserve(ctx, "prn_a", "key1", "hash1", time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !ok4 {
		t.Fatal("Reserve after Release should succeed again")
	}
}

func TestExpiredIdempotencyLeaseCanBeReclaimedOnlyBySameRequest(t *testing.T) {
	ctx := context.Background()
	s := New()
	past := time.Now().UTC().Add(-time.Minute)
	if _, ok, err := s.Reserve(ctx, "prn", "stale", "hash-a", past); err != nil || !ok {
		t.Fatalf("initial reserve = ok:%v err:%v", ok, err)
	}
	if rec, ok, err := s.Reserve(ctx, "prn", "stale", "hash-a", time.Now().UTC().Add(time.Minute)); err != nil || !ok || rec.RequestHash != "hash-a" {
		t.Fatalf("same request did not reclaim expired lease: rec=%+v ok=%v err=%v", rec, ok, err)
	}
	if rec, ok, err := s.Reserve(ctx, "prn", "stale", "hash-b", time.Now().UTC().Add(time.Minute)); err != nil || ok || rec.RequestHash != "hash-a" {
		t.Fatalf("different request reclaimed key: rec=%+v ok=%v err=%v", rec, ok, err)
	}
}
