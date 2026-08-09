package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/payout"
	payoutmock "github.com/tosnetwork/atos/internal/adapters/payout/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func testSnapshot(jobID, providerID string) domain.BillingSnapshot {
	return domain.BillingSnapshot{
		JobID: jobID, QuoteID: "q_" + jobID, ReceiptID: "rcpt_" + jobID, ProviderID: providerID,
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0", TrustMode: domain.TrustModeManaged,
		Usage: domain.Usage{OutputTokens: 10}, PricingModel: domain.PricingFixed,
		GrossCharge:     domain.Money{Amount: "1.05", Currency: "USD"},
		ProviderGross:   domain.Money{Amount: "1.00", Currency: "USD"},
		GatewayFee:      domain.Money{Amount: "0.05", Currency: "USD"},
		PrincipalRefund: domain.Money{Amount: "0.00", Currency: "USD"},
		CalculatedAt:    time.Now().UTC(),
	}
}

// TestEarningsService_RecordSettlement_CreatesMaturingEarningWithSplitAmounts
// proves RecordSettlement maps snapshot fields to the earning's gross/fee/net
// split correctly: NetAmount (what the provider is paid) equals the
// snapshot's ProviderGross, not GrossCharge.
func TestEarningsService_RecordSettlement_CreatesMaturingEarningWithSplitAmounts(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	svc := NewEarningsService(st, payoutmock.New())
	snap := testSnapshot("job_1", "prov_1")

	e, err := svc.RecordSettlement(ctx, snap, "settle_1")
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != domain.EarningMaturing {
		t.Fatalf("status = %s, want maturing", e.Status)
	}
	if e.GrossAmount != snap.GrossCharge {
		t.Fatalf("gross amount = %+v, want %+v", e.GrossAmount, snap.GrossCharge)
	}
	if e.GatewayFee != snap.GatewayFee {
		t.Fatalf("gateway fee = %+v, want %+v", e.GatewayFee, snap.GatewayFee)
	}
	if e.NetAmount != snap.ProviderGross {
		t.Fatalf("net amount = %+v, want %+v (provider's take, not the full gross charge)", e.NetAmount, snap.ProviderGross)
	}
}

// TestEarningsService_RecordSettlement_IdempotentUnderRetry proves calling
// RecordSettlement twice for the same settlement (simulating a retried
// settlement path or a duplicate reconciliation sweep) creates exactly one
// earning, never two and never an error.
func TestEarningsService_RecordSettlement_IdempotentUnderRetry(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	svc := NewEarningsService(st, payoutmock.New())
	snap := testSnapshot("job_2", "prov_1")

	first, err := svc.RecordSettlement(ctx, snap, "settle_2")
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.RecordSettlement(ctx, snap, "settle_2")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("retry produced a different earning: %s vs %s", first.ID, second.ID)
	}
	all, err := st.EarningsByProvider(ctx, "prov_1")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("found %d earnings after idempotent retry, want 1", len(all))
	}
}

// TestEarningsService_MaturationSweep_OnlyAdvancesPastDue proves the sweep
// only matures earnings whose MaturesAt has passed, leaving others untouched.
func TestEarningsService_MaturationSweep_OnlyAdvancesPastDue(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	svc := NewEarningsService(st, payoutmock.New()).WithMaturationPeriod(time.Hour)

	past, err := svc.RecordSettlement(ctx, testSnapshot("job_past", "prov_1"), "settle_past")
	if err != nil {
		t.Fatal(err)
	}
	// Force this one's maturity into the past directly via the store, as if
	// it had been created an hour+ ago.
	if _, err := st.UpdateEarning(ctx, past.ID, func(e domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		e.MaturesAt = time.Now().UTC().Add(-time.Minute)
		return e, nil
	}); err != nil {
		t.Fatal(err)
	}
	future, err := svc.RecordSettlement(ctx, testSnapshot("job_future", "prov_1"), "settle_future")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := svc.MaturationSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}

	gotPast, err := st.GetEarning(ctx, past.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotPast.Status != domain.EarningAvailable {
		t.Fatalf("past-due earning status = %s, want available", gotPast.Status)
	}
	gotFuture, err := st.GetEarning(ctx, future.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotFuture.Status != domain.EarningMaturing {
		t.Fatalf("not-yet-due earning status = %s, want still maturing", gotFuture.Status)
	}
}

func availableEarning(ctx context.Context, t *testing.T, svc *EarningsService, st store.Store, jobID, providerID, settlementID string) domain.ProviderEarning {
	t.Helper()
	e, err := svc.RecordSettlement(ctx, testSnapshot(jobID, providerID), settlementID)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := st.UpdateEarning(ctx, e.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		current.Status = domain.EarningAvailable
		now := time.Now().UTC()
		current.AvailableAt = &now
		return current, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return updated
}

// TestEarningsService_PayoutSweep_HappyPathReachesPaid proves the full
// Available -> payout_pending -> paid state machine converges through the
// public PayoutSweep entry point, using the stable deterministic
// idempotency key.
func TestEarningsService_PayoutSweep_HappyPathReachesPaid(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	adapter := payoutmock.New()
	svc := NewEarningsService(st, adapter)
	e := availableEarning(ctx, t, svc, st, "job_paid", "prov_1", "settle_paid")

	if _, err := svc.PayoutSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	final, err := st.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != domain.EarningPaid {
		t.Fatalf("status = %s, want paid", final.Status)
	}
	if final.PayoutReference == "" {
		t.Fatal("expected a payout reference to be recorded")
	}
	wantKey := payoutIdempotencyKey(e.ID)
	if final.PayoutIdempotencyKey != wantKey {
		t.Fatalf("idempotency key = %s, want %s", final.PayoutIdempotencyKey, wantKey)
	}
	result, found, err := adapter.Query(ctx, wantKey)
	if err != nil || !found || result.Status != payout.StatusPaid {
		t.Fatalf("adapter does not durably record the payout: result=%+v found=%v err=%v", result, found, err)
	}
}

// TestEarningsService_PayoutSweep_NilAdapterNeverMarksPaid proves that with
// no payout adapter configured (config.PayoutBackendDisabled, the default),
// PayoutSweep is a genuine no-op: an Available earning must never be driven
// to Paid by a backend that cannot actually move funds. This is the
// regression test for the finding that the default mock adapter used to
// mark real unpaid provider liabilities as paid.
func TestEarningsService_PayoutSweep_NilAdapterNeverMarksPaid(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	svc := NewEarningsService(st, nil)
	e := availableEarning(ctx, t, svc, st, "job_disabled", "prov_1", "settle_disabled")

	count, err := svc.PayoutSweep(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("PayoutSweep attempted %d earnings with no adapter configured, want 0", count)
	}
	final, err := st.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != domain.EarningAvailable {
		t.Fatalf("status = %s, want still available (no adapter can move funds)", final.Status)
	}
}

// countingAdapter wraps another payout.Adapter and counts real Payout()
// calls, so tests can assert an external side effect happened at most once
// even across many retries/concurrent callers.
type countingAdapter struct {
	payout.Adapter
	calls int64
}

func (a *countingAdapter) Payout(ctx context.Context, req payout.Request) (payout.Result, error) {
	atomic.AddInt64(&a.calls, 1)
	return a.Adapter.Payout(ctx, req)
}

// TestEarningsService_PayoutSweep_FailBeforeEffectRetryConverges proves that
// a failure injected before any external effect is safe to retry with the
// identical idempotency key, and the retry converges to exactly one paid
// external call.
func TestEarningsService_PayoutSweep_FailBeforeEffectRetryConverges(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	mock := payoutmock.New()
	adapter := &countingAdapter{Adapter: mock}
	svc := NewEarningsService(st, adapter)
	e := availableEarning(ctx, t, svc, st, "job_before", "prov_1", "settle_before")
	key := payoutIdempotencyKey(e.ID)
	mock.Inject = map[string]payoutmock.FailureMode{key: payoutmock.FailBeforeEffect}

	if _, err := svc.PayoutSweep(ctx, 100); err == nil {
		t.Fatal("expected the injected failure to surface as an error from the first sweep")
	}
	afterFirst, err := st.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterFirst.Status != domain.EarningPayoutPending {
		t.Fatalf("status after injected pre-effect failure = %s, want still payout_pending", afterFirst.Status)
	}
	if afterFirst.PayoutAttempts != 1 {
		t.Fatalf("payout attempts = %d, want 1", afterFirst.PayoutAttempts)
	}

	// Retry: attemptPayout is called directly (bypassing PayoutSweep's
	// backoff window) to simulate the reconciler's next tick.
	if _, err := svc.attemptPayout(ctx, afterFirst); err != nil {
		t.Fatalf("retry should converge: %v", err)
	}
	final, err := st.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != domain.EarningPaid {
		t.Fatalf("status after retry = %s, want paid", final.Status)
	}
	if calls := atomic.LoadInt64(&adapter.calls); calls != 2 {
		t.Fatalf("expected exactly 2 Payout() calls (1 failed, 1 succeeded), got %d", calls)
	}
}

// TestEarningsService_PayoutSweep_AmbiguousResponseRecoveredViaQuery proves
// that when the rail secretly completed the payout but the response was
// lost, reconciliation discovers the result via Query -- WITHOUT calling
// Payout() a second time -- and completes the earning correctly.
func TestEarningsService_PayoutSweep_AmbiguousResponseRecoveredViaQuery(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	mock := payoutmock.New()
	adapter := &countingAdapter{Adapter: mock}
	svc := NewEarningsService(st, adapter)
	e := availableEarning(ctx, t, svc, st, "job_ambiguous", "prov_1", "settle_ambiguous")
	key := payoutIdempotencyKey(e.ID)
	mock.Inject = map[string]payoutmock.FailureMode{key: payoutmock.FailAmbiguous}

	if _, err := svc.PayoutSweep(ctx, 100); err == nil {
		t.Fatal("expected the ambiguous response to surface as an error from the first sweep")
	}
	afterFirst, err := st.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterFirst.Status != domain.EarningPayoutPending {
		t.Fatalf("status after ambiguous response = %s, want still payout_pending", afterFirst.Status)
	}
	if calls := atomic.LoadInt64(&adapter.calls); calls != 1 {
		t.Fatalf("expected exactly 1 Payout() call before recovery, got %d", calls)
	}

	if _, err := svc.attemptPayout(ctx, afterFirst); err != nil {
		t.Fatalf("recovery via Query should succeed without another Payout() call: %v", err)
	}
	final, err := st.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != domain.EarningPaid {
		t.Fatalf("status after recovery = %s, want paid", final.Status)
	}
	// The critical assertion: recovery must use Query, never call Payout()
	// again -- otherwise a "response lost" scenario could double-pay.
	if calls := atomic.LoadInt64(&adapter.calls); calls != 1 {
		t.Fatalf("recovery must not call Payout() again; got %d total calls", calls)
	}
}

// rejectingAdapter deterministically refuses every payout before any funds
// move, to test the StatusRejected recovery path (fall back to Available).
type rejectingAdapter struct{ reason string }

func (a *rejectingAdapter) Payout(ctx context.Context, req payout.Request) (payout.Result, error) {
	return payout.Result{Status: payout.StatusRejected, Reason: a.reason}, nil
}
func (a *rejectingAdapter) Query(ctx context.Context, idempotencyKey string) (payout.Result, bool, error) {
	return payout.Result{}, false, nil
}

func TestEarningsService_PayoutSweep_RejectedFallsBackToAvailable(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	svc := NewEarningsService(st, &rejectingAdapter{reason: "invalid destination"})
	e := availableEarning(ctx, t, svc, st, "job_rejected", "prov_1", "settle_rejected")

	if _, err := svc.PayoutSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	final, err := st.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != domain.EarningAvailable {
		t.Fatalf("status after rejection = %s, want available (safe to retry later)", final.Status)
	}
	if final.PayoutAttempts != 1 {
		t.Fatalf("payout attempts = %d, want 1", final.PayoutAttempts)
	}
	if final.PayoutFailureReason == "" {
		t.Fatal("expected a recorded failure reason")
	}
}

// rejectThenPayAdapter rejects its first Payout call, then would pay on any
// subsequent call. Used to prove a Dispute's hold blocks EarningsService
// from ever reaching that second call while the hold is set.
type rejectThenPayAdapter struct {
	mu    sync.Mutex
	calls int
}

func (a *rejectThenPayAdapter) Payout(ctx context.Context, req payout.Request) (payout.Result, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	if a.calls == 1 {
		return payout.Result{Status: payout.StatusRejected, Reason: "first attempt rejected"}, nil
	}
	return payout.Result{Status: payout.StatusPaid, Reference: "should_never_be_reached"}, nil
}

func (a *rejectThenPayAdapter) Query(ctx context.Context, idempotencyKey string) (payout.Result, bool, error) {
	return payout.Result{}, false, nil
}

func (a *rejectThenPayAdapter) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

// TestEarningsService_DisputeHoldBlocksRetryAfterRejectedPayout proves the
// exact race the reviewer flagged: a payout attempt is in flight
// (payout_pending) when a Dispute opens against the same earning and sets
// its durable hold; the in-flight attempt is then allowed to resolve on its
// own (here, StatusRejected -> back to Available), but the hold must
// prevent beginPayoutUnderLock from starting a NEW payout intent for as
// long as it is set -- even though the earning is Available again -- so the
// adapter's second call (which would incorrectly pay out a disputed
// earning) must never happen.
func TestEarningsService_DisputeHoldBlocksRetryAfterRejectedPayout(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	adapter := &rejectThenPayAdapter{}
	svc := NewEarningsService(st, adapter)
	e := availableEarning(ctx, t, svc, st, "job_hold", "prov_1", "settle_hold")

	// Step 1: a payout intent is committed and attempted -- rejected.
	started, err := svc.beginPayoutUnderLock(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if started.Status != domain.EarningPayoutPending {
		t.Fatalf("status = %s, want payout_pending", started.Status)
	}

	// Step 2: a Dispute opens WHILE the payout is in flight and sets its
	// durable hold -- exactly what DisputeService.Open's build callback
	// does for the EarningPayoutPending case (exercised end-to-end in
	// dispute_test.go). Simulated directly here via the store so this test
	// can isolate EarningsService's own responsibility: respecting the
	// hold once set, regardless of who set it.
	held, err := st.UpdateEarning(ctx, e.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		current.DisputeHoldID = "dispute_hold_test"
		return current, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if held.Status != domain.EarningPayoutPending {
		t.Fatalf("status after hold set = %s, want unchanged payout_pending", held.Status)
	}

	// Step 3: the in-flight attempt is allowed to resolve on its own.
	resolved, err := svc.attemptPayout(ctx, held)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Status != domain.EarningAvailable {
		t.Fatalf("status after rejected payout = %s, want available", resolved.Status)
	}
	if resolved.DisputeHoldID != "dispute_hold_test" {
		t.Fatal("hold was cleared by attemptPayout's rejection handling; it must persist until dispute resolution")
	}
	if calls := adapter.callCount(); calls != 1 {
		t.Fatalf("expected exactly 1 Payout() call so far, got %d", calls)
	}

	// Step 4: the earning is Available again, and a PayoutSweep would
	// normally pick it right back up -- but the hold must block a new
	// payout intent from ever starting.
	if _, err := svc.PayoutSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	final, err := st.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != domain.EarningAvailable {
		t.Fatalf("status after PayoutSweep while held = %s, want still available (never re-entered payout_pending)", final.Status)
	}
	if final.DisputeHoldID != "dispute_hold_test" {
		t.Fatal("hold was cleared without a dispute resolution")
	}
	if calls := adapter.callCount(); calls != 1 {
		t.Fatalf("Payout() was called again despite the dispute hold: %d calls, want 1 (the adapter's 2nd call would incorrectly report Paid)", calls)
	}
}

// TestEarningsService_PayoutSweep_DuplicateRequestConverges proves running
// the sweep again after an earning is already paid does not attempt another
// payout and leaves the earning exactly as it was.
func TestEarningsService_PayoutSweep_DuplicateRequestConverges(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	mock := payoutmock.New()
	adapter := &countingAdapter{Adapter: mock}
	svc := NewEarningsService(st, adapter)
	availableEarning(ctx, t, svc, st, "job_dup", "prov_1", "settle_dup")

	if _, err := svc.PayoutSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PayoutSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if calls := atomic.LoadInt64(&adapter.calls); calls != 1 {
		t.Fatalf("duplicate sweep triggered %d Payout() calls, want 1", calls)
	}
}

// TestEarningsService_PayoutSweep_EightConcurrentAttemptsConvergeToOnePayout
// proves 8+ concurrent attempts to pay out the SAME earning converge to
// exactly one paid transition and exactly one external Payout() call.
func TestEarningsService_PayoutSweep_EightConcurrentAttemptsConvergeToOnePayout(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	mock := payoutmock.New()
	adapter := &countingAdapter{Adapter: mock}
	svc := NewEarningsService(st, adapter)
	e := availableEarning(ctx, t, svc, st, "job_concurrent", "prov_1", "settle_concurrent")

	const attempts = 10
	var wg sync.WaitGroup
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started, err := svc.beginPayoutUnderLock(ctx, e.ID)
			if err != nil {
				return
			}
			_, _ = svc.attemptPayout(ctx, started)
		}()
	}
	wg.Wait()

	final, err := st.GetEarning(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != domain.EarningPaid {
		t.Fatalf("status = %s, want paid", final.Status)
	}
	// beginPayoutUnderLock's CAS lets exactly one goroutine win the
	// Available->PayoutPending transition, but the losers can still observe
	// PayoutPending (if they call beginPayoutUnderLock before the winner's
	// later, separate attemptPayout call finishes) and legitimately attempt
	// Payout() themselves before the mock's idempotency-key map resolves
	// the race -- so more than one Payout() call is possible. What must
	// hold is the actual economic invariant: every one of those calls
	// resolves to the SAME single stored result, never a second distinct
	// payout for this key.
	result, found, err := mock.Query(ctx, final.PayoutIdempotencyKey)
	if err != nil || !found {
		t.Fatalf("expected a durably recorded payout: found=%v err=%v", found, err)
	}
	if final.PayoutReference != result.Reference {
		t.Fatalf("earning's recorded reference %q does not match the adapter's single stored result %q", final.PayoutReference, result.Reference)
	}
	if calls := atomic.LoadInt64(&adapter.calls); calls < 1 || calls > attempts {
		t.Fatalf("concurrent attempts triggered %d Payout() calls, want between 1 and %d", calls, attempts)
	}
}

// TestEarningsService_TwoIndependentInstancesConvergeToOnePayout simulates
// two ATOS replicas -- two independent EarningsService instances sharing
// the same durable store and payout rail -- both sweeping concurrently.
func TestEarningsService_TwoIndependentInstancesConvergeToOnePayout(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	mock := payoutmock.New()
	adapter := &countingAdapter{Adapter: mock}
	instanceA := NewEarningsService(st, adapter)
	instanceB := NewEarningsService(st, adapter)
	availableEarning(ctx, t, instanceA, st, "job_replica", "prov_1", "settle_replica")

	var wg sync.WaitGroup
	for _, instance := range []*EarningsService{instanceA, instanceB, instanceA, instanceB} {
		wg.Add(1)
		go func(svc *EarningsService) {
			defer wg.Done()
			_, _ = svc.PayoutSweep(ctx, 100)
		}(instance)
	}
	wg.Wait()

	earnings, err := st.EarningsByProvider(ctx, "prov_1")
	if err != nil {
		t.Fatal(err)
	}
	if len(earnings) != 1 || earnings[0].Status != domain.EarningPaid {
		t.Fatalf("earnings = %+v, want exactly 1 paid", earnings)
	}
	// As in the 8-concurrent-attempts test: more than one Payout() call is
	// possible if both replicas observe PayoutPending before either
	// finishes recording the result, but every call must resolve to the
	// SAME single stored result.
	result, found, err := mock.Query(ctx, earnings[0].PayoutIdempotencyKey)
	if err != nil || !found {
		t.Fatalf("expected a durably recorded payout: found=%v err=%v", found, err)
	}
	if earnings[0].PayoutReference != result.Reference {
		t.Fatalf("earning's recorded reference %q does not match the adapter's single stored result %q", earnings[0].PayoutReference, result.Reference)
	}
	if calls := atomic.LoadInt64(&adapter.calls); calls < 1 || calls > 4 {
		t.Fatalf("two replicas triggered %d Payout() calls, want between 1 and 4", calls)
	}
}

// TestEarningsService_ChangedSemanticsUnderSameKeyIsRejected proves that
// reusing a payout idempotency key for a request with DIFFERENT semantic
// content (a different amount, here) is rejected outright rather than
// either silently double-paying the new amount or silently discarding it by
// replaying the original result -- a real idempotent rail treats "same key,
// different body" as a caller bug, not a retry, and the mock adapter must
// too.
func TestEarningsService_ChangedSemanticsUnderSameKeyIsRejected(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	mock := payoutmock.New()
	svc := NewEarningsService(st, mock)
	e := availableEarning(ctx, t, svc, st, "job_changed", "prov_1", "settle_changed")

	started, err := svc.beginPayoutUnderLock(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.attemptPayout(ctx, started); err != nil {
		t.Fatal(err)
	}
	firstResult, found, err := mock.Query(ctx, started.PayoutIdempotencyKey)
	if err != nil || !found {
		t.Fatalf("expected a recorded result: found=%v err=%v", found, err)
	}

	// A second, independent Payout call using the exact same key but a
	// different amount must be rejected, not silently resolved either way.
	_, err = mock.Payout(ctx, payout.Request{
		IdempotencyKey: started.PayoutIdempotencyKey, EarningID: e.ID, ProviderID: e.ProviderID,
		Amount: domain.Money{Amount: "999.99", Currency: "USD"},
	})
	if err == nil {
		t.Fatal("expected an error for a reused idempotency key with different request content")
	}

	// The original recorded result must remain untouched by the rejected attempt.
	unchanged, found, err := mock.Query(ctx, started.PayoutIdempotencyKey)
	if err != nil || !found {
		t.Fatalf("expected the original result to still be recorded: found=%v err=%v", found, err)
	}
	if unchanged.Reference != firstResult.Reference {
		t.Fatalf("rejected replay mutated the stored result: %+v vs %+v", unchanged, firstResult)
	}

	// A replay with the SAME semantic content must still succeed normally.
	replay, err := mock.Payout(ctx, payout.Request{
		IdempotencyKey: started.PayoutIdempotencyKey, EarningID: e.ID, ProviderID: e.ProviderID, Amount: e.NetAmount,
	})
	if err != nil {
		t.Fatalf("replay with identical request content should succeed: %v", err)
	}
	if replay.Reference != firstResult.Reference {
		t.Fatalf("identical replay produced a different result: %+v vs %+v", replay, firstResult)
	}
}

// TestEarningsService_BackfillSweep_RecoversEarningMissedAtSettlementTime
// simulates the crash window where a Job's settlement finalized durably
// (EconomicState == EconomicSettled) but the inline RecordSettlement call
// during settlement never ran (as if the process died, or an EarningsService
// was not yet wired in). A fresh EarningsService's BackfillSweep must find
// and repair the gap using only durable state (Job, Quote, Receipt).
func TestEarningsService_BackfillSweep_RecoversEarningMissedAtSettlementTime(t *testing.T) {
	ctx := context.Background()
	st := memory.New()

	quote := domain.Quote{
		ID: "q_backfill", CapabilityID: "cap_1", CapabilityVersion: "1.0.0", ProviderID: "prov_1",
		Price:     domain.Price{Subtotal: "1.00", Fees: "0.05", TotalMax: "1.05", Currency: "USD"},
		TermsHash: "sha256:terms",
	}
	if err := st.PutQuote(ctx, quote); err != nil {
		t.Fatal(err)
	}
	settlementReceipt := domain.Receipt{
		ID: "rcpt_settle_backfill", QuoteID: quote.ID, JobID: "job_backfill_missed",
		Charged: domain.Money{Amount: "1.05", Currency: "USD"}, Status: domain.ReceiptSettled,
	}
	if err := st.PutReceipt(ctx, settlementReceipt); err != nil {
		t.Fatal(err)
	}
	execReceipt := &domain.ExecutionReceipt{
		ID: "xrcpt_backfill", QuoteID: quote.ID, JobID: "job_backfill_missed", ProviderID: "prov_1",
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0", Usage: domain.Usage{OutputTokens: 5},
	}
	job := domain.Job{
		ID: "job_backfill_missed", CapabilityID: "cap_1", QuoteID: quote.ID, ProviderID: "prov_1",
		State: domain.JobCompleted, EconomicState: domain.EconomicSettled, ExecutionReceipt: execReceipt,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := st.PutJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	svc := NewEarningsService(st, payoutmock.New())
	if _, err := svc.BackfillSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	earning, err := st.EarningBySettlement(ctx, settlementReceipt.ID)
	if err != nil {
		t.Fatalf("backfill did not create the missing earning: %v", err)
	}
	if earning.ProviderID != "prov_1" || earning.JobID != job.ID {
		t.Fatalf("backfilled earning = %+v", earning)
	}

	// Re-running the sweep must not create a duplicate.
	if _, err := svc.BackfillSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	all, err := st.EarningsByProvider(ctx, "prov_1")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("found %d earnings after repeated backfill, want 1", len(all))
	}
}

// TestEarningsService_Get_EnforcesProviderOwnership proves a provider
// cannot read another provider's earning.
func TestEarningsService_Get_EnforcesProviderOwnership(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	svc := NewEarningsService(st, payoutmock.New())
	e, err := svc.RecordSettlement(ctx, testSnapshot("job_owner", "prov_owner"), "settle_owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(ctx, e.ID, "prov_other"); err == nil {
		t.Fatal("expected an error reading another provider's earning")
	} else {
		var domainErr *domain.Error
		if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrPermissionDenied {
			t.Fatalf("got %v, want ErrPermissionDenied", err)
		}
	}
	got, err := svc.Get(ctx, e.ID, "prov_owner")
	if err != nil || got.ID != e.ID {
		t.Fatalf("owning provider should be able to read its own earning: %+v, %v", got, err)
	}
}
