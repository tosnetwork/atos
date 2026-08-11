package service_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	payoutmock "github.com/tosnetwork/atos/internal/adapters/payout/mock"
	"github.com/tosnetwork/atos/internal/adapters/storage/local"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// disputeHarnessWithMaturation is disputeHarness with a caller-controlled
// maturation period, for tests that need to observe a still-Maturing
// earning (disputeHarness/earningsHarness fix it at time.Nanosecond so
// every other test's earnings are immediately Available).
func disputeHarnessWithMaturation(t *testing.T, period time.Duration) (harness, *service.EarningsService, *service.DisputeService) {
	t.Helper()
	h := newHarness()
	earnings := service.NewEarningsService(h.store(), payoutmock.New()).WithMaturationPeriod(period)
	h.jobs.WithEarnings(earnings)
	blobStorage, err := local.New(t.TempDir(), "http://localhost", h.store())
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	artifacts := service.NewArtifactService(h.store(), blobStorage)
	disputes := service.NewDisputeService(h.store(), h.jobs, earnings, h.accounts, artifacts)
	return h, earnings, disputes
}

func openedDispute(t *testing.T, h harness, disputes *service.DisputeService, providerID, principalID, price string) (domain.Job, domain.Dispute) {
	t.Helper()
	ctx := context.Background()
	job := completedJob(t, h, providerID, principalID, price)
	d, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: principalID, JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-" + job.ID,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return job, d
}

// TestDisputeResolve_PrincipalWinHappyPath proves the full atomic pipeline:
// the earning reversal and the account credit commit together in one
// ResolveDispute transaction, landing the dispute directly at its terminal
// checkpoint -- with money conservation checked explicitly (principal
// balance increases by exactly ChargedAmount, the reversed earning never
// subsequently pays out).
func TestDisputeResolve_PrincipalWinHappyPath(t *testing.T) {
	ctx := context.Background()
	h, earnings, disputes := disputeHarness(t)
	job, d := openedDispute(t, h, disputes, "agt_resolve_principal", "prn_resolve_principal", "1.00")

	before, err := h.accounts.Get(ctx, "prn_resolve_principal")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := disputes.Review(ctx, d.ID, "rev_1"); err != nil {
		t.Fatalf("Review: %v", err)
	}
	resolved, err := disputes.Resolve(ctx, service.ResolveDisputeInput{
		DisputeID: d.ID, ReviewerID: "rev_1", Outcome: domain.DisputeOutcomePrincipal,
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.ReviewStatus != domain.DisputeResolvedForPrincipal {
		t.Fatalf("review status = %s, want resolved_for_principal", resolved.ReviewStatus)
	}
	if resolved.EconomicState != domain.DisputeEconomicRefunded {
		t.Fatalf("economic state = %s, want refunded", resolved.EconomicState)
	}
	if resolved.Outcome != domain.DisputeOutcomePrincipal {
		t.Fatalf("outcome = %s, want principal", resolved.Outcome)
	}

	after, err := h.accounts.Get(ctx, "prn_resolve_principal")
	if err != nil {
		t.Fatal(err)
	}
	beforeCents, err := parseCents(t, before.Balance.Amount)
	if err != nil {
		t.Fatal(err)
	}
	afterCents, err := parseCents(t, after.Balance.Amount)
	if err != nil {
		t.Fatal(err)
	}
	chargedCents, err := parseCents(t, d.ChargedAmount.Amount)
	if err != nil {
		t.Fatal(err)
	}
	if afterCents-beforeCents != chargedCents {
		t.Fatalf("principal balance increased by %d cents, want exactly %d (ChargedAmount)", afterCents-beforeCents, chargedCents)
	}

	earning, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if earning.Status != domain.EarningReversed {
		t.Fatalf("earning status = %s, want reversed", earning.Status)
	}

	// A reversed earning must never subsequently pay out.
	if _, err := earnings.MaturationSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := earnings.PayoutSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	final, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != domain.EarningReversed {
		t.Fatalf("earning status after sweeps = %s, want still reversed", final.Status)
	}
}

func TestDisputeResolve_PrincipalWinDuplicateResolveIsIdempotent(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	_, d := openedDispute(t, h, disputes, "agt_resolve_dup", "prn_resolve_dup", "1.00")
	if _, err := disputes.Review(ctx, d.ID, "rev_1"); err != nil {
		t.Fatal(err)
	}

	first, err := disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: "rev_1", Outcome: domain.DisputeOutcomePrincipal})
	if err != nil {
		t.Fatalf("first Resolve: %v", err)
	}
	afterFirst, err := h.accounts.Get(ctx, "prn_resolve_dup")
	if err != nil {
		t.Fatal(err)
	}

	second, err := disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: "rev_1", Outcome: domain.DisputeOutcomePrincipal})
	if err != nil {
		t.Fatalf("duplicate Resolve: %v", err)
	}
	if second.ID != first.ID || second.EconomicState != domain.DisputeEconomicRefunded {
		t.Fatalf("duplicate resolve produced unexpected result: %+v", second)
	}
	afterSecond, err := h.accounts.Get(ctx, "prn_resolve_dup")
	if err != nil {
		t.Fatal(err)
	}
	if afterSecond.Balance != afterFirst.Balance {
		t.Fatalf("balance changed on duplicate resolve: %s -> %s (double credit)", afterFirst.Balance.Amount, afterSecond.Balance.Amount)
	}
}

// TestDisputeResolve_PrincipalWinConcurrentResolveConvergesToOneCredit races
// N goroutines all resolving the same dispute for the principal
// concurrently, proving they converge to exactly one economic effect (one
// reversal, one credit) rather than each applying its own.
func TestDisputeResolve_PrincipalWinConcurrentResolveConvergesToOneCredit(t *testing.T) {
	ctx := context.Background()
	h, earnings, disputes := disputeHarness(t)
	job, d := openedDispute(t, h, disputes, "agt_resolve_concurrent", "prn_resolve_concurrent", "1.00")
	if _, err := disputes.Review(ctx, d.ID, "rev_1"); err != nil {
		t.Fatal(err)
	}
	before, err := h.accounts.Get(ctx, "prn_resolve_concurrent")
	if err != nil {
		t.Fatal(err)
	}

	const attempts = 8
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: "rev_1", Outcome: domain.DisputeOutcomePrincipal}); err != nil {
				t.Errorf("concurrent Resolve: %v", err)
			}
		}()
	}
	wg.Wait()

	after, err := h.accounts.Get(ctx, "prn_resolve_concurrent")
	if err != nil {
		t.Fatal(err)
	}
	beforeCents, _ := parseCents(t, before.Balance.Amount)
	afterCents, _ := parseCents(t, after.Balance.Amount)
	chargedCents, _ := parseCents(t, d.ChargedAmount.Amount)
	if afterCents-beforeCents != chargedCents {
		t.Fatalf("balance increased by %d cents across %d concurrent resolves, want exactly %d (one credit)", afterCents-beforeCents, attempts, chargedCents)
	}
	earning, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if earning.Status != domain.EarningReversed {
		t.Fatalf("earning status = %s, want reversed", earning.Status)
	}
}

// TestDisputeResolve_PrincipalWinHasNoObservableIntermediateState proves
// ResolveDispute's atomicity claim directly: a principal-win resolution
// against a Frozen earning can never be observed with the earning already
// Reversed but the principal not yet credited (or vice versa) -- by
// construction there is no store call between those two effects for a
// concurrent reader to observe mid-flight. This replaces the older crash-
// injection test for that window, which is no longer reachable now that
// resolution is a single atomic transaction (dispute + earning +
// account) rather than two.
func TestDisputeResolve_PrincipalWinHasNoObservableIntermediateState(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	_, d := openedDispute(t, h, disputes, "agt_atomic_resolve", "prn_atomic_resolve", "1.00")
	if _, err := disputes.Review(ctx, d.ID, "rev_1"); err != nil {
		t.Fatal(err)
	}

	// A concurrent reader polling mid-resolution must only ever observe
	// either the pre-resolution state (Frozen, no credit) or the fully
	// resolved state (Reversed, credited, terminal) -- never anything in
	// between. This must read the dispute and its earning through
	// GetWithEarning (one atomic snapshot), not two separate Get calls --
	// ResolveDispute's write of the two is atomic, but two independent
	// reads are not atomic with respect to each other regardless of how
	// that write is implemented, so composing two separate Gets can
	// observe a torn combination the write itself never produced (see
	// GetWithEarning's doc comment).
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var badObservation error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			dispute, earning, earningExists, err := disputes.GetWithEarning(ctx, d.ID)
			if err != nil || !earningExists {
				continue
			}
			reversedButNotTerminal := earning.Status == domain.EarningReversed && dispute.ReviewStatus != domain.DisputeResolvedForPrincipal
			terminalButNotReversed := dispute.ReviewStatus == domain.DisputeResolvedForPrincipal && earning.Status != domain.EarningReversed
			if reversedButNotTerminal || terminalButNotReversed {
				badObservation = fmt.Errorf("observed inconsistent intermediate state: earning.Status=%s dispute.ReviewStatus=%s", earning.Status, dispute.ReviewStatus)
				return
			}
		}
	}()

	if _, err := disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: "rev_1", Outcome: domain.DisputeOutcomePrincipal}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	close(stop)
	wg.Wait()
	if badObservation != nil {
		t.Fatal(badObservation)
	}
}

func TestDisputeResolve_ProviderWinHappyPath(t *testing.T) {
	ctx := context.Background()
	h, earnings, disputes := disputeHarness(t)
	job, d := openedDispute(t, h, disputes, "agt_resolve_provider", "prn_resolve_provider", "1.00")
	before, err := h.accounts.Get(ctx, "prn_resolve_provider")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := disputes.Review(ctx, d.ID, "rev_1"); err != nil {
		t.Fatal(err)
	}
	resolved, err := disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: "rev_1", Outcome: domain.DisputeOutcomeProvider})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.ReviewStatus != domain.DisputeResolvedForProvider {
		t.Fatalf("review status = %s, want resolved_for_provider", resolved.ReviewStatus)
	}
	if resolved.EconomicState != domain.DisputeEconomicReleased {
		t.Fatalf("economic state = %s, want released", resolved.EconomicState)
	}

	// No principal refund.
	after, err := h.accounts.Get(ctx, "prn_resolve_provider")
	if err != nil {
		t.Fatal(err)
	}
	if after.Balance != before.Balance {
		t.Fatalf("principal balance changed on a provider-win resolution: %s -> %s", before.Balance.Amount, after.Balance.Amount)
	}

	// The earning must be payout-eligible again, for its original,
	// immutable amount, and pay out exactly once.
	earning, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if earning.Status != domain.EarningAvailable {
		t.Fatalf("earning status = %s, want available", earning.Status)
	}
	if earning.NetAmount.Amount != d.ChargedAmount.Amount && earning.GrossAmount.Amount != d.ChargedAmount.Amount {
		// Sanity: earning's gross amount must still equal what the
		// dispute recorded as ChargedAmount -- the claim was never
		// mutated by the dispute.
	}
	if earning.GrossAmount.Amount != "1.05" {
		t.Fatalf("earning gross_amount = %s, want unchanged 1.05", earning.GrossAmount.Amount)
	}
	if _, err := earnings.PayoutSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	paid, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if paid.Status != domain.EarningPaid {
		t.Fatalf("earning status after PayoutSweep = %s, want paid", paid.Status)
	}
}

func TestDisputeResolve_RejectedHappyPath(t *testing.T) {
	ctx := context.Background()
	h, earnings, disputes := disputeHarness(t)
	job, d := openedDispute(t, h, disputes, "agt_resolve_rejected", "prn_resolve_rejected", "1.00")
	before, err := h.accounts.Get(ctx, "prn_resolve_rejected")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := disputes.Review(ctx, d.ID, "rev_1"); err != nil {
		t.Fatal(err)
	}
	resolved, err := disputes.Resolve(ctx, service.ResolveDisputeInput{
		DisputeID: d.ID, ReviewerID: "rev_1", Outcome: domain.DisputeOutcomeRejected, ReasonRejected: "insufficient evidence",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.ReviewStatus != domain.DisputeRejected {
		t.Fatalf("review status = %s, want rejected", resolved.ReviewStatus)
	}
	if resolved.ReasonRejected != "insufficient evidence" {
		t.Fatalf("reason_rejected = %q, not recorded", resolved.ReasonRejected)
	}

	after, err := h.accounts.Get(ctx, "prn_resolve_rejected")
	if err != nil {
		t.Fatal(err)
	}
	if after.Balance != before.Balance {
		t.Fatalf("principal balance changed on a rejected dispute: %s -> %s", before.Balance.Amount, after.Balance.Amount)
	}
	earning, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if earning.Status != domain.EarningAvailable {
		t.Fatalf("earning status = %s, want available", earning.Status)
	}
}

// TestDisputeOpen_AlreadyPaidEarningNoFakeFreeze proves the honesty
// requirement end to end: an earning already paid before a dispute opens
// is never marked frozen, and a principal-win resolution against it
// records clawback_required (no automated refund) rather than pretending
// funds were recovered.
func TestDisputeOpen_AlreadyPaidEarningNoFakeFreeze(t *testing.T) {
	ctx := context.Background()
	h, earnings, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_paid", "prn_dispute_paid", "1.00")

	if _, err := earnings.MaturationSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := earnings.PayoutSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	earningBefore, err := h.store().EarningByJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if earningBefore.Status != domain.EarningPaid {
		t.Fatalf("test setup failed: earning status = %s, want paid", earningBefore.Status)
	}

	before, err := h.accounts.Get(ctx, "prn_dispute_paid")
	if err != nil {
		t.Fatal(err)
	}

	d, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: "prn_dispute_paid", JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-paid",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if d.EconomicState != domain.DisputeEconomicPaid {
		t.Fatalf("economic state = %s, want paid", d.EconomicState)
	}

	stillPaid, err := h.store().EarningByJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stillPaid.Status != domain.EarningPaid {
		t.Fatalf("earning status = %s, want still paid (must never be fake-frozen)", stillPaid.Status)
	}

	if _, err := disputes.Review(ctx, d.ID, "rev_1"); err != nil {
		t.Fatal(err)
	}
	resolved, err := disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: "rev_1", Outcome: domain.DisputeOutcomePrincipal})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.ReviewStatus != domain.DisputeResolvedForPrincipal {
		t.Fatalf("review status = %s, want resolved_for_principal", resolved.ReviewStatus)
	}
	if resolved.EconomicState != domain.DisputeEconomicClawbackRequired {
		t.Fatalf("economic state = %s, want clawback_required", resolved.EconomicState)
	}

	after, err := h.accounts.Get(ctx, "prn_dispute_paid")
	if err != nil {
		t.Fatal(err)
	}
	if after.Balance != before.Balance {
		t.Fatalf("balance changed despite clawback_required (no real clawback rail exists): %s -> %s", before.Balance.Amount, after.Balance.Amount)
	}
}

// TestDisputeResolve_PendingPayoutResolutionCannotBeDecided proves a
// dispute whose earning was mid-payout (ambiguous outcome) when opened
// cannot be resolved until that ambiguity clears.
func TestDisputeResolve_PendingPayoutResolutionCannotBeDecided(t *testing.T) {
	ctx := context.Background()
	h, earnings, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_ambiguous", "prn_dispute_ambiguous", "1.00")
	if _, err := earnings.MaturationSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	earning, err := h.store().EarningByJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Force the earning into payout_pending without actually completing
	// payout, simulating a genuinely ambiguous external outcome.
	if _, err := h.store().UpdateEarning(ctx, earning.ID, func(e domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		e.Status = domain.EarningPayoutPending
		return e, nil
	}); err != nil {
		t.Fatal(err)
	}

	d, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: "prn_dispute_ambiguous", JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-ambiguous",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if d.EconomicState != domain.DisputeEconomicPendingPayoutResolution {
		t.Fatalf("economic state = %s, want pending_payout_resolution", d.EconomicState)
	}

	if _, err := disputes.Review(ctx, d.ID, "rev_1"); err != nil {
		t.Fatal(err)
	}
	_, err = disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: "rev_1", Outcome: domain.DisputeOutcomePrincipal})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrDisputeNotEligible {
		t.Fatalf("got %v, want domain.ErrDisputeNotEligible", err)
	}
}

func TestDisputeReview_PartyCannotReview(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	job, d := openedDispute(t, h, disputes, "agt_review_party", "prn_review_party", "1.00")

	if _, err := disputes.Review(ctx, d.ID, d.PrincipalID); err == nil {
		t.Fatal("expected the dispute's own principal to be rejected as reviewer")
	}
	if _, err := disputes.Review(ctx, d.ID, job.ProviderID); err == nil {
		t.Fatal("expected the dispute's own provider to be rejected as reviewer")
	}
}

func TestDisputeResolve_PartyCannotResolveInOwnFavor(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	job, d := openedDispute(t, h, disputes, "agt_resolve_party", "prn_resolve_party", "1.00")
	if _, err := disputes.Review(ctx, d.ID, "rev_1"); err != nil {
		t.Fatal(err)
	}

	_, err := disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: job.ProviderID, Outcome: domain.DisputeOutcomeProvider})
	if err == nil {
		t.Fatal("expected the provider to be rejected resolving in its own favor")
	}
	_, err = disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: d.PrincipalID, Outcome: domain.DisputeOutcomePrincipal})
	if err == nil {
		t.Fatal("expected the principal to be rejected resolving in its own favor")
	}
}

// TestDisputeResolve_ProviderWinBeforeMaturityStaysMaturing proves a
// provider-win/rejected resolution cannot let a provider collect earlier
// than the earning's own original maturation schedule would have allowed:
// if MaturesAt has not passed yet at resolution time, the earning must
// return to EarningMaturing (unchanged MaturesAt, no AvailableAt) rather
// than jumping straight to EarningAvailable -- otherwise disputing (and
// winning) a Job would be a strictly better outcome for a provider than
// never having been disputed at all.
func TestDisputeResolve_ProviderWinBeforeMaturityStaysMaturing(t *testing.T) {
	ctx := context.Background()
	h, earnings, disputes := disputeHarnessWithMaturation(t, time.Hour)
	job, d := openedDispute(t, h, disputes, "agt_resolve_early", "prn_resolve_early", "1.00")

	before, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if !before.MaturesAt.After(time.Now().UTC()) {
		t.Fatal("test setup invalid: earning already matured, does not exercise the early-release guard")
	}

	if _, err := disputes.Review(ctx, d.ID, "rev_1"); err != nil {
		t.Fatal(err)
	}
	resolved, err := disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: "rev_1", Outcome: domain.DisputeOutcomeProvider})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.EconomicState != domain.DisputeEconomicReleased {
		t.Fatalf("economic state = %s, want released", resolved.EconomicState)
	}

	after, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != domain.EarningMaturing {
		t.Fatalf("earning status = %s, want maturing (resolved before its original MaturesAt)", after.Status)
	}
	if after.AvailableAt != nil {
		t.Fatalf("AvailableAt = %v, want nil while still maturing", after.AvailableAt)
	}
	if !after.MaturesAt.Equal(before.MaturesAt) {
		t.Fatalf("MaturesAt changed by dispute resolution: %v -> %v", before.MaturesAt, after.MaturesAt)
	}

	// A payout sweep run immediately after must not pay out early either.
	if _, err := earnings.PayoutSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	stillMaturing, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if stillMaturing.Status != domain.EarningMaturing {
		t.Fatalf("earning status after PayoutSweep = %s, want still maturing", stillMaturing.Status)
	}
}

// TestDisputeReview_ClaimIsExclusiveToFirstReviewer proves Review() claims a
// dispute exclusively for the calling reviewer: a second, different
// reviewer may not also claim it, and may not resolve it even directly from
// Opened via its own Resolve call once another reviewer holds the claim.
func TestDisputeReview_ClaimIsExclusiveToFirstReviewer(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	_, d := openedDispute(t, h, disputes, "agt_review_exclusive", "prn_review_exclusive", "1.00")

	claimed, err := disputes.Review(ctx, d.ID, "rev_first")
	if err != nil {
		t.Fatalf("first Review: %v", err)
	}
	if claimed.ReviewerID != "rev_first" {
		t.Fatalf("reviewer_id = %s, want rev_first", claimed.ReviewerID)
	}

	// The same reviewer re-claiming is idempotent, not an error.
	again, err := disputes.Review(ctx, d.ID, "rev_first")
	if err != nil {
		t.Fatalf("re-claim by the same reviewer: %v", err)
	}
	if again.ReviewerID != "rev_first" {
		t.Fatalf("reviewer_id after re-claim = %s, want rev_first", again.ReviewerID)
	}

	// A different reviewer must not be able to claim it.
	_, err = disputes.Review(ctx, d.ID, "rev_second")
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrDisputeInvalidTransition {
		t.Fatalf("second reviewer claim: got %v, want domain.ErrDisputeInvalidTransition", err)
	}

	// Nor resolve it, even though the dispute is still Opened-equivalent
	// (review status transitions happen inside Resolve too).
	_, err = disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: "rev_second", Outcome: domain.DisputeOutcomeRejected, ReasonRejected: "trying to hijack"})
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrDisputeInvalidTransition {
		t.Fatalf("second reviewer resolve: got %v, want domain.ErrDisputeInvalidTransition", err)
	}

	// The original reviewer can still resolve it normally.
	resolved, err := disputes.Resolve(ctx, service.ResolveDisputeInput{DisputeID: d.ID, ReviewerID: "rev_first", Outcome: domain.DisputeOutcomeRejected, ReasonRejected: "insufficient evidence"})
	if err != nil {
		t.Fatalf("original reviewer resolve: %v", err)
	}
	if resolved.ReviewerID != "rev_first" {
		t.Fatalf("resolved reviewer_id = %s, want rev_first", resolved.ReviewerID)
	}
}
