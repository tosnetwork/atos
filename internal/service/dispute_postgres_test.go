// Integration tests against a real Postgres — skipped unless
// ATOS_TEST_DATABASE_URL is set. Run with:
//
//	ATOS_TEST_DATABASE_URL="postgres://user@localhost:5432/atos_test?sslmode=disable" go test ./internal/service/... -run TestDispute
package service_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	payoutmock "github.com/tosnetwork/atos/internal/adapters/payout/mock"
	"github.com/tosnetwork/atos/internal/adapters/storage/local"
	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

// newPostgresHarness builds the same harness job_test.go's newHarness does,
// but backed by a real, independent postgres.Store connection -- used to
// simulate a separate ATOS replica, not a single in-process store.
func newPostgresHarness(t *testing.T, databaseURL string) (harness, func()) {
	t.Helper()
	st, err := postgres.Open(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	provider := tosaimock.New()
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)
	return harness{capabilities: capabilities, quotes: quotes, accounts: accounts, jobs: jobs, st: st}, st.Close
}

// newPostgresDisputeService wires a DisputeService against h and the
// caller-supplied EarningsService, exactly as cmd/api/main.go does --
// callers control the EarningsService (and its payout adapter) themselves
// so tests can wire a real payout adapter when they need one.
func newPostgresDisputeService(t *testing.T, h harness, earnings *service.EarningsService) *service.DisputeService {
	t.Helper()
	blobStorage, err := local.New(t.TempDir(), "http://localhost", h.store())
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	artifacts := service.NewArtifactService(h.store(), blobStorage)
	return service.NewDisputeService(h.store(), h.jobs, earnings, h.accounts, artifacts)
}

// TestDisputeOpen_TwoIndependentPostgresInstancesConvergeToOne is the
// dispute-workflow analog of
// TestEarningsService_TwoRealPostgresInstancesConvergeToOnePayout: two
// independent postgres.Store connections (simulating two ATOS replicas)
// each drive their own DisputeService, both racing Open for the SAME Job
// concurrently. The database-level UNIQUE(job_id) constraint plus the
// advisory transaction lock in OpenDispute must ensure exactly one dispute
// (and exactly one frozen earning) results, regardless of which replica's
// request happened to arrive at either process.
func TestDisputeOpen_TwoIndependentPostgresInstancesConvergeToOne(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres two-replica dispute test")
	}
	ctx := context.Background()

	hA, closeA := newPostgresHarness(t, databaseURL)
	defer closeA()
	hB, closeB := newPostgresHarness(t, databaseURL)
	defer closeB()
	earningsA := service.NewEarningsService(hA.store(), nil)
	hA.jobs.WithEarnings(earningsA)
	earningsB := service.NewEarningsService(hB.store(), nil)
	hB.jobs.WithEarnings(earningsB)

	disputesA := newPostgresDisputeService(t, hA, earningsA)
	disputesB := newPostgresDisputeService(t, hB, earningsB)

	suffix := time.Now().UTC().Format("20060102T150405.000000000")
	providerID := "agt_pg_replica_dispute_" + suffix
	principalID := "prn_pg_replica_dispute_" + suffix

	job := completedJob(t, hA, providerID, principalID, "1.00")

	const attempts = 10
	var wg sync.WaitGroup
	ids := make(chan string, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		svc := disputesA
		if i%2 == 1 {
			svc = disputesB
		}
		go func(svc *service.DisputeService, i int) {
			defer wg.Done()
			d, err := svc.Open(ctx, service.OpenDisputeInput{
				PrincipalID: principalID, JobID: job.ID, Reason: "not delivered",
				IdempotencyKey: "dispute-replica-" + suffix + "-" + string(rune('a'+i)),
			})
			if err != nil {
				t.Errorf("Open (replica %d): %v", i, err)
				return
			}
			ids <- d.ID
		}(svc, i)
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]bool)
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("observed %d distinct dispute ids across two replicas, want exactly 1: %v", len(seen), seen)
	}

	earning, err := hA.store().EarningByJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if earning.Status != domain.EarningFrozen {
		t.Fatalf("earning status = %s, want frozen", earning.Status)
	}
	disputesFound, err := hA.store().DisputesByPrincipal(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if len(disputesFound) != 1 {
		t.Fatalf("found %d dispute rows, want exactly 1", len(disputesFound))
	}
}

// TestDisputeOpen_RacesRealPayoutSweepExactlyOneOutcome races OpenDispute
// against a full, real EarningsService.PayoutSweep (Available ->
// PayoutPending -> Query/Payout -> Paid through a real, shared mock payout
// adapter -- not merely a simulated status CAS) for the same earning, from
// two independent Postgres connections, repeated across many freshly
// created Jobs. The only two legitimate outcomes are: the dispute won the
// row lock first and froze the earning before any payout could begin, or
// the payout won first and completed for real, in which case the dispute
// must observe DisputeEconomicPaid -- never DisputeEconomicFrozen paired
// with an actually-Paid earning.
func TestDisputeOpen_RacesRealPayoutSweepExactlyOneOutcome(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres real-payout-vs-dispute race test")
	}
	ctx := context.Background()

	hA, closeA := newPostgresHarness(t, databaseURL)
	defer closeA()
	hB, closeB := newPostgresHarness(t, databaseURL)
	defer closeB()

	sharedAdapter := &pgCountingAdapter{inner: payoutmock.New()}
	earningsA := service.NewEarningsService(hA.store(), sharedAdapter).WithMaturationPeriod(time.Nanosecond)
	hA.jobs.WithEarnings(earningsA)
	earningsB := service.NewEarningsService(hB.store(), sharedAdapter).WithMaturationPeriod(time.Nanosecond)
	hB.jobs.WithEarnings(earningsB)
	disputesA := newPostgresDisputeService(t, hA, earningsA)

	const iterations = 15
	frozenCount, paidCount := 0, 0
	for i := 0; i < iterations; i++ {
		suffix := time.Now().UTC().Format("20060102T150405.000000000") + "_" + string(rune('a'+i))
		providerID := "agt_payoutrace_" + suffix
		principalID := "prn_payoutrace_" + suffix
		job := completedJob(t, hA, providerID, principalID, "1.00")
		if _, err := earningsA.MaturationSweep(ctx, 100); err != nil {
			t.Fatalf("iteration %d: MaturationSweep: %v", i, err)
		}

		var wg sync.WaitGroup
		var openErr, sweepErr error
		var dispute domain.Dispute
		wg.Add(2)
		go func() {
			defer wg.Done()
			dispute, openErr = disputesA.Open(ctx, service.OpenDisputeInput{
				PrincipalID: principalID, JobID: job.ID, Reason: "payout race",
				IdempotencyKey: "payoutrace-open-" + job.ID,
			})
		}()
		go func() {
			defer wg.Done()
			_, sweepErr = earningsB.PayoutSweep(ctx, 100)
		}()
		wg.Wait()
		if openErr != nil {
			t.Fatalf("iteration %d: Open: %v", i, openErr)
		}
		// sweepErr is deliberately not treated as fatal: if the dispute won
		// the row lock first and froze the earning, beginPayoutUnderLock's
		// own CAS correctly reports store.ErrConflict (the earning is no
		// longer Available) -- an expected, benign outcome of this race,
		// not a bug. The invariant checks below are the real assertion.
		_ = sweepErr

		// PayoutSweep is itself two separate commits (beginPayoutUnderLock:
		// Available->PayoutPending, then attemptPayout: ...->Paid) with a
		// real window between them, so OpenDispute racing it can
		// legitimately observe PayoutPending, not just the two endpoints --
		// reconcile until that resolves, exactly as the production
		// reconciler would.
		if dispute.EconomicState == domain.DisputeEconomicPendingPayoutResolution {
			for attempt := 0; attempt < 10 && dispute.EconomicState == domain.DisputeEconomicPendingPayoutResolution; attempt++ {
				_, _ = earningsA.PayoutSweep(ctx, 100)
				var reconcileErr error
				dispute, reconcileErr = disputesA.ReconcileDispute(ctx, dispute.ID)
				if reconcileErr != nil {
					t.Fatalf("iteration %d: ReconcileDispute: %v", i, reconcileErr)
				}
			}
			if dispute.EconomicState == domain.DisputeEconomicPendingPayoutResolution {
				t.Fatalf("iteration %d: dispute never resolved out of pending_payout_resolution", i)
			}
		}

		finalEarning, err := hA.store().EarningByJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("iteration %d: EarningByJob: %v", i, err)
		}

		switch dispute.EconomicState {
		case domain.DisputeEconomicFrozen:
			frozenCount++
			if finalEarning.Status != domain.EarningFrozen {
				t.Fatalf("iteration %d: dispute froze but final earning status = %s, want frozen", i, finalEarning.Status)
			}
		case domain.DisputeEconomicPaid:
			paidCount++
			if finalEarning.Status != domain.EarningPaid {
				t.Fatalf("iteration %d: dispute observed paid but final earning status = %s, want paid", i, finalEarning.Status)
			}
		default:
			t.Fatalf("iteration %d: unexpected economic state %s (want frozen or paid after reconciliation)", i, dispute.EconomicState)
		}
		// The impossible combination this test exists to rule out,
		// checked explicitly regardless of which branch above matched.
		if dispute.EconomicState == domain.DisputeEconomicFrozen && finalEarning.Status == domain.EarningPaid {
			t.Fatalf("iteration %d: IMPOSSIBLE STATE -- dispute claims frozen but earning was actually paid", i)
		}
	}
	t.Logf("payout-vs-dispute race over %d iterations: dispute won %d, payout won %d", iterations, frozenCount, paidCount)
}

// TestDisputeResolve_TwoReplicasConflictingOutcomesExactlyOneWins races two
// independent Postgres connections resolving the SAME dispute with
// deliberately conflicting outcomes (principal-win vs provider-win)
// concurrently. ResolveDispute's row lock on the dispute must serialize
// them: exactly one succeeds with its own requested outcome, and the
// other observes the now-terminal dispute already committed to a
// different outcome and is rejected outright -- never silently
// no-opping into the winner's outcome, and never producing a split
// economic effect (e.g. both a principal credit AND an available earning).
func TestDisputeResolve_TwoReplicasConflictingOutcomesExactlyOneWins(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres two-replica conflicting-resolve test")
	}
	ctx := context.Background()

	hA, closeA := newPostgresHarness(t, databaseURL)
	defer closeA()
	hB, closeB := newPostgresHarness(t, databaseURL)
	defer closeB()
	earningsA := service.NewEarningsService(hA.store(), nil)
	hA.jobs.WithEarnings(earningsA)
	earningsB := service.NewEarningsService(hB.store(), nil)
	hB.jobs.WithEarnings(earningsB)
	disputesA := newPostgresDisputeService(t, hA, earningsA)
	disputesB := newPostgresDisputeService(t, hB, earningsB)

	suffix := time.Now().UTC().Format("20060102T150405.000000000")
	providerID := "agt_conflict_" + suffix
	principalID := "prn_conflict_" + suffix
	job := completedJob(t, hA, providerID, principalID, "1.00")

	dispute, err := disputesA.Open(ctx, service.OpenDisputeInput{
		PrincipalID: principalID, JobID: job.ID, Reason: "conflicting resolve race",
		IdempotencyKey: "conflict-open-" + job.ID,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := disputesA.Review(ctx, dispute.ID, "rev_conflict"); err != nil {
		t.Fatalf("Review: %v", err)
	}
	before, err := hA.accounts.Get(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var principalErr, providerErr error
	var principalResult, providerResult domain.Dispute
	wg.Add(2)
	go func() {
		defer wg.Done()
		principalResult, principalErr = disputesA.Resolve(ctx, service.ResolveDisputeInput{
			DisputeID: dispute.ID, ReviewerID: "rev_conflict", Outcome: domain.DisputeOutcomePrincipal,
		})
	}()
	go func() {
		defer wg.Done()
		providerResult, providerErr = disputesB.Resolve(ctx, service.ResolveDisputeInput{
			DisputeID: dispute.ID, ReviewerID: "rev_conflict", Outcome: domain.DisputeOutcomeProvider,
		})
	}()
	wg.Wait()

	successes := 0
	var winningOutcome domain.DisputeOutcome
	if principalErr == nil {
		successes++
		winningOutcome = principalResult.Outcome
	}
	if providerErr == nil {
		successes++
		winningOutcome = providerResult.Outcome
	}
	if successes != 1 {
		t.Fatalf("expected exactly one of two conflicting concurrent resolves to succeed, got %d (principalErr=%v providerErr=%v)", successes, principalErr, providerErr)
	}

	final, err := hA.store().GetDispute(ctx, dispute.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Outcome != winningOutcome {
		t.Fatalf("stored dispute outcome = %s, want %s (the winner)", final.Outcome, winningOutcome)
	}

	after, err := hA.accounts.Get(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	finalEarning, err := hA.store().EarningByJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}

	if winningOutcome == domain.DisputeOutcomePrincipal {
		if final.EconomicState != domain.DisputeEconomicRefunded {
			t.Fatalf("economic state = %s, want refunded", final.EconomicState)
		}
		if finalEarning.Status != domain.EarningReversed {
			t.Fatalf("earning status = %s, want reversed", finalEarning.Status)
		}
		beforeCents, _ := parseCents(t, before.Balance.Amount)
		afterCents, _ := parseCents(t, after.Balance.Amount)
		chargedCents, _ := parseCents(t, dispute.ChargedAmount.Amount)
		if afterCents-beforeCents != chargedCents {
			t.Fatalf("principal balance increased by %d cents, want exactly %d", afterCents-beforeCents, chargedCents)
		}
	} else {
		if final.EconomicState != domain.DisputeEconomicReleased {
			t.Fatalf("economic state = %s, want released", final.EconomicState)
		}
		if finalEarning.Status != domain.EarningAvailable {
			t.Fatalf("earning status = %s, want available", finalEarning.Status)
		}
		if after.Balance != before.Balance {
			t.Fatalf("principal balance changed despite a provider-win resolution: %s -> %s", before.Balance.Amount, after.Balance.Amount)
		}
	}
}

// TestDispute_ConcurrentSweepsAndDisputeOpsPreserveInvariants races
// MaturationSweep, PayoutSweep, Review and Resolve against each other from
// two independent Postgres connections (simulating two ATOS replicas)
// across many already-opened disputes simultaneously, using a real (mock)
// payout adapter, then asserts the illegal states are unreachable: no
// earning is ever both Paid and the disputed earning of a Dispute claiming
// Frozen/Refunded, and a Paid earning's amounts are never mutated by any
// dispute path.
func TestDispute_ConcurrentSweepsAndDisputeOpsPreserveInvariants(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres dispute/sweep concurrency stress test")
	}
	ctx := context.Background()

	hA, closeA := newPostgresHarness(t, databaseURL)
	defer closeA()
	hB, closeB := newPostgresHarness(t, databaseURL)
	defer closeB()
	sharedAdapter := &pgCountingAdapter{inner: payoutmock.New()}
	earningsA := service.NewEarningsService(hA.store(), sharedAdapter).WithMaturationPeriod(time.Nanosecond)
	hA.jobs.WithEarnings(earningsA)
	earningsB := service.NewEarningsService(hB.store(), sharedAdapter).WithMaturationPeriod(time.Nanosecond)
	hB.jobs.WithEarnings(earningsB)
	disputesA := newPostgresDisputeService(t, hA, earningsA)
	disputesB := newPostgresDisputeService(t, hB, earningsB)

	suffix := time.Now().UTC().Format("20060102T150405.000000000")
	const jobCount = 6
	jobIDs := make([]string, 0, jobCount)
	disputeIDs := make([]string, 0, jobCount)
	for i := 0; i < jobCount; i++ {
		providerID := "agt_stress_" + suffix + "_" + string(rune('a'+i))
		principalID := "prn_stress_" + suffix + "_" + string(rune('a'+i))
		job := completedJob(t, hA, providerID, principalID, "1.00")
		jobIDs = append(jobIDs, job.ID)
		// Even-indexed jobs stay undisputed (so sweeps can race them to
		// Paid); odd-indexed jobs are disputed and put under review up
		// front, so the concurrent phase below exercises real Resolve
		// calls racing real sweeps, not a resolve against a nonexistent ID.
		if i%2 == 1 {
			d, err := disputesA.Open(ctx, service.OpenDisputeInput{
				PrincipalID: principalID, JobID: job.ID, Reason: "concurrent stress", IdempotencyKey: "stress-open-" + job.ID,
			})
			if err != nil {
				t.Fatalf("Open(%s): %v", job.ID, err)
			}
			if _, err := disputesA.Review(ctx, d.ID, "rev_stress"); err != nil {
				t.Fatalf("Review(%s): %v", d.ID, err)
			}
			disputeIDs = append(disputeIDs, d.ID)
		}
	}

	var wg sync.WaitGroup
	wg.Add(4)
	go func() { defer wg.Done(); _, _ = earningsA.MaturationSweep(ctx, 100) }()
	go func() { defer wg.Done(); _, _ = earningsB.MaturationSweep(ctx, 100) }()
	go func() { defer wg.Done(); _, _ = earningsA.PayoutSweep(ctx, 100) }()
	go func() { defer wg.Done(); _, _ = earningsB.PayoutSweep(ctx, 100) }()
	for i, disputeID := range disputeIDs {
		wg.Add(1)
		svc := disputesA
		outcome := domain.DisputeOutcomeProvider
		if i%2 == 1 {
			svc = disputesB
			outcome = domain.DisputeOutcomeRejected
		}
		id, oc := disputeID, outcome
		go func() {
			defer wg.Done()
			// A dispute_not_eligible error here (still
			// pending_payout_resolution because a sweep won a race first)
			// is an expected, harmless outcome, not a test failure -- only
			// the final invariant checks below matter.
			_, _ = svc.Resolve(ctx, service.ResolveDisputeInput{DisputeID: id, ReviewerID: "rev_stress", Outcome: oc})
		}()
	}
	wg.Wait()

	// Drain: let any still-ambiguous payout resolve, then reconcile
	// disputes left in pending_payout_resolution, so the final invariant
	// checks below observe a quiescent, converged system.
	for i := 0; i < 3; i++ {
		_, _ = earningsA.PayoutSweep(ctx, 100)
		for _, disputeID := range disputeIDs {
			_, _ = disputesA.ReconcileDispute(ctx, disputeID)
		}
	}

	for _, jobID := range jobIDs {
		earning, err := hA.store().EarningByJob(ctx, jobID)
		if err != nil {
			t.Fatalf("EarningByJob(%s): %v", jobID, err)
		}
		dispute, err := hA.store().DisputeByJob(ctx, jobID)
		hasDispute := err == nil
		if hasDispute {
			// The impossible combination: dispute claims the earning is
			// safely frozen/reversed while the earning itself shows it was
			// actually paid out.
			if (dispute.EconomicState == domain.DisputeEconomicFrozen ||
				dispute.EconomicState == domain.DisputeEconomicRefunded) && earning.Status == domain.EarningPaid {
				t.Fatalf("job %s: IMPOSSIBLE STATE -- dispute economic_state=%s but earning status=paid", jobID, dispute.EconomicState)
			}
			if dispute.EconomicState == domain.DisputeEconomicFrozen && earning.Status != domain.EarningFrozen {
				t.Fatalf("job %s: dispute claims frozen but earning status=%s", jobID, earning.Status)
			}
			if dispute.EconomicState == domain.DisputeEconomicRefunded && earning.Status != domain.EarningReversed {
				t.Fatalf("job %s: dispute claims refunded but earning status=%s, want reversed", jobID, earning.Status)
			}
			if dispute.EconomicState == domain.DisputeEconomicReleased && earning.Status != domain.EarningAvailable && earning.Status != domain.EarningPaid {
				t.Fatalf("job %s: dispute claims released but earning status=%s", jobID, earning.Status)
			}
		}
		if earning.Status == domain.EarningPaid {
			// Money conservation: a Paid earning's amount must still equal
			// its original immutable claim -- no dispute path ever rewrote
			// it.
			if earning.GrossAmount.Amount != "1.05" || earning.NetAmount.Amount != "1.00" {
				t.Fatalf("job %s: paid earning amounts mutated: gross=%s net=%s", jobID, earning.GrossAmount.Amount, earning.NetAmount.Amount)
			}
		}
	}
}
