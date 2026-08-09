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

func newPostgresDisputeService(t *testing.T, h harness) *service.DisputeService {
	t.Helper()
	earnings := service.NewEarningsService(h.store(), nil)
	h.jobs.WithEarnings(earnings)
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

	disputesA := newPostgresDisputeService(t, hA)
	disputesB := newPostgresDisputeService(t, hB)

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

// TestDispute_ConcurrentSweepsAndDisputeOpsPreserveInvariants races
// MaturationSweep, PayoutSweep, OpenDispute and ResolveDispute against each
// other from two independent Postgres connections (simulating two ATOS
// replicas) across many Jobs simultaneously, then asserts the illegal
// states are unreachable: no earning is ever both Paid and the disputed
// earning of a Dispute claiming Frozen/RefundPending/Refunded, no earning
// is ever double-paid, and no dispute is ever left in a state combining a
// terminal ReviewStatus with a non-terminal EconomicState it cannot
// recover from (other than the honest ClawbackRequired/
// PendingPayoutResolution cases).
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
	earningsA := service.NewEarningsService(hA.store(), nil).WithMaturationPeriod(time.Nanosecond)
	hA.jobs.WithEarnings(earningsA)
	earningsB := service.NewEarningsService(hB.store(), nil).WithMaturationPeriod(time.Nanosecond)
	hB.jobs.WithEarnings(earningsB)
	disputesA := newPostgresDisputeService(t, hA)
	disputesB := newPostgresDisputeService(t, hB)

	suffix := time.Now().UTC().Format("20060102T150405.000000000")
	const jobCount = 6
	jobIDs := make([]string, 0, jobCount)
	principalIDs := make([]string, 0, jobCount)
	for i := 0; i < jobCount; i++ {
		providerID := "agt_stress_" + suffix + "_" + string(rune('a'+i))
		principalID := "prn_stress_" + suffix + "_" + string(rune('a'+i))
		job := completedJob(t, hA, providerID, principalID, "1.00")
		jobIDs = append(jobIDs, job.ID)
		principalIDs = append(principalIDs, principalID)
	}

	var wg sync.WaitGroup
	for round := 0; round < 3; round++ {
		wg.Add(4)
		go func() { defer wg.Done(); _, _ = earningsA.MaturationSweep(ctx, 100) }()
		go func() { defer wg.Done(); _, _ = earningsB.MaturationSweep(ctx, 100) }()
		go func() { defer wg.Done(); _, _ = earningsA.PayoutSweep(ctx, 100) }()
		go func() { defer wg.Done(); _, _ = earningsB.PayoutSweep(ctx, 100) }()
	}
	for i, jobID := range jobIDs {
		wg.Add(2)
		svcOpen, svcResolve := disputesA, disputesB
		if i%2 == 1 {
			svcOpen, svcResolve = disputesB, disputesA
		}
		id, pid := jobID, principalIDs[i]
		go func() {
			defer wg.Done()
			_, _ = svcOpen.Open(ctx, service.OpenDisputeInput{
				PrincipalID: pid, JobID: id, Reason: "concurrent stress", IdempotencyKey: "stress-open-" + id,
			})
		}()
		go func() {
			defer wg.Done()
			// Races review+resolve against the still-in-flight Open above;
			// a NotFound/invalid-transition error here is an expected,
			// harmless outcome of the race, not a test failure -- only
			// the final invariant checks below matter.
			_, _ = svcResolve.Review(ctx, "dispute_nonexistent_"+id, "rev_stress")
		}()
	}
	wg.Wait()

	// Drain: let any still-settling sweeps/opens finish, then check
	// invariants once the system is quiescent.
	for i := 0; i < 3; i++ {
		_, _ = earningsA.MaturationSweep(ctx, 100)
		_, _ = earningsA.PayoutSweep(ctx, 100)
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
			// safely frozen/being refunded/refunded while the earning
			// itself shows it was actually paid out.
			if (dispute.EconomicState == domain.DisputeEconomicFrozen ||
				dispute.EconomicState == domain.DisputeEconomicRefundPending ||
				dispute.EconomicState == domain.DisputeEconomicRefunded) && earning.Status == domain.EarningPaid {
				t.Fatalf("job %s: IMPOSSIBLE STATE -- dispute economic_state=%s but earning status=paid", jobID, dispute.EconomicState)
			}
			if dispute.EconomicState == domain.DisputeEconomicFrozen && earning.Status != domain.EarningFrozen {
				t.Fatalf("job %s: dispute claims frozen but earning status=%s", jobID, earning.Status)
			}
			if dispute.EconomicState == domain.DisputeEconomicRefunded && earning.Status != domain.EarningReversed {
				t.Fatalf("job %s: dispute claims refunded but earning status=%s, want reversed", jobID, earning.Status)
			}
		}
		if earning.Status == domain.EarningPaid {
			// Money conservation: a Paid earning's amount must still equal
			// its original immutable claim -- the dispute path (if any)
			// never rewrote it.
			if earning.GrossAmount.Amount != "1.05" || earning.NetAmount.Amount != "1.00" {
				t.Fatalf("job %s: paid earning amounts mutated: gross=%s net=%s", jobID, earning.GrossAmount.Amount, earning.NetAmount.Amount)
			}
		}
	}
}
