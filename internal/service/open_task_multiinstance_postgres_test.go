package service_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

type openTaskReplica struct {
	store        *postgres.Store
	capabilities *service.CapabilityService
	jobs         *service.JobService
	openTasks    *service.OpenTaskService
}

func newOpenTaskReplica(t *testing.T, databaseURL string) openTaskReplica {
	t.Helper()
	ctx := context.Background()
	st, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	capabilities := service.NewCapabilityService(st)
	accounts := service.NewAccountService(st)
	quotes := service.NewQuoteService(st).WithAccountService(accounts)
	core := toscoremock.New(st)
	jobs := service.NewJobService(st, tosaimock.New(), core, accounts)
	openTasks := service.NewOpenTaskService(st, quotes, jobs)
	return openTaskReplica{store: st, capabilities: capabilities, jobs: jobs, openTasks: openTasks}
}

// TestOpenTaskService_TwoRealPostgresInstancesConvergeToOneAccept is Phase
// 3C's own version of the Roadmap's "N concurrent accepts from >=2
// independent Postgres-backed instances yield exactly one accepted
// proposal and one bound Job" success criterion (atos-spec
// docs/IMPLEMENTATION_ROADMAP.md §7.3), mirroring
// TestEarningsService_TwoRealPostgresInstancesConvergeToOnePayout's shape
// exactly: two independent *postgres.Store connections (two separate
// service stacks, not goroutines sharing one in-process store) race
// concurrent Accept calls for proposals from two different providers. This
// exercises the REAL quote -> job pipeline end to end, not a stub -- a bug
// in QuoteService/JobService wiring would surface here just as much as a
// bug in the acceptance journal itself.
func TestOpenTaskService_TwoRealPostgresInstancesConvergeToOneAccept(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres two-replica open task test")
	}
	ctx := context.Background()
	replicaA := newOpenTaskReplica(t, databaseURL)
	replicaB := newOpenTaskReplica(t, databaseURL)

	suffix := time.Now().UTC().Format("20060102T150405.000000000")
	principalID := "prn_pg_ot_" + suffix

	capA, err := replicaA.capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_pg_ot_a_" + suffix, Name: "t", Description: "t", DeliveryMode: domain.DeliveryInstant,
		InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-a-" + suffix,
	})
	if err != nil {
		t.Fatalf("register capability A: %v", err)
	}
	capB, err := replicaA.capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_pg_ot_b_" + suffix, Name: "t", Description: "t", DeliveryMode: domain.DeliveryInstant,
		InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "2.00", Currency: "USD"}},
		IdempotencyKey: "register-b-" + suffix,
	})
	if err != nil {
		t.Fatalf("register capability B: %v", err)
	}

	task, err := replicaA.openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: principalID, Title: "multi-instance accept race", Input: map[string]any{},
		ExpiresAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "publish-" + suffix,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p1, err := replicaA.openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: capA.ProviderID, TaskID: task.ID, CapabilityID: capA.ID, IdempotencyKey: "propose-a-" + suffix,
	})
	if err != nil {
		t.Fatalf("Propose A: %v", err)
	}
	p2, err := replicaB.openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: capB.ProviderID, TaskID: task.ID, CapabilityID: capB.ID, IdempotencyKey: "propose-b-" + suffix,
	})
	if err != nil {
		t.Fatalf("Propose B: %v", err)
	}

	const attempts = 12
	var wg sync.WaitGroup
	type outcome struct {
		task domain.OpenTask
		op   domain.AcceptanceOperation
		err  error
	}
	results := make(chan outcome, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		replica, proposalID := replicaA, p1.ID
		if i%2 == 1 {
			replica, proposalID = replicaB, p2.ID
		}
		go func(i int, replica openTaskReplica, proposalID string) {
			defer wg.Done()
			gotTask, op, err := replica.openTasks.Accept(ctx, service.AcceptProposalInput{
				PrincipalID: principalID, TaskID: task.ID, ProposalID: proposalID,
				IdempotencyKey: fmt.Sprintf("accept-%s-%d", suffix, i),
			})
			results <- outcome{gotTask, op, err}
		}(i, replica, proposalID)
	}
	wg.Wait()
	close(results)

	completed := 0
	var winnerProposalID, winnerQuoteID, winnerJobID string
	for r := range results {
		switch {
		case r.err == nil && r.op.Checkpoint == domain.AcceptanceCompleted:
			completed++
			winnerProposalID, winnerQuoteID, winnerJobID = r.op.ProposalID, r.op.QuoteID, r.op.JobID
		case r.err != nil:
			derr, ok := r.err.(*domain.Error)
			if !ok || (derr.Code != domain.ErrOpenTaskAcceptanceInProgress && derr.Code != domain.ErrOpenTaskNotOpen) {
				t.Fatalf("unexpected non-loser error: %v", r.err)
			}
		default:
			t.Fatalf("unexpected non-completed, non-error outcome: %+v", r)
		}
	}
	if completed != 1 {
		t.Fatalf("completed acceptances = %d, want exactly 1", completed)
	}
	if winnerQuoteID == "" || winnerJobID == "" {
		t.Fatal("winner did not bind a quote/job")
	}

	finalTask, err := replicaA.store.GetOpenTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetOpenTask: %v", err)
	}
	if finalTask.Status != domain.OpenTaskFulfilled {
		t.Fatalf("final status = %s, want fulfilled", finalTask.Status)
	}
	if finalTask.AcceptedProposalID != winnerProposalID || finalTask.BoundQuoteID != winnerQuoteID || finalTask.BoundJobID != winnerJobID {
		t.Fatalf("final task binding mismatch: %+v (want proposal=%s quote=%s job=%s)", finalTask, winnerProposalID, winnerQuoteID, winnerJobID)
	}

	losingProviderID := capB.ProviderID
	if winnerProposalID == p2.ID {
		losingProviderID = capA.ProviderID
	}
	losingJobs, err := replicaA.store.JobsByProvider(ctx, losingProviderID)
	if err != nil {
		t.Fatalf("JobsByProvider: %v", err)
	}
	if len(losingJobs) != 0 {
		t.Fatalf("losing provider has %d jobs, want 0: %+v", len(losingJobs), losingJobs)
	}
}
