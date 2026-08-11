package service_test

import (
	"context"
	"sync"
	"testing"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type failLiveEscrowReplayCore struct {
	toscore.Core
	mu   sync.Mutex
	gets int
}

func (c *failLiveEscrowReplayCore) GetEscrow(ctx context.Context, req toscore.GetEscrowRequest) (domain.Escrow, bool, error) {
	c.mu.Lock()
	c.gets++
	n := c.gets
	c.mu.Unlock()
	if n >= 2 {
		return domain.Escrow{}, false, domain.NewError(domain.ErrNetworkUnavailable, "injected live finality outage", true)
	}
	return c.Core.GetEscrow(ctx, req)
}

type countingProvider struct {
	tosai.Provider
	mu      sync.Mutex
	submits int
}

func (p *countingProvider) SubmitJob(ctx context.Context, req tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	p.mu.Lock()
	p.submits++
	p.mu.Unlock()
	return p.Provider.SubmitJob(ctx, req)
}

type failAfterEscrowGateProvider struct{ tosai.Provider }

func (failAfterEscrowGateProvider) SubmitJob(context.Context, tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	return tosai.SubmitJobResult{State: domain.JobFailed}, nil
}

func TestVerifiedJobPersistsFinalizedEscrowOperationBeforeExecution(t *testing.T) {
	quotes, _, core, st, cap := verifiedQuoteHarness(t)
	ctx := context.Background()
	quote, err := quotes.Create(ctx, service.CreateQuoteInput{PrincipalID: "requester", CapabilityID: cap.ID, RequestedTrustMode: domain.RequestedTrustVerified, IdempotencyKey: "phase4b2-quote"})
	if err != nil {
		t.Fatal(err)
	}
	jobs := service.NewJobService(st, failAfterEscrowGateProvider{tosaimock.NewContractFixture()}, core, service.NewAccountService(st))
	result, err := jobs.Invoke(ctx, service.SubmitInput{PrincipalID: "requester", CapabilityID: cap.ID, QuoteID: quote.ID, Input: map[string]any{"task": "phase4b2"}, IdempotencyKey: "phase4b2-job", MaxWaitMS: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if result.Job.ID == "" {
		t.Fatalf("Invoke returned no job: %+v", result)
	}
	op, err := st.GetEscrowOperation(ctx, result.Job.ID, domain.EscrowOperationReserve)
	if err != nil {
		t.Fatalf("operation missing for job=%+v: %v", result.Job, err)
	}
	if op.Checkpoint != domain.EscrowOperationCompleted || !op.Escrow.Finalized || op.Escrow.FinalizedCheckpoint == 0 || op.Escrow.Status != domain.EscrowReserved {
		t.Fatalf("unsafe reserve operation: %+v", op)
	}
	if result.Job.EscrowID != op.Escrow.ID {
		t.Fatalf("job escrow=%s authority escrow=%s", result.Job.EscrowID, op.Escrow.ID)
	}
}

func TestCompletedEscrowOperationCannotBeRegressedByStaleReplica(t *testing.T) {
	s := verifiedTaskEscrowCompletedOperation(t)
	got, err := s.store.UpdateEscrowOperation(context.Background(), s.jobID, domain.EscrowOperationReserve, func(op domain.EscrowOperation) (domain.EscrowOperation, error) {
		op.Checkpoint = domain.EscrowOperationReconciling
		op.LastError = "late network error"
		return op, nil
	})
	if err != nil || got.Checkpoint != domain.EscrowOperationCompleted || got.LastError != "" {
		t.Fatalf("terminal operation regressed: %+v err=%v", got, err)
	}
}

func TestVerifiedProviderFailureUsesFinalizedReleaseOperation(t *testing.T) {
	fixture := verifiedTaskEscrowCompletedOperation(t)
	op, err := fixture.fullStore.GetEscrowOperation(context.Background(), fixture.jobID, domain.EscrowOperationRelease)
	if err != nil {
		t.Fatal(err)
	}
	if op.Checkpoint != domain.EscrowOperationCompleted || op.Escrow.Status != domain.EscrowReleased || !op.Escrow.Finalized || op.Escrow.FinalizedCheckpoint == 0 {
		t.Fatalf("unsafe verified release: %+v", op)
	}
}

func TestVerifiedExecutionFailsClosedWhenLiveEscrowReplayUnavailable(t *testing.T) {
	quotes, _, base, st, cap := verifiedQuoteHarness(t)
	ctx := context.Background()
	q, err := quotes.Create(ctx, service.CreateQuoteInput{PrincipalID: "requester", CapabilityID: cap.ID, RequestedTrustMode: domain.RequestedTrustVerified, IdempotencyKey: "live-gate-quote"})
	if err != nil {
		t.Fatal(err)
	}
	core := &failLiveEscrowReplayCore{Core: base}
	provider := &countingProvider{Provider: tosaimock.NewContractFixture()}
	jobs := service.NewJobService(st, provider, core, service.NewAccountService(st))
	result, err := jobs.Invoke(ctx, service.SubmitInput{PrincipalID: "requester", CapabilityID: cap.ID, QuoteID: q.ID, Input: map[string]any{}, IdempotencyKey: "live-gate-job", MaxWaitMS: 100})
	if err == nil {
		t.Fatal("live authority outage allowed execution")
	}
	provider.mu.Lock()
	calls := provider.submits
	provider.mu.Unlock()
	if calls != 0 {
		t.Fatalf("provider submissions=%d", calls)
	}
	if result.Job.EconomicState != domain.EconomicEscrowReserved || !result.Job.ReconciliationRequired {
		t.Fatalf("job did not fail closed: %+v", result.Job)
	}
}

type completedEscrowFixture struct {
	store interface {
		UpdateEscrowOperation(context.Context, string, domain.EscrowOperationKind, func(domain.EscrowOperation) (domain.EscrowOperation, error)) (domain.EscrowOperation, error)
	}
	jobID     string
	fullStore interface {
		GetEscrowOperation(context.Context, string, domain.EscrowOperationKind) (domain.EscrowOperation, error)
	}
}

func verifiedTaskEscrowCompletedOperation(t *testing.T) completedEscrowFixture {
	t.Helper()
	quotes, _, core, st, cap := verifiedQuoteHarness(t)
	ctx := context.Background()
	q, err := quotes.Create(ctx, service.CreateQuoteInput{PrincipalID: "requester", CapabilityID: cap.ID, RequestedTrustMode: domain.RequestedTrustVerified, IdempotencyKey: "terminal-quote"})
	if err != nil {
		t.Fatal(err)
	}
	jobs := service.NewJobService(st, failAfterEscrowGateProvider{tosaimock.NewContractFixture()}, core, service.NewAccountService(st))
	result, err := jobs.Invoke(ctx, service.SubmitInput{PrincipalID: "requester", CapabilityID: cap.ID, QuoteID: q.ID, Input: map[string]any{}, IdempotencyKey: "terminal-job", MaxWaitMS: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return completedEscrowFixture{store: st, fullStore: st, jobID: result.Job.ID}
}
