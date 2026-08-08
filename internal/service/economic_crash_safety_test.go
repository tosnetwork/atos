package service_test

import (
	"context"
	"testing"
	"time"

	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/atos/internal/store/memory"
)

type loseCreateEscrowResponseCore struct {
	toscore.Core
	lost bool
}

func (c *loseCreateEscrowResponseCore) CreateEscrow(ctx context.Context, req toscore.CreateEscrowRequest) (domain.Escrow, error) {
	escrow, err := c.Core.CreateEscrow(ctx, req)
	if err == nil && !c.lost {
		c.lost = true
		return domain.Escrow{}, domain.NewError(domain.ErrNetworkUnavailable, "injected lost CreateEscrow response", true)
	}
	return escrow, err
}

type loseSettleResponseCore struct {
	toscore.Core
	lost bool
}

func (c *loseSettleResponseCore) SettleJob(ctx context.Context, req toscore.SettleJobRequest) (toscore.SettleJobResult, error) {
	result, err := c.Core.SettleJob(ctx, req)
	if err == nil && !c.lost {
		c.lost = true
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrNetworkUnavailable, "injected lost SettleJob response", true)
	}
	return result, err
}

func crashHarness(coreWrapper func(toscore.Core) toscore.Core) harness {
	st := memory.New()
	provider := tosaimock.New()
	base := toscoremock.New(st)
	core := toscore.Core(base)
	if coreWrapper != nil {
		core = coreWrapper(core)
	}
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)
	return harness{capabilities: capabilities, quotes: quotes, accounts: accounts, jobs: jobs, st: st}
}

func TestCrashRecoveryLostCreateEscrowResponseDoesNotDoubleDebit(t *testing.T) {
	ctx := context.Background()
	h := crashHarness(func(core toscore.Core) toscore.Core { return &loseCreateEscrowResponseCore{Core: core} })
	cap := registerCapability(t, h, "agt_crash_create", "1.00")
	quote := createQuote(t, h, cap.ID)
	result, err := h.jobs.Invoke(ctx, service.SubmitInput{PrincipalID: "prn_crash_create", CapabilityID: cap.ID, QuoteID: quote.ID, Input: map[string]any{"x": 1}, IdempotencyKey: "crash-create"})
	if err == nil {
		t.Fatal("injected lost CreateEscrow response unexpectedly returned success")
	}
	if result.Job.State != domain.JobReconciling || result.Job.EconomicState != domain.EconomicEscrowPending {
		t.Fatalf("checkpoint after lost CreateEscrow = state %q economic %q", result.Job.State, result.Job.EconomicState)
	}
	account, err := h.accounts.Get(ctx, "prn_crash_create")
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance.Amount != "23.95" {
		t.Fatalf("balance after ambiguous create = %s", account.Balance.Amount)
	}
	if _, err := h.st.EscrowByJob(ctx, result.Job.ID); err != nil {
		t.Fatalf("external escrow side effect was not recoverable: %v", err)
	}

	recovered, err := h.jobs.ReconcileJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatalf("ReconcileJob: %v", err)
	}
	if recovered.State != domain.JobCompleted || recovered.EconomicState != domain.EconomicSettled {
		t.Fatalf("recovered job = %+v", recovered)
	}
	account, err = h.accounts.Get(ctx, "prn_crash_create")
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance.Amount != "23.95" {
		t.Fatalf("recovery double-debited or refunded: %s", account.Balance.Amount)
	}
}

func TestCrashRecoveryLostSettlementResponseFinalizesExactlyOnce(t *testing.T) {
	ctx := context.Background()
	h := crashHarness(func(core toscore.Core) toscore.Core { return &loseSettleResponseCore{Core: core} })
	cap := registerCapability(t, h, "agt_crash_settle", "1.00")
	quote := createQuote(t, h, cap.ID)
	result, err := h.jobs.Invoke(ctx, service.SubmitInput{PrincipalID: "prn_crash_settle", CapabilityID: cap.ID, QuoteID: quote.ID, Input: map[string]any{"x": 2}, IdempotencyKey: "crash-settle"})
	if err != nil {
		t.Fatalf("Invoke should surface a durable reconciling job, got %v", err)
	}
	if result.Job.State != domain.JobReconciling || result.Job.EconomicState != domain.EconomicSettlementPending || result.Job.ExecutionReceipt == nil {
		t.Fatalf("settlement checkpoint missing after lost response: %+v", result.Job)
	}
	recovered, err := h.jobs.ReconcileJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatalf("ReconcileJob: %v", err)
	}
	if recovered.State != domain.JobCompleted || recovered.EconomicState != domain.EconomicSettled {
		t.Fatalf("recovered job = %+v", recovered)
	}
	for i := 0; i < 3; i++ {
		again, err := h.jobs.ReconcileJob(ctx, result.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if again.State != domain.JobCompleted {
			t.Fatalf("terminal replay changed state: %s", again.State)
		}
	}
	account, err := h.accounts.Get(ctx, "prn_crash_settle")
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance.Amount != "23.95" {
		t.Fatalf("settlement replay mutated balance twice: %s", account.Balance.Amount)
	}
}

func TestReconcilerRestoresDebitedJobBeforeEscrow(t *testing.T) {
	ctx := context.Background()
	h := crashHarness(nil)
	cap := registerCapability(t, h, "agt_crash_debit", "1.00")
	quote := createQuote(t, h, cap.ID)
	account, err := h.accounts.Get(ctx, "prn_crash_debit")
	if err != nil {
		t.Fatal(err)
	}
	account.Balance.Amount = "23.95"
	account.SpendPolicy.RemainingToday.Amount = "18.95"
	if err := h.st.PutAccount(ctx, account); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	job := domain.Job{ID: "job_crash_debit", CapabilityID: cap.ID, CapabilityVersion: cap.Version, ProviderID: cap.ProviderID, QuoteID: quote.ID, PrincipalID: "prn_crash_debit", TrustMode: quote.TrustMode, ProofProfile: quote.ProofProfile, State: domain.JobSubmitted, EconomicState: domain.EconomicDebited, Input: map[string]any{"x": 3}, IdempotencyKey: "crash-debit", CreatedAt: now, UpdatedAt: now, ExecutionDeadline: quote.ExecutionDeadline, ServiceQuoteID: quote.ServiceQuoteID, Artifacts: []domain.Artifact{}}
	if err := h.st.PutJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	recovered, err := h.jobs.ReconcileJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.State != domain.JobCompleted || recovered.EconomicState != domain.EconomicSettled {
		t.Fatalf("recovered job = %+v", recovered)
	}
	account, err = h.accounts.Get(ctx, job.PrincipalID)
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance.Amount != "23.95" {
		t.Fatalf("recovery repeated debit: %s", account.Balance.Amount)
	}
}

func TestReconcileScanFindsStaleEconomicJob(t *testing.T) {
	ctx := context.Background()
	h := crashHarness(nil)
	cap := registerCapability(t, h, "agt_scan", "1.00")
	quote := createQuote(t, h, cap.ID)
	account, _ := h.accounts.Get(ctx, "prn_scan")
	account.Balance.Amount = "23.95"
	account.SpendPolicy.RemainingToday.Amount = "18.95"
	_ = h.st.PutAccount(ctx, account)
	old := time.Now().UTC().Add(-2 * time.Minute)
	job := domain.Job{ID: "job_scan", CapabilityID: cap.ID, CapabilityVersion: cap.Version, ProviderID: cap.ProviderID, QuoteID: quote.ID, PrincipalID: "prn_scan", TrustMode: quote.TrustMode, ProofProfile: quote.ProofProfile, State: domain.JobSubmitted, EconomicState: domain.EconomicDebited, Input: map[string]any{}, IdempotencyKey: "scan", CreatedAt: old, UpdatedAt: old, ExecutionDeadline: quote.ExecutionDeadline, ServiceQuoteID: quote.ServiceQuoteID, Artifacts: []domain.Artifact{}}
	if err := h.st.PutJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	count, err := h.jobs.ReconcileStaleJobs(ctx, time.Now().UTC().Add(-30*time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("reconciled count = %d, want 1", count)
	}
	got, err := h.st.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.JobCompleted {
		t.Fatalf("stale job state = %s", got.State)
	}
}

var _ store.Store
