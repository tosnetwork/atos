package service_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/atos/internal/store/memory"
	"github.com/tosnetwork/atos/internal/store/postgres"
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

type loseReleaseResponseCore struct {
	toscore.Core
	lost bool
}

func (c *loseReleaseResponseCore) ReleaseEscrow(ctx context.Context, req toscore.ReleaseEscrowRequest) (toscore.ReleaseEscrowResult, error) {
	result, err := c.Core.ReleaseEscrow(ctx, req)
	if err == nil && !c.lost {
		c.lost = true
		return toscore.ReleaseEscrowResult{}, domain.NewError(domain.ErrNetworkUnavailable, "injected lost ReleaseEscrow response", true)
	}
	return result, err
}

type terminalFailureProvider struct{ tosai.Provider }

func (p *terminalFailureProvider) GetJob(ctx context.Context, jobID string) (tosai.SubmitJobResult, error) {
	return tosai.SubmitJobResult{}, domain.NewError(domain.ErrNotFound, "injected provider job not found", false)
}

func (p *terminalFailureProvider) SubmitJob(ctx context.Context, req tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	return tosai.SubmitJobResult{State: domain.JobFailed}, nil
}

type unavailableProvider struct{ tosai.Provider }

func (p *unavailableProvider) GetJob(ctx context.Context, jobID string) (tosai.SubmitJobResult, error) {
	return tosai.SubmitJobResult{}, domain.NewError(domain.ErrNetworkUnavailable, "injected provider unavailable after restart", true)
}

func (p *unavailableProvider) SubmitJob(ctx context.Context, req tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	return tosai.SubmitJobResult{}, domain.NewError(domain.ErrNetworkUnavailable, "injected provider unavailable after restart", true)
}

func TestCrashRecoveryLostReleaseResponseRefundsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	baseProvider := tosaimock.New()
	provider := &terminalFailureProvider{Provider: baseProvider}
	baseCore := toscoremock.New(st)
	core := &loseReleaseResponseCore{Core: baseCore}
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)
	h := harness{capabilities: capabilities, quotes: quotes, accounts: accounts, jobs: jobs, st: st}

	cap := registerCapability(t, h, "agt_crash_release", "1.00")
	quote := createQuote(t, h, cap.ID)
	result, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_crash_release", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"release": true}, IdempotencyKey: "crash-release",
	})
	if err != nil {
		t.Fatalf("Invoke should leave a durable reconciliation record, got %v", err)
	}
	if result.Job.State != domain.JobReconciling || result.Job.EconomicState != domain.EconomicReleasePending {
		t.Fatalf("release checkpoint after lost response = state %q economic %q", result.Job.State, result.Job.EconomicState)
	}
	account, err := h.accounts.Get(ctx, "prn_crash_release")
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance.Amount != "23.95" {
		t.Fatalf("ambiguous release credited before confirmation: %s", account.Balance.Amount)
	}

	recovered, err := h.jobs.ReconcileJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatalf("ReconcileJob: %v", err)
	}
	if recovered.State != domain.JobFailed || recovered.EconomicState != domain.EconomicReleased {
		t.Fatalf("release recovery did not reach failed/released: %+v", recovered)
	}
	account, err = h.accounts.Get(ctx, "prn_crash_release")
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance.Amount != "25.00" {
		t.Fatalf("release recovery did not restore exact balance: %s", account.Balance.Amount)
	}
	for i := 0; i < 3; i++ {
		again, err := h.jobs.ReconcileJob(ctx, result.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if again.State != domain.JobFailed || again.EconomicState != domain.EconomicReleased {
			t.Fatalf("terminal release replay changed state: %+v", again)
		}
	}
	account, err = h.accounts.Get(ctx, "prn_crash_release")
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance.Amount != "25.00" {
		t.Fatalf("repeated release recovery double-credited balance: %s", account.Balance.Amount)
	}
}

func TestPostgresRestartRecoversPersistedSettlementWithoutProvider(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres crash-recovery test")
	}
	ctx := context.Background()
	st, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	baseProvider := tosaimock.New()
	baseCore := toscoremock.New(st)
	losingCore := &loseSettleResponseCore{Core: baseCore}
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, baseProvider, losingCore, accounts)
	h := harness{capabilities: capabilities, quotes: quotes, accounts: accounts, jobs: jobs, st: st}

	suffix := time.Now().UTC().Format("20060102T150405.000000000")
	cap := registerCapability(t, h, "agt_pg_restart_"+suffix, "1.00")
	quote := createQuote(t, h, cap.ID)
	principalID := "prn_pg_restart_" + suffix
	result, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: principalID, CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"restart": true}, IdempotencyKey: "pg-restart-" + suffix,
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Job.State != domain.JobReconciling || result.Job.EconomicState != domain.EconomicSettlementPending {
		t.Fatalf("expected durable settlement_pending before restart, got %+v", result.Job)
	}
	persisted, err := st.GetJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.ExecutionReceipt == nil || persisted.ExecutionReceipt.ID == "" {
		t.Fatal("exact ExecutionReceipt was not persisted before ambiguous settlement")
	}

	// Simulate an ATOS process restart: construct a new JobService and make the
	// provider unavailable. Because settlement_pending already has the exact
	// receipt, recovery must not query or resubmit provider work.
	restartedProvider := &unavailableProvider{Provider: baseProvider}
	restartedJobs := service.NewJobService(st, restartedProvider, baseCore, service.NewAccountService(st))
	recovered, err := restartedJobs.ReconcileJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatalf("restart ReconcileJob: %v", err)
	}
	if recovered.State != domain.JobCompleted || recovered.EconomicState != domain.EconomicSettled {
		t.Fatalf("restart recovery did not settle: %+v", recovered)
	}
	account, err := st.GetAccount(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance.Amount != "23.95" {
		t.Fatalf("restart settlement recovery mutated balance incorrectly: %s", account.Balance.Amount)
	}
	for i := 0; i < 3; i++ {
		again, err := restartedJobs.ReconcileJob(ctx, result.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if again.State != domain.JobCompleted {
			t.Fatalf("terminal replay changed job: %+v", again)
		}
	}
	account, err = st.GetAccount(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance.Amount != "23.95" {
		t.Fatalf("restart settlement replay double-mutated balance: %s", account.Balance.Amount)
	}
}

var _ store.Store
