from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one target, found {count}: {old[:80]!r}")
    p.write_text(text.replace(old, new, 1))


# Permanent PostgreSQL CI must execute the state-machine tests that opt in via
# ATOS_TEST_DATABASE_URL, not only the store and HTTP packages.
replace_once(
    ".github/workflows/ci.yml",
    "        run: go test -race ./internal/store/postgres ./internal/httpapi\n",
    "        run: go test -race ./internal/store/postgres ./internal/httpapi ./internal/service\n",
)

# Append the final failure-injection fixtures/tests.
p = Path("internal/service/economic_crash_safety_test.go")
text = p.read_text()
text = text.replace('''\t"context"\n\t"testing"\n\t"time"\n''', '''\t"context"\n\t"os"\n\t"testing"\n\t"time"\n''', 1)
text = text.replace('''\t"github.com/tosnetwork/atos/internal/adapters/toscore"\n''', '''\t"github.com/tosnetwork/atos/internal/adapters/tosai"\n\t"github.com/tosnetwork/atos/internal/adapters/toscore"\n''', 1)
text = text.replace('''\t"github.com/tosnetwork/atos/internal/store/memory"\n''', '''\t"github.com/tosnetwork/atos/internal/store/memory"\n\t"github.com/tosnetwork/atos/internal/store/postgres"\n''', 1)
marker = "\nvar _ store.Store\n"
if text.count(marker) != 1:
    raise SystemExit("economic_crash_safety_test.go final marker not found")
addition = r'''

type loseReleaseResponseCore struct {
	toscore.Core
	lost bool
}

func (c *loseReleaseResponseCore) ReleaseEscrow(ctx context.Context, escrowID string) (domain.Receipt, error) {
	receipt, err := c.Core.ReleaseEscrow(ctx, escrowID)
	if err == nil && !c.lost {
		c.lost = true
		return domain.Receipt{}, domain.NewError(domain.ErrNetworkUnavailable, "injected lost ReleaseEscrow response", true)
	}
	return receipt, err
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
	if err != nil { t.Fatal(err) }
	if account.Balance.Amount != "23.95" {
		t.Fatalf("ambiguous release credited before confirmation: %s", account.Balance.Amount)
	}

	recovered, err := h.jobs.ReconcileJob(ctx, result.Job.ID)
	if err != nil { t.Fatalf("ReconcileJob: %v", err) }
	if recovered.State != domain.JobFailed || recovered.EconomicState != domain.EconomicReleased {
		t.Fatalf("release recovery did not reach failed/released: %+v", recovered)
	}
	account, err = h.accounts.Get(ctx, "prn_crash_release")
	if err != nil { t.Fatal(err) }
	if account.Balance.Amount != "25.00" {
		t.Fatalf("release recovery did not restore exact balance: %s", account.Balance.Amount)
	}
	for i := 0; i < 3; i++ {
		again, err := h.jobs.ReconcileJob(ctx, result.Job.ID)
		if err != nil { t.Fatal(err) }
		if again.State != domain.JobFailed || again.EconomicState != domain.EconomicReleased {
			t.Fatalf("terminal release replay changed state: %+v", again)
		}
	}
	account, err = h.accounts.Get(ctx, "prn_crash_release")
	if err != nil { t.Fatal(err) }
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
	if err != nil { t.Fatal(err) }
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
	if err != nil { t.Fatalf("Invoke: %v", err) }
	if result.Job.State != domain.JobReconciling || result.Job.EconomicState != domain.EconomicSettlementPending {
		t.Fatalf("expected durable settlement_pending before restart, got %+v", result.Job)
	}
	persisted, err := st.GetJob(ctx, result.Job.ID)
	if err != nil { t.Fatal(err) }
	if persisted.ExecutionReceipt == nil || persisted.ExecutionReceipt.ID == "" {
		t.Fatal("exact ExecutionReceipt was not persisted before ambiguous settlement")
	}

	// Simulate an ATOS process restart: construct a new JobService and make the
	// provider unavailable. Because settlement_pending already has the exact
	// receipt, recovery must not query or resubmit provider work.
	restartedProvider := &unavailableProvider{Provider: baseProvider}
	restartedJobs := service.NewJobService(st, restartedProvider, baseCore, service.NewAccountService(st))
	recovered, err := restartedJobs.ReconcileJob(ctx, result.Job.ID)
	if err != nil { t.Fatalf("restart ReconcileJob: %v", err) }
	if recovered.State != domain.JobCompleted || recovered.EconomicState != domain.EconomicSettled {
		t.Fatalf("restart recovery did not settle: %+v", recovered)
	}
	account, err := st.GetAccount(ctx, principalID)
	if err != nil { t.Fatal(err) }
	if account.Balance.Amount != "23.95" {
		t.Fatalf("restart settlement recovery mutated balance incorrectly: %s", account.Balance.Amount)
	}
	for i := 0; i < 3; i++ {
		again, err := restartedJobs.ReconcileJob(ctx, result.Job.ID)
		if err != nil { t.Fatal(err) }
		if again.State != domain.JobCompleted { t.Fatalf("terminal replay changed job: %+v", again) }
	}
	account, err = st.GetAccount(ctx, principalID)
	if err != nil { t.Fatal(err) }
	if account.Balance.Amount != "23.95" {
		t.Fatalf("restart settlement replay double-mutated balance: %s", account.Balance.Amount)
	}
}
'''
text = text.replace(marker, addition + marker, 1)
p.write_text(text)

print("final Phase 1 crash-safety failure injection materialized")
