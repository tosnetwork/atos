package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/financial"
	"github.com/tosnetwork/atos/internal/store/memory"
)

type policyFinancialStub struct {
	financial.ATOSFinancialAdapter
	reserveCalls int
	failReserve  bool
}

func (s *policyFinancialStub) Reserve(_ context.Context, request financial.TransferRequest) (financial.Event, error) {
	s.reserveCalls++
	if s.failReserve {
		return financial.Event{}, errors.New("injected uncertain reserve")
	}
	return financial.Event{Commitment: financial.Commitment{IdempotencyIdentity: request.IdempotencyIdentity}, State: "finalized"}, nil
}

func TestFinancialReserveConsumesDailyPolicyExactlyOnceAcrossRetry(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	accounts := NewAccountService(st)
	stub := &policyFinancialStub{failReserve: true}
	accounts.WithFinancialAuthority(stub)
	jobs := &JobService{store: st, accounts: accounts, financial: stub}
	job := domain.Job{ID: "job-policy", PrincipalID: "principal-policy", QuoteID: "quote-policy", State: domain.JobSubmitted, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := st.PutJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	quote := domain.Quote{ID: job.QuoteID, Price: domain.Price{TotalMax: "5.00", Currency: "USD"}}

	pending, err := jobs.atomicDebitCheckpoint(ctx, job, quote)
	if err == nil || pending.EconomicState != domain.EconomicPolicyPending {
		t.Fatalf("first reserve checkpoint=%s err=%v", pending.EconomicState, err)
	}
	account, err := st.GetAccount(ctx, job.PrincipalID)
	if err != nil || account.SpendPolicy.RemainingToday.Amount != "15.00" {
		t.Fatalf("policy after first attempt=%+v err=%v", account.SpendPolicy, err)
	}

	stub.failReserve = false
	final, err := jobs.atomicDebitCheckpoint(ctx, pending, quote)
	if err != nil || final.EconomicState != domain.EconomicDebited {
		t.Fatalf("retry checkpoint=%s err=%v", final.EconomicState, err)
	}
	account, err = st.GetAccount(ctx, job.PrincipalID)
	if err != nil || account.SpendPolicy.RemainingToday.Amount != "15.00" || account.Balance.Amount != "25.00" {
		t.Fatalf("retry changed policy twice or mutated balance: account=%+v err=%v", account, err)
	}
	if stub.reserveCalls != 2 {
		t.Fatalf("reserve calls=%d want 2 stable attempts", stub.reserveCalls)
	}
}
