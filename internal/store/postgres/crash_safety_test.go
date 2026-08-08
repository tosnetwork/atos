package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func TestUpdateJobAndAccountRollsBackTogether(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	principalID := "prn_atomic_" + suffix
	jobID := "job_atomic_" + suffix
	now := time.Now().UTC()
	job := domain.Job{ID: jobID, CapabilityID: "cap_" + suffix, QuoteID: "q_" + suffix, PrincipalID: principalID, State: domain.JobSubmitted, Input: map[string]any{}, Artifacts: []domain.Artifact{}, CreatedAt: now, UpdatedAt: now}
	if err := s.PutJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	seed := domain.Account{PrincipalID: principalID, Balance: domain.Money{Amount: "10", Currency: "USD"}}
	if err := s.PutAccount(ctx, seed); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("inject rollback")
	_, _, err := s.UpdateJobAndAccount(ctx, jobID, principalID, seed, func(j domain.Job, _ bool, a domain.Account, _ bool) (domain.Job, domain.Account, error) {
		j.EconomicState = domain.EconomicDebited
		a.Balance.Amount = "9"
		return j, a, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("UpdateJobAndAccount error = %v", err)
	}
	gotJob, err := s.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	gotAccount, err := s.GetAccount(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if gotJob.EconomicState != domain.EconomicNone || gotAccount.Balance.Amount != "10" {
		t.Fatalf("partial transaction persisted: job=%q balance=%q", gotJob.EconomicState, gotAccount.Balance.Amount)
	}
}

func TestEscrowByJobIsUniqueAndRecoverable(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	jobID := "job_escrow_lookup_" + suffix
	now := time.Now().UTC()
	e := domain.Escrow{ID: "esc_" + suffix, QuoteID: "q_" + suffix, JobID: jobID, PrincipalID: "p_" + suffix, ProviderID: "a_" + suffix, CapabilityID: "c_" + suffix, CapabilityVersion: "1", TrustMode: domain.TrustModeManaged, Reserved: domain.Money{Amount: "1.00", Currency: "USD"}, Status: domain.EscrowReserved, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := s.PutEscrow(ctx, e); err != nil {
		t.Fatal(err)
	}
	got, err := s.EscrowByJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != e.ID {
		t.Fatalf("EscrowByJob = %s, want %s", got.ID, e.ID)
	}
	duplicate := e
	duplicate.ID = "esc_duplicate_" + suffix
	if err := s.PutEscrow(ctx, duplicate); err == nil {
		t.Fatal("duplicate escrow for one job unexpectedly succeeded")
	}
	if _, err := s.EscrowByJob(ctx, "missing_"+suffix); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing EscrowByJob error = %v", err)
	}
}
