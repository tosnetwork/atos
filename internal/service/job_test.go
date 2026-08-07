package service_test

import (
	"context"
	"testing"

	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// harness wires the same components cmd/api/main.go does, so these tests
// exercise the real quote -> escrow -> tos-ai execute -> tos-core
// verify/settle pipeline end to end, not a stubbed-out shortcut.
type harness struct {
	capabilities *service.CapabilityService
	quotes       *service.QuoteService
	accounts     *service.AccountService
	jobs         *service.JobService
	st           store.Store
}

func (h harness) store() store.Store { return h.st }

func newHarness() harness {
	st := memory.New()
	provider := tosaimock.New()
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	jobs := service.NewJobService(st, provider, core, accounts)
	return harness{capabilities: capabilities, quotes: quotes, accounts: accounts, jobs: jobs, st: st}
}

func registerCapability(t *testing.T, h harness, providerID, priceAmount string) domain.Capability {
	t.Helper()
	cap, err := h.capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID:   providerID,
		Name:         "Test Capability",
		Description:  "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{
			Model:     domain.PricingFixed,
			PriceHint: domain.PriceHint{Amount: priceAmount, Currency: "USD"},
		},
		IdempotencyKey: "register-" + providerID + "-" + priceAmount,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return cap
}

func createQuote(t *testing.T, h harness, capabilityID string) domain.Quote {
	t.Helper()
	q, err := h.quotes.Create(context.Background(), service.CreateQuoteInput{CapabilityID: capabilityID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	return q
}

func TestJobLifecycleHappyPath(t *testing.T) {
	ctx := context.Background()
	h := newHarness()

	cap := registerCapability(t, h, "agt_provider", "1.00")
	quote := createQuote(t, h, cap.ID)

	before, err := h.accounts.Get(ctx, "prn_client")
	if err != nil {
		t.Fatal(err)
	}

	result, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID:    "prn_client",
		CapabilityID:   cap.ID,
		QuoteID:        quote.ID,
		Input:          map[string]any{"x": 1},
		IdempotencyKey: "happy-path-1",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Type != service.ResultCompleted {
		t.Fatalf("result_type = %q, want completed", result.Type)
	}
	if result.Job.State != domain.JobCompleted {
		t.Fatalf("job state = %q, want completed", result.Job.State)
	}

	after, err := h.accounts.Get(ctx, "prn_client")
	if err != nil {
		t.Fatal(err)
	}
	// $1.00 + 5% fee = $1.05, and the mock provider always charges the
	// full quote (no metered pricing yet), so the drop must be exact.
	if before.Balance.Amount != "25.00" {
		t.Fatalf("unexpected seeded balance %q", before.Balance.Amount)
	}
	if after.Balance.Amount != "23.95" {
		t.Errorf("balance after = %q, want 23.95 (25.00 - 1.05)", after.Balance.Amount)
	}

	// Replaying the exact same idempotency_key must not charge again.
	replay, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID:    "prn_client",
		CapabilityID:   cap.ID,
		QuoteID:        quote.ID,
		Input:          map[string]any{"x": 1},
		IdempotencyKey: "happy-path-1",
	})
	if err != nil {
		t.Fatalf("replay Invoke: %v", err)
	}
	if replay.Job.ID != result.Job.ID {
		t.Errorf("replay returned a different job: %s vs %s", replay.Job.ID, result.Job.ID)
	}
	afterReplay, err := h.accounts.Get(ctx, "prn_client")
	if err != nil {
		t.Fatal(err)
	}
	if afterReplay.Balance.Amount != "23.95" {
		t.Errorf("balance after replay = %q, want unchanged 23.95 (no double charge)", afterReplay.Balance.Amount)
	}
}

// TestJobLifecycleConfirmationFlow guards Fix #4 from review.codex.md: a
// quote above the per-call autonomous limit must actually be executable
// via the confirmed-reissue path, not stuck in input_required forever.
func TestJobLifecycleConfirmationFlow(t *testing.T) {
	ctx := context.Background()
	h := newHarness()

	// $5.00 + 5% fee = $5.25, above the account's $2.00 per-call limit.
	cap := registerCapability(t, h, "agt_provider", "5.00")
	quote := createQuote(t, h, cap.ID)

	pending, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID:    "prn_client2",
		CapabilityID:   cap.ID,
		QuoteID:        quote.ID,
		Input:          map[string]any{},
		IdempotencyKey: "confirm-1",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if pending.Type != service.ResultInputRequired {
		t.Fatalf("result_type = %q, want input_required", pending.Type)
	}
	if pending.Job.State != domain.JobInputRequired {
		t.Fatalf("job state = %q, want input_required", pending.Job.State)
	}

	// The job must actually be persisted and fetchable — this was
	// previously broken (see review.codex.md finding #4).
	stored, err := h.jobs.Get(ctx, pending.Job.ID)
	if err != nil {
		t.Fatalf("GetJob after input_required: %v", err)
	}
	if stored.State != domain.JobInputRequired {
		t.Fatalf("stored job state = %q, want input_required", stored.State)
	}

	// No funds should have moved yet.
	before, err := h.accounts.Get(ctx, "prn_client2")
	if err != nil {
		t.Fatal(err)
	}
	if before.Balance.Amount != "25.00" {
		t.Errorf("balance before confirmation = %q, want unchanged 25.00", before.Balance.Amount)
	}

	confirmed, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID:    "prn_client2",
		CapabilityID:   cap.ID,
		QuoteID:        quote.ID,
		Input:          map[string]any{},
		IdempotencyKey: "confirm-1",
		Confirmed:      true,
	})
	if err != nil {
		t.Fatalf("confirmed Invoke: %v", err)
	}
	if confirmed.Type != service.ResultCompleted {
		t.Fatalf("result_type after confirmation = %q, want completed", confirmed.Type)
	}
	if confirmed.Job.ID != pending.Job.ID {
		t.Errorf("confirmation created a new job instead of continuing %s", pending.Job.ID)
	}

	after, err := h.accounts.Get(ctx, "prn_client2")
	if err != nil {
		t.Fatal(err)
	}
	if after.Balance.Amount != "19.75" {
		t.Errorf("balance after confirmed execution = %q, want 19.75 (25.00 - 5.25)", after.Balance.Amount)
	}
}

// TestJobCancelBeforeConfirmation guards Fix #2's cancellation path and
// idempotent-cancel semantics.
func TestJobCancelBeforeConfirmation(t *testing.T) {
	ctx := context.Background()
	h := newHarness()

	cap := registerCapability(t, h, "agt_provider", "5.00")
	quote := createQuote(t, h, cap.ID)

	pending, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID:    "prn_client3",
		CapabilityID:   cap.ID,
		QuoteID:        quote.ID,
		Input:          map[string]any{},
		IdempotencyKey: "cancel-1",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if pending.Job.State != domain.JobInputRequired {
		t.Fatalf("expected input_required before cancel, got %q", pending.Job.State)
	}

	canceled, err := h.jobs.Cancel(ctx, pending.Job.ID, "prn_client3", "changed my mind", "cancel-op-1")
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if canceled.State != domain.JobCanceled {
		t.Fatalf("job state after cancel = %q, want canceled", canceled.State)
	}

	// Idempotent replay of the cancel must return the same result, not error.
	replay, err := h.jobs.Cancel(ctx, pending.Job.ID, "prn_client3", "changed my mind", "cancel-op-1")
	if err != nil {
		t.Fatalf("replay Cancel: %v", err)
	}
	if replay.State != domain.JobCanceled {
		t.Errorf("replay cancel state = %q, want canceled", replay.State)
	}

	// A cancel with a *different* idempotency key against an
	// already-terminal job must fail with job_not_cancelable.
	_, err = h.jobs.Cancel(ctx, pending.Job.ID, "prn_client3", "again", "cancel-op-2")
	if err == nil {
		t.Fatal("expected job_not_cancelable error for a second distinct cancel attempt")
	}
	de, ok := err.(*domain.Error)
	if !ok || de.Code != domain.ErrJobNotCancelable {
		t.Errorf("got error %v, want domain.ErrJobNotCancelable", err)
	}

	// No escrow was ever created (canceled before confirmation), so no
	// funds should have moved either.
	acct, err := h.accounts.Get(ctx, "prn_client3")
	if err != nil {
		t.Fatal(err)
	}
	if acct.Balance.Amount != "25.00" {
		t.Errorf("balance = %q, want unchanged 25.00 (canceled before any escrow existed)", acct.Balance.Amount)
	}
}
