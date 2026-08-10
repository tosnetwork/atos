package service_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	"github.com/tosnetwork/atos/internal/adapters/tosai/dispatch"
	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// pendingDeliveryHarness wires a Capability bound to a real in-process HTTP
// server that always reports "pending" -- simulating an async, pull-model
// provider that never completes a Job through the automatic dispatch path,
// so DeliverResult is the only way the Job ever reaches Completed.
func pendingDeliveryHarness(t *testing.T) (harness, domain.Capability) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "pending"})
	}))
	t.Cleanup(srv.Close)

	st := memory.New()
	adapter := httpadapter.New(httpadapter.Config{Client: srv.Client()})
	provider := dispatch.New(tosaimock.New(), provideradapter.NewResolver(adapter))
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)
	h := harness{capabilities: capabilities, quotes: quotes, accounts: accounts, jobs: jobs, st: st}

	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: "agt_deliver_provider", Name: "Pull-model Capability", Description: "async delivery",
		DeliveryMode: domain.DeliveryAsync,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: srv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
		},
		IdempotencyKey: "register-deliver-cap",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return h, cap
}

func TestDeliverResult_HappyPath(t *testing.T) {
	ctx := context.Background()
	h, cap := pendingDeliveryHarness(t)
	quote, err := h.quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatal(err)
	}
	submitted, err := h.jobs.CreateJob(ctx, service.SubmitInput{
		PrincipalID: "prn_deliver_1", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "deliver-create-1",
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if submitted.Job.State == domain.JobCompleted {
		t.Fatal("test setup invalid: job completed automatically, DeliverResult would not be exercised")
	}

	delivered, err := h.jobs.DeliverResult(ctx, service.DeliverResultInput{
		JobID: submitted.Job.ID, ProviderID: "agt_deliver_provider",
		Output: map[string]any{"result": "manually delivered"}, IdempotencyKey: "deliver-1",
	})
	if err != nil {
		t.Fatalf("DeliverResult: %v", err)
	}
	if delivered.State != domain.JobCompleted {
		t.Fatalf("state = %s, want completed", delivered.State)
	}
	if delivered.Output["result"] != "manually delivered" {
		t.Fatalf("output = %+v", delivered.Output)
	}
	if delivered.EconomicState != domain.EconomicSettled {
		t.Fatalf("economic state = %s, want settled", delivered.EconomicState)
	}
}

func TestDeliverResult_DuplicateDeliveryIsIdempotent(t *testing.T) {
	ctx := context.Background()
	h, cap := pendingDeliveryHarness(t)
	quote, err := h.quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatal(err)
	}
	submitted, err := h.jobs.CreateJob(ctx, service.SubmitInput{
		PrincipalID: "prn_deliver_2", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "deliver-create-2",
	})
	if err != nil {
		t.Fatal(err)
	}

	in := service.DeliverResultInput{JobID: submitted.Job.ID, ProviderID: "agt_deliver_provider", Output: map[string]any{"result": "ok"}, IdempotencyKey: "deliver-2"}
	first, err := h.jobs.DeliverResult(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.jobs.DeliverResult(ctx, in)
	if err != nil {
		t.Fatalf("duplicate delivery should be a safe no-op, got: %v", err)
	}
	if second.State != domain.JobCompleted || second.UpdatedAt != first.UpdatedAt {
		t.Fatalf("duplicate delivery re-settled the job: first=%+v second=%+v", first, second)
	}
}

func TestDeliverResult_WrongProviderRejected(t *testing.T) {
	ctx := context.Background()
	h, cap := pendingDeliveryHarness(t)
	quote, err := h.quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatal(err)
	}
	submitted, err := h.jobs.CreateJob(ctx, service.SubmitInput{
		PrincipalID: "prn_deliver_3", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "deliver-create-3",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.jobs.DeliverResult(ctx, service.DeliverResultInput{
		JobID: submitted.Job.ID, ProviderID: "agt_someone_else",
		Output: map[string]any{"result": "hijacked"}, IdempotencyKey: "deliver-3",
	})
	if err == nil {
		t.Fatal("expected delivery from a non-owning provider to be rejected")
	}
}

func TestDeliverResult_UnknownJobRejected(t *testing.T) {
	ctx := context.Background()
	h, _ := pendingDeliveryHarness(t)
	_, err := h.jobs.DeliverResult(ctx, service.DeliverResultInput{
		JobID: "job_does_not_exist", ProviderID: "agt_deliver_provider",
		Output: map[string]any{"result": "x"}, IdempotencyKey: "deliver-4",
	})
	if err == nil {
		t.Fatal("expected an error for an unknown job")
	}
}

func TestJobService_ListByProvider_OnlyOwnJobs(t *testing.T) {
	ctx := context.Background()
	h, cap := pendingDeliveryHarness(t)
	quote, err := h.quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.jobs.CreateJob(ctx, service.SubmitInput{
		PrincipalID: "prn_list_1", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "list-create-1",
	}); err != nil {
		t.Fatal(err)
	}

	owned, err := h.jobs.ListByProvider(ctx, "agt_deliver_provider")
	if err != nil {
		t.Fatal(err)
	}
	if len(owned) != 1 {
		t.Fatalf("found %d jobs for the owning provider, want 1", len(owned))
	}

	other, err := h.jobs.ListByProvider(ctx, "agt_totally_different_provider")
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatalf("found %d jobs for an unrelated provider, want 0", len(other))
	}
}
