package service_test

import (
	"context"
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

// TestJobInvoke_RejectsInputViolatingFrozenSchema proves "before outbound
// dispatch, request data must satisfy the frozen input schema": a Job
// whose Input does not conform to the Capability's input_schema is
// rejected before the provider is ever contacted, and before any escrow/
// debit takes place.
func TestJobInvoke_RejectsInputViolatingFrozenSchema(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	cap, err := h.capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_input_schema", Name: "Test Capability", Description: "for tests",
		DeliveryMode:   domain.DeliveryInstant,
		InputSchema:    map[string]any{"type": "object", "required": []any{"required_field"}},
		OutputSchema:   map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-input-schema",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	quote, err := h.quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	before, err := h.accounts.Get(ctx, "prn_input_schema")
	if err != nil {
		t.Fatal(err)
	}

	_, err = h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_input_schema", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"wrong_field": "x"}, IdempotencyKey: "invoke-input-schema",
	})
	if err == nil {
		t.Fatal("expected an error for input violating the frozen input_schema")
	}

	after, err := h.accounts.Get(ctx, "prn_input_schema")
	if err != nil {
		t.Fatal(err)
	}
	if after.Balance != before.Balance {
		t.Fatalf("balance changed for a rejected-before-dispatch submission: %s -> %s", before.Balance.Amount, after.Balance.Amount)
	}
}

// TestJobInvoke_ValidInputAgainstSchemaSucceeds proves the same schema
// enforcement does not reject conforming input.
func TestJobInvoke_ValidInputAgainstSchemaSucceeds(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	cap, err := h.capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_input_schema_ok", Name: "Test Capability", Description: "for tests",
		DeliveryMode:   domain.DeliveryInstant,
		InputSchema:    map[string]any{"type": "object", "required": []any{"required_field"}},
		OutputSchema:   map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-input-schema-ok",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	quote, err := h.quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	result, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_input_schema_ok", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"required_field": "x"}, IdempotencyKey: "invoke-input-schema-ok",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Job.State != domain.JobCompleted {
		t.Fatalf("state = %s, want completed", result.Job.State)
	}
}

// TestJobExecution_ProviderOutputViolatingFrozenSchemaFailsNotSettles
// proves "before accepting provider output into a successful settlement,
// output must satisfy the frozen output schema; schema failure is a
// provider failure, never a successful settlement" for the automatic
// (push-model, dispatch-routed) execution path.
func TestJobExecution_ProviderOutputViolatingFrozenSchemaFailsNotSettles(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// The provider returns output that does not conform to the
		// frozen output_schema (missing the required field).
		_, _ = w.Write([]byte(`{"status":"completed","output":{"wrong_field":"x"}}`))
	}))
	defer srv.Close()

	st := memory.New()
	adapter := httpadapter.New(httpadapter.Config{Client: srv.Client()})
	provider := dispatch.New(tosaimock.New(), provideradapter.NewResolver(adapter))
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_output_schema", Name: "Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object", "required": []any{"required_field"}},
		Pricing:      domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: srv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
		},
		IdempotencyKey: "register-output-schema",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	quote, err := quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	before, err := accounts.Get(ctx, "prn_output_schema")
	if err != nil {
		t.Fatal(err)
	}

	result, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_output_schema", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "invoke-output-schema",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Job.State != domain.JobFailed {
		t.Fatalf("state = %s, want failed (schema-invalid output is a provider failure, never a successful settlement)", result.Job.State)
	}
	if result.Job.EconomicState != domain.EconomicReleased {
		t.Fatalf("economic_state = %s, want released (escrow refunded, not settled)", result.Job.EconomicState)
	}
	after, err := accounts.Get(ctx, "prn_output_schema")
	if err != nil {
		t.Fatal(err)
	}
	if after.Balance != before.Balance {
		t.Fatalf("principal was charged for a schema-invalid (failed) settlement: %s -> %s", before.Balance.Amount, after.Balance.Amount)
	}
}

// TestDeliverResult_RejectsOutputViolatingFrozenSchema proves the same
// invariant for the pull-model atos_deliver_job path
// (JobService.DeliverResult).
func TestDeliverResult_RejectsOutputViolatingFrozenSchema(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"pending"}`))
	}))
	defer srv.Close()

	st := memory.New()
	adapter := httpadapter.New(httpadapter.Config{Client: srv.Client()})
	provider := dispatch.New(tosaimock.New(), provideradapter.NewResolver(adapter))
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_deliver_output_schema", Name: "Pull-model Capability", Description: "async delivery",
		DeliveryMode: domain.DeliveryAsync,
		InputSchema:  map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object", "required": []any{"required_field"}},
		Pricing:      domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: srv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
		},
		IdempotencyKey: "register-deliver-output-schema",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	quote, err := quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	submitted, err := jobs.CreateJob(ctx, service.SubmitInput{
		PrincipalID: "prn_deliver_output_schema", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "create-deliver-output-schema",
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	_, err = jobs.DeliverResult(ctx, service.DeliverResultInput{
		JobID: submitted.Job.ID, ProviderID: "agt_deliver_output_schema",
		Output: map[string]any{"wrong_field": "x"}, IdempotencyKey: "deliver-output-schema",
	})
	if err == nil {
		t.Fatal("expected DeliverResult to reject output violating the frozen output_schema")
	}

	current, err := jobs.Get(ctx, submitted.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State == domain.JobCompleted {
		t.Fatal("job must not have settled with schema-invalid delivered output")
	}
}
