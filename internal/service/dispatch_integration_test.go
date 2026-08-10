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

// TestJobInvoke_HTTPBoundCapability_EndToEnd proves a Capability registered
// with a real http CapabilityBinding actually executes against that
// endpoint through the full Quote -> Invoke -> execute -> settle pipeline
// -- not a stubbed shortcut -- using a real in-process HTTP server and
// JobService completely unmodified from how cmd/api/main.go wires it,
// with only the tosai.Provider implementation swapped for
// dispatch.New(mock, resolver).
func TestJobInvoke_HTTPBoundCapability_EndToEnd(t *testing.T) {
	ctx := context.Background()

	var gotJobID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			// dispatch.Provider.SubmitJob Querys by idempotency key before
			// ever Invoking -- a real third-party provider with no record
			// of this attempt yet reports 404, exactly like
			// httpadapter.Adapter.Query's own documented contract.
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		gotJobID, _ = body["job_id"].(string)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "completed",
			"output": map[string]any{"summary": "processed by third-party HTTP provider"},
		})
	}))
	defer srv.Close()

	st := memory.New()
	httpAdapter := httpadapter.New(httpadapter.Config{Client: srv.Client()})
	provider := dispatch.New(tosaimock.New(), provideradapter.NewResolver(httpAdapter))
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_http_provider", Name: "HTTP Capability", Description: "third-party HTTP",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: srv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
		},
		IdempotencyKey: "register-http-cap",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if len(cap.Bindings) != 1 || cap.Bindings[0].Transport != domain.AdapterHTTP {
		t.Fatalf("registered bindings = %+v", cap.Bindings)
	}

	quote, err := quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}

	result, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_http_client", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"text": "hello"}, IdempotencyKey: "invoke-http-e2e",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Job.State != domain.JobCompleted {
		t.Fatalf("job state = %s, want completed: %+v", result.Job.State, result.Job)
	}
	if result.Job.Output["summary"] != "processed by third-party HTTP provider" {
		t.Fatalf("job output = %+v, want the third-party HTTP response", result.Job.Output)
	}
	if result.Job.ExecutionReceipt == nil {
		t.Fatal("expected a synthesized execution receipt so settlement can proceed")
	}
	if gotJobID != result.Job.ID {
		t.Fatalf("provider observed job_id %q, want %q", gotJobID, result.Job.ID)
	}

	// Settlement must have actually completed -- not merely "the Job
	// looks done" -- proving the synthesized receipt satisfies
	// settleProviderResultUnderLock's requirements end to end.
	if result.Job.EconomicState != domain.EconomicSettled {
		t.Fatalf("economic state = %s, want settled", result.Job.EconomicState)
	}
}
