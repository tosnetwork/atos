// Integration test against a real Postgres — skipped unless
// ATOS_TEST_DATABASE_URL is set. Run with:
//
//	ATOS_TEST_DATABASE_URL="postgres://user@localhost:5432/atos_test?sslmode=disable" go test ./internal/service/... -run TestJobService_SubmitUsesQuoteFrozenBindingAfterCapabilityUpdate_Postgres
package service_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	"github.com/tosnetwork/atos/internal/adapters/tosai/dispatch"
	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

// TestJobService_SubmitUsesQuoteFrozenBindingAfterCapabilityUpdate_Postgres
// is the real-Postgres analog of the memory-store test of the same name:
// atos-spec IMPLEMENTATION_ROADMAP.md §7.1.0's binding-freeze rule depends
// on domain.Quote.Binding/InputSchema/OutputSchema surviving a real
// store.PutQuote/GetQuote JSON-payload round trip, not just an in-process
// struct reference -- a real concern the memory store's implicit
// pass-by-value semantics cannot expose a bug in.
func TestJobService_SubmitUsesQuoteFrozenBindingAfterCapabilityUpdate_Postgres(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres quote-continuity test")
	}
	ctx := context.Background()

	st, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	defer st.Close()

	var bindingBCalled bool
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bindingBCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srvB.Close()

	var gotJobID string
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
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
			"output": map[string]any{"summary": "processed by the frozen binding A"},
		})
	}))
	defer srvA.Close()

	httpAdapter := httpadapter.New(httpadapter.Config{Client: srvA.Client()})
	provider := dispatch.New(tosaimock.New(), provideradapter.NewResolver(httpAdapter))
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)

	providerID := "agt_quote_continuity_pg_" + uuid.NewString()
	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: srvA.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
		},
		IdempotencyKey: "register-quote-continuity-pg-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	originalVersion, originalManifest := cap.Version, cap.ManifestCommitment

	quote, err := quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	if quote.CapabilityVersion != originalVersion {
		t.Fatalf("quote.CapabilityVersion = %s, want %s", quote.CapabilityVersion, originalVersion)
	}
	if quote.Binding == nil || quote.Binding.EndpointRef != srvA.URL {
		t.Fatalf("quote.Binding = %+v, want the frozen binding A (%s)", quote.Binding, srvA.URL)
	}

	updated, err := capabilities.Update(ctx, cap.ID, providerID, map[string]any{
		"bindings": []map[string]any{
			{"transport": "http", "endpoint_ref": srvB.URL, "eligible_trust_modes": []string{"managed"}},
		},
	}, "update-quote-continuity-pg-"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version == originalVersion {
		t.Fatal("test setup invalid: capability version must change after the binding update")
	}
	if updated.ManifestCommitment == originalManifest {
		t.Fatal("test setup invalid: manifest commitment must change after the binding update")
	}

	// Re-fetch the Quote from Postgres itself -- not the in-memory struct
	// quotes.Create just returned -- so this test actually exercises the
	// JSON-payload round trip store.PutQuote/GetQuote perform, not merely
	// an in-process reference that was never actually re-decoded.
	reloadedQuote, err := st.GetQuote(ctx, quote.ID)
	if err != nil {
		t.Fatalf("GetQuote (reload from Postgres): %v", err)
	}
	if reloadedQuote.Binding == nil || reloadedQuote.Binding.EndpointRef != srvA.URL {
		t.Fatalf("reloaded quote.Binding = %+v, want the frozen binding A (%s) to survive the Postgres round trip", reloadedQuote.Binding, srvA.URL)
	}

	result, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_quote_continuity_pg", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "invoke-quote-continuity-pg-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("Invoke: %v -- a still-valid Quote must continue to resolve its own frozen version/binding, not fail merely because the Capability moved to a new version", err)
	}
	if result.Job.State != domain.JobCompleted {
		t.Fatalf("state = %s, want completed: %+v", result.Job.State, result.Job)
	}
	if bindingBCalled {
		t.Fatal("binding B received a call -- a still-valid Quote must never dispatch against a binding it did not freeze")
	}
	if gotJobID != result.Job.ID {
		t.Fatalf("binding A observed job_id %q, want %q", gotJobID, result.Job.ID)
	}
	if result.Job.CapabilityVersion != originalVersion {
		t.Fatalf("job.CapabilityVersion = %s, want the Quote's frozen version %s (Capability is now at %s)", result.Job.CapabilityVersion, originalVersion, updated.Version)
	}
	if result.Job.Binding == nil || result.Job.Binding.EndpointRef != srvA.URL {
		t.Fatalf("job.Binding = %+v, want the frozen binding A (%s)", result.Job.Binding, srvA.URL)
	}
	if !reflect.DeepEqual(result.Job.InputSchema, quote.InputSchema) {
		t.Fatalf("job.InputSchema = %+v, want the Quote's frozen input schema %+v", result.Job.InputSchema, quote.InputSchema)
	}
	if result.Job.ExecutionReceipt == nil {
		t.Fatal("expected a synthesized execution receipt -- Receipt/settlement must remain the existing normal path")
	}
	if result.Job.EconomicState != domain.EconomicSettled {
		t.Fatalf("economic state = %s, want settled", result.Job.EconomicState)
	}

	// The Job's own frozen binding/version must ALSO survive a real
	// Postgres round trip.
	reloadedJob, err := st.GetJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatalf("GetJob (reload from Postgres): %v", err)
	}
	if reloadedJob.Binding == nil || reloadedJob.Binding.EndpointRef != srvA.URL {
		t.Fatalf("reloaded job.Binding = %+v, want the frozen binding A (%s) to survive the Postgres round trip", reloadedJob.Binding, srvA.URL)
	}
	if reloadedJob.CapabilityVersion != originalVersion {
		t.Fatalf("reloaded job.CapabilityVersion = %s, want %s", reloadedJob.CapabilityVersion, originalVersion)
	}
}
