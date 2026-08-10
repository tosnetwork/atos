package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
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

// TestCapabilityUpdate_ChangedBindingBumpsVersionAndManifest is the
// roadmap's explicit "schema update atomicity"-style requirement applied
// to transport bindings: a changed provider endpoint/tool/agent binding
// must change the Capability's version and manifest commitment, exactly
// like a changed pricing or schema does -- a binding change is execution-
// semantics-relevant, never a silent metadata tweak.
func TestCapabilityUpdate_ChangedBindingBumpsVersionAndManifest(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)

	original, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_binding_ver", Name: "Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: "https://provider-a.example.com", EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
		},
		IdempotencyKey: "register-binding-ver",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	updated, err := capabilities.Update(ctx, original.ID, "agt_binding_ver", map[string]any{
		"bindings": []map[string]any{
			{"transport": "http", "endpoint_ref": "https://provider-b.example.com", "eligible_trust_modes": []string{"managed"}},
		},
	}, "update-binding-ver")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	if updated.Version == original.Version {
		t.Fatalf("version did not change after a binding update: still %s", updated.Version)
	}
	if updated.ManifestCommitment == original.ManifestCommitment {
		t.Fatal("manifest_commitment did not change after a binding update")
	}
	if len(updated.Bindings) != 1 || updated.Bindings[0].EndpointRef != "https://provider-b.example.com" {
		t.Fatalf("bindings = %+v, want the new endpoint", updated.Bindings)
	}
}

// countingInvokeAdapter counts Invoke calls and records which endpoint each
// one targeted, for proving both "an adapter is never reached against the
// wrong binding" and "an adapter IS reached against the correct, frozen
// binding." failFirst, if set, makes the first call return an error
// (simulating a submission attempt that never reached the provider,
// leaving the Job durably stuck pre-admission for reconciliation to retry)
// while every subsequent call succeeds.
type countingInvokeAdapter struct {
	transport        domain.EndpointAdapterType
	failFirst        bool
	invokeCalls      int
	invokedEndpoints []string
}

func (a *countingInvokeAdapter) Transport() domain.EndpointAdapterType { return a.transport }
func (a *countingInvokeAdapter) Invoke(ctx context.Context, req provideradapter.InvokeRequest) (provideradapter.InvokeResult, error) {
	a.invokeCalls++
	a.invokedEndpoints = append(a.invokedEndpoints, req.EndpointRef)
	if a.failFirst && a.invokeCalls == 1 {
		return provideradapter.InvokeResult{}, errors.New("simulated submission failure -- provider never admitted this attempt")
	}
	return provideradapter.InvokeResult{Status: provideradapter.InvokeCompleted, Output: map[string]any{}}, nil
}
func (a *countingInvokeAdapter) Query(ctx context.Context, endpointRef, idempotencyKey string) (provideradapter.InvokeResult, bool, error) {
	return provideradapter.InvokeResult{}, false, nil
}
func (a *countingInvokeAdapter) Cancel(ctx context.Context, endpointRef, idempotencyKey, reason string) error {
	return provideradapter.ErrCancelUnsupported
}
func (a *countingInvokeAdapter) Health(ctx context.Context, endpointRef string) domain.AdapterHealthCheck {
	return domain.AdapterHealthCheck{Transport: a.transport, Status: domain.AdapterHealthHealthy}
}

// TestReconcileJob_ContinuesToResolveTheOldBindingAfterCapabilityUpdate
// proves the roadmap's explicit 3A-S acceptance criterion verbatim: "a
// test changes only the provider binding on a Capability and proves a new
// version/manifest is produced while an older Quote/Job continues to
// resolve the old binding." A Job is placed directly into the exact
// durable state a crash/reconcile-before-first-submission window would
// produce, with its Binding frozen to the ORIGINAL endpoint exactly as
// JobService.submit itself would have set it; the Capability is then
// updated to a DIFFERENT endpoint (bumping its version); ReconcileJob must
// still dispatch to the OLD, frozen endpoint -- proving execution never
// re-resolves Capability.Bindings live, only ever the Job's own frozen
// domain.Job.Binding (see its doc comment).
func TestReconcileJob_ContinuesToResolveTheOldBindingAfterCapabilityUpdate(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	// The first Invoke attempt fails (simulating a submission that never
	// reached the provider -- e.g. a transient network error), leaving the
	// Job durably parked pre-admission with its Binding already frozen by
	// the real submit() path; the second call (via ReconcileJob, after the
	// Capability has been updated) succeeds.
	adapter := &countingInvokeAdapter{transport: domain.AdapterHTTP, failFirst: true}
	provider := dispatch.New(tosaimock.New(), provideradapter.NewResolver(adapter))
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)

	const originalEndpoint = "https://provider-original.example.com"
	const updatedEndpoint = "https://provider-updated.example.com"

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_stale_binding", Name: "Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: originalEndpoint, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
		},
		IdempotencyKey: "register-stale-binding",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	quote, err := quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}

	// Real end-to-end submission through JobService.Invoke -- this is what
	// actually freezes job.Binding to the ORIGINAL endpoint (via
	// domain.SelectBinding at creation time). The adapter's first call
	// fails, so the Job ends up durably parked (JobWorking,
	// EconomicEscrowReserved, not yet admitted by the provider) rather
	// than completed.
	submitted, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_stale_binding", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "invoke-stale-binding",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if submitted.Job.State == domain.JobCompleted {
		t.Fatal("test setup invalid: job completed on the first (meant-to-fail) attempt")
	}
	if adapter.invokeCalls != 1 {
		t.Fatalf("test setup invalid: expected exactly 1 failed attempt so far, got %d", adapter.invokeCalls)
	}

	// Now the capability is updated -- its binding changes to a
	// different endpoint and its version bumps.
	updated, err := capabilities.Update(ctx, cap.ID, "agt_stale_binding", map[string]any{
		"bindings": []map[string]any{
			{"transport": "http", "endpoint_ref": updatedEndpoint, "eligible_trust_modes": []string{"managed"}},
		},
	}, "update-stale-binding")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version == cap.Version {
		t.Fatal("test setup invalid: capability version must change after the binding update")
	}

	final, reconcileErr := jobs.ReconcileJob(ctx, submitted.Job.ID)
	if reconcileErr != nil {
		t.Fatalf("ReconcileJob: %v -- an older Job must continue to resolve its frozen binding, not fail merely because the Capability was updated", reconcileErr)
	}
	if final.State != domain.JobCompleted {
		t.Fatalf("state = %s, want completed (execution against the frozen binding must still succeed)", final.State)
	}
	if adapter.invokeCalls != 2 {
		t.Fatalf("adapter.Invoke called %d times, want exactly 2 (1 failed + 1 succeeded)", adapter.invokeCalls)
	}
	for i, endpoint := range adapter.invokedEndpoints {
		if endpoint != originalEndpoint {
			t.Fatalf("call %d invoked endpoint %q, want the ORIGINAL frozen endpoint %q, never the capability's updated one %q", i, endpoint, originalEndpoint, updatedEndpoint)
		}
	}
}

// TestJobService_SubmitUsesQuoteFrozenBindingAfterCapabilityUpdate proves
// atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.1.0's binding-freeze
// acceptance criterion at the point it was previously NOT satisfied: Job
// creation (internal/service/job.go's submit), not just later
// reconciliation of an already-created Job (already covered above by
// TestReconcileJob_ContinuesToResolveTheOldBindingAfterCapabilityUpdate).
//
// Before this test's corresponding fix, submit() hard-rejected
// (ErrQuoteMismatch) whenever capability.Version != quote.CapabilityVersion,
// so a Capability update landing between QuoteService.Create and
// JobService.Invoke made an otherwise still-valid, unexpired Quote
// permanently unusable -- safe (no substitution), but not what the
// roadmap's binding-freeze rule requires: "an already-issued Quote/Job
// MUST continue to use its frozen version/binding semantics after a
// Capability update." This test proves the Quote's own frozen
// binding/version/schema (domain.Quote.Binding/InputSchema/OutputSchema,
// set by QuoteService.Create) is what Job creation now uses instead.
//
// Uses real in-process HTTP servers for both the original and updated
// bindings (not a mocked resolver interface), matching this repository's
// atos-spec §3.6 real-protocol-tests convention -- the same pattern
// TestJobInvoke_HTTPBoundCapability_EndToEnd already establishes.
func TestJobService_SubmitUsesQuoteFrozenBindingAfterCapabilityUpdate(t *testing.T) {
	ctx := context.Background()

	var bindingBCalled bool
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bindingBCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srvB.Close()

	var gotJobID string
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			// dispatch.Provider.SubmitJob Querys by idempotency key before
			// ever Invoking -- no record of this attempt exists yet.
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

	st := memory.New()
	// A single httpadapter.Adapter dials whichever EndpointRef a given
	// request carries (the local/dev dispatch model -- distinct from
	// tos-ai's operator-curated remote allowlist), so one adapter instance
	// can prove which of the two real servers actually received the call.
	httpAdapter := httpadapter.New(httpadapter.Config{Client: srvA.Client()})
	provider := dispatch.New(tosaimock.New(), provideradapter.NewResolver(httpAdapter))
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_quote_continuity", Name: "Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: srvA.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
		},
		IdempotencyKey: "register-quote-continuity",
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

	// Update ONLY the binding -- A -> B.
	updated, err := capabilities.Update(ctx, cap.ID, "agt_quote_continuity", map[string]any{
		"bindings": []map[string]any{
			{"transport": "http", "endpoint_ref": srvB.URL, "eligible_trust_modes": []string{"managed"}},
		},
	}, "update-quote-continuity")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version == originalVersion {
		t.Fatal("test setup invalid: capability version must change after the binding update")
	}
	if updated.ManifestCommitment == originalManifest {
		t.Fatal("test setup invalid: manifest commitment must change after the binding update")
	}

	// Submit a Job using the still-valid Quote AFTER the Capability moved
	// on to a new version/binding.
	result, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_quote_continuity", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "invoke-quote-continuity",
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
		t.Fatalf("binding A observed job_id %q, want %q -- execution must go to the frozen binding A", gotJobID, result.Job.ID)
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
	if !reflect.DeepEqual(result.Job.OutputSchema, quote.OutputSchema) {
		t.Fatalf("job.OutputSchema = %+v, want the Quote's frozen output schema %+v", result.Job.OutputSchema, quote.OutputSchema)
	}
	if result.Job.ExecutionReceipt == nil {
		t.Fatal("expected a synthesized execution receipt -- Receipt/settlement must remain the existing normal path")
	}
	if result.Job.EconomicState != domain.EconomicSettled {
		t.Fatalf("economic state = %s, want settled", result.Job.EconomicState)
	}

	// A retry (idempotent replay) after the update must still resolve to
	// the same already-committed Job, never reroute to binding B.
	replay, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_quote_continuity", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "invoke-quote-continuity",
	})
	if err != nil {
		t.Fatalf("replay Invoke: %v", err)
	}
	if replay.Job.ID != result.Job.ID {
		t.Fatalf("replay produced a different job: %s vs %s", replay.Job.ID, result.Job.ID)
	}
	if bindingBCalled {
		t.Fatal("binding B received a call on replay")
	}
}

// TestQuoteService_NewQuoteAfterCapabilityUpdateUsesNewBinding is the
// inverse of TestJobService_SubmitUsesQuoteFrozenBindingAfterCapabilityUpdate:
// a NEW Quote created after the Capability update must resolve the
// UPDATED binding/version, not the old one -- proving old committed
// history (an already-issued Quote) and new live configuration are
// cleanly separated, not that Quotes simply ignore Capability updates
// altogether.
func TestQuoteService_NewQuoteAfterCapabilityUpdateUsesNewBinding(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)

	const bindingA = "https://provider-a.example.com"
	const bindingB = "https://provider-b.example.com"

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_new_quote_binding", Name: "Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: bindingA, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
		},
		IdempotencyKey: "register-new-quote-binding",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	updated, err := capabilities.Update(ctx, cap.ID, "agt_new_quote_binding", map[string]any{
		"bindings": []map[string]any{
			{"transport": "http", "endpoint_ref": bindingB, "eligible_trust_modes": []string{"managed"}},
		},
	}, "update-new-quote-binding")
	if err != nil {
		t.Fatal(err)
	}

	quote, err := quotes.Create(ctx, service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	if quote.CapabilityVersion != updated.Version {
		t.Fatalf("quote.CapabilityVersion = %s, want the current version %s", quote.CapabilityVersion, updated.Version)
	}
	if quote.Binding == nil || quote.Binding.EndpointRef != bindingB {
		t.Fatalf("quote.Binding = %+v, want the UPDATED binding B (%s), not the old binding A", quote.Binding, bindingB)
	}
}
