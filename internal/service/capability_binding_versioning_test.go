package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
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
