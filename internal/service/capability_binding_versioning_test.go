package service_test

import (
	"context"
	"testing"
	"time"

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

// countingInvokeAdapter counts Invoke calls, for proving an adapter is
// NEVER reached in a scenario that must fail closed before dispatch.
type countingInvokeAdapter struct {
	transport   domain.EndpointAdapterType
	invokeCalls int
}

func (a *countingInvokeAdapter) Transport() domain.EndpointAdapterType { return a.transport }
func (a *countingInvokeAdapter) Invoke(ctx context.Context, req provideradapter.InvokeRequest) (provideradapter.InvokeResult, error) {
	a.invokeCalls++
	return provideradapter.InvokeResult{Status: provideradapter.InvokeCompleted}, nil
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

// TestReconcileJob_CapabilityVersionChangedAfterQuoteFrozen_NeverExecutes
// proves the roadmap's explicit invariant: "a previously issued Quote/Job
// must never silently execute against a semantically different provider
// binding" / "do not live-switch an already-committed Job to a new
// provider endpoint merely because the Capability was later updated." A
// Job is placed directly into the exact durable state a crash/reconcile-
// before-first-submission window would produce (EconomicEscrowReserved,
// JobWorking, CapabilityVersion frozen at the ORIGINAL capability
// version), the capability is then updated (bumping its version and
// changing its binding), and ReconcileJob must fail closed -- the
// adapter for the NEW binding must never be invoked at all.
func TestReconcileJob_CapabilityVersionChangedAfterQuoteFrozen_NeverExecutes(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	adapter := &countingInvokeAdapter{transport: domain.AdapterHTTP}
	provider := dispatch.New(tosaimock.New(), provideradapter.NewResolver(adapter))
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_stale_binding", Name: "Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: "https://provider-original.example.com", EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
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

	// Directly construct the Job in the exact state a crash between
	// escrow reservation and the first SubmitJob attempt would leave it
	// in -- bypassing JobService.Invoke's own goroutine entirely so there
	// is no execution race to control; CapabilityVersion is frozen at the
	// capability's CURRENT (pre-update) version, exactly as submit()
	// itself would have set it.
	now := time.Now().UTC()
	job := domain.Job{
		ID: "job_stale_binding_test", CapabilityID: cap.ID, CapabilityVersion: cap.Version,
		QuoteID: quote.ID, PrincipalID: "prn_stale_binding", ProviderID: cap.ProviderID,
		TrustMode: domain.TrustModeManaged, State: domain.JobWorking, EconomicState: domain.EconomicEscrowReserved,
		Input: map[string]any{}, Artifacts: []domain.Artifact{}, CreatedAt: now, UpdatedAt: now,
		ExecutionDeadline: now.Add(time.Hour),
	}
	if err := st.PutJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	// Now the capability is updated -- its binding changes (to a
	// different, still-HTTP endpoint the counting adapter WOULD serve)
	// and its version bumps.
	updated, err := capabilities.Update(ctx, cap.ID, "agt_stale_binding", map[string]any{
		"bindings": []map[string]any{
			{"transport": "http", "endpoint_ref": "https://provider-updated.example.com", "eligible_trust_modes": []string{"managed"}},
		},
	}, "update-stale-binding")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version == cap.Version {
		t.Fatal("test setup invalid: capability version must change after the binding update")
	}

	_, reconcileErr := jobs.ReconcileJob(ctx, job.ID)
	if reconcileErr == nil {
		t.Fatal("expected ReconcileJob to fail closed for a version-mismatched job")
	}
	if adapter.invokeCalls != 0 {
		t.Fatalf("adapter.Invoke called %d times -- the job must NEVER execute against a binding that changed after its quote was frozen", adapter.invokeCalls)
	}

	// Read directly from the store rather than through JobService.Get,
	// which itself retries reconciliation (and would return the same
	// deferred error again) for a JobReconciling job -- the reconcileErr
	// assertion above already proves the fail-closed behavior; this only
	// needs to confirm the job's durable state was never advanced to
	// Completed.
	final, err := st.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State == domain.JobCompleted {
		t.Fatal("job must not reach Completed against a mismatched binding")
	}
}
