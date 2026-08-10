package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestHealthService_SweepStaleCapabilities_ChecksMissingHealthEvidence
// proves the production-wiring gap this closes: before this sweep
// existed, nothing ever called CheckCapability automatically, so a
// capability with a third-party binding and zero recorded health
// evidence would stay that way forever without an explicit caller.
func TestHealthService_SweepStaleCapabilities_ChecksMissingHealthEvidence(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	prober := &fakeThirdPartyHealthProber{result: domain.AdapterHealthCheck{Status: domain.AdapterHealthHealthy}}
	health := service.NewHealthService(st, capabilities, provideradapter.NewResolver()).WithRemoteProber(prober)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_sweep_missing", "https://provider.example.com", []domain.TrustMode{domain.TrustModeManaged})

	if err := health.SweepStaleCapabilities(ctx, 100); err != nil {
		t.Fatalf("SweepStaleCapabilities: %v", err)
	}
	if prober.calls != 1 {
		t.Fatalf("prober calls = %d, want exactly 1 (missing evidence must be checked)", prober.calls)
	}
	_, found, err := st.HealthCheck(ctx, cap.ID, cap.Version, domain.AdapterHTTP)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected the sweep to have recorded a health check")
	}
}

// TestHealthService_SweepStaleCapabilities_SkipsFreshEvidence proves the
// sweep does not re-probe a capability whose health evidence is already
// fresh -- it is a targeted sweep, not an unconditional re-check of
// everything on every tick.
func TestHealthService_SweepStaleCapabilities_SkipsFreshEvidence(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	prober := &fakeThirdPartyHealthProber{result: domain.AdapterHealthCheck{Status: domain.AdapterHealthHealthy}}
	health := service.NewHealthService(st, capabilities, provideradapter.NewResolver()).WithRemoteProber(prober)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_sweep_fresh", "https://provider.example.com", []domain.TrustMode{domain.TrustModeManaged})
	if err := st.PutHealthCheck(ctx, domain.AdapterHealthCheck{
		CapabilityID: cap.ID, CapabilityVersion: cap.Version, Transport: domain.AdapterHTTP,
		EndpointRef: "https://provider.example.com", Status: domain.AdapterHealthHealthy, CheckedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	if err := health.SweepStaleCapabilities(ctx, 100); err != nil {
		t.Fatalf("SweepStaleCapabilities: %v", err)
	}
	if prober.calls != 0 {
		t.Fatalf("prober calls = %d, want 0 (fresh evidence must not be re-probed)", prober.calls)
	}
}

// TestHealthService_SweepStaleCapabilities_ChecksAgedEvidence proves a
// capability whose most recent health check has exceeded maxHealthAge is
// re-checked, not treated as indefinitely fresh.
func TestHealthService_SweepStaleCapabilities_ChecksAgedEvidence(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	prober := &fakeThirdPartyHealthProber{result: domain.AdapterHealthCheck{Status: domain.AdapterHealthHealthy}}
	health := service.NewHealthService(st, capabilities, provideradapter.NewResolver()).WithRemoteProber(prober)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_sweep_aged", "https://provider.example.com", []domain.TrustMode{domain.TrustModeManaged})
	if err := st.PutHealthCheck(ctx, domain.AdapterHealthCheck{
		CapabilityID: cap.ID, CapabilityVersion: cap.Version, Transport: domain.AdapterHTTP,
		EndpointRef: "https://provider.example.com", Status: domain.AdapterHealthHealthy,
		CheckedAt: time.Now().UTC().Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	if err := health.SweepStaleCapabilities(ctx, 100); err != nil {
		t.Fatalf("SweepStaleCapabilities: %v", err)
	}
	if prober.calls != 1 {
		t.Fatalf("prober calls = %d, want exactly 1 (aged evidence must be re-checked)", prober.calls)
	}
}

// TestHealthService_RunReconciler_SweepsImmediatelyOnStart proves
// RunReconciler runs an initial sweep before the first ticker fires, the
// same "sweep once, then on each tick" contract JobService.RunReconciler
// already has -- a short-lived context cancelled before any ticker
// interval could plausibly elapse must still observe one sweep.
func TestHealthService_RunReconciler_SweepsImmediatelyOnStart(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	prober := &fakeThirdPartyHealthProber{result: domain.AdapterHealthCheck{Status: domain.AdapterHealthHealthy}}
	health := service.NewHealthService(st, capabilities, provideradapter.NewResolver()).WithRemoteProber(prober)
	registerHTTPBoundCapability(t, capabilities, "agt_sweep_start", "https://provider.example.com", []domain.TrustMode{domain.TrustModeManaged})

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		health.RunReconciler(runCtx, time.Hour, 100, nil)
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for prober.calls == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if prober.calls == 0 {
		t.Fatal("expected RunReconciler to sweep at least once immediately on start")
	}
}
