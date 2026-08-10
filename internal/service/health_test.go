package service_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/mcpadapter"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// certifiableHTTPHandler returns an httptest handler whose GET responses
// cleanly report "no record" (404) for a Query-shaped request (one
// carrying an idempotency_key query param) -- the expected,
// certification-passing answer for httpadapter's ProbeCertification,
// which probes a synthetic idempotency key that never corresponds to a
// real Job. A bare GET with no idempotency_key (what Health itself sends)
// still succeeds with 200, and any other method (Invoke's POST) succeeds
// trivially too. This lets one handler serve both Health-only tests and
// tests that also run certification through the same server.
func certifiableHTTPHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("idempotency_key") != "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func registerHTTPBoundCapability(t *testing.T, capabilities *service.CapabilityService, providerID, endpointRef string, requestedModes []domain.TrustMode) domain.Capability {
	t.Helper()
	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "HTTP Capability", Description: "third-party HTTP",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: requestedModes,
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: endpointRef, EligibleTrustModes: requestedModes},
		},
		IdempotencyKey: "register-" + providerID,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return cap
}

func TestHealthService_CheckCapability_RecordsHealthyResult(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_health_1", srv.URL, []domain.TrustMode{domain.TrustModeManaged})
	checks, err := health.CheckCapability(ctx, cap.ID)
	if err != nil {
		t.Fatalf("CheckCapability: %v", err)
	}
	if len(checks) != 1 || checks[0].Status != domain.AdapterHealthHealthy {
		t.Fatalf("checks = %+v", checks)
	}

	stored, found, err := st.HealthCheck(ctx, cap.ID, cap.Version, domain.AdapterHTTP)
	if err != nil || !found {
		t.Fatalf("HealthCheck: found=%v err=%v", found, err)
	}
	if stored.Status != domain.AdapterHealthHealthy {
		t.Fatalf("stored status = %s", stored.Status)
	}
}

func TestHealthService_CheckCapability_RecordsUnhealthyResult(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{}))
	health := service.NewHealthService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_health_2", "http://127.0.0.1:1/invoke", []domain.TrustMode{domain.TrustModeManaged})
	checks, err := health.CheckCapability(ctx, cap.ID)
	if err != nil {
		t.Fatalf("CheckCapability: %v", err)
	}
	if len(checks) != 1 || checks[0].Status != domain.AdapterHealthUnhealthy {
		t.Fatalf("checks = %+v", checks)
	}
}

// TestHealthService_CheckCapability_NeverMutatesModeSupport is the
// roadmap's explicit regression requirement: a health check success must
// not directly activate Verified or Native, or change SupportedTrustModes
// at all.
func TestHealthService_CheckCapability_NeverMutatesModeSupport(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_health_3", srv.URL, []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified, domain.TrustModeNative})
	before, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatalf("CheckCapability: %v", err)
	}

	after, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.SupportedTrustModes) != len(before.SupportedTrustModes) {
		t.Fatalf("supported_trust_modes changed by a health check: %v -> %v", before.SupportedTrustModes, after.SupportedTrustModes)
	}
	for mode := range after.ModeSupport {
		if after.ModeSupport[mode].Status != before.ModeSupport[mode].Status {
			t.Fatalf("mode_support[%s] changed by a health check: %s -> %s", mode, before.ModeSupport[mode].Status, after.ModeSupport[mode].Status)
		}
	}
	if after.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("a healthy transport check must never independently activate Verified")
	}
	if after.ModeSupport.Active(domain.TrustModeNative) {
		t.Fatal("a healthy transport check must never independently activate Native")
	}
}

func TestHealthService_Availability_ReadyRequiresHealthyAndCertified(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)
	certifications := service.NewCertificationService(st, capabilities, resolver)

	// Verified is requested but -- as of Phase 3A -- can never be Active
	// (that path is Phase 3B/4 only); it starts Pending. This is the mode
	// that actually exercises "Ready=true must never imply Active=true",
	// unlike Managed, which is unconditionally Active from registration by
	// existing (pre-Phase-3A) design regardless of health/certification.
	cap := registerHTTPBoundCapability(t, capabilities, "agt_avail_1", srv.URL, []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified})
	var verified domain.ModeAvailability
	findVerified := func(avail []domain.ModeAvailability) domain.ModeAvailability {
		for _, a := range avail {
			if a.Mode == domain.TrustModeVerified {
				return a
			}
		}
		t.Fatal("expected a ModeAvailability entry for verified")
		return domain.ModeAvailability{}
	}

	// Before any health check or certification: not ready.
	availBefore, err := health.Availability(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	verified = findVerified(availBefore)
	if verified.Ready {
		t.Fatalf("availability before health/certification = %+v, want not ready", verified)
	}

	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	availAfterHealth, err := health.Availability(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	verified = findVerified(availAfterHealth)
	if !verified.TransportHealthy || verified.Ready {
		t.Fatalf("availability after health only = %+v, want healthy but not ready (uncertified)", verified)
	}

	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_avail_1", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-1",
	}); err != nil {
		t.Fatalf("certifications.Open: %v", err)
	}
	availAfterCert, err := health.Availability(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	verified = findVerified(availAfterCert)
	if !verified.Ready {
		t.Fatalf("availability after health+certification = %+v, want ready", verified)
	}
	if verified.Active {
		t.Fatal("Ready=true must never imply Active=true -- activation is Phase 3B/4, not Phase 3A")
	}
}

// TestHealthService_Availability_CertificationDoesNotCarryAcrossCapabilityVersionBump
// is atos-spec IMPLEMENTATION_ROADMAP.md §7.1.3's explicit acceptance
// scenario: "stale results do not certify a new Capability version."
// AdapterHealthCheck and SandboxCertification are both stored keyed by
// (capability_id, capability_version, transport); this test proves that
// keying is actually load-bearing in HealthService.Availability's
// projection, not just present on the struct -- a certification passed
// against version N must stop counting toward Certified/Ready the moment
// the Capability is updated to version N+1, even though nothing about the
// binding's reachability changed and even after health is re-checked
// against the new version.
func TestHealthService_Availability_CertificationDoesNotCarryAcrossCapabilityVersionBump(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_avail_stale", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	findVerified := func(avail []domain.ModeAvailability) domain.ModeAvailability {
		for _, a := range avail {
			if a.Mode == domain.TrustModeVerified {
				return a
			}
		}
		t.Fatal("expected a ModeAvailability entry for verified")
		return domain.ModeAvailability{}
	}

	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_avail_stale", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-stale-v1",
	}); err != nil {
		t.Fatalf("certifications.Open (version 1): %v", err)
	}
	beforeBump, err := health.Availability(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v := findVerified(beforeBump); !v.Ready {
		t.Fatalf("test setup invalid: availability before the version bump = %+v, want ready", v)
	}

	// Bump the Capability's version via an unrelated field (price) --
	// deliberately NOT changing the binding's endpoint_ref, to isolate
	// "certification is version-scoped" from "certification is
	// endpoint-scoped" (already covered by the binding-freeze tests).
	updated, err := capabilities.Update(ctx, cap.ID, "agt_avail_stale", map[string]any{
		"pricing": map[string]any{
			"model":      "fixed",
			"price_hint": map[string]any{"amount": "2.00", "currency": "USD"},
		},
	}, "update-avail-stale-price")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Version == cap.Version {
		t.Fatal("test setup invalid: capability version must change after the price update")
	}

	// Re-check health against the NEW version -- transport reachability is
	// genuinely fresh -- but do NOT re-certify. If certification were
	// (incorrectly) capability-scoped rather than version-scoped, Ready
	// would stay true here; it must not.
	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	afterBump, err := health.Availability(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	verifiedAfterBump := findVerified(afterBump)
	if !verifiedAfterBump.TransportHealthy {
		t.Fatalf("availability after version bump = %+v, want transport_healthy (freshly re-checked against the new version)", verifiedAfterBump)
	}
	if verifiedAfterBump.Certified {
		t.Fatalf("availability after version bump = %+v, want NOT certified -- the version-1 certification must not carry over to version %s", verifiedAfterBump, updated.Version)
	}
	if verifiedAfterBump.Ready {
		t.Fatalf("availability after version bump = %+v, want NOT ready -- a stale certification must never make a new version ready", verifiedAfterBump)
	}

	// Re-certifying against the new (current) version must resolve it --
	// this isn't a permanently broken state, only a correctly-scoped one.
	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_avail_stale", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-stale-v2",
	}); err != nil {
		t.Fatalf("certifications.Open (version 2): %v", err)
	}
	afterRecert, err := health.Availability(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v := findVerified(afterRecert); !v.Ready {
		t.Fatalf("availability after re-certifying against the current version = %+v, want ready", v)
	}
}

// TestHealthService_CheckCapability_OneUnhealthyBindingDoesNotAffectOthers
// proves that when a Capability has multiple distinct transport bindings
// for different trust modes, one being unhealthy does not incorrectly mark
// every mode unavailable -- health is tracked per (capability, version,
// transport), not collapsed into one capability-wide boolean.
func TestHealthService_CheckCapability_OneUnhealthyBindingDoesNotAffectOthers(t *testing.T) {
	ctx := context.Background()
	healthySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer healthySrv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(
		httpadapter.New(httpadapter.Config{Client: healthySrv.Client()}),
		mcpadapter.New(mcpadapter.Config{Timeout: 200 * time.Millisecond}),
	)
	health := service.NewHealthService(st, capabilities, resolver)

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_mixed_bindings", Name: "Mixed Bindings", Description: "two transports",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: healthySrv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
			{Transport: domain.AdapterMCP, EndpointRef: "http://127.0.0.1:1#unreachable", EligibleTrustModes: []domain.TrustMode{domain.TrustModeVerified}},
		},
		IdempotencyKey: "register-mixed-bindings",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	avail, err := health.Availability(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	var managed, verified domain.ModeAvailability
	for _, a := range avail {
		switch a.Mode {
		case domain.TrustModeManaged:
			managed = a
		case domain.TrustModeVerified:
			verified = a
		}
	}
	if !managed.TransportHealthy {
		t.Fatalf("managed (healthy HTTP binding) = %+v, want transport_healthy", managed)
	}
	if verified.TransportHealthy {
		t.Fatalf("verified (unreachable MCP binding) = %+v, want NOT transport_healthy -- one bad binding must not be masked by the other's health", verified)
	}
}

func TestHealthService_Availability_StaleHealthNotTreatedAsFresh(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{}))
	health := service.NewHealthService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_avail_stale", "https://provider.example.com", []domain.TrustMode{domain.TrustModeManaged})
	// Directly write a stale-but-healthy check, simulating an old success
	// that must not be treated as indefinitely fresh.
	if err := st.PutHealthCheck(ctx, domain.AdapterHealthCheck{
		CapabilityID: cap.ID, CapabilityVersion: cap.Version, Transport: domain.AdapterHTTP,
		EndpointRef: "https://provider.example.com", Status: domain.AdapterHealthHealthy,
		CheckedAt: time.Now().UTC().Add(-24 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	avail, err := health.Availability(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if avail[0].TransportHealthy {
		t.Fatal("a stale health check must not be treated as currently healthy")
	}
}
