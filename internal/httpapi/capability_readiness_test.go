package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func capabilitiesAccessToken(t *testing.T, svc *auth.Service) string {
	t.Helper()
	grant, err := svc.StartDevice("test", "REST Test", []string{string(auth.ScopeCapabilitiesRead)})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	return pair.AccessToken
}

// TestHandleGetCapability_IncludesReadinessProjection proves
// GET /v1/capabilities/{id} -- not a new endpoint, the existing one --
// carries the §7.2.3 readiness projection when the server is configured
// with a HealthService.
func TestHandleGetCapability_IncludesReadinessProjection(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer httpSrv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: httpSrv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)

	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: "agt_rest_readiness", Name: "Readiness Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: httpSrv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified}},
		},
		IdempotencyKey: "register-rest-readiness",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	server := &Server{Auth: authorization, Capabilities: capabilities, Health: health}
	token := capabilitiesAccessToken(t, authorization)
	req := httptest.NewRequest(http.MethodGet, "/v1/capabilities/"+cap.ID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	readiness, ok := decoded["readiness"].(map[string]any)
	if !ok {
		t.Fatalf("expected a readiness object in the response, got %s", recorder.Body.String())
	}
	if _, present := readiness["managed"]; present {
		t.Fatal("managed must never appear in the readiness projection")
	}
	verified, ok := readiness["verified"].(map[string]any)
	if !ok {
		t.Fatalf("expected a verified entry in readiness, got %+v", readiness)
	}
	for _, field := range []string{"requested", "status", "transport_healthy", "health_fresh", "certification_current", "signer_authorized", "activation_authority_satisfied"} {
		if _, present := verified[field]; !present {
			t.Fatalf("readiness.verified is missing %q: %+v", field, verified)
		}
	}
	if decoded["mode_support"] == nil {
		t.Fatal("readiness must extend the existing mode_support response, not replace it")
	}
}

// TestHandleGetCapability_NoHealthServiceOmitsReadiness proves a server
// with no HealthService configured (Health: nil) still serves
// GET /v1/capabilities/{id} successfully, just without the readiness
// field -- no panic, no error, unchanged response shape for callers that
// have not wired one.
func TestHandleGetCapability_NoHealthServiceOmitsReadiness(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: "agt_rest_no_health", Name: "No Health Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-rest-no-health",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	server := &Server{Auth: authorization, Capabilities: capabilities}
	token := capabilitiesAccessToken(t, authorization)
	req := httptest.NewRequest(http.MethodGet, "/v1/capabilities/"+cap.ID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, present := decoded["readiness"]; present {
		t.Fatalf("expected no readiness field with Health: nil, got %+v", decoded["readiness"])
	}
}
