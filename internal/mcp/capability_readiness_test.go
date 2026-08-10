package mcp

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

// TestToolGetCapability_IncludesReadinessProjection proves
// atos_get_capability -- not a new tool, the existing one -- carries the
// §7.2.3 readiness projection when the server is configured with a
// HealthService, matching "not a new endpoint" from the frozen public
// surface.
func TestToolGetCapability_IncludesReadinessProjection(t *testing.T) {
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
		ProviderID: "agt_mcp_readiness", Name: "Readiness Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: httpSrv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified}},
		},
		IdempotencyKey: "register-mcp-readiness",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	server := &Server{Auth: authorization, Capabilities: capabilities, Health: health}
	token := accessToken(t, authorization, auth.ScopeCapabilitiesRead)
	resp := callTool(t, server, token, "atos_get_capability", map[string]any{"capability_id": cap.ID})
	if toolCallFailed(t, resp) {
		t.Fatalf("atos_get_capability failed: %+v", resp)
	}

	encoded, err := json.Marshal(resp.Result.(map[string]any)["structuredContent"])
	if err != nil {
		t.Fatalf("re-encode structuredContent: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode structuredContent: %v", err)
	}
	readiness, ok := decoded["readiness"].(map[string]any)
	if !ok {
		t.Fatalf("expected a readiness object in the response, got %+v", decoded)
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
}

// TestToolGetCapability_NoHealthServiceOmitsReadiness proves a server
// with no HealthService configured (Health: nil) still serves
// atos_get_capability successfully, just without the readiness field --
// no panic, no error.
func TestToolGetCapability_NoHealthServiceOmitsReadiness(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: "agt_mcp_no_health", Name: "No Health Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-mcp-no-health",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	server := &Server{Auth: authorization, Capabilities: capabilities}
	token := accessToken(t, authorization, auth.ScopeCapabilitiesRead)
	resp := callTool(t, server, token, "atos_get_capability", map[string]any{"capability_id": cap.ID})
	if toolCallFailed(t, resp) {
		t.Fatalf("atos_get_capability failed: %+v", resp)
	}
	encoded, err := json.Marshal(resp.Result.(map[string]any)["structuredContent"])
	if err != nil {
		t.Fatalf("re-encode structuredContent: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode structuredContent: %v", err)
	}
	if _, present := decoded["readiness"]; present {
		t.Fatalf("expected no readiness field with Health: nil, got %+v", decoded["readiness"])
	}
}
