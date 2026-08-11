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

// certifiableToolHandler mirrors internal/service/health_test.go's
// certifiableHTTPHandler -- a GET carrying idempotency_key
// (ProbeCertification's synthetic query) 404s cleanly (a passing
// certification signal); every other request succeeds.
func certifiableToolHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("idempotency_key") != "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func newCertificationToolTestServer(t *testing.T) (*Server, domain.Capability, string) {
	t.Helper()
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	certSrv := httptest.NewServer(certifiableToolHandler())
	t.Cleanup(certSrv.Close)

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: certSrv.Client()}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	token, providerID := signerToolAccessToken(t, authorization, auth.ScopeCertificationsWrite, auth.ScopeCertificationsRead)
	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Certification MCP Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeVerified},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: certSrv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeVerified}},
		},
		IdempotencyKey: "register-mcp-cert",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	server := &Server{Auth: authorization, Capabilities: capabilities, Certifications: certifications}
	return server, cap, token
}

func TestToolOpenCertification_GoldenPath(t *testing.T) {
	server, cap, token := newCertificationToolTestServer(t)
	resp := callTool(t, server, token, "atos_open_certification", map[string]any{
		"capability_id": cap.ID, "transport": "http", "idempotency_key": "cert-mcp-1",
	})
	if toolCallFailed(t, resp) {
		t.Fatalf("atos_open_certification failed: %+v", resp)
	}
	encoded, _ := json.Marshal(resp.Result.(map[string]any)["structuredContent"])
	var decoded domain.SandboxCertification
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode structuredContent: %v", err)
	}
	if decoded.Status != domain.CertificationPassed {
		t.Fatalf("status = %s, want passed", decoded.Status)
	}
}

func TestToolOpenCertification_RequiresIdempotencyKey(t *testing.T) {
	server, cap, token := newCertificationToolTestServer(t)
	resp := callTool(t, server, token, "atos_open_certification", map[string]any{
		"capability_id": cap.ID, "transport": "http",
	})
	if !toolCallFailed(t, resp) {
		t.Fatalf("expected failure without idempotency_key: %+v", resp)
	}
}

func TestToolGetCertificationStatus_GoldenPath(t *testing.T) {
	server, cap, token := newCertificationToolTestServer(t)
	openResp := callTool(t, server, token, "atos_open_certification", map[string]any{
		"capability_id": cap.ID, "transport": "http", "idempotency_key": "cert-mcp-status-1",
	})
	if toolCallFailed(t, openResp) {
		t.Fatalf("open failed: %+v", openResp)
	}

	statusResp := callTool(t, server, token, "atos_get_certification_status", map[string]any{
		"capability_id": cap.ID,
	})
	if toolCallFailed(t, statusResp) {
		t.Fatalf("atos_get_certification_status failed: %+v", statusResp)
	}
	encoded, _ := json.Marshal(statusResp.Result.(map[string]any)["structuredContent"])
	var decoded []domain.SandboxCertification
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode structuredContent: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("history length = %d, want 1", len(decoded))
	}
}

func TestToolOpenCertification_HiddenWithoutScope(t *testing.T) {
	server, cap, _ := newCertificationToolTestServer(t)
	readOnlyToken, _ := signerToolAccessToken(t, server.Auth, auth.ScopeCertificationsRead)
	resp := callTool(t, server, readOnlyToken, "atos_open_certification", map[string]any{
		"capability_id": cap.ID, "transport": "http", "idempotency_key": "cert-mcp-hidden",
	})
	if resp.Error == nil {
		t.Fatalf("expected a protocol-level error calling a write tool with a read-only token, got %+v", resp)
	}
	if resp.Error.Code != codeMethodNotFound {
		t.Fatalf("error code = %d, want codeMethodNotFound (%d) -- a scope-gated tool must appear unknown, not merely forbidden: %+v", resp.Error.Code, codeMethodNotFound, resp.Error)
	}
}
