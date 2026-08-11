package httpapi

import (
	"bytes"
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

// certifiableHandler mirrors internal/service/health_test.go's identically
// named helper -- a GET carrying idempotency_key (ProbeCertification's
// synthetic query) 404s cleanly (a passing certification signal); every
// other request succeeds.
func certifiableHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("idempotency_key") != "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func newCertificationTestServer(t *testing.T) (*Server, *httptest.Server, domain.Capability, string) {
	t.Helper()
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	certSrv := httptest.NewServer(certifiableHandler())
	t.Cleanup(certSrv.Close)

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: certSrv.Client()}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	token, providerID := signerAccessToken(t, authorization, auth.ScopeCertificationsWrite, auth.ScopeCertificationsRead)
	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Certification REST Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeVerified},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: certSrv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeVerified}},
		},
		IdempotencyKey: "register-rest-cert",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	server := &Server{Auth: authorization, Capabilities: capabilities, Certifications: certifications}
	return server, certSrv, cap, token
}

func TestHandleOpenCertification_GoldenPath(t *testing.T) {
	server, _, cap, token := newCertificationTestServer(t)

	body, _ := json.Marshal(map[string]any{"transport": "http"})
	req := httptest.NewRequest(http.MethodPost, "/v1/capabilities/"+cap.ID+"/certification", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "cert-rest-1")
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if decoded["status"] != "passed" {
		t.Fatalf("status = %v, want passed: %s", decoded["status"], recorder.Body.String())
	}
	if decoded["capability_id"] != cap.ID {
		t.Fatalf("capability_id = %v, want %s", decoded["capability_id"], cap.ID)
	}
}

func TestHandleOpenCertification_RequiresIdempotencyKey(t *testing.T) {
	server, _, cap, token := newCertificationTestServer(t)

	body, _ := json.Marshal(map[string]any{"transport": "http"})
	req := httptest.NewRequest(http.MethodPost, "/v1/capabilities/"+cap.ID+"/certification", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 without Idempotency-Key: %s", recorder.Code, recorder.Body.String())
	}
}

func TestHandleOpenCertification_RequiresWriteScope(t *testing.T) {
	server, _, cap, _ := newCertificationTestServer(t)
	readOnlyToken, _ := signerAccessToken(t, server.Auth, auth.ScopeCertificationsRead)

	body, _ := json.Marshal(map[string]any{"transport": "http"})
	req := httptest.NewRequest(http.MethodPost, "/v1/capabilities/"+cap.ID+"/certification", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+readOnlyToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "cert-rest-scope")
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 with read-only scope: %s", recorder.Code, recorder.Body.String())
	}
}

func TestHandleGetCertificationStatus_GoldenPath(t *testing.T) {
	server, _, cap, token := newCertificationTestServer(t)

	openReq := httptest.NewRequest(http.MethodPost, "/v1/capabilities/"+cap.ID+"/certification",
		bytes.NewReader(mustJSON(map[string]any{"transport": "http"})))
	openReq.Header.Set("Authorization", "Bearer "+token)
	openReq.Header.Set("Content-Type", "application/json")
	openReq.Header.Set("Idempotency-Key", "cert-rest-status-1")
	openRecorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(openRecorder, openReq)
	if openRecorder.Code != http.StatusOK {
		t.Fatalf("open: status = %d, body = %s", openRecorder.Code, openRecorder.Body.String())
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/v1/capabilities/"+cap.ID+"/certification", nil)
	statusReq.Header.Set("Authorization", "Bearer "+token)
	statusRecorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(statusRecorder, statusReq)
	if statusRecorder.Code != http.StatusOK {
		t.Fatalf("status: status = %d, body = %s", statusRecorder.Code, statusRecorder.Body.String())
	}
	var decoded []map[string]any
	if err := json.Unmarshal(statusRecorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("history length = %d, want 1: %s", len(decoded), statusRecorder.Body.String())
	}
	if decoded[0]["status"] != "passed" {
		t.Fatalf("status = %v, want passed", decoded[0]["status"])
	}
}

func TestHandleGetCertificationStatus_RejectsNonOwningProvider(t *testing.T) {
	server, _, cap, _ := newCertificationTestServer(t)
	impostorToken, _ := signerAccessToken(t, server.Auth, auth.ScopeCertificationsRead)

	req := httptest.NewRequest(http.MethodGet, "/v1/capabilities/"+cap.ID+"/certification", nil)
	req.Header.Set("Authorization", "Bearer "+impostorToken)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (permission_denied) for a non-owning provider: %s", recorder.Code, recorder.Body.String())
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
