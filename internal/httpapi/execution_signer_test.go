package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// signerAccessToken returns both the token and the resulting principal
// ID -- Principal.ID is server-generated per device grant, not
// caller-specifiable, so a test that needs a capability OWNED BY the
// token holder must register it against this returned ID, not a literal
// string chosen in advance.
func signerAccessToken(t *testing.T, svc *auth.Service, scopes ...auth.Scope) (token, principalID string) {
	t.Helper()
	raw := make([]string, len(scopes))
	for i, scope := range scopes {
		raw[i] = string(scope)
	}
	grant, err := svc.StartDevice("test", "REST Signer Test", raw)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	return pair.AccessToken, pair.Principal.ID
}

func newSignerTestServer(t *testing.T) (*Server, *service.CapabilityService, domain.Capability, string) {
	t.Helper()
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	executionSigners := service.NewExecutionSignerService(st, core, capabilities)

	token, providerID := signerAccessToken(t, authorization, auth.ScopeExecutionSignersWrite, auth.ScopeExecutionSignersRead)
	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Signer REST Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-rest-signer",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	server := &Server{Auth: authorization, Capabilities: capabilities, ExecutionSigners: executionSigners}
	return server, capabilities, cap, token
}

func TestHandleAuthorizeExecutionSigner_GoldenPath(t *testing.T) {
	server, _, cap, token := newSignerTestServer(t)

	body, _ := json.Marshal(map[string]any{
		"execution_signer_id": "sig_rest_1",
		"signer_public_key":   "base64:" + base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")),
		"signature_algorithm": "ed25519",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/capabilities/"+cap.ID+"/execution-signer/authorize", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "authz-rest-1")
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if decoded["checkpoint"] != "completed" {
		t.Fatalf("checkpoint = %v, want completed: %s", decoded["checkpoint"], recorder.Body.String())
	}
	if decoded["current_execution_signer_id"] != "sig_rest_1" {
		t.Fatalf("current_execution_signer_id = %v, want sig_rest_1: %s", decoded["current_execution_signer_id"], recorder.Body.String())
	}
}

func TestHandleAuthorizeExecutionSigner_RejectsBadBase64Key(t *testing.T) {
	server, _, cap, token := newSignerTestServer(t)

	body, _ := json.Marshal(map[string]any{
		"execution_signer_id": "sig_rest_bad",
		"signer_public_key":   "not-valid-base64!!!",
		"signature_algorithm": "ed25519",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/capabilities/"+cap.ID+"/execution-signer/authorize", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "authz-rest-bad")
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
	}
}

func TestHandleAuthorizeExecutionSigner_RequiresWriteScope(t *testing.T) {
	server, _, cap, _ := newSignerTestServer(t)
	readOnlyToken, _ := signerAccessToken(t, server.Auth, auth.ScopeExecutionSignersRead)

	body, _ := json.Marshal(map[string]any{
		"execution_signer_id": "sig_rest_scope",
		"signer_public_key":   "base64:" + base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")),
		"signature_algorithm": "ed25519",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/capabilities/"+cap.ID+"/execution-signer/authorize", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+readOnlyToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "authz-rest-scope")
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 with read-only scope: %s", recorder.Code, recorder.Body.String())
	}
}

func TestHandleGetExecutionSignerStatus_RejectsNonOwningProvider(t *testing.T) {
	server, _, cap, _ := newSignerTestServer(t)
	impostorToken, _ := signerAccessToken(t, server.Auth, auth.ScopeExecutionSignersRead)
	// A fresh device grant's Principal.ID is a new random device identity,
	// distinct from the capability's real owner (see newSignerTestServer)
	// -- exactly the impostor scenario under test.

	req := httptest.NewRequest(http.MethodGet, "/v1/capabilities/"+cap.ID+"/execution-signer", nil)
	req.Header.Set("Authorization", "Bearer "+impostorToken)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (permission_denied) for a non-owning provider: %s", recorder.Code, recorder.Body.String())
	}
}

func TestHandleExecutionSignerEndpoints_FullLifecycleThroughREST(t *testing.T) {
	server, _, cap, token := newSignerTestServer(t)
	publicKey := func() string {
		return "base64:" + base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	}

	post := func(path, idempotencyKey string, payload map[string]any) map[string]any {
		t.Helper()
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		recorder := httptest.NewRecorder()
		server.Mux().ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK {
			t.Fatalf("POST %s: status = %d, body = %s", path, recorder.Code, recorder.Body.String())
		}
		var decoded map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		return decoded
	}
	getStatus := func() map[string]any {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/v1/capabilities/"+cap.ID+"/execution-signer", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		server.Mux().ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET status: status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		var decoded map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		return decoded
	}

	base := "/v1/capabilities/" + cap.ID + "/execution-signer/"
	authorized := post(base+"authorize", "lifecycle-authorize", map[string]any{
		"execution_signer_id": "sig_lifecycle_1", "signer_public_key": publicKey(), "signature_algorithm": "ed25519",
	})
	if authorized["current_execution_signer_id"] != "sig_lifecycle_1" {
		t.Fatalf("after authorize: %+v", authorized)
	}

	rotated := post(base+"rotate", "lifecycle-rotate", map[string]any{
		"execution_signer_id": "sig_lifecycle_2", "signer_public_key": publicKey(), "signature_algorithm": "ed25519",
	})
	if rotated["checkpoint"] != "completed" || rotated["current_execution_signer_id"] != "sig_lifecycle_2" {
		t.Fatalf("after rotate: %+v", rotated)
	}

	status := getStatus()
	if status["current_execution_signer_id"] != "sig_lifecycle_2" {
		t.Fatalf("status after rotate: %+v", status)
	}

	revoked := post(base+"revoke", "lifecycle-revoke", map[string]any{"reason_code": "test-teardown"})
	if revoked["checkpoint"] != "completed" {
		t.Fatalf("after revoke: %+v", revoked)
	}
	if _, present := revoked["current_execution_signer_id"]; present {
		t.Fatalf("current_execution_signer_id must be absent (omitempty) once revoked, got %+v", revoked)
	}
}
