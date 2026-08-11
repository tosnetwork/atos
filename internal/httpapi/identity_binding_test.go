package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func newIdentityBindingTestServer(t *testing.T) (*Server, *toscoremock.Core, string) {
	t.Helper()
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	identities := service.NewIdentityBindingService(st, core)

	token, _ := activationAccessToken(t, authorization, auth.ScopeIdentityBindingsWrite, auth.ScopeIdentityBindingsRead)
	server := &Server{Auth: authorization, IdentityBindings: identities}
	return server, core, token
}

func callBindIdentity(t *testing.T, server *Server, token, principalID, agentID, idempotencyKey string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"agent_id": agentID})
	req := httptest.NewRequest(http.MethodPost, "/v1/identity-bindings/"+principalID+"/bind", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)
	return recorder
}

func callRevokeIdentity(t *testing.T, server *Server, token, principalID, idempotencyKey string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"reason_code": "test"})
	req := httptest.NewRequest(http.MethodPost, "/v1/identity-bindings/"+principalID+"/revoke", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)
	return recorder
}

func callIdentityBindingStatus(t *testing.T, server *Server, token, principalID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/identity-bindings/"+principalID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)
	return recorder
}

func TestHandleBindIdentity_GoldenPath(t *testing.T) {
	server, core, token := newIdentityBindingTestServer(t)
	core.SeedAgentIdentity("agt_rest_bind")

	recorder := callBindIdentity(t, server, token, "prn_rest_bind", "agt_rest_bind", "bind-rest-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var decoded bindIdentityResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Created || decoded.AgentID != "agt_rest_bind" || decoded.Network != "tos-devnet" {
		t.Fatalf("unexpected bind response: %+v", decoded)
	}
}

func TestHandleBindIdentity_UnresolvedAgentRejected(t *testing.T) {
	server, _, token := newIdentityBindingTestServer(t)
	recorder := callBindIdentity(t, server, token, "prn_rest_unresolved", "agt_never_seeded", "bind-rest-2")
	if recorder.Code == http.StatusOK {
		t.Fatalf("expected an error for an unresolved agent_id, got 200: %s", recorder.Body.String())
	}
}

func TestHandleBindIdentity_RequiresIdempotencyKey(t *testing.T) {
	server, core, token := newIdentityBindingTestServer(t)
	core.SeedAgentIdentity("agt_rest_noidem")
	recorder := callBindIdentity(t, server, token, "prn_rest_noidem", "agt_rest_noidem", "")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a missing Idempotency-Key: %s", recorder.Code, recorder.Body.String())
	}
}

func TestHandleBindIdentity_RequiresScope(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	core := toscoremock.NewContractFixture(st)
	identities := service.NewIdentityBindingService(st, core)
	server := &Server{Auth: authorization, IdentityBindings: identities}
	// Token deliberately carries no identity_bindings:write scope.
	token, _ := activationAccessToken(t, authorization, auth.ScopeCapabilitiesRead)
	recorder := callBindIdentity(t, server, token, "prn_rest_noscope", "agt_x", "bind-rest-3")
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %s", recorder.Code, recorder.Body.String())
	}
}

func TestHandleBindIdentity_RetryWithSameIdempotencyKeyReturnsIdenticalResponse(t *testing.T) {
	server, core, token := newIdentityBindingTestServer(t)
	core.SeedAgentIdentity("agt_rest_retry")
	first := callBindIdentity(t, server, token, "prn_rest_retry", "agt_rest_retry", "bind-rest-retry")
	if first.Code != http.StatusOK {
		t.Fatalf("first call status = %d, body = %s", first.Code, first.Body.String())
	}
	retry := callBindIdentity(t, server, token, "prn_rest_retry", "agt_rest_retry", "bind-rest-retry")
	if retry.Code != http.StatusOK {
		t.Fatalf("retry status = %d, body = %s", retry.Code, retry.Body.String())
	}
	if first.Body.String() != retry.Body.String() {
		t.Fatalf("retry body diverged: first=%s retry=%s", first.Body.String(), retry.Body.String())
	}
}

func TestHandleIdentityBindingStatus_NeverBound(t *testing.T) {
	server, _, token := newIdentityBindingTestServer(t)
	recorder := callIdentityBindingStatus(t, server, token, "prn_rest_never_bound")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var decoded identityBindingStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Bound || decoded.Status != "unspecified" {
		t.Fatalf("unexpected status for never-bound principal: %+v", decoded)
	}
}

func TestHandleIdentityBindingStatus_BoundThenRevoked(t *testing.T) {
	server, core, token := newIdentityBindingTestServer(t)
	core.SeedAgentIdentity("agt_rest_status")
	if r := callBindIdentity(t, server, token, "prn_rest_status", "agt_rest_status", "bind-rest-status"); r.Code != http.StatusOK {
		t.Fatalf("bind failed: %s", r.Body.String())
	}

	bound := callIdentityBindingStatus(t, server, token, "prn_rest_status")
	if bound.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", bound.Code, bound.Body.String())
	}
	var decoded identityBindingStatusResponse
	if err := json.Unmarshal(bound.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Bound || decoded.AgentID != "agt_rest_status" {
		t.Fatalf("expected bound status: %+v", decoded)
	}

	revokeRecorder := callRevokeIdentity(t, server, token, "prn_rest_status", "revoke-rest-status")
	if revokeRecorder.Code != http.StatusOK {
		t.Fatalf("revoke status = %d, body = %s", revokeRecorder.Code, revokeRecorder.Body.String())
	}
	var revokeDecoded revokeIdentityResponse
	if err := json.Unmarshal(revokeRecorder.Body.Bytes(), &revokeDecoded); err != nil {
		t.Fatal(err)
	}
	if !revokeDecoded.Revoked {
		t.Fatalf("expected revoked=true: %s", revokeRecorder.Body.String())
	}

	afterRevoke := callIdentityBindingStatus(t, server, token, "prn_rest_status")
	var afterDecoded identityBindingStatusResponse
	if err := json.Unmarshal(afterRevoke.Body.Bytes(), &afterDecoded); err != nil {
		t.Fatal(err)
	}
	if afterDecoded.Bound {
		t.Fatalf("expected unbound status after revoke: %+v", afterDecoded)
	}
}

func TestHandleRevokeIdentity_NothingToRevokeIsNotAnError(t *testing.T) {
	server, _, token := newIdentityBindingTestServer(t)
	recorder := callRevokeIdentity(t, server, token, "prn_rest_revoke_noop", "revoke-rest-noop")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for a no-op revoke: %s", recorder.Code, recorder.Body.String())
	}
	var decoded revokeIdentityResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Revoked {
		t.Fatalf("expected revoked=false, got true")
	}
}
