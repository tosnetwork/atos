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

// TestHandleBindIdentity_RebindSameAgentWithFreshKeyReportsCreatedFalse
// proves Created reflects whether CreatePrincipalBinding reported a
// genuinely NEW binding, not merely Checkpoint reaching Completed (which is
// true for both a new bind AND an idempotent replay/no-op rebind of an
// already-existing same-principal/same-agent binding under a DIFFERENT
// idempotency key) -- docs/API.md §9A documents created:false as the real,
// expected outcome for this exact case.
func TestHandleBindIdentity_RebindSameAgentWithFreshKeyReportsCreatedFalse(t *testing.T) {
	server, core, token := newIdentityBindingTestServer(t)
	core.SeedAgentIdentity("agt_rest_rebind")
	first := callBindIdentity(t, server, token, "prn_rest_rebind", "agt_rest_rebind", "bind-rest-rebind-1")
	if first.Code != http.StatusOK {
		t.Fatalf("first call status = %d, body = %s", first.Code, first.Body.String())
	}
	var firstDecoded bindIdentityResponse
	if err := json.Unmarshal(first.Body.Bytes(), &firstDecoded); err != nil {
		t.Fatal(err)
	}
	if !firstDecoded.Created {
		t.Fatalf("first bind must report created=true: %+v", firstDecoded)
	}

	// A DIFFERENT idempotency key, rebinding to the SAME agent -- a
	// documented no-op, not a fresh key replay of the original bind.
	rebind := callBindIdentity(t, server, token, "prn_rest_rebind", "agt_rest_rebind", "bind-rest-rebind-2")
	if rebind.Code != http.StatusOK {
		t.Fatalf("rebind status = %d, body = %s", rebind.Code, rebind.Body.String())
	}
	var rebindDecoded bindIdentityResponse
	if err := json.Unmarshal(rebind.Body.Bytes(), &rebindDecoded); err != nil {
		t.Fatal(err)
	}
	if rebindDecoded.Created {
		t.Fatalf("rebinding to the SAME agent under a fresh key must report created=false, not true: %+v", rebindDecoded)
	}
	if rebindDecoded.BindingRef != firstDecoded.BindingRef {
		t.Fatalf("rebind must return the ORIGINAL binding_ref: first=%q rebind=%q", firstDecoded.BindingRef, rebindDecoded.BindingRef)
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
	// docs/API.md §9A's frozen revoke response includes network+
	// revocation_ref (the same NetworkReference shape bind's binding_ref
	// uses) -- a caller needs this to audit the revocation itself, not just
	// its local side effect.
	if revokeDecoded.Network == "" || revokeDecoded.RevocationRef == "" {
		t.Fatalf("expected network/revocation_ref to be populated on a real revoke: %+v", revokeDecoded)
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

// TestHandleRevokeIdentity_RetryWithFreshKeyIsInternallyConsistent proves a
// lost-response retry under a DIFFERENT Idempotency-Key (the documented
// safe retry pattern) reports revoked:true with the ORIGINAL network/
// revocation_ref -- not revoked:false alongside a populated network/
// revocation_ref, which would violate this endpoint's own contract that
// those fields are only ever populated when revoked:true.
func TestHandleRevokeIdentity_RetryWithFreshKeyIsInternallyConsistent(t *testing.T) {
	server, core, token := newIdentityBindingTestServer(t)
	core.SeedAgentIdentity("agt_rest_retry")
	if r := callBindIdentity(t, server, token, "prn_rest_retry", "agt_rest_retry", "bind-rest-retry"); r.Code != http.StatusOK {
		t.Fatalf("bind failed: %s", r.Body.String())
	}

	first := callRevokeIdentity(t, server, token, "prn_rest_retry", "revoke-rest-retry-1")
	if first.Code != http.StatusOK {
		t.Fatalf("first revoke status = %d, body = %s", first.Code, first.Body.String())
	}
	var firstDecoded revokeIdentityResponse
	if err := json.Unmarshal(first.Body.Bytes(), &firstDecoded); err != nil {
		t.Fatal(err)
	}
	if !firstDecoded.Revoked || firstDecoded.Network == "" || firstDecoded.RevocationRef == "" {
		t.Fatalf("unexpected first revoke response: %+v", firstDecoded)
	}

	// Simulates the response being lost and the caller retrying under a
	// DIFFERENT Idempotency-Key -- this principal's local binding row is
	// already gone, but the response must still be internally consistent.
	retry := callRevokeIdentity(t, server, token, "prn_rest_retry", "revoke-rest-retry-2")
	if retry.Code != http.StatusOK {
		t.Fatalf("retry revoke status = %d, body = %s", retry.Code, retry.Body.String())
	}
	var retryDecoded revokeIdentityResponse
	if err := json.Unmarshal(retry.Body.Bytes(), &retryDecoded); err != nil {
		t.Fatal(err)
	}
	if !retryDecoded.Revoked {
		t.Fatalf("retry must report revoked=true (consistent with the original outcome), got: %+v", retryDecoded)
	}
	if retryDecoded.Network != firstDecoded.Network || retryDecoded.RevocationRef != firstDecoded.RevocationRef {
		t.Fatalf("retry must return the ORIGINAL network/revocation_ref: first=%+v retry=%+v", firstDecoded, retryDecoded)
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
	if decoded.Network != "" || decoded.RevocationRef != "" {
		t.Fatalf("a no-op revoke (nothing bound) must not fabricate a ref: %+v", decoded)
	}
}
