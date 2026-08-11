package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	vwa "github.com/descope/virtualwebauthn"
	"github.com/go-webauthn/webauthn/webauthn"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

const passkeyHTTPTestRPID = "localhost"
const passkeyHTTPTestOrigin = "http://localhost"

func newPasskeyHTTPTestServer(t *testing.T) *Server {
	t.Helper()
	st := memory.New()
	authorization, err := auth.Open(auth.Config{})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := webauthn.New(&webauthn.Config{RPID: passkeyHTTPTestRPID, RPDisplayName: "Test ATOS", RPOrigins: []string{passkeyHTTPTestOrigin}})
	if err != nil {
		t.Fatal(err)
	}
	return &Server{Auth: authorization, Passkeys: service.NewPasskeyService(st, instance, authorization)}
}

type ceremonyBeginResponse struct {
	CeremonyID string          `json:"ceremony_id"`
	Options    json.RawMessage `json:"options"`
}

func TestPasskeyHTTP_RegisterThenLogin(t *testing.T) {
	server := newPasskeyHTTPTestServer(t)

	beginReq := httptest.NewRequest(http.MethodPost, "/v1/auth/passkey/register/begin", nil)
	beginRecorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(beginRecorder, beginReq)
	if beginRecorder.Code != http.StatusOK {
		t.Fatalf("register/begin status = %d, body = %s", beginRecorder.Code, beginRecorder.Body.String())
	}
	var begin ceremonyBeginResponse
	if err := json.Unmarshal(beginRecorder.Body.Bytes(), &begin); err != nil {
		t.Fatalf("decode register/begin response: %v", err)
	}

	attestationOptions, err := vwa.ParseAttestationOptions(string(begin.Options))
	if err != nil {
		t.Fatal(err)
	}
	rp := vwa.RelyingParty{ID: passkeyHTTPTestRPID, Name: "Test ATOS", Origin: passkeyHTTPTestOrigin}
	authenticator := vwa.NewAuthenticator()
	authenticator.Options.UserHandle = []byte(attestationOptions.UserID)
	credential := vwa.NewCredential(vwa.KeyTypeEC2)
	authenticator.AddCredential(credential)
	attestationResponse := vwa.CreateAttestationResponse(rp, authenticator, credential, *attestationOptions)

	finishReq := httptest.NewRequest(http.MethodPost, "/v1/auth/passkey/register/finish/"+begin.CeremonyID, bytes.NewReader([]byte(attestationResponse)))
	finishReq.Header.Set("Content-Type", "application/json")
	finishRecorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(finishRecorder, finishReq)
	if finishRecorder.Code != http.StatusCreated {
		t.Fatalf("register/finish status = %d, body = %s", finishRecorder.Code, finishRecorder.Body.String())
	}
	var signupToken struct {
		AccessToken string   `json:"access_token"`
		PrincipalID string   `json:"principal_id"`
		Scopes      []string `json:"scopes"`
	}
	if err := json.Unmarshal(finishRecorder.Body.Bytes(), &signupToken); err != nil {
		t.Fatalf("decode register/finish response: %v", err)
	}
	if signupToken.AccessToken == "" || signupToken.PrincipalID == "" || len(signupToken.Scopes) == 0 {
		t.Fatalf("signup token response incomplete: %+v", signupToken)
	}

	// Now log back in with the same credential.
	loginBeginReq := httptest.NewRequest(http.MethodPost, "/v1/auth/passkey/login/begin", nil)
	loginBeginRecorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(loginBeginRecorder, loginBeginReq)
	if loginBeginRecorder.Code != http.StatusOK {
		t.Fatalf("login/begin status = %d, body = %s", loginBeginRecorder.Code, loginBeginRecorder.Body.String())
	}
	var loginBegin ceremonyBeginResponse
	if err := json.Unmarshal(loginBeginRecorder.Body.Bytes(), &loginBegin); err != nil {
		t.Fatalf("decode login/begin response: %v", err)
	}
	assertionOptions, err := vwa.ParseAssertionOptions(string(loginBegin.Options))
	if err != nil {
		t.Fatal(err)
	}
	assertionResponse := vwa.CreateAssertionResponse(rp, authenticator, credential, *assertionOptions)

	loginFinishReq := httptest.NewRequest(http.MethodPost, "/v1/auth/passkey/login/finish/"+loginBegin.CeremonyID, bytes.NewReader([]byte(assertionResponse)))
	loginFinishReq.Header.Set("Content-Type", "application/json")
	loginFinishRecorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(loginFinishRecorder, loginFinishReq)
	if loginFinishRecorder.Code != http.StatusOK {
		t.Fatalf("login/finish status = %d, body = %s", loginFinishRecorder.Code, loginFinishRecorder.Body.String())
	}
	var loginToken struct {
		PrincipalID string `json:"principal_id"`
	}
	if err := json.Unmarshal(loginFinishRecorder.Body.Bytes(), &loginToken); err != nil {
		t.Fatalf("decode login/finish response: %v", err)
	}
	if loginToken.PrincipalID != signupToken.PrincipalID {
		t.Fatalf("login principal_id = %s, want signup's %s", loginToken.PrincipalID, signupToken.PrincipalID)
	}
}

func TestPasskeyHTTP_NotConfiguredReturns503(t *testing.T) {
	st := memory.New()
	authorization, err := auth.Open(auth.Config{})
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{Auth: authorization, Passkeys: service.NewPasskeyService(st, nil, authorization)}

	req := httptest.NewRequest(http.MethodPost, "/v1/auth/passkey/register/begin", nil)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when passkey auth is unconfigured: %s", recorder.Code, recorder.Body.String())
	}
}

// TestPasskeyHTTP_InvalidWebAuthnResponseIsAuthFailureNotInternalError is a
// regression test for a real P2: a *protocol.Error (go-webauthn's own
// typed error for every challenge/origin/signature/CBOR validation
// failure -- an ordinary, expected outcome when a caller submits an
// invalid response, not a server malfunction) used to fall through to the
// catch-all branch, forwarding err.Error() verbatim to the anonymous
// caller and logging it as a server error. It must instead read as a
// generic, non-leaky authentication failure.
func TestPasskeyHTTP_InvalidWebAuthnResponseIsAuthFailureNotInternalError(t *testing.T) {
	server := newPasskeyHTTPTestServer(t)

	beginReq := httptest.NewRequest(http.MethodPost, "/v1/auth/passkey/register/begin", nil)
	beginRecorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(beginRecorder, beginReq)
	if beginRecorder.Code != http.StatusOK {
		t.Fatalf("register/begin status = %d, body = %s", beginRecorder.Code, beginRecorder.Body.String())
	}
	var begin ceremonyBeginResponse
	if err := json.Unmarshal(beginRecorder.Body.Bytes(), &begin); err != nil {
		t.Fatalf("decode register/begin response: %v", err)
	}

	// Garbage, not a valid WebAuthn attestation response -- go-webauthn's
	// FinishRegistration fails with its own *protocol.Error.
	finishReq := httptest.NewRequest(http.MethodPost, "/v1/auth/passkey/register/finish/"+begin.CeremonyID, bytes.NewReader([]byte(`{"not":"a valid attestation response"}`)))
	finishReq.Header.Set("Content-Type", "application/json")
	finishRecorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(finishRecorder, finishReq)

	if finishRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 for an invalid WebAuthn response (an ordinary auth failure, not an internal error): %s", finishRecorder.Code, finishRecorder.Body.String())
	}
	body := finishRecorder.Body.String()
	for _, leaky := range []string{"webauthn", "cbor", "json:", "unmarshal", "parse error"} {
		if strings.Contains(strings.ToLower(body), leaky) {
			t.Fatalf("response body leaks internal error detail (%q): %s", leaky, body)
		}
	}
}

// TestPasskeyHTTP_RateLimitCannotBeBypassedBySpoofedHeader is a regression
// test for a real P1: clientIP used to trust a caller-suppliable X-Real-IP
// header unconditionally, so an anonymous attacker could pick a fresh
// rate-limit bucket on every single request just by varying that header,
// completely defeating the limiter. All requests here share one
// RemoteAddr (as httptest.NewRequest always sets) but carry a different
// X-Real-IP each time -- the limit must still trigger based on the real
// connection address.
func TestPasskeyHTTP_RateLimitCannotBeBypassedBySpoofedHeader(t *testing.T) {
	server := newPasskeyHTTPTestServer(t)

	var last *httptest.ResponseRecorder
	for i := 0; i < 11; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/auth/passkey/register/begin", nil)
		req.Header.Set("X-Real-IP", fmt.Sprintf("198.51.100.%d", i))
		recorder := httptest.NewRecorder()
		server.Mux().ServeHTTP(recorder, req)
		last = recorder
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("11th request (with a fresh spoofed X-Real-IP each time) status = %d, want 429 -- rate limiting must key on the real connection address, not a client-suppliable header: %s", last.Code, last.Body.String())
	}
}
