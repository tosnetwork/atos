package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
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

func TestClientIP_IgnoresForwardedHeaderWithoutTrustedProxyConfig(t *testing.T) {
	server := &Server{}
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = "192.0.2.1:5555"
	req.Header.Set("X-Real-IP", "203.0.113.99")

	if got := server.clientIP(req); got != "192.0.2.1" {
		t.Fatalf("clientIP = %q, want the real peer address (192.0.2.1) since no trusted proxy is configured", got)
	}
}

func trustedCIDRServer(t *testing.T, cidrs ...string) *Server {
	t.Helper()
	prefixes := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		prefix, err := netip.ParsePrefix(c)
		if err != nil {
			t.Fatal(err)
		}
		prefixes = append(prefixes, prefix)
	}
	return &Server{TrustedProxyCIDRs: prefixes}
}

func TestClientIP_TrustsForwardedHeaderOnlyFromConfiguredProxyCIDR(t *testing.T) {
	server := trustedCIDRServer(t, "192.0.2.0/24")

	trusted := httptest.NewRequest(http.MethodPost, "/", nil)
	trusted.RemoteAddr = "192.0.2.1:5555" // inside the trusted CIDR
	trusted.Header.Set("X-Real-IP", "203.0.113.99")
	if got := server.clientIP(trusted); got != "203.0.113.99" {
		t.Fatalf("clientIP = %q, want the forwarded address (203.0.113.99) since the peer is a trusted proxy", got)
	}

	untrusted := httptest.NewRequest(http.MethodPost, "/", nil)
	untrusted.RemoteAddr = "198.51.100.1:5555" // outside the trusted CIDR
	untrusted.Header.Set("X-Real-IP", "203.0.113.99")
	if got := server.clientIP(untrusted); got != "198.51.100.1" {
		t.Fatalf("clientIP = %q, want the real peer address (198.51.100.1) -- an untrusted peer's forwarded header must never be trusted even when SOME proxy CIDR is configured", got)
	}
}

// TestClientIP_XForwardedForAppendModeAttackIsRejected is a regression
// test for a real P1: nginx's default proxy_add_x_forwarded_for (and many
// other reverse proxies) APPENDS to an incoming X-Forwarded-For rather
// than overwriting it, so a client can pre-seed an arbitrary value and
// have the trusted proxy append the real address after it. Naively taking
// the leftmost/first entry (the previous implementation) would return the
// client's own forged value, letting an attacker pick a fresh rate-limit
// bucket on every request just by varying that string. The fix walks
// right-to-left, skipping entries that are themselves inside a trusted
// CIDR (the proxy hops this deployment trusts), and returns the first one
// that isn't -- correctly landing on the real client here despite the
// attacker-controlled leftmost entry.
func TestClientIP_XForwardedForAppendModeAttackIsRejected(t *testing.T) {
	server := trustedCIDRServer(t, "192.0.2.0/24")

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = "192.0.2.1:5555" // the trusted proxy's own peer address
	// Attacker-forged leftmost entry, with the trusted proxy's own append
	// (its own address, matching how nginx's $proxy_add_x_forwarded_for
	// appends $remote_addr) landing right before the real client.
	req.Header.Set("X-Forwarded-For", "203.0.113.250, 192.0.2.1, 203.0.113.7")
	if got := server.clientIP(req); got != "203.0.113.7" {
		t.Fatalf("clientIP = %q, want 203.0.113.7 (the real client, immediately left of the trusted proxy's own appended hop) -- not the attacker-forged leftmost entry 203.0.113.250", got)
	}
}

func TestClientIP_FallsBackToForwardedForWhenNoRealIP(t *testing.T) {
	server := trustedCIDRServer(t, "192.0.2.0/24")

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = "192.0.2.1:5555"
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 192.0.2.1")
	if got := server.clientIP(req); got != "203.0.113.7" {
		t.Fatalf("clientIP = %q, want 203.0.113.7 -- the rightmost entry (192.0.2.1) is the trusted proxy's own appended hop and must be skipped", got)
	}
}

func TestClientIP_RejectsUnparseableForwardedValues(t *testing.T) {
	server := trustedCIDRServer(t, "192.0.2.0/24")

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = "192.0.2.1:5555"
	req.Header.Set("X-Real-IP", "not-an-ip")
	req.Header.Set("X-Forwarded-For", "also-not-an-ip, 192.0.2.1")
	if got := server.clientIP(req); got != "192.0.2.1" {
		t.Fatalf("clientIP = %q, want the trusted peer address (192.0.2.1) as a safe fallback when nothing in either forwarded header parses as an IP", got)
	}
}

func TestClientIP_TrustsIPv6Peer(t *testing.T) {
	server := trustedCIDRServer(t, "2001:db8::/32")

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = "[2001:db8::1]:5555"
	req.Header.Set("X-Real-IP", "2001:db8:1::42")
	if got := server.clientIP(req); got != "2001:db8:1::42" {
		t.Fatalf("clientIP = %q, want the forwarded IPv6 address (2001:db8:1::42)", got)
	}
}
