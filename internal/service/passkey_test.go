package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	vwa "github.com/descope/virtualwebauthn"
	"github.com/go-webauthn/webauthn/webauthn"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

const passkeyTestRPID = "localhost"
const passkeyTestOrigin = "http://localhost"

func newPasskeyTestService(t *testing.T) *service.PasskeyService {
	t.Helper()
	st := memory.New()
	authorization, err := auth.Open(auth.Config{})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := webauthn.New(&webauthn.Config{RPID: passkeyTestRPID, RPDisplayName: "Test ATOS", RPOrigins: []string{passkeyTestOrigin}})
	if err != nil {
		t.Fatal(err)
	}
	return service.NewPasskeyService(st, instance, authorization)
}

// registerPasskeyAccount drives a full, cryptographically real signup
// ceremony end-to-end using a software (virtual) authenticator in place of
// a browser + physical security key, mirroring tosnetwork/atos-aidrop's own
// registerWebAuthnCredential test helper. Returns the authenticator (for a
// later login ceremony) and the resulting token pair (which carries the
// newly minted principal_id).
func registerPasskeyAccount(t *testing.T, s *service.PasskeyService) (vwa.Authenticator, auth.TokenPair) {
	t.Helper()
	ctx := context.Background()
	ceremonyID, options, err := s.BeginRegistration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	optionsJSON, err := json.Marshal(options.Response)
	if err != nil {
		t.Fatal(err)
	}
	attestationOptions, err := vwa.ParseAttestationOptions(string(optionsJSON))
	if err != nil {
		t.Fatal(err)
	}
	// webauthnUser.WebAuthnID() returns []byte(principalID) (see
	// internal/service/passkey.go), and the parsed CredentialCreation
	// options carry that exact value back as attestationOptions.UserID --
	// read it from there rather than depending on any other channel,
	// exactly mirroring how a real browser/authenticator only ever sees
	// what the options payload says.
	rp := vwa.RelyingParty{ID: passkeyTestRPID, Name: "Test ATOS", Origin: passkeyTestOrigin}
	authenticator := vwa.NewAuthenticator()
	authenticator.Options.UserHandle = []byte(attestationOptions.UserID)
	credential := vwa.NewCredential(vwa.KeyTypeEC2)
	authenticator.AddCredential(credential)
	attestationResponse := vwa.CreateAttestationResponse(rp, authenticator, credential, *attestationOptions)

	req := httptest.NewRequest(http.MethodPost, "/v1/auth/passkey/register/finish/"+ceremonyID, bytes.NewReader([]byte(attestationResponse)))
	req.Header.Set("Content-Type", "application/json")
	pair, err := s.FinishRegistration(ctx, ceremonyID, req)
	if err != nil {
		t.Fatal(err)
	}
	return authenticator, pair
}

func TestPasskeySignup_IssuesTokenWithDefaultScopeBundle(t *testing.T) {
	s := newPasskeyTestService(t)
	_, pair := registerPasskeyAccount(t, s)

	if pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatalf("pair=%+v, want non-empty tokens", pair)
	}
	if pair.Principal.ID == "" {
		t.Fatalf("principal ID is empty: %+v", pair)
	}
	want := map[auth.Scope]bool{
		auth.ScopeCapabilitiesRead: true, auth.ScopeQuotesRead: true, auth.ScopeInvocationsCreate: true,
		auth.ScopeJobsCreate: true, auth.ScopeJobsRead: true, auth.ScopeJobsCancel: true, auth.ScopeAccountRead: true,
		auth.ScopeDisputesOpen: true, auth.ScopeDisputesRead: true,
		auth.ScopeOpenTasksRead: true, auth.ScopeOpenTasksWrite: true,
		auth.ScopeCapabilitiesWrite: true, auth.ScopeOpenTaskProposalsWrite: true,
	}
	if len(pair.Principal.Scopes) != len(want) {
		t.Fatalf("scopes = %v, want exactly %v", pair.Principal.Scopes, want)
	}
	for scope := range want {
		if !pair.Principal.Has(scope) {
			t.Fatalf("missing expected default scope %q: %+v", scope, pair.Principal.Scopes)
		}
	}
	// Explicit-grant-only scopes must never be included.
	for _, forbidden := range []auth.Scope{auth.ScopeExecutionSignersWrite, auth.ScopeSettlementWrite, auth.ScopeDisputesReview, auth.ScopeActivationEvaluate} {
		if pair.Principal.Has(forbidden) {
			t.Fatalf("passkey signup must never grant explicit-grant-only scope %q", forbidden)
		}
	}
}

func TestPasskeyLogin_SucceedsWithRegisteredCredential(t *testing.T) {
	s := newPasskeyTestService(t)
	ctx := context.Background()
	authenticator, signupPair := registerPasskeyAccount(t, s)

	ceremonyID, assertion, err := s.BeginLogin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertionOptionsJSON, err := json.Marshal(assertion.Response)
	if err != nil {
		t.Fatal(err)
	}
	assertionOptions, err := vwa.ParseAssertionOptions(string(assertionOptionsJSON))
	if err != nil {
		t.Fatal(err)
	}
	rp := vwa.RelyingParty{ID: passkeyTestRPID, Name: "Test ATOS", Origin: passkeyTestOrigin}
	credential := authenticator.Credentials[0]
	assertionResponse := vwa.CreateAssertionResponse(rp, authenticator, credential, *assertionOptions)

	req := httptest.NewRequest(http.MethodPost, "/v1/auth/passkey/login/finish/"+ceremonyID, bytes.NewReader([]byte(assertionResponse)))
	req.Header.Set("Content-Type", "application/json")
	loginPair, err := s.FinishLogin(ctx, ceremonyID, req)
	if err != nil {
		t.Fatal(err)
	}
	if loginPair.Principal.ID != signupPair.Principal.ID {
		t.Fatalf("login principal = %s, want the same account as signup (%s)", loginPair.Principal.ID, signupPair.Principal.ID)
	}
	if loginPair.AccessToken == signupPair.AccessToken {
		t.Fatal("login must mint a fresh token pair, not reuse signup's")
	}

	// The ceremony is single-use.
	if _, err := s.FinishLogin(ctx, ceremonyID, req); err == nil {
		t.Fatal("expected the already-consumed ceremony to be rejected")
	}
}

func TestPasskeyLogin_FailsWithUnregisteredCredential(t *testing.T) {
	s := newPasskeyTestService(t)
	ctx := context.Background()
	registerPasskeyAccount(t, s) // an unrelated account exists, but the login below uses a different, unregistered authenticator

	ceremonyID, assertion, err := s.BeginLogin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertionOptionsJSON, err := json.Marshal(assertion.Response)
	if err != nil {
		t.Fatal(err)
	}
	assertionOptions, err := vwa.ParseAssertionOptions(string(assertionOptionsJSON))
	if err != nil {
		t.Fatal(err)
	}
	rp := vwa.RelyingParty{ID: passkeyTestRPID, Name: "Test ATOS", Origin: passkeyTestOrigin}
	authenticator := vwa.NewAuthenticator()
	authenticator.Options.UserHandle = []byte("prn_never-registered")
	credential := vwa.NewCredential(vwa.KeyTypeEC2)
	authenticator.AddCredential(credential)
	assertionResponse := vwa.CreateAssertionResponse(rp, authenticator, credential, *assertionOptions)

	req := httptest.NewRequest(http.MethodPost, "/v1/auth/passkey/login/finish/"+ceremonyID, bytes.NewReader([]byte(assertionResponse)))
	req.Header.Set("Content-Type", "application/json")
	if _, err := s.FinishLogin(ctx, ceremonyID, req); err == nil {
		t.Fatal("expected an unrecognized credential to be rejected")
	}
}

func TestPasskeyNotConfigured_FailsClosed(t *testing.T) {
	st := memory.New()
	authorization, err := auth.Open(auth.Config{})
	if err != nil {
		t.Fatal(err)
	}
	s := service.NewPasskeyService(st, nil, authorization)

	if _, _, err := s.BeginRegistration(context.Background()); !errors.Is(err, service.ErrPasskeyNotConfigured) {
		t.Fatalf("BeginRegistration error = %v, want ErrPasskeyNotConfigured", err)
	}
	if _, _, err := s.BeginLogin(context.Background()); !errors.Is(err, service.ErrPasskeyNotConfigured) {
		t.Fatalf("BeginLogin error = %v, want ErrPasskeyNotConfigured", err)
	}
}
