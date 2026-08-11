// Package service: passkey/WebAuthn human account authentication --
// atos-spec docs/AUTH.md's "Human Account Authentication (Passkey/WebAuthn)"
// section. This is the "who is this human" identity primitive Device
// Authorization's /activate consent page always assumed existed elsewhere
// (a "trusted login/reverse-proxy boundary" injecting X-ATOS-Approval-Token
// and X-ATOS-Principal-ID) but never actually had -- confirmed absent from
// every repo in this workspace. Modeled directly on
// tosnetwork/atos-aidrop's proven tos-wallet-web implementation: same
// go-webauthn library, same usernameless/discoverable-credential ceremony
// shape. Unlike atos-aidrop, there is no wallet/ledger provisioning,
// referral bookkeeping, captcha, or public-key encryption-at-rest here --
// none of those are part of establishing "who is this human" for atos.im;
// see docs/AUTH.md for the explicit scope boundary.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// passkeyCeremonyTTL bounds how long a registration/login ceremony has to
// complete once begun, matching atos-aidrop's own window.
const passkeyCeremonyTTL = 5 * time.Minute

// displayHandleLength mirrors atos-aidrop's accountIDLength: 12 uppercase
// hex characters is 48 bits of entropy, ample for a collision-retry loop
// rather than a database-enforced sequence.
const displayHandleLength = 12

var (
	ErrPasskeyNotConfigured = errors.New("passkey sign-in is not configured")
	ErrNoPasskeyCredentials = errors.New("no passkeys are registered for this account")
	ErrPasskeyRateLimited   = errors.New("too many attempts, please try again shortly")
)

// passkeyBeginRateLimit/Window bound how many ceremony-begin calls one
// remote subject (IP) may make -- see passkeyRateLimiter's doc comment for
// why this exists at all (BeginRegistration/BeginLogin are the only two
// genuinely anonymous, unauthenticated entry points in this service).
const (
	passkeyBeginRateLimit  = 10
	passkeyBeginRateWindow = time.Minute
)

// passkeyDefaultScopes is the fixed v1 scope bundle every passkey-issued
// token carries -- auth.DefaultConsumerScopes() plus the two provider-role
// scopes atos.im's own web product needs today (capabilities:write,
// open_task_proposals:write), so a signed-up user can immediately publish
// tasks, manage capabilities and bid as a provider without a second
// consent step. See docs/AUTH.md's own doc comment for why v1 deliberately
// keeps one bundle instead of a narrower default plus an upgrade flow.
// Never includes an explicit-grant-only/admin scope -- passkey login must
// remain exactly as unprivileged as an ordinary self-service Device
// Authorization grant.
func passkeyDefaultScopes() []auth.Scope {
	return append(auth.DefaultConsumerScopes(), auth.ScopeCapabilitiesWrite, auth.ScopeOpenTaskProposalsWrite)
}

// webauthnUser adapts a PasskeyAccount and its stored credentials to the
// webauthn.User interface. The WebAuthn "user handle" is the account's own
// PrincipalID -- stable, unique, and already the primary key elsewhere, so
// no separate handle needs to be minted.
type webauthnUser struct {
	handle      []byte
	displayName string
	credentials []webauthn.Credential
}

func (u *webauthnUser) WebAuthnID() []byte                         { return u.handle }
func (u *webauthnUser) WebAuthnName() string                       { return u.displayName }
func (u *webauthnUser) WebAuthnDisplayName() string                { return u.displayName }
func (u *webauthnUser) WebAuthnCredentials() []webauthn.Credential { return u.credentials }

type PasskeyService struct {
	store       store.Store
	webAuthn    *webauthn.WebAuthn
	auth        *auth.Service
	rateLimiter *passkeyRateLimiter
}

// NewPasskeyService returns nil webAuthn safely -- every method below
// checks for it and returns ErrPasskeyNotConfigured, mirroring
// atos-aidrop's own "not configured" fallback, so a deployment that hasn't
// set ATOS_WEBAUTHN_RP_ID yet fails closed rather than panicking.
func NewPasskeyService(s store.Store, webAuthn *webauthn.WebAuthn, authService *auth.Service) *PasskeyService {
	return &PasskeyService{store: s, webAuthn: webAuthn, auth: authService, rateLimiter: newPasskeyRateLimiter()}
}

func toWebAuthnCredentials(records []domain.WebAuthnCredentialRecord) []webauthn.Credential {
	out := make([]webauthn.Credential, 0, len(records))
	for _, r := range records {
		var transports []protocol.AuthenticatorTransport
		if r.Transports != "" {
			for _, t := range strings.Split(r.Transports, ",") {
				transports = append(transports, protocol.AuthenticatorTransport(t))
			}
		}
		out = append(out, webauthn.Credential{
			ID: r.CredentialID, PublicKey: r.PublicKey,
			AttestationType: r.AttestationType, AttestationFormat: r.AttestationFormat,
			Transport: transports, Flags: webauthn.CredentialFlagsFromMsgpByte(r.Flags),
			Authenticator: webauthn.Authenticator{AAGUID: r.AAGUID, SignCount: r.SignCount, CloneWarning: r.CloneWarning},
		})
	}
	return out
}

func newWebauthnUser(principalID, displayHandle string, credentials []webauthn.Credential) *webauthnUser {
	return &webauthnUser{handle: []byte(principalID), displayName: displayHandle, credentials: credentials}
}

// generateDisplayHandle mints a fresh public display handle and retries on
// the astronomically unlikely event of a collision, mirroring
// atos-aidrop's generateAccountID exactly.
func (s *PasskeyService) generateDisplayHandle(ctx context.Context) (string, error) {
	for i := 0; i < 5; i++ {
		candidate := strings.ToUpper(strings.ReplaceAll(uuid.NewString(), "-", "")[:displayHandleLength])
		if _, err := s.store.PasskeyAccountByDisplayHandle(ctx, candidate); errors.Is(err, store.ErrNotFound) {
			return candidate, nil
		}
	}
	return "", errors.New("failed to generate a unique display handle")
}

func (s *PasskeyService) saveCeremony(ctx context.Context, principalID string, purpose domain.WebAuthnCeremonyPurpose, sessionData []byte) (string, error) {
	ceremonyID := "pkcer_" + uuid.NewString()
	now := time.Now().UTC()
	if err := s.store.CreateWebAuthnCeremony(ctx, domain.WebAuthnCeremony{
		ID: ceremonyID, PrincipalID: principalID, Purpose: purpose,
		SessionData: sessionData, ExpiresAt: now.Add(passkeyCeremonyTTL), CreatedAt: now,
	}); err != nil {
		return "", err
	}
	return ceremonyID, nil
}

// signupCeremonyPayload is the signup ceremony's session_data JSON: unlike
// a login ceremony, no account row exists yet at begin time, so the future
// account's identity travels alongside the WebAuthn challenge itself,
// mirroring atos-aidrop's signupCeremonyPayload exactly.
type signupCeremonyPayload struct {
	PrincipalID   string               `json:"principal_id"`
	DisplayHandle string               `json:"display_handle"`
	Session       webauthn.SessionData `json:"session"`
}

// BeginRegistration starts account creation: mints a fresh principal_id and
// display handle, then issues a resident-key (discoverable) passkey
// creation challenge for a not-yet-created account. There is no email step
// at all -- this and a successful passkey attestation are the entire
// signup flow.
func (s *PasskeyService) BeginRegistration(ctx context.Context, remoteSubject string) (string, *protocol.CredentialCreation, error) {
	if s.webAuthn == nil {
		return "", nil, ErrPasskeyNotConfigured
	}
	if !s.rateLimiter.allow("register:"+remoteSubject, passkeyBeginRateLimit, passkeyBeginRateWindow, time.Now().UTC()) {
		return "", nil, ErrPasskeyRateLimited
	}
	principalID := "prn_" + uuid.NewString()
	displayHandle, err := s.generateDisplayHandle(ctx)
	if err != nil {
		return "", nil, err
	}
	wu := newWebauthnUser(principalID, displayHandle, nil)
	options, session, err := s.webAuthn.BeginRegistration(wu, webauthn.WithResidentKeyRequirement(protocol.ResidentKeyRequirementRequired))
	if err != nil {
		return "", nil, err
	}
	payloadJSON, err := json.Marshal(signupCeremonyPayload{PrincipalID: principalID, DisplayHandle: displayHandle, Session: *session})
	if err != nil {
		return "", nil, err
	}
	ceremonyID, err := s.saveCeremony(ctx, "", domain.CeremonySignup, payloadJSON)
	if err != nil {
		return "", nil, err
	}
	return ceremonyID, options, nil
}

// FinishRegistration verifies the attestation, creates the account, and
// persists the newly attested passkey as that account's first credential --
// any failure after the attestation check means nothing was ever
// persisted, since CreateWebAuthnCredential only runs after
// CreatePasskeyAccount succeeds. Issues a token pair through
// auth.Service.IssueForPrincipal exactly as a successful login would.
func (s *PasskeyService) FinishRegistration(ctx context.Context, ceremonyID string, r *http.Request) (auth.TokenPair, error) {
	if s.webAuthn == nil {
		return auth.TokenPair{}, ErrPasskeyNotConfigured
	}
	ceremony, err := s.store.ConsumeWebAuthnCeremony(ctx, ceremonyID, domain.CeremonySignup)
	if err != nil {
		return auth.TokenPair{}, err
	}
	var payload signupCeremonyPayload
	if err := json.Unmarshal(ceremony.SessionData, &payload); err != nil {
		return auth.TokenPair{}, err
	}
	wu := newWebauthnUser(payload.PrincipalID, payload.DisplayHandle, nil)
	credential, err := s.webAuthn.FinishRegistration(wu, payload.Session, r)
	if err != nil {
		return auth.TokenPair{}, err
	}
	now := time.Now().UTC()
	record := buildCredentialRecord(payload.PrincipalID, credential, "Passkey")
	// One transaction: an account row must never durably exist without a
	// credential able to authenticate it (see
	// store.PasskeyAccounts.CreatePasskeyAccountWithCredential's doc
	// comment) -- a partial failure here (crash, credential_id collision)
	// rolls both writes back together rather than stranding an
	// unauthenticatable account.
	if err := s.store.CreatePasskeyAccountWithCredential(ctx,
		domain.PasskeyAccount{PrincipalID: payload.PrincipalID, DisplayHandle: payload.DisplayHandle, CreatedAt: now},
		record,
	); err != nil {
		return auth.TokenPair{}, err
	}
	return s.auth.IssueForPrincipal(payload.PrincipalID, passkeyDefaultScopes(), "web", "atos.im")
}

// buildCredentialRecord translates a just-attested go-webauthn Credential
// into the durable record shape, without persisting it -- callers decide
// how the write is committed (FinishRegistration commits it atomically
// alongside the new account; a future "add another passkey" flow would
// call SaveWebAuthnCredential directly against an already-existing
// account).
func buildCredentialRecord(principalID string, credential *webauthn.Credential, nickname string) domain.WebAuthnCredentialRecord {
	transports := make([]string, len(credential.Transport))
	for i, t := range credential.Transport {
		transports[i] = string(t)
	}
	nickname = strings.TrimSpace(nickname)
	if nickname == "" {
		nickname = "Passkey"
	}
	if len(nickname) > 100 {
		nickname = nickname[:100]
	}
	return domain.WebAuthnCredentialRecord{
		ID: "pkcred_" + uuid.NewString(), PrincipalID: principalID,
		CredentialID: credential.ID, PublicKey: credential.PublicKey,
		AttestationType: credential.AttestationType, AttestationFormat: credential.AttestationFormat,
		Transports: strings.Join(transports, ","), AAGUID: credential.Authenticator.AAGUID,
		Flags: credential.Flags.MsgpByte(), SignCount: credential.Authenticator.SignCount,
		BackupEligible: credential.Flags.BackupEligible, BackupState: credential.Flags.BackupState,
		Nickname: nickname, CreatedAt: time.Now().UTC(),
	}
}

// BeginLogin issues a usernameless, discoverable-credential login
// challenge: the browser's own passkey picker resolves which account is
// signing in, so no principal_id or other identifier is submitted here at
// all.
func (s *PasskeyService) BeginLogin(ctx context.Context, remoteSubject string) (string, *protocol.CredentialAssertion, error) {
	if s.webAuthn == nil {
		return "", nil, ErrPasskeyNotConfigured
	}
	if !s.rateLimiter.allow("login:"+remoteSubject, passkeyBeginRateLimit, passkeyBeginRateWindow, time.Now().UTC()) {
		return "", nil, ErrPasskeyRateLimited
	}
	options, session, err := s.webAuthn.BeginDiscoverableLogin()
	if err != nil {
		return "", nil, err
	}
	sessionJSON, err := json.Marshal(session)
	if err != nil {
		return "", nil, err
	}
	ceremonyID, err := s.saveCeremony(ctx, "", domain.CeremonyLogin, sessionJSON)
	if err != nil {
		return "", nil, err
	}
	return ceremonyID, options, nil
}

// FinishLogin completes passwordless sign-in. It does not know in advance
// which account is authenticating: the assertion response carries a user
// handle (this account's principal_id), which the resolver below uses to
// look up the account and its credentials on demand.
func (s *PasskeyService) FinishLogin(ctx context.Context, ceremonyID string, r *http.Request) (auth.TokenPair, error) {
	if s.webAuthn == nil {
		return auth.TokenPair{}, ErrPasskeyNotConfigured
	}
	ceremony, err := s.store.ConsumeWebAuthnCeremony(ctx, ceremonyID, domain.CeremonyLogin)
	if err != nil {
		return auth.TokenPair{}, err
	}
	var session webauthn.SessionData
	if err := json.Unmarshal(ceremony.SessionData, &session); err != nil {
		return auth.TokenPair{}, err
	}
	var resolvedPrincipalID string
	var resolvedRecords []domain.WebAuthnCredentialRecord
	resolve := func(_, userHandle []byte) (webauthn.User, error) {
		principalID := string(userHandle)
		account, lookupErr := s.store.PasskeyAccountByPrincipalID(ctx, principalID)
		if lookupErr != nil {
			return nil, lookupErr
		}
		records, recErr := s.store.WebAuthnCredentialsByPrincipalID(ctx, account.PrincipalID)
		if recErr != nil {
			return nil, recErr
		}
		if len(records) == 0 {
			return nil, ErrNoPasskeyCredentials
		}
		resolvedPrincipalID, resolvedRecords = account.PrincipalID, records
		return newWebauthnUser(account.PrincipalID, account.DisplayHandle, toWebAuthnCredentials(records)), nil
	}
	matched, err := s.webAuthn.FinishDiscoverableLogin(resolve, session, r)
	if err != nil {
		return auth.TokenPair{}, err
	}
	// The sign counter/clone-warning/backup-state update MUST succeed
	// before a token is issued -- silently discarding this error would let
	// go-webauthn's clone-detection signal (a counter that fails to
	// advance, or regresses, across logins) go unrecorded, weakening every
	// later login's ability to notice a cloned authenticator. A failed
	// write here means the caller genuinely could not durably confirm this
	// login, so it must not proceed as if it had.
	touched := false
	for _, record := range resolvedRecords {
		if string(record.CredentialID) == string(matched.ID) {
			if err := s.store.TouchWebAuthnCredential(ctx, record.ID, matched.Authenticator.SignCount, matched.Authenticator.CloneWarning, matched.Flags.BackupState); err != nil {
				return auth.TokenPair{}, err
			}
			touched = true
			break
		}
	}
	if !touched {
		// The assertion verified against a credential this resolve() call
		// itself supplied, so failing to find it back by ID here would be
		// an internal inconsistency, not a caller-facing auth failure --
		// fail closed rather than issuing a token for an untouched credential.
		return auth.TokenPair{}, errors.New("matched credential not found among resolved records")
	}
	return s.auth.IssueForPrincipal(resolvedPrincipalID, passkeyDefaultScopes(), "web", "atos.im")
}

// PurgeExpiredCeremonies clears abandoned registration/login/signup
// ceremonies -- called periodically by the same background-sweep
// discipline every other reconciler in this codebase already uses.
func (s *PasskeyService) PurgeExpiredCeremonies(ctx context.Context) (int, error) {
	return s.store.PurgeExpiredWebAuthnCeremonies(ctx, time.Now().UTC())
}
