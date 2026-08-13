// Package auth implements ATOS's scoped Device Authorization boundary.
// Device grants are pending until a trusted consent surface approves them;
// development auto-approval is an explicit option rather than an implicit
// production fallback.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Scope string

const (
	ScopeCapabilitiesRead    Scope = "capabilities:read"
	ScopeCapabilitiesWrite   Scope = "capabilities:write"
	ScopeQuotesRead          Scope = "quotes:read"
	ScopeInvocationsCreate   Scope = "invocations:create"
	ScopeJobsCreate          Scope = "jobs:create"
	ScopeJobsRead            Scope = "jobs:read"
	ScopeJobsCancel          Scope = "jobs:cancel"
	ScopeAccountRead         Scope = "account:read"
	ScopeProviderJobsRead    Scope = "provider_jobs:read"
	ScopeProviderJobsDeliver Scope = "provider_jobs:deliver"
	ScopeEarningsRead        Scope = "earnings:read"
	ScopeSettlementRead      Scope = "settlement:read"
	// ScopeSettlementWrite authorizes settlement-mutation operations
	// (atos_request_settlement) -- deliberately a separate scope from
	// ScopeSettlementRead, never overloading a read-only scope to
	// authorize a money-changing operation. Explicit-grant-only, like
	// ScopeDisputesReview: never a default consumer scope, and never
	// implied by ScopeProviderJobsDeliver or any other provider scope.
	ScopeSettlementWrite Scope = "settlement:write"
	ScopeProofsRead      Scope = "proofs:read"
	ScopeNetworkRead     Scope = "network:read"
	// Native gateway scopes control transport access only. On-chain
	// controller signatures remain the sole semantic authorization.
	ScopeNativeRead     Scope = "native:read"
	ScopeNativeRelay    Scope = "native:relay"
	ScopeDisputesOpen   Scope = "disputes:open"
	ScopeDisputesRead   Scope = "disputes:read"
	ScopeDisputesReview Scope = "disputes:review"
	// ScopeExecutionSignersRead/Write authorize the Phase 3B
	// execution-signer authorize/rotate/revoke/status surface
	// (atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.2.3). Deliberately a
	// separate scope pair from ScopeCapabilitiesRead/Write, never
	// implied by them: signer mutation is a distinct trust-side effect
	// class from ordinary Capability metadata, explicit-grant-only like
	// ScopeSettlementWrite/ScopeDisputesReview, never a default
	// consumer scope.
	ScopeExecutionSignersRead  Scope = "execution_signers:read"
	ScopeExecutionSignersWrite Scope = "execution_signers:write"
	// ScopeActivationEvaluate authorizes the admin-triggered entry point
	// for domain.ActivationAuthority.Evaluate (atos-spec
	// docs/IMPLEMENTATION_ROADMAP.md §7.2.1, docs/API.md §2.2).
	// Deliberately a separate scope from ScopeExecutionSignersWrite and
	// every other provider scope: this is an activation-authority-side
	// operation, not a provider one, so it carries no ownership
	// precondition at all (unlike every other Capability mutation
	// scope) -- explicit-grant-only like ScopeDisputesReview, never a
	// default consumer scope.
	ScopeActivationEvaluate Scope = "activation:evaluate"
	// ScopeOpenTasksRead/Write authorize Phase 3C's Open Task Marketplace
	// (atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.3) from the task
	// OWNER's side: browsing/publishing/cancelling/accepting a proposal
	// for one's own OpenTask. Default consumer scopes, mirroring
	// ScopeJobsCreate/Read/Cancel -- publishing a task and accepting a
	// proposal are ordinary consumer actions, not a distinct role.
	ScopeOpenTasksRead  Scope = "open_tasks:read"
	ScopeOpenTasksWrite Scope = "open_tasks:write"
	// ScopeOpenTaskProposalsWrite authorizes the PROVIDER side: submitting
	// or withdrawing a proposal against someone else's OpenTask.
	// Deliberately NOT a default consumer scope and NOT implied by
	// ScopeOpenTasksWrite -- mirrors ScopeProviderJobsDeliver's
	// explicit-grant-only, provider-role pattern: applying to fulfill
	// other principals' tasks is a distinct trust-side effect class from
	// managing one's own tasks.
	ScopeOpenTaskProposalsWrite Scope = "open_task_proposals:write"
	// ScopeCertificationsRead/Write authorize the Phase 3A sandbox
	// certification workflow (service.CertificationService.Open, atos-spec
	// docs/API.md §2.3) from the owning provider's side: triggering a
	// certification attempt against one's own capability binding and
	// reading its history. Provider-role and ownership-checked, mirroring
	// ScopeExecutionSignersRead/Write's exact pattern -- never a default
	// consumer scope (a consumer never certifies someone else's binding).
	ScopeCertificationsRead  Scope = "certifications:read"
	ScopeCertificationsWrite Scope = "certifications:write"
	// ScopeIdentityBindingsRead/Write authorize Phase 4A's identity-binding
	// operator surface (atos-spec docs/IMPLEMENTATION_ROADMAP.md §8.1,
	// docs/API.md §9A): binding/revoking/reading which TOS Agent Identity a
	// principal_id is bound to. Like ScopeActivationEvaluate, this is an
	// activation-authority/operator-side action, not owner-scoped -- a
	// principal cannot bind ITSELF to an arbitrary claimed TOS identity
	// merely by being authenticated, so this carries no ownership
	// precondition and is explicit-grant-only, never a default scope.
	ScopeIdentityBindingsRead  Scope = "identity_bindings:read"
	ScopeIdentityBindingsWrite Scope = "identity_bindings:write"
)

var allowedScopes = map[Scope]struct{}{
	ScopeCapabilitiesRead: {}, ScopeCapabilitiesWrite: {}, ScopeQuotesRead: {},
	ScopeInvocationsCreate: {}, ScopeJobsCreate: {}, ScopeJobsRead: {}, ScopeJobsCancel: {},
	ScopeAccountRead: {}, ScopeProviderJobsRead: {}, ScopeProviderJobsDeliver: {},
	ScopeEarningsRead: {}, ScopeSettlementRead: {}, ScopeSettlementWrite: {}, ScopeProofsRead: {}, ScopeNetworkRead: {},
	ScopeNativeRead: {}, ScopeNativeRelay: {},
	ScopeDisputesOpen: {}, ScopeDisputesRead: {}, ScopeDisputesReview: {},
	ScopeExecutionSignersRead: {}, ScopeExecutionSignersWrite: {},
	ScopeActivationEvaluate: {},
	ScopeOpenTasksRead:      {}, ScopeOpenTasksWrite: {}, ScopeOpenTaskProposalsWrite: {},
	ScopeCertificationsRead: {}, ScopeCertificationsWrite: {},
	ScopeIdentityBindingsRead: {}, ScopeIdentityBindingsWrite: {},
}

var defaultConsumerScopes = []Scope{
	ScopeCapabilitiesRead,
	ScopeQuotesRead,
	ScopeInvocationsCreate,
	ScopeJobsCreate,
	ScopeJobsRead,
	ScopeJobsCancel,
	ScopeAccountRead,
	// A principal can dispute and view their own completed Jobs by
	// default, the same way they can already cancel or read them.
	// disputes:review is deliberately NOT a default scope -- reviewing is
	// a distinct, explicitly-granted role, never implied by being a
	// consumer.
	ScopeDisputesOpen,
	ScopeDisputesRead,
	// Publishing an OpenTask and accepting/cancelling one's own is an
	// ordinary consumer action -- see ScopeOpenTasksRead/Write's doc
	// comment. ScopeOpenTaskProposalsWrite (the provider side) is
	// deliberately NOT included here.
	ScopeOpenTasksRead,
	ScopeOpenTasksWrite,
}

// DefaultConsumerScopes returns a copy of the scope bundle an ordinary
// self-service Device Authorization grant defaults to when no
// requested_scopes are given -- exported so other issuance paths (e.g.
// service.PasskeyService's passkey signup/login) can build on the exact
// same canonical bundle instead of maintaining a second copy that could
// silently drift from it.
func DefaultConsumerScopes() []Scope {
	return append([]Scope(nil), defaultConsumerScopes...)
}

// adminScopes carries system-wide, ownership-independent trust-side power
// -- a holder acts on ANY provider's ANY capability, not just their own.
// Explicit-grant-only scopes (ScopeExecutionSignersWrite, ScopeSettlementWrite,
// ScopeDisputesReview) are already never issued by default, but that alone
// only gates issuance behind the same self-service Device Authorization
// consent flow every ordinary scope uses -- nothing distinguishes "an
// authenticated user approved their own device's request" from "an
// administrator approved it." Scopes in this set additionally require
// RequiresAdminApproval's stronger operator-secret gate at approval time
// (see internal/httpapi/auth.go's DecideDevice callers) before a pending
// grant can ever be approved. The set is every scope with zero ownership
// scoping at all -- not ScopeSettlementWrite/ScopeDisputesReview, which is
// a deliberate, separate decision this set makes easy to revisit later,
// not an oversight. ScopeIdentityBindingsRead is included alongside
// ScopeIdentityBindingsWrite despite being read-only: it returns any
// principal_id's TOS agent_id/network/binding_ref with the same zero
// ownership precondition as the write side (see its declaration above),
// which is exactly this set's inclusion criterion -- a read scope isn't
// automatically less sensitive than a write one when it's the only thing
// gating cross-tenant identity-binding data.
var adminScopes = map[Scope]struct{}{
	ScopeActivationEvaluate:    {},
	ScopeIdentityBindingsRead:  {},
	ScopeIdentityBindingsWrite: {},
}

// RequiresAdminApproval reports whether scopes contains any scope in
// adminScopes -- callers deciding a pending Device Authorization grant
// MUST additionally require the stronger admin-approval gate before
// approving when this returns true.
func RequiresAdminApproval(scopes []Scope) bool {
	for _, scope := range scopes {
		if _, admin := adminScopes[scope]; admin {
			return true
		}
	}
	return false
}

type Principal struct {
	ID        string
	DeviceID  string
	Scopes    map[Scope]struct{}
	ExpiresAt time.Time
}

func (p Principal) Has(scope Scope) bool {
	_, ok := p.Scopes[scope]
	return ok
}

func (p Principal) HasAll(scopes ...Scope) bool {
	for _, scope := range scopes {
		if !p.Has(scope) {
			return false
		}
	}
	return true
}

func (p Principal) HasAny(scopes ...Scope) bool {
	for _, scope := range scopes {
		if p.Has(scope) {
			return true
		}
	}
	return false
}

func (p Principal) ScopeStrings() []string {
	out := make([]string, 0, len(p.Scopes))
	for scope := range p.Scopes {
		out = append(out, string(scope))
	}
	sort.Strings(out)
	return out
}

type DeviceGrantStatus string

const (
	DeviceGrantPending  DeviceGrantStatus = "pending"
	DeviceGrantApproved DeviceGrantStatus = "approved"
	DeviceGrantDenied   DeviceGrantStatus = "denied"
	DeviceGrantConsumed DeviceGrantStatus = "consumed"
)

type DeviceGrant struct {
	DeviceCodeHash string            `json:"device_code_hash"`
	DeviceCode     string            `json:"-"`
	UserCode       string            `json:"user_code"`
	ClientType     string            `json:"client_type"`
	ClientName     string            `json:"client_name"`
	Scopes         []Scope           `json:"scopes"`
	Status         DeviceGrantStatus `json:"status"`
	PrincipalID    string            `json:"principal_id,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	ExpiresAt      time.Time         `json:"expires_at"`
	LastPolledAt   time.Time         `json:"last_polled_at,omitempty"`
	DecidedAt      *time.Time        `json:"decided_at,omitempty"`
}

type Device struct {
	ID          string     `json:"device_id"`
	PrincipalID string     `json:"principal_id"`
	ClientType  string     `json:"client_type"`
	ClientName  string     `json:"client_name"`
	Scopes      []Scope    `json:"scopes"`
	CreatedAt   time.Time  `json:"created_at"`
	LastUsedAt  time.Time  `json:"last_used_at"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
}

type TokenPair struct {
	AccessToken  string
	RefreshToken string
	Principal    Principal
	ExpiresIn    int64
}

type OAuthError struct {
	Code        string
	Description string
	Retryable   bool
}

func (e *OAuthError) Error() string {
	if e.Description == "" {
		return e.Code
	}
	return e.Code + ": " + e.Description
}

type credential struct {
	PrincipalID string    `json:"principal_id"`
	DeviceID    string    `json:"device_id"`
	Scopes      []Scope   `json:"scopes"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type snapshot struct {
	Grants  map[string]DeviceGrant `json:"grants"`
	Devices map[string]Device      `json:"devices"`
	Access  map[string]credential  `json:"access"`
	Refresh map[string]credential  `json:"refresh"`
}

type Persistence interface {
	Load() (snapshot, error)
	Save(snapshot) error
	Close() error
}

type Config struct {
	AutoApprove  bool
	TokenTTL     time.Duration
	DeviceTTL    time.Duration
	PollInterval time.Duration
	Persistence  Persistence
	Now          func() time.Time
}

type Service struct {
	mu           sync.Mutex
	grants       map[string]DeviceGrant // keyed by hash(device_code)
	userCodes    map[string]string      // user_code -> device-code hash
	devices      map[string]Device
	access       map[string]credential // keyed by hash(token)
	refresh      map[string]credential
	now          func() time.Time
	tokenTTL     time.Duration
	deviceTTL    time.Duration
	pollInterval time.Duration
	autoApprove  bool
	persistence  Persistence
}

func NewService() *Service {
	svc, err := Open(Config{})
	if err != nil {
		panic(err)
	}
	return svc
}

func Open(cfg Config) (*Service, error) {
	if cfg.TokenTTL == 0 {
		cfg.TokenTTL = time.Hour
	}
	if cfg.DeviceTTL == 0 {
		cfg.DeviceTTL = 15 * time.Minute
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.TokenTTL <= 0 || cfg.TokenTTL > 30*24*time.Hour ||
		cfg.DeviceTTL <= 0 || cfg.DeviceTTL > time.Hour ||
		cfg.PollInterval < time.Second || cfg.PollInterval > time.Minute {
		return nil, errors.New("invalid authorization timing configuration")
	}
	s := &Service{
		grants: make(map[string]DeviceGrant), userCodes: make(map[string]string),
		devices: make(map[string]Device), access: make(map[string]credential),
		refresh: make(map[string]credential), now: cfg.Now,
		tokenTTL: cfg.TokenTTL, deviceTTL: cfg.DeviceTTL,
		pollInterval: cfg.PollInterval, autoApprove: cfg.AutoApprove,
		persistence: cfg.Persistence,
	}
	if cfg.Persistence != nil {
		state, err := cfg.Persistence.Load()
		if err != nil {
			return nil, fmt.Errorf("load authorization state: %w", err)
		}
		s.restore(state)
	}
	return s, nil
}

func (s *Service) Close() error {
	if s == nil || s.persistence == nil {
		return nil
	}
	return s.persistence.Close()
}

func (s *Service) PollInterval() time.Duration { return s.pollInterval }

func (s *Service) StartDevice(clientType, clientName string, requested []string) (DeviceGrant, error) {
	scopes, err := normalizeScopes(requested)
	if err != nil {
		return DeviceGrant{}, err
	}
	clientType = strings.TrimSpace(clientType)
	clientName = strings.TrimSpace(clientName)
	if clientType == "" || len(clientType) > 64 || clientName == "" || len(clientName) > 128 {
		return DeviceGrant{}, errors.New("client_type and client_name are required and bounded")
	}
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupLocked(now)

	var deviceCode, deviceCodeHash, code string
	for attempt := 0; attempt < 16; attempt++ {
		candidateDevice := "dc_" + opaqueToken(32)
		candidateHash := tokenHash(candidateDevice)
		candidateCode := userCode()
		if _, exists := s.grants[candidateHash]; exists {
			continue
		}
		if _, exists := s.userCodes[candidateCode]; exists {
			continue
		}
		deviceCode, deviceCodeHash, code = candidateDevice, candidateHash, candidateCode
		break
	}
	if deviceCode == "" {
		return DeviceGrant{}, errors.New("could not allocate a unique device authorization code")
	}
	grant := DeviceGrant{
		DeviceCodeHash: deviceCodeHash, DeviceCode: deviceCode,
		UserCode: code, ClientType: clientType, ClientName: clientName,
		Scopes: scopes, Status: DeviceGrantPending,
		CreatedAt: now, ExpiresAt: now.Add(s.deviceTTL),
	}
	if s.autoApprove {
		grant.Status = DeviceGrantApproved
		grant.PrincipalID = "prn_" + uuid.NewString()
		grant.DecidedAt = &now
	}
	s.grants[grant.DeviceCodeHash] = grant
	s.userCodes[grant.UserCode] = grant.DeviceCodeHash
	if err := s.persistLocked(); err != nil {
		delete(s.grants, grant.DeviceCodeHash)
		delete(s.userCodes, grant.UserCode)
		return DeviceGrant{}, err
	}
	return grant, nil
}

func (s *Service) GrantByUserCode(userCode string) (DeviceGrant, error) {
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupLocked(now)
	hash, ok := s.userCodes[normalizeUserCode(userCode)]
	if !ok {
		return DeviceGrant{}, errors.New("device authorization not found")
	}
	grant, ok := s.grants[hash]
	if !ok {
		return DeviceGrant{}, errors.New("device authorization not found")
	}
	grant.DeviceCode = ""
	return grant, nil
}

func (s *Service) DecideDevice(userCode, principalID string, approve bool) (DeviceGrant, error) {
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupLocked(now)
	hash, ok := s.userCodes[normalizeUserCode(userCode)]
	if !ok {
		return DeviceGrant{}, errors.New("device authorization not found")
	}
	grant, ok := s.grants[hash]
	if !ok || !now.Before(grant.ExpiresAt) {
		return DeviceGrant{}, errors.New("device authorization expired")
	}
	if grant.Status != DeviceGrantPending {
		return DeviceGrant{}, errors.New("device authorization was already decided")
	}
	grant.DecidedAt = &now
	if approve {
		grant.Status = DeviceGrantApproved
		principalID = strings.TrimSpace(principalID)
		if principalID == "" {
			principalID = "prn_" + uuid.NewString()
		}
		if len(principalID) > 160 {
			return DeviceGrant{}, errors.New("principal_id is too long")
		}
		grant.PrincipalID = principalID
	} else {
		grant.Status = DeviceGrantDenied
	}
	s.grants[hash] = grant
	if err := s.persistLocked(); err != nil {
		return DeviceGrant{}, err
	}
	grant.DeviceCode = ""
	return grant, nil
}

func (s *Service) ExchangeDevice(deviceCode string) (TokenPair, error) {
	now := s.now().UTC()
	hash := tokenHash(strings.TrimSpace(deviceCode))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupLocked(now)
	grant, ok := s.grants[hash]
	if !ok {
		return TokenPair{}, &OAuthError{Code: "invalid_grant", Description: "invalid device_code"}
	}
	if !now.Before(grant.ExpiresAt) {
		s.deleteGrantLocked(hash, grant)
		_ = s.persistLocked()
		return TokenPair{}, &OAuthError{Code: "expired_token", Description: "device_code expired"}
	}
	if !grant.LastPolledAt.IsZero() && now.Sub(grant.LastPolledAt) < s.pollInterval {
		grant.LastPolledAt = now
		s.grants[hash] = grant
		_ = s.persistLocked()
		return TokenPair{}, &OAuthError{Code: "slow_down", Description: "polling faster than the advertised interval", Retryable: true}
	}
	grant.LastPolledAt = now
	s.grants[hash] = grant
	switch grant.Status {
	case DeviceGrantPending:
		_ = s.persistLocked()
		return TokenPair{}, &OAuthError{Code: "authorization_pending", Retryable: true}
	case DeviceGrantDenied:
		s.deleteGrantLocked(hash, grant)
		_ = s.persistLocked()
		return TokenPair{}, &OAuthError{Code: "access_denied", Description: "the user denied this device"}
	case DeviceGrantApproved:
		deviceID := "dev_" + uuid.NewString()
		device := Device{
			ID: deviceID, PrincipalID: grant.PrincipalID,
			ClientType: grant.ClientType, ClientName: grant.ClientName,
			Scopes: append([]Scope(nil), grant.Scopes...), CreatedAt: now, LastUsedAt: now,
		}
		s.devices[deviceID] = device
		grant.Status = DeviceGrantConsumed
		s.grants[hash] = grant
		s.deleteGrantLocked(hash, grant)
		principal := Principal{
			ID: grant.PrincipalID, DeviceID: deviceID,
			Scopes: scopeSet(grant.Scopes), ExpiresAt: now.Add(s.tokenTTL),
		}
		pair := s.issueLocked(principal)
		if err := s.persistLocked(); err != nil {
			return TokenPair{}, err
		}
		return pair, nil
	default:
		return TokenPair{}, &OAuthError{Code: "invalid_grant", Description: "device_code already consumed"}
	}
}

// IssueForPrincipal mints a token pair directly for principalID, skipping
// the grant/user-code/poll ceremony entirely -- used by passkey
// registration/login (atos-spec docs/AUTH.md's "Human Account
// Authentication (Passkey/WebAuthn)" section), where the ceremony itself (a
// successful WebAuthn attestation/assertion) IS the identification step
// Device Authorization's /activate page otherwise assumes already happened
// via its own trusted-boundary precondition. Reuses the exact same
// Device/credential/issueLocked machinery ExchangeDevice's
// DeviceGrantApproved branch uses, so revocation, refresh and every other
// Bearer-token mechanic behaves identically regardless of which front door
// issued the token.
func (s *Service) IssueForPrincipal(principalID string, scopes []Scope, clientType, clientName string) (TokenPair, error) {
	if principalID == "" {
		return TokenPair{}, errors.New("principal_id is required")
	}
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupLocked(now)
	deviceID := "dev_" + uuid.NewString()
	device := Device{
		ID: deviceID, PrincipalID: principalID,
		ClientType: clientType, ClientName: clientName,
		Scopes: append([]Scope(nil), scopes...), CreatedAt: now, LastUsedAt: now,
	}
	s.devices[deviceID] = device
	principal := Principal{ID: principalID, DeviceID: deviceID, Scopes: scopeSet(scopes), ExpiresAt: now.Add(s.tokenTTL)}
	pair := s.issueLocked(principal)
	if err := s.persistLocked(); err != nil {
		return TokenPair{}, err
	}
	return pair, nil
}

func (s *Service) Refresh(refreshToken string) (TokenPair, error) {
	now := s.now().UTC()
	hash := tokenHash(strings.TrimSpace(refreshToken))
	s.mu.Lock()
	defer s.mu.Unlock()
	cred, ok := s.refresh[hash]
	if !ok || !now.Before(cred.ExpiresAt) {
		delete(s.refresh, hash)
		_ = s.persistLocked()
		return TokenPair{}, &OAuthError{Code: "invalid_grant", Description: "invalid or expired refresh_token"}
	}
	device, ok := s.devices[cred.DeviceID]
	if !ok || device.RevokedAt != nil {
		delete(s.refresh, hash)
		_ = s.persistLocked()
		return TokenPair{}, &OAuthError{Code: "access_denied", Description: "device has been revoked"}
	}
	delete(s.refresh, hash)
	principal := Principal{
		ID: cred.PrincipalID, DeviceID: cred.DeviceID,
		Scopes: scopeSet(cred.Scopes), ExpiresAt: now.Add(s.tokenTTL),
	}
	device.LastUsedAt = now
	s.devices[device.ID] = device
	pair := s.issueLocked(principal)
	if err := s.persistLocked(); err != nil {
		return TokenPair{}, err
	}
	return pair, nil
}

func (s *Service) Authenticate(accessToken string) (Principal, error) {
	if strings.TrimSpace(accessToken) == "" {
		return Principal{}, errors.New("missing bearer token")
	}
	now := s.now().UTC()
	hash := tokenHash(strings.TrimSpace(accessToken))
	s.mu.Lock()
	defer s.mu.Unlock()
	cred, ok := s.access[hash]
	if !ok {
		return Principal{}, errors.New("invalid access token")
	}
	if !now.Before(cred.ExpiresAt) {
		delete(s.access, hash)
		_ = s.persistLocked()
		return Principal{}, errors.New("access token expired")
	}
	device, ok := s.devices[cred.DeviceID]
	if !ok || device.RevokedAt != nil {
		delete(s.access, hash)
		_ = s.persistLocked()
		return Principal{}, errors.New("device has been revoked")
	}
	device.LastUsedAt = now
	s.devices[device.ID] = device
	_ = s.persistLocked()
	return Principal{
		ID: cred.PrincipalID, DeviceID: cred.DeviceID,
		Scopes: scopeSet(cred.Scopes), ExpiresAt: cred.ExpiresAt,
	}, nil
}

func (s *Service) Revoke(token string) {
	if strings.TrimSpace(token) == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	hash := tokenHash(strings.TrimSpace(token))
	delete(s.access, hash)
	delete(s.refresh, hash)
	_ = s.persistLocked()
}

func (s *Service) Devices(principalID string) []Device {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Device, 0)
	for _, device := range s.devices {
		if device.PrincipalID == principalID {
			copy := device
			copy.Scopes = append([]Scope(nil), device.Scopes...)
			out = append(out, copy)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out
}

func (s *Service) RevokeDevice(principalID, deviceID string) error {
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	device, ok := s.devices[deviceID]
	if !ok || device.PrincipalID != principalID {
		return errors.New("device not found")
	}
	if device.RevokedAt == nil {
		device.RevokedAt = &now
		s.devices[deviceID] = device
	}
	for key, cred := range s.access {
		if cred.DeviceID == deviceID {
			delete(s.access, key)
		}
	}
	for key, cred := range s.refresh {
		if cred.DeviceID == deviceID {
			delete(s.refresh, key)
		}
	}
	return s.persistLocked()
}

func (s *Service) issueLocked(principal Principal) TokenPair {
	accessToken := "at_" + opaqueToken(32)
	refreshToken := "rt_" + opaqueToken(32)
	cred := credential{
		PrincipalID: principal.ID, DeviceID: principal.DeviceID,
		Scopes: principalScopes(principal), ExpiresAt: principal.ExpiresAt,
	}
	s.access[tokenHash(accessToken)] = cred
	// Phase 1 keeps refresh lifetime equal to access lifetime. A production
	// policy can lengthen it without changing the public flow.
	s.refresh[tokenHash(refreshToken)] = cred
	return TokenPair{
		AccessToken: accessToken, RefreshToken: refreshToken,
		Principal: principal, ExpiresIn: int64(s.tokenTTL.Seconds()),
	}
}

func (s *Service) cleanupLocked(now time.Time) {
	for key, grant := range s.grants {
		if !now.Before(grant.ExpiresAt) {
			s.deleteGrantLocked(key, grant)
		}
	}
	for key, cred := range s.access {
		if !now.Before(cred.ExpiresAt) {
			delete(s.access, key)
		}
	}
	for key, cred := range s.refresh {
		if !now.Before(cred.ExpiresAt) {
			delete(s.refresh, key)
		}
	}
}

func (s *Service) deleteGrantLocked(hash string, grant DeviceGrant) {
	delete(s.grants, hash)
	delete(s.userCodes, grant.UserCode)
}

func (s *Service) restore(state snapshot) {
	if state.Grants != nil {
		s.grants = state.Grants
	}
	if state.Devices != nil {
		s.devices = state.Devices
	}
	if state.Access != nil {
		s.access = state.Access
	}
	if state.Refresh != nil {
		s.refresh = state.Refresh
	}
	for hash, grant := range s.grants {
		s.userCodes[grant.UserCode] = hash
	}
	s.cleanupLocked(s.now().UTC())
}

func (s *Service) persistLocked() error {
	if s.persistence == nil {
		return nil
	}
	return s.persistence.Save(snapshot{
		Grants: cloneGrantMap(s.grants), Devices: cloneDeviceMap(s.devices),
		Access: cloneCredentialMap(s.access), Refresh: cloneCredentialMap(s.refresh),
	})
}

func normalizeScopes(requested []string) ([]Scope, error) {
	if len(requested) == 0 {
		return append([]Scope(nil), defaultConsumerScopes...), nil
	}
	seen := make(map[Scope]struct{})
	out := make([]Scope, 0, len(requested))
	for _, raw := range requested {
		scope := Scope(strings.TrimSpace(raw))
		if _, ok := allowedScopes[scope]; !ok {
			return nil, fmt.Errorf("unsupported scope %q", raw)
		}
		if _, duplicate := seen[scope]; duplicate {
			continue
		}
		seen[scope] = struct{}{}
		out = append(out, scope)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func scopeSet(scopes []Scope) map[Scope]struct{} {
	out := make(map[Scope]struct{}, len(scopes))
	for _, scope := range scopes {
		out[scope] = struct{}{}
	}
	return out
}

func principalScopes(principal Principal) []Scope {
	out := make([]Scope, 0, len(principal.Scopes))
	for scope := range principal.Scopes {
		out = append(out, scope)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func cloneGrantMap(in map[string]DeviceGrant) map[string]DeviceGrant {
	out := make(map[string]DeviceGrant, len(in))
	for key, value := range in {
		value.DeviceCode = ""
		value.Scopes = append([]Scope(nil), value.Scopes...)
		out[key] = value
	}
	return out
}

func cloneDeviceMap(in map[string]Device) map[string]Device {
	out := make(map[string]Device, len(in))
	for key, value := range in {
		value.Scopes = append([]Scope(nil), value.Scopes...)
		out[key] = value
	}
	return out
}

func cloneCredentialMap(in map[string]credential) map[string]credential {
	out := make(map[string]credential, len(in))
	for key, value := range in {
		value.Scopes = append([]Scope(nil), value.Scopes...)
		out[key] = value
	}
	return out
}

func opaqueToken(bytes int) string {
	buf := make([]byte, bytes)
	if _, err := rand.Read(buf); err != nil {
		return strings.ReplaceAll(uuid.NewString()+uuid.NewString(), "-", "")
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func userCode() string {
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	buf := make([]byte, 8)
	random := make([]byte, len(buf))
	if _, err := rand.Read(random); err != nil {
		random = []byte(strings.ReplaceAll(uuid.NewString(), "-", ""))[:len(buf)]
	}
	for i := range buf {
		buf[i] = alphabet[int(random[i])%len(alphabet)]
	}
	return string(buf[:4]) + "-" + string(buf[4:])
}

func normalizeUserCode(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, " ", "")
	if len(value) == 8 && !strings.Contains(value, "-") {
		value = value[:4] + "-" + value[4:]
	}
	return value
}
