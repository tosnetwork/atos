// Package auth implements the Phase 0/1 scoped gateway identity boundary.
// It keeps tokens in memory but enforces the same principal/scope semantics the
// production OAuth-compatible service must preserve.
package auth

import (
	"crypto/rand"
	"encoding/base64"
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
	ScopeProofsRead          Scope = "proofs:read"
	ScopeNetworkRead         Scope = "network:read"
)

var allowedScopes = map[Scope]struct{}{
	ScopeCapabilitiesRead: {}, ScopeCapabilitiesWrite: {}, ScopeQuotesRead: {},
	ScopeInvocationsCreate: {}, ScopeJobsCreate: {}, ScopeJobsRead: {}, ScopeJobsCancel: {},
	ScopeAccountRead: {}, ScopeProviderJobsRead: {}, ScopeProviderJobsDeliver: {},
	ScopeEarningsRead: {}, ScopeSettlementRead: {}, ScopeProofsRead: {}, ScopeNetworkRead: {},
}

var defaultConsumerScopes = []Scope{
	ScopeCapabilitiesRead,
	ScopeQuotesRead,
	ScopeInvocationsCreate,
	ScopeJobsCreate,
	ScopeJobsRead,
	ScopeJobsCancel,
	ScopeAccountRead,
}

type Principal struct {
	ID        string
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

type DeviceGrant struct {
	DeviceCode string
	UserCode   string
	Scopes     []Scope
	ExpiresAt  time.Time
	Authorized bool
}

type TokenPair struct {
	AccessToken  string
	RefreshToken string
	Principal    Principal
	ExpiresIn    int64
}

type Service struct {
	mu       sync.Mutex
	devices  map[string]DeviceGrant
	access   map[string]Principal
	refresh  map[string]Principal
	revoked  map[string]struct{}
	now      func() time.Time
	tokenTTL time.Duration
}

func NewService() *Service {
	return &Service{
		devices: make(map[string]DeviceGrant), access: make(map[string]Principal),
		refresh: make(map[string]Principal), revoked: make(map[string]struct{}),
		now: time.Now, tokenTTL: time.Hour,
	}
}

func (s *Service) StartDevice(requested []string) (DeviceGrant, error) {
	scopes, err := normalizeScopes(requested)
	if err != nil {
		return DeviceGrant{}, err
	}
	now := s.now().UTC()
	deviceCode := "dc_" + opaqueToken(24)
	grant := DeviceGrant{
		DeviceCode: deviceCode,
		UserCode:   strings.ToUpper(uuid.NewString()[0:8]),
		Scopes:     scopes, ExpiresAt: now.Add(15 * time.Minute),
		// Phase 0: authorization is immediate. Production replaces this flag
		// with the user-facing verification flow without changing token scopes.
		Authorized: true,
	}
	s.mu.Lock()
	s.devices[deviceCode] = grant
	s.mu.Unlock()
	return grant, nil
}

func (s *Service) ExchangeDevice(deviceCode string) (TokenPair, error) {
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	grant, ok := s.devices[deviceCode]
	if !ok {
		return TokenPair{}, fmt.Errorf("invalid device_code")
	}
	if !now.Before(grant.ExpiresAt) {
		delete(s.devices, deviceCode)
		return TokenPair{}, fmt.Errorf("device_code expired")
	}
	if !grant.Authorized {
		return TokenPair{}, fmt.Errorf("authorization_pending")
	}
	delete(s.devices, deviceCode)
	principal := Principal{
		ID: "prn_" + uuid.NewString(), Scopes: scopeSet(grant.Scopes),
		ExpiresAt: now.Add(s.tokenTTL),
	}
	return s.issueLocked(principal), nil
}

func (s *Service) Refresh(refreshToken string) (TokenPair, error) {
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, revoked := s.revoked[refreshToken]; revoked {
		return TokenPair{}, fmt.Errorf("refresh token revoked")
	}
	principal, ok := s.refresh[refreshToken]
	if !ok {
		return TokenPair{}, fmt.Errorf("invalid refresh_token")
	}
	delete(s.refresh, refreshToken)
	s.revoked[refreshToken] = struct{}{}
	principal.ExpiresAt = now.Add(s.tokenTTL)
	return s.issueLocked(principal), nil
}

func (s *Service) Authenticate(accessToken string) (Principal, error) {
	if accessToken == "" {
		return Principal{}, fmt.Errorf("missing bearer token")
	}
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, revoked := s.revoked[accessToken]; revoked {
		return Principal{}, fmt.Errorf("access token revoked")
	}
	principal, ok := s.access[accessToken]
	if !ok {
		return Principal{}, fmt.Errorf("invalid access token")
	}
	if !now.Before(principal.ExpiresAt) {
		delete(s.access, accessToken)
		return Principal{}, fmt.Errorf("access token expired")
	}
	principal.Scopes = cloneScopeSet(principal.Scopes)
	return principal, nil
}

func (s *Service) Revoke(token string) {
	if token == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.access, token)
	delete(s.refresh, token)
	s.revoked[token] = struct{}{}
}

func (s *Service) issueLocked(principal Principal) TokenPair {
	accessToken := "at_" + opaqueToken(32)
	refreshToken := "rt_" + opaqueToken(32)
	stored := principal
	stored.Scopes = cloneScopeSet(principal.Scopes)
	s.access[accessToken] = stored
	s.refresh[refreshToken] = stored
	return TokenPair{
		AccessToken: accessToken, RefreshToken: refreshToken,
		Principal: principal, ExpiresIn: int64(s.tokenTTL.Seconds()),
	}
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

func cloneScopeSet(in map[Scope]struct{}) map[Scope]struct{} {
	out := make(map[Scope]struct{}, len(in))
	for scope := range in {
		out[scope] = struct{}{}
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
