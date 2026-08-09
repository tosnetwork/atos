package auth

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func autoService(t *testing.T) *Service {
	t.Helper()
	svc, err := Open(Config{AutoApprove: true, PollInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestDeviceAuthorizationRequiresDecisionAndPreservesScopes(t *testing.T) {
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	svc, err := Open(Config{PollInterval: time.Second, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := svc.StartDevice("codex", "Codex Test", []string{string(ScopeCapabilitiesRead), string(ScopeQuotesRead)})
	if err != nil {
		t.Fatal(err)
	}
	if grant.Status != DeviceGrantPending {
		t.Fatalf("grant status = %q, want pending", grant.Status)
	}
	if _, err := svc.ExchangeDevice(grant.DeviceCode); err == nil {
		t.Fatal("pending grant unexpectedly issued a token")
	} else if oauth, ok := err.(*OAuthError); !ok || oauth.Code != "authorization_pending" {
		t.Fatalf("pending exchange error = %v", err)
	}
	now = now.Add(2 * time.Second)
	if _, err := svc.DecideDevice(grant.UserCode, "prn_test", true); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := svc.Authenticate(pair.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	if principal.ID != "prn_test" || !principal.HasAll(ScopeCapabilitiesRead, ScopeQuotesRead) {
		t.Fatalf("principal = %+v", principal)
	}
	if principal.Has(ScopeCapabilitiesWrite) {
		t.Fatal("read-only device grant unexpectedly received capabilities:write")
	}
}

func TestPollingTooFastReturnsSlowDown(t *testing.T) {
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	svc, err := Open(Config{PollInterval: 5 * time.Second, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := svc.StartDevice("codex", "Codex", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = svc.ExchangeDevice(grant.DeviceCode)
	now = now.Add(time.Second)
	_, err = svc.ExchangeDevice(grant.DeviceCode)
	oauth, ok := err.(*OAuthError)
	if !ok || oauth.Code != "slow_down" {
		t.Fatalf("error = %v, want slow_down", err)
	}
}

func TestUnknownScopeIsRejected(t *testing.T) {
	svc := autoService(t)
	if _, err := svc.StartDevice("codex", "Codex", []string{"root:everything"}); err == nil {
		t.Fatal("expected unknown scope rejection")
	}
}

func TestRefreshRotatesRefreshToken(t *testing.T) {
	svc := autoService(t)
	grant, err := svc.StartDevice("codex", "Codex", nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Refresh(first.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if second.RefreshToken == first.RefreshToken || second.AccessToken == first.AccessToken {
		t.Fatal("refresh did not rotate tokens")
	}
	if _, err := svc.Refresh(first.RefreshToken); err == nil {
		t.Fatal("old refresh token remained usable after rotation")
	}
	if _, err := svc.Authenticate(second.AccessToken); err != nil {
		t.Fatalf("new access token is unusable: %v", err)
	}
}

func TestDeviceVisibilityAndRevocation(t *testing.T) {
	svc := autoService(t)
	grant, _ := svc.StartDevice("claude", "Claude Desktop", nil)
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	devices := svc.Devices(pair.Principal.ID)
	if len(devices) != 1 || devices[0].ClientName != "Claude Desktop" {
		t.Fatalf("devices = %+v", devices)
	}
	if err := svc.RevokeDevice(pair.Principal.ID, pair.Principal.DeviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Authenticate(pair.AccessToken); err == nil {
		t.Fatal("device access token remained valid after device revocation")
	}
	if _, err := svc.Refresh(pair.RefreshToken); err == nil {
		t.Fatal("device refresh token remained valid after device revocation")
	}
}

func TestBoltPersistenceSurvivesRestartWithoutRawTokenLookup(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "auth.db")
	persistence, err := OpenBoltPersistence(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := Open(Config{AutoApprove: true, PollInterval: time.Second, Persistence: persistence})
	if err != nil {
		t.Fatal(err)
	}
	grant, _ := first.StartDevice("codex", "Codex", nil)
	pair, err := first.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	persistence, err = OpenBoltPersistence(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(Config{PollInterval: time.Second, Persistence: persistence})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err := second.Authenticate(pair.AccessToken); err != nil {
		t.Fatalf("persisted access token could not be authenticated: %v", err)
	}
}
