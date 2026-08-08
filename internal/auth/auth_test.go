package auth

import "testing"

func TestDeviceAuthorizationPreservesRequestedScopes(t *testing.T) {
	svc := NewService()
	grant, err := svc.StartDevice([]string{string(ScopeCapabilitiesRead), string(ScopeQuotesRead)})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := svc.Authenticate(pair.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	if !principal.HasAll(ScopeCapabilitiesRead, ScopeQuotesRead) {
		t.Fatalf("principal scopes = %v", principal.ScopeStrings())
	}
	if principal.Has(ScopeCapabilitiesWrite) {
		t.Fatal("read-only device grant unexpectedly received capabilities:write")
	}
}

func TestUnknownScopeIsRejected(t *testing.T) {
	svc := NewService()
	if _, err := svc.StartDevice([]string{"root:everything"}); err == nil {
		t.Fatal("expected unknown scope rejection")
	}
}

func TestRefreshRotatesRefreshToken(t *testing.T) {
	svc := NewService()
	grant, err := svc.StartDevice(nil)
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

func TestRevokeInvalidatesAccessToken(t *testing.T) {
	svc := NewService()
	grant, err := svc.StartDevice(nil)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	svc.Revoke(pair.AccessToken)
	if _, err := svc.Authenticate(pair.AccessToken); err == nil {
		t.Fatal("revoked access token remained valid")
	}
}
