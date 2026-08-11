package config

import (
	"strings"
	"testing"
)

func TestLoadDefaultsToDevelopmentMockBackend(t *testing.T) {
	clearEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Environment != EnvironmentDevelopment || cfg.TOSBackend != TOSBackendMock {
		t.Fatalf("unexpected development defaults: %#v", cfg)
	}
	if !cfg.Auth.AutoApprove {
		t.Fatal("development default should explicitly auto-approve for zero-setup local use")
	}
	// The default payout backend must be "disabled", never "mock" -- an
	// operator who never set ATOS_PAYOUT_BACKEND must not silently get a
	// backend that marks unpaid earnings as paid.
	if cfg.PayoutBackend != PayoutBackendDisabled {
		t.Fatalf("default PayoutBackend = %q, want %q", cfg.PayoutBackend, PayoutBackendDisabled)
	}
}

func TestLoadRejectsUnknownPayoutBackend(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_PAYOUT_BACKEND", "stripe")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "ATOS_PAYOUT_BACKEND") {
		t.Fatalf("Load() error = %v, want invalid payout backend error", err)
	}
}

func TestProductionRejectsMockPayoutBackend(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_ENV", "production")
	t.Setenv("ATOS_DATABASE_URL", "postgres://atos@example/atos")
	t.Setenv("ATOS_PUBLIC_BASE_URL", "https://api.atos.example")
	t.Setenv("ATOS_AUTH_AUTO_APPROVE", "false")
	t.Setenv("ATOS_APPROVAL_TOKEN", strings.Repeat("a", 32))
	t.Setenv("ATOS_AUTH_STATE_PATH", "/var/lib/atos/auth.db")
	t.Setenv("ATOS_TOS_BACKEND", "rpc")
	t.Setenv("ATOS_TOS_RPC_URL", "https://tos-protocol.internal")
	t.Setenv("ATOS_TOS_RPC_TOKEN", "integration-token")
	t.Setenv("ATOS_REMOTE_THIRD_PARTY_EXECUTION", "true")
	t.Setenv("ATOS_PAYOUT_BACKEND", "mock")
	setProductionFinancial(t)
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "never moves real funds") {
		t.Fatalf("Load() error = %v, want mock-payout-in-production rejection", err)
	}
}

// TestProductionRequiresRemoteThirdPartyExecution proves production
// configuration fails closed rather than silently allowing this Gateway
// process to dial a third-party HTTP/MCP/A2A provider endpoint itself --
// see atos-spec docs/THIRD_PARTY_EXECUTION_PLANE.md §7.1.1's placement
// rule, which ATOS_REMOTE_THIRD_PARTY_EXECUTION defaulting to false would
// otherwise silently violate in production.
func TestProductionRequiresRemoteThirdPartyExecution(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_ENV", "production")
	t.Setenv("ATOS_DATABASE_URL", "postgres://atos@example/atos")
	t.Setenv("ATOS_PUBLIC_BASE_URL", "https://api.atos.example")
	t.Setenv("ATOS_AUTH_AUTO_APPROVE", "false")
	t.Setenv("ATOS_APPROVAL_TOKEN", strings.Repeat("a", 32))
	t.Setenv("ATOS_AUTH_STATE_PATH", "/var/lib/atos/auth.db")
	t.Setenv("ATOS_TOS_BACKEND", "rpc")
	t.Setenv("ATOS_TOS_RPC_URL", "https://tos-protocol.internal")
	t.Setenv("ATOS_TOS_RPC_TOKEN", "integration-token")
	setProductionFinancial(t)
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "ATOS_REMOTE_THIRD_PARTY_EXECUTION=true is required in production") {
		t.Fatalf("Load() error = %v, want remote-third-party-execution-required-in-production rejection", err)
	}
}

func TestManualDeviceAuthorizationRequiresApprovalToken(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_AUTH_AUTO_APPROVE", "false")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "ATOS_APPROVAL_TOKEN") {
		t.Fatalf("Load() error = %v, want approval token requirement", err)
	}
}

func TestLoadRPCBackendRequiresEndpointAndToken(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_TOS_BACKEND", "rpc")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "ATOS_TOS_RPC_URL") {
		t.Fatalf("Load() error = %v, want missing RPC endpoint/token error", err)
	}
}

func TestLoadRPCBackendAcceptsCompleteConfiguration(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_TOS_BACKEND", "rpc")
	t.Setenv("ATOS_TOS_RPC_URL", "https://tos-protocol.internal")
	t.Setenv("ATOS_TOS_RPC_TOKEN", "integration-token")
	t.Setenv("ATOS_TOS_RPC_SERVER_NAME", "tos-protocol.internal")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TOSBackend != TOSBackendRPC || cfg.TOSRPC.URL != "https://tos-protocol.internal" || cfg.TOSRPC.Token == "" {
		t.Fatalf("unexpected RPC configuration: %#v", cfg)
	}
}

func TestProductionFailsClosedWithoutDurabilityHTTPSAndRPC(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_ENV", "production")
	t.Setenv("ATOS_AUTH_AUTO_APPROVE", "false")
	t.Setenv("ATOS_APPROVAL_TOKEN", strings.Repeat("a", 32))
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "ATOS_DATABASE_URL") {
		t.Fatalf("Load() error = %v, want production database requirement", err)
	}
}

func TestProductionAcceptsCompleteManagedConfiguration(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_ENV", "production")
	t.Setenv("ATOS_DATABASE_URL", "postgres://atos@example/atos")
	t.Setenv("ATOS_PUBLIC_BASE_URL", "https://api.atos.example")
	t.Setenv("ATOS_AUTH_AUTO_APPROVE", "false")
	t.Setenv("ATOS_APPROVAL_TOKEN", strings.Repeat("a", 32))
	t.Setenv("ATOS_AUTH_STATE_PATH", "/var/lib/atos/auth.db")
	t.Setenv("ATOS_TOS_BACKEND", "rpc")
	t.Setenv("ATOS_TOS_RPC_URL", "https://tos-protocol.internal")
	t.Setenv("ATOS_TOS_RPC_TOKEN", "integration-token")
	t.Setenv("ATOS_REMOTE_THIRD_PARTY_EXECUTION", "true")
	setProductionFinancial(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Environment != EnvironmentProduction || cfg.Auth.AutoApprove || cfg.TOSBackend != TOSBackendRPC || !cfg.RemoteThirdPartyExecution {
		t.Fatalf("unexpected production config: %#v", cfg)
	}
}

func setProductionFinancial(t *testing.T) {
	t.Helper()
	t.Setenv("ATOS_FINANCIAL_BACKEND", "blnk")
	t.Setenv("ATOS_BLNK_URL", "https://blnk.internal")
	t.Setenv("ATOS_BLNK_KEY", "runtime-key")
	t.Setenv("ATOS_FINANCIAL_GATEWAY_ID", "gateway.example")
	t.Setenv("ATOS_FINANCIAL_NETWORK_ID", "tos-mainnet")
	t.Setenv("ATOS_FINANCIAL_SIGNER_URL", "https://kms.internal/sign")
	t.Setenv("ATOS_FINANCIAL_SIGNER_TOKEN", strings.Repeat("s", 32))
	t.Setenv("ATOS_FINANCIAL_SIGNING_KEY_ID", "kms-key-1")
	t.Setenv("ATOS_FINANCIAL_SIGNING_PUBLIC_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	t.Setenv("ATOS_FINANCIAL_RETENTION_URL", "https://worm.internal")
	t.Setenv("ATOS_FINANCIAL_RETENTION_HMAC_KEY", strings.Repeat("w", 32))
}

func TestBlnkOpeningCreditRequiresBoundedIssuance(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_DATABASE_URL", "postgres://atos@example/atos")
	t.Setenv("ATOS_FINANCIAL_BACKEND", "blnk")
	t.Setenv("ATOS_BLNK_URL", "https://blnk.internal")
	t.Setenv("ATOS_FINANCIAL_GATEWAY_ID", "gateway.example")
	t.Setenv("ATOS_FINANCIAL_NETWORK_ID", "tos-testnet")
	t.Setenv("ATOS_MANAGED_INITIAL_BALANCE", "10.00")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "ISSUANCE_LIMIT") {
		t.Fatalf("Load() error = %v, want bounded issuance requirement", err)
	}
	t.Setenv("ATOS_FINANCIAL_ISSUANCE_LIMIT", "1000.00")
	if _, err := Load(); err != nil {
		t.Fatalf("bounded issuance configuration rejected: %v", err)
	}
}

func TestFinancialDomainsAndWORMAuthenticationFailClosed(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_DATABASE_URL", "postgres://atos@example/atos")
	t.Setenv("ATOS_FINANCIAL_BACKEND", "blnk")
	t.Setenv("ATOS_BLNK_URL", "https://blnk.internal")
	t.Setenv("ATOS_FINANCIAL_GATEWAY_ID", "gateway/ambiguous")
	t.Setenv("ATOS_FINANCIAL_NETWORK_ID", "tos-testnet")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "canonical domain identifier") {
		t.Fatalf("unsafe financial domain error=%v", err)
	}
	t.Setenv("ATOS_FINANCIAL_GATEWAY_ID", "gateway.example")
	t.Setenv("ATOS_FINANCIAL_RETENTION_URL", "https://worm.internal")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RETENTION_HMAC_KEY") {
		t.Fatalf("unauthenticated WORM configuration error=%v", err)
	}
	t.Setenv("ATOS_FINANCIAL_RETENTION_HMAC_KEY", strings.Repeat("r", 32))
	if _, err := Load(); err != nil {
		t.Fatalf("authenticated WORM configuration rejected: %v", err)
	}
}

func TestLoadRejectsUnknownBackend(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_TOS_BACKEND", "automatic")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "expected mock or rpc") {
		t.Fatalf("Load() error = %v, want invalid backend error", err)
	}
}

func TestLoadRequiresClientCertificatePair(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_TOS_BACKEND", "rpc")
	t.Setenv("ATOS_TOS_RPC_URL", "https://tos-protocol.internal")
	t.Setenv("ATOS_TOS_RPC_TOKEN", "integration-token")
	t.Setenv("ATOS_TOS_RPC_CLIENT_CERT_FILE", "/tmp/client.crt")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "must be configured together") {
		t.Fatalf("Load() error = %v, want incomplete mTLS pair error", err)
	}
}

func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"ATOS_ENV", "ATOS_ADDR", "ATOS_DATABASE_URL", "ATOS_BLOB_DIR", "ATOS_PUBLIC_BASE_URL",
		"ATOS_AUTH_AUTO_APPROVE", "ATOS_AUTH_STATE_PATH", "ATOS_APPROVAL_TOKEN",
		"ATOS_AUTH_TOKEN_TTL", "ATOS_AUTH_DEVICE_TTL", "ATOS_AUTH_POLL_INTERVAL",
		"ATOS_MANAGED_CURRENCY", "ATOS_MANAGED_INITIAL_BALANCE", "ATOS_MANAGED_PER_CALL_LIMIT", "ATOS_MANAGED_DAILY_LIMIT",
		"ATOS_TOS_BACKEND", "ATOS_TOS_RPC_URL", "ATOS_TOS_RPC_TOKEN", "ATOS_TOS_RPC_INSECURE",
		"ATOS_TOS_RPC_TIMEOUT", "ATOS_TOS_RPC_MAX_MESSAGE_BYTES", "ATOS_TOS_RPC_SERVER_NAME",
		"ATOS_TOS_RPC_CA_FILE", "ATOS_TOS_RPC_CLIENT_CERT_FILE", "ATOS_TOS_RPC_CLIENT_KEY_FILE",
		"ATOS_PAYOUT_BACKEND",
		"ATOS_FINANCIAL_BACKEND", "ATOS_BLNK_URL", "ATOS_BLNK_KEY", "ATOS_BLNK_TIMEOUT",
		"ATOS_FINANCIAL_GATEWAY_ID", "ATOS_FINANCIAL_NETWORK_ID", "ATOS_FINANCIAL_SIGNER_URL", "ATOS_FINANCIAL_SIGNER_TOKEN",
		"ATOS_FINANCIAL_ISSUANCE_LIMIT",
		"ATOS_FINANCIAL_SIGNING_KEY_ID", "ATOS_FINANCIAL_SIGNING_ALGORITHM", "ATOS_FINANCIAL_RETENTION_URL",
		"ATOS_FINANCIAL_SIGNING_PUBLIC_KEY", "ATOS_FINANCIAL_RETENTION_HMAC_KEY",
		"ATOS_FINANCIAL_SEAL_INTERVAL", "ATOS_FINANCIAL_MAX_ANCHOR_LAG", "ATOS_FINANCIAL_FULL_AUDIT_INTERVAL", "ATOS_FINANCIAL_BATCH_SIZE",
		"ATOS_WEBAUTHN_RP_ID", "ATOS_WEBAUTHN_RP_NAME", "ATOS_WEBAUTHN_RP_ORIGINS", "ATOS_TRUSTED_PROXY_CIDRS",
	} {
		t.Setenv(name, "")
	}
}

// TestLoadDefaultsWebAuthnDisabled is a regression test for a real P1: RPID
// used to be defaulted from PublicBaseURL (which always has a non-empty
// fallback), which silently turned "passkey auth is opt-in" into "always
// on" for any deployment that never set ATOS_WEBAUTHN_RP_ID. RPID must stay
// empty -- and RPOrigins must stay unset -- when the operator never
// explicitly configured it.
func TestLoadDefaultsWebAuthnDisabled(t *testing.T) {
	clearEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WebAuthn.RPID != "" {
		t.Fatalf("WebAuthn.RPID = %q, want empty (passkey auth must default to disabled)", cfg.WebAuthn.RPID)
	}
	if len(cfg.WebAuthn.RPOrigins) != 0 {
		t.Fatalf("WebAuthn.RPOrigins = %v, want empty when RPID was never set", cfg.WebAuthn.RPOrigins)
	}
}

func TestLoadWebAuthnEnabledDerivesOriginFromPublicBaseURL(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_WEBAUTHN_RP_ID", "atos.im")
	t.Setenv("ATOS_PUBLIC_BASE_URL", "https://atos.im")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WebAuthn.RPID != "atos.im" {
		t.Fatalf("WebAuthn.RPID = %q, want atos.im", cfg.WebAuthn.RPID)
	}
	if len(cfg.WebAuthn.RPOrigins) != 1 || cfg.WebAuthn.RPOrigins[0] != "https://atos.im" {
		t.Fatalf("WebAuthn.RPOrigins = %v, want [https://atos.im]", cfg.WebAuthn.RPOrigins)
	}
}

func TestLoadDefaultsTrustedProxyCIDRsEmpty(t *testing.T) {
	clearEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.TrustedProxyCIDRs) != 0 {
		t.Fatalf("TrustedProxyCIDRs = %v, want empty by default -- an unconfigured deployment must never trust a forwarded-IP header", cfg.TrustedProxyCIDRs)
	}
}

func TestLoadParsesTrustedProxyCIDRs(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_TRUSTED_PROXY_CIDRS", "10.0.0.0/8, 172.16.0.0/12")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.TrustedProxyCIDRs) != 2 || cfg.TrustedProxyCIDRs[0] != "10.0.0.0/8" || cfg.TrustedProxyCIDRs[1] != "172.16.0.0/12" {
		t.Fatalf("TrustedProxyCIDRs = %v, want [10.0.0.0/8 172.16.0.0/12]", cfg.TrustedProxyCIDRs)
	}
}

func TestLoadRejectsInvalidTrustedProxyCIDR(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("ATOS_TRUSTED_PROXY_CIDRS", "not-a-cidr")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "ATOS_TRUSTED_PROXY_CIDRS") {
		t.Fatalf("Load() error = %v, want an invalid trusted proxy CIDR error", err)
	}
}
