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
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Environment != EnvironmentProduction || cfg.Auth.AutoApprove || cfg.TOSBackend != TOSBackendRPC {
		t.Fatalf("unexpected production config: %#v", cfg)
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
	} {
		t.Setenv(name, "")
	}
}
