package config

import (
	"strings"
	"testing"
)

func TestLoadDefaultsToDevelopmentMockBackend(t *testing.T) {
	clearTOSEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TOSBackend != TOSBackendMock {
		t.Fatalf("TOSBackend = %q, want %q", cfg.TOSBackend, TOSBackendMock)
	}
}

func TestLoadRPCBackendRequiresEndpointAndToken(t *testing.T) {
	clearTOSEnvironment(t)
	t.Setenv("ATOS_TOS_BACKEND", "rpc")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "ATOS_TOS_RPC_URL") {
		t.Fatalf("Load() error = %v, want missing RPC endpoint/token error", err)
	}
}

func TestLoadRPCBackendAcceptsCompleteConfiguration(t *testing.T) {
	clearTOSEnvironment(t)
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

func TestLoadRejectsUnknownBackend(t *testing.T) {
	clearTOSEnvironment(t)
	t.Setenv("ATOS_TOS_BACKEND", "automatic")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "expected mock or rpc") {
		t.Fatalf("Load() error = %v, want invalid backend error", err)
	}
}

func TestLoadRequiresClientCertificatePair(t *testing.T) {
	clearTOSEnvironment(t)
	t.Setenv("ATOS_TOS_BACKEND", "rpc")
	t.Setenv("ATOS_TOS_RPC_URL", "https://tos-protocol.internal")
	t.Setenv("ATOS_TOS_RPC_TOKEN", "integration-token")
	t.Setenv("ATOS_TOS_RPC_CLIENT_CERT_FILE", "/tmp/client.crt")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "must be configured together") {
		t.Fatalf("Load() error = %v, want incomplete mTLS pair error", err)
	}
}

func clearTOSEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"ATOS_TOS_BACKEND",
		"ATOS_TOS_RPC_URL",
		"ATOS_TOS_RPC_TOKEN",
		"ATOS_TOS_RPC_INSECURE",
		"ATOS_TOS_RPC_TIMEOUT",
		"ATOS_TOS_RPC_MAX_MESSAGE_BYTES",
		"ATOS_TOS_RPC_SERVER_NAME",
		"ATOS_TOS_RPC_CA_FILE",
		"ATOS_TOS_RPC_CLIENT_CERT_FILE",
		"ATOS_TOS_RPC_CLIENT_KEY_FILE",
	} {
		t.Setenv(name, "")
	}
}
