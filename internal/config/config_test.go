package config

import (
	"strings"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{Addr: ":8080", NativeReadToken: "read", NativeRelayToken: "relay",
		TOSRPC: TOSRPCConfig{URL: "https://tos-protocol.internal", Token: "backend", Timeout: time.Second, MaxMessageBytes: 1024}}
}

func TestValidateNativeOnlyConfig(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	cfg := validConfig()
	cfg.NativeRelayToken = cfg.NativeReadToken
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "distinct") {
		t.Fatalf("error = %v", err)
	}
	cfg = validConfig()
	cfg.TOSRPC.URL = "http://127.0.0.1:9443"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "INSECURE") {
		t.Fatalf("error = %v", err)
	}
}
