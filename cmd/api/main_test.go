package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-service-gateway/internal/config"
)

type readinessStub struct{ err error }

func (s readinessStub) CheckReady(context.Context) error { return s.err }

func TestReadinessHandlerFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name    string
		checker readinessChecker
		want    int
	}{
		{name: "missing", checker: nil, want: http.StatusServiceUnavailable},
		{name: "failed", checker: readinessStub{err: errors.New("down")}, want: http.StatusServiceUnavailable},
		{name: "ready", checker: readinessStub{}, want: http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			readinessHandler(test.checker, time.Second)(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d", response.Code, test.want)
			}
		})
	}
}

func TestGatewayDiscoveryUsesConfiguredOriginAndNetwork(t *testing.T) {
	now := time.Unix(1786800000, 0)
	cfg := config.Config{PublicBaseURL: "https://gateway.example", TOSRPC: config.TOSRPCConfig{MaxMessageBytes: 2048},
		Catalog: config.CatalogConfig{NetworkID: "tos-test", GenesisRootHash: "sha256:root", GenesisFileHash: "sha256:file", RegistryCodeHash: "tvm-cell-sha256:code"}}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "https://attacker.invalid/.well-known/tos-service.json", nil)
	request.Host = "attacker.invalid"
	gatewayDiscoveryHandler(cfg, func() time.Time { return now })(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" ||
		!strings.Contains(response.Body.String(), `"native_connect":"https://gateway.example"`) ||
		!strings.Contains(response.Body.String(), `"dns_alias_connect":"https://gateway.example"`) ||
		strings.Contains(response.Body.String(), "attacker.invalid") ||
		!strings.Contains(response.Body.String(), `"max_response_bytes":2048`) ||
		!strings.Contains(response.Body.String(), `"expires_at_unix_seconds":1786803600`) {
		t.Fatalf("unexpected discovery response: status=%d body=%s", response.Code, response.Body.String())
	}
}
