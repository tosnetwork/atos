package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func signerToolAccessToken(t *testing.T, svc *auth.Service, scopes ...auth.Scope) (token, principalID string) {
	t.Helper()
	raw := make([]string, len(scopes))
	for i, scope := range scopes {
		raw[i] = string(scope)
	}
	grant, err := svc.StartDevice("test", "MCP Signer Test", raw)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	return pair.AccessToken, pair.Principal.ID
}

func newSignerToolTestServer(t *testing.T) (*Server, domain.Capability, string) {
	t.Helper()
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	executionSigners := service.NewExecutionSignerService(st, core, capabilities)

	token, providerID := signerToolAccessToken(t, authorization, auth.ScopeExecutionSignersWrite, auth.ScopeExecutionSignersRead)
	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Signer MCP Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-mcp-signer",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	server := &Server{Auth: authorization, Capabilities: capabilities, ExecutionSigners: executionSigners}
	return server, cap, token
}

func testSignerPublicKeyArg() string {
	return "base64:" + base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
}

func TestToolAuthorizeExecutionSigner_GoldenPath(t *testing.T) {
	server, cap, token := newSignerToolTestServer(t)
	resp := callTool(t, server, token, "atos_authorize_execution_signer", map[string]any{
		"capability_id": cap.ID, "execution_signer_id": "sig_mcp_1",
		"signer_public_key": testSignerPublicKeyArg(), "signature_algorithm": "ed25519",
		"idempotency_key": "authz-mcp-1",
	})
	if toolCallFailed(t, resp) {
		t.Fatalf("atos_authorize_execution_signer failed: %+v", resp)
	}
	encoded, _ := json.Marshal(resp.Result.(map[string]any)["structuredContent"])
	var decoded service.SignerOperationStatusView
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode structuredContent: %v", err)
	}
	if decoded.Checkpoint != "completed" || decoded.CurrentExecutionSignerID != "sig_mcp_1" {
		t.Fatalf("unexpected status: %+v", decoded)
	}
}

func TestToolAuthorizeExecutionSigner_HiddenWithoutScope(t *testing.T) {
	server, cap, _ := newSignerToolTestServer(t)
	readOnlyToken, _ := signerToolAccessToken(t, server.Auth, auth.ScopeExecutionSignersRead)
	resp := callTool(t, server, readOnlyToken, "atos_authorize_execution_signer", map[string]any{
		"capability_id": cap.ID, "execution_signer_id": "sig_mcp_hidden",
		"signer_public_key": testSignerPublicKeyArg(), "signature_algorithm": "ed25519",
		"idempotency_key": "authz-mcp-hidden",
	})
	if resp.Error == nil {
		t.Fatalf("expected a protocol-level error calling a write tool with a read-only token, got %+v", resp)
	}
	if resp.Error.Code != codeMethodNotFound {
		t.Fatalf("error code = %d, want codeMethodNotFound (%d) -- a scope-gated tool must appear unknown, not merely forbidden: %+v", resp.Error.Code, codeMethodNotFound, resp.Error)
	}
}

func TestToolGetExecutionSignerStatus_RejectsNonOwningProvider(t *testing.T) {
	server, cap, _ := newSignerToolTestServer(t)
	impostorToken, _ := signerToolAccessToken(t, server.Auth, auth.ScopeExecutionSignersRead)
	resp := callTool(t, server, impostorToken, "atos_get_execution_signer_status", map[string]any{"capability_id": cap.ID})
	if !toolCallFailed(t, resp) {
		t.Fatalf("expected atos_get_execution_signer_status to fail for a non-owning provider, got %+v", resp)
	}
}

func TestToolExecutionSigners_FullLifecycle(t *testing.T) {
	server, cap, token := newSignerToolTestServer(t)

	authorize := callTool(t, server, token, "atos_authorize_execution_signer", map[string]any{
		"capability_id": cap.ID, "execution_signer_id": "sig_mcp_lifecycle_1",
		"signer_public_key": testSignerPublicKeyArg(), "signature_algorithm": "ed25519",
		"idempotency_key": "lifecycle-authz",
	})
	if toolCallFailed(t, authorize) {
		t.Fatalf("authorize failed: %+v", authorize)
	}

	rotate := callTool(t, server, token, "atos_rotate_execution_signer", map[string]any{
		"capability_id": cap.ID, "execution_signer_id": "sig_mcp_lifecycle_2",
		"signer_public_key": testSignerPublicKeyArg(), "signature_algorithm": "ed25519",
		"idempotency_key": "lifecycle-rotate",
	})
	if toolCallFailed(t, rotate) {
		t.Fatalf("rotate failed: %+v", rotate)
	}
	encoded, _ := json.Marshal(rotate.Result.(map[string]any)["structuredContent"])
	var rotated service.SignerOperationStatusView
	if err := json.Unmarshal(encoded, &rotated); err != nil {
		t.Fatalf("decode structuredContent: %v", err)
	}
	if rotated.CurrentExecutionSignerID != "sig_mcp_lifecycle_2" {
		t.Fatalf("after rotate: %+v", rotated)
	}

	status := callTool(t, server, token, "atos_get_execution_signer_status", map[string]any{"capability_id": cap.ID})
	if toolCallFailed(t, status) {
		t.Fatalf("status failed: %+v", status)
	}
	encoded, _ = json.Marshal(status.Result.(map[string]any)["structuredContent"])
	var statusView service.SignerOperationStatusView
	if err := json.Unmarshal(encoded, &statusView); err != nil {
		t.Fatalf("decode structuredContent: %v", err)
	}
	if statusView.CurrentExecutionSignerID != "sig_mcp_lifecycle_2" {
		t.Fatalf("status: %+v", statusView)
	}

	revoke := callTool(t, server, token, "atos_revoke_execution_signer", map[string]any{
		"capability_id": cap.ID, "idempotency_key": "lifecycle-revoke",
	})
	if toolCallFailed(t, revoke) {
		t.Fatalf("revoke failed: %+v", revoke)
	}
	encoded, _ = json.Marshal(revoke.Result.(map[string]any)["structuredContent"])
	var revoked service.SignerOperationStatusView
	if err := json.Unmarshal(encoded, &revoked); err != nil {
		t.Fatalf("decode structuredContent: %v", err)
	}
	if revoked.CurrentExecutionSignerID != "" {
		t.Fatalf("expected no current signer after revoke: %+v", revoked)
	}
}
