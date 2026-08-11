package mcp

import (
	"testing"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func newIdentityBindingToolTestServer(t *testing.T, scopes ...auth.Scope) (*Server, *toscoremock.Core, string) {
	t.Helper()
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	identities := service.NewIdentityBindingService(st, core)

	token, _ := activationToolAccessToken(t, authorization, scopes...)
	server := &Server{Auth: authorization, IdentityBindings: identities}
	return server, core, token
}

func TestToolBindIdentity_GoldenPath(t *testing.T) {
	server, core, token := newIdentityBindingToolTestServer(t, auth.ScopeIdentityBindingsWrite)
	core.SeedAgentIdentity("agt_mcp_bind")

	resp := callTool(t, server, token, "atos_bind_identity", map[string]any{
		"principal_id": "prn_mcp_bind", "agent_id": "agt_mcp_bind", "idempotency_key": "bind-mcp-1",
	})
	if toolCallFailed(t, resp) {
		t.Fatalf("tool call failed: %+v", resp)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result shape: %+v", resp.Result)
	}
	structured, ok := result["structuredContent"].(map[string]any)
	if !ok {
		t.Fatalf("missing structuredContent: %+v", result)
	}
	if structured["created"] != true || structured["agent_id"] != "agt_mcp_bind" {
		t.Fatalf("unexpected bind result: %+v", structured)
	}
}

func TestToolBindIdentity_HiddenWithoutScope(t *testing.T) {
	server, core, token := newIdentityBindingToolTestServer(t, auth.ScopeCapabilitiesRead)
	core.SeedAgentIdentity("agt_mcp_hidden")

	listResp := listTools(t, server, token)
	for _, name := range names(listResp.Result.Tools) {
		if name == "atos_bind_identity" || name == "atos_revoke_identity" {
			t.Fatalf("%s must not appear in tools/list without identity_bindings:write", name)
		}
	}

	resp := callTool(t, server, token, "atos_bind_identity", map[string]any{
		"principal_id": "prn_mcp_hidden", "agent_id": "agt_mcp_hidden", "idempotency_key": "bind-mcp-hidden",
	})
	if resp.Error == nil {
		t.Fatalf("expected a JSON-RPC error for a scope-gated tool call, got: %+v", resp)
	}
}

func TestToolIdentityBindingStatus_ReadScopeAloneCanRead(t *testing.T) {
	server, core, writeToken := newIdentityBindingToolTestServer(t, auth.ScopeIdentityBindingsWrite, auth.ScopeIdentityBindingsRead)
	core.SeedAgentIdentity("agt_mcp_status")
	if resp := callTool(t, server, writeToken, "atos_bind_identity", map[string]any{
		"principal_id": "prn_mcp_status", "agent_id": "agt_mcp_status", "idempotency_key": "bind-mcp-status",
	}); toolCallFailed(t, resp) {
		t.Fatalf("bind failed: %+v", resp)
	}

	resp := callTool(t, server, writeToken, "atos_identity_binding_status", map[string]any{
		"principal_id": "prn_mcp_status",
	})
	if toolCallFailed(t, resp) {
		t.Fatalf("status call failed: %+v", resp)
	}
	result := resp.Result.(map[string]any)
	structured := result["structuredContent"].(map[string]any)
	if structured["bound"] != true || structured["status"] != "active" {
		t.Fatalf("unexpected status result: %+v", structured)
	}
}

func TestToolRevokeIdentity_NothingToRevokeIsNotAnError(t *testing.T) {
	server, _, token := newIdentityBindingToolTestServer(t, auth.ScopeIdentityBindingsWrite)
	resp := callTool(t, server, token, "atos_revoke_identity", map[string]any{
		"principal_id": "prn_mcp_revoke_noop", "idempotency_key": "revoke-mcp-noop",
	})
	if toolCallFailed(t, resp) {
		t.Fatalf("expected a normal (isError:false) result for a no-op revoke, got: %+v", resp)
	}
	result := resp.Result.(map[string]any)
	structured := result["structuredContent"].(map[string]any)
	if structured["revoked"] != false {
		t.Fatalf("revoked = %v, want false", structured["revoked"])
	}
}
