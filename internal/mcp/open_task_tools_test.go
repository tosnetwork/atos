package mcp

import (
	"context"
	"testing"
	"time"

	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func newOpenTaskMCPServer(t *testing.T) (*Server, *auth.Service) {
	t.Helper()
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	accounts := service.NewAccountService(st)
	quotes := service.NewQuoteService(st).WithAccountService(accounts)
	core := toscoremock.New(st)
	jobs := service.NewJobService(st, tosaimock.New(), core, accounts)
	openTasks := service.NewOpenTaskService(st, quotes, jobs)
	server := &Server{Auth: authorization, Capabilities: capabilities, OpenTasks: openTasks, Quotes: quotes, Jobs: jobs, Accounts: accounts}
	return server, authorization
}

// TestOpenTaskTools_HiddenWithoutScope proves every Phase 3C tool is
// absent from tools/list, and rejected as an unknown tool by tools/call,
// for a caller lacking the required scope -- mirroring
// TestProviderJobsTool_HiddenWithoutScope's shape exactly.
func TestOpenTaskTools_HiddenWithoutScope(t *testing.T) {
	server, authorization := newOpenTaskMCPServer(t)
	token := accessToken(t, authorization, auth.ScopeCapabilitiesRead)

	list := listTools(t, server, token)
	for _, name := range names(list.Result.Tools) {
		if name == "atos_publish_open_task" || name == "atos_apply_to_open_task" {
			t.Fatalf("tool %q should be hidden without the required scope", name)
		}
	}

	resp := callTool(t, server, token, "atos_publish_open_task", map[string]any{"title": "x"})
	if resp.Error == nil || resp.Error.Code != codeMethodNotFound {
		t.Fatalf("got %+v, want method-not-found for hidden tool", resp.Error)
	}
	resp = callTool(t, server, token, "atos_apply_to_open_task", map[string]any{"task_id": "x"})
	if resp.Error == nil || resp.Error.Code != codeMethodNotFound {
		t.Fatalf("got %+v, want method-not-found for hidden tool", resp.Error)
	}
}

// TestOpenTaskTools_GoldenPathThroughMCP exercises publish -> apply ->
// accept end to end through the MCP tool surface, proving REST and MCP
// dispatch into the exact same service methods (no parallel semantics).
func TestOpenTaskTools_GoldenPathThroughMCP(t *testing.T) {
	server, authorization := newOpenTaskMCPServer(t)
	ownerToken := accessToken(t, authorization, auth.ScopeOpenTasksRead, auth.ScopeOpenTasksWrite)
	providerToken := accessToken(t, authorization, auth.ScopeOpenTaskProposalsWrite)
	providerID := mustPrincipal(t, authorization, providerToken)

	cap, err := server.Capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "MCP Open Task Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-mcp-open-task",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	publishResp := callTool(t, server, ownerToken, "atos_publish_open_task", map[string]any{
		"title": "mcp task", "input": map[string]any{},
		"expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339), "idempotency_key": "mcp-publish-1",
	})
	if toolCallFailed(t, publishResp) {
		t.Fatalf("publish failed: %+v", publishResp)
	}
	taskID := structuredField(t, publishResp, "id")

	applyResp := callTool(t, server, providerToken, "atos_apply_to_open_task", map[string]any{
		"task_id": taskID, "capability_id": cap.ID, "idempotency_key": "mcp-apply-1",
	})
	if toolCallFailed(t, applyResp) {
		t.Fatalf("apply failed: %+v", applyResp)
	}
	proposalID := structuredField(t, applyResp, "id")

	acceptResp := callTool(t, server, ownerToken, "atos_accept_open_task_proposal", map[string]any{
		"task_id": taskID, "proposal_id": proposalID, "idempotency_key": "mcp-accept-1",
	})
	if toolCallFailed(t, acceptResp) {
		t.Fatalf("accept failed: %+v", acceptResp)
	}
	content := acceptResp.Result.(map[string]any)["structuredContent"].(map[string]any)
	openTask := content["open_task"].(map[string]any)
	if openTask["status"] != "fulfilled" {
		t.Fatalf("open_task.status = %v, want fulfilled: %+v", openTask["status"], content)
	}
}

func structuredField(t *testing.T, resp rpcResponse, field string) string {
	t.Helper()
	content, ok := resp.Result.(map[string]any)["structuredContent"].(map[string]any)
	if !ok {
		t.Fatalf("no structuredContent in %+v", resp.Result)
	}
	value, _ := content[field].(string)
	if value == "" {
		t.Fatalf("structuredContent.%s is empty: %+v", field, content)
	}
	return value
}
