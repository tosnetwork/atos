package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func openTaskA2AAccessToken(t *testing.T, svc *auth.Service, scopes ...auth.Scope) (token, principalID string) {
	t.Helper()
	raw := make([]string, len(scopes))
	for i, scope := range scopes {
		raw[i] = string(scope)
	}
	grant, err := svc.StartDevice("test", "A2A OpenTask Test", raw)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	return pair.AccessToken, pair.Principal.ID
}

// newOpenTaskA2ATestServer wires the same real quote -> escrow -> tos-ai
// execute -> tos-core verify/settle pipeline internal/service/job_test.go's
// harness does, so TestOpenTasksA2A_FullLifecycle exercises Accept() all
// the way to a bound, completed Job -- not a stubbed-out shortcut.
func newOpenTaskA2ATestServer(t *testing.T) (*Server, *service.CapabilityService) {
	t.Helper()
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	provider := tosaimock.New()
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)
	openTasks := service.NewOpenTaskService(st, quotes, jobs)

	server := &Server{Auth: authorization, Quotes: quotes, Jobs: jobs, OpenTasks: openTasks}
	return server, capabilities
}

func callA2A(t *testing.T, server *Server, token, method string, params any) rpcResponse {
	t.Helper()
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": json.RawMessage(paramsJSON)})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/a2a", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Handler()(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("method %s: HTTP status = %d, body = %s", method, recorder.Code, recorder.Body.String())
	}
	var resp rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode rpc response: %v, body = %s", err, recorder.Body.String())
	}
	return resp
}

func decodeResult[T any](t *testing.T, resp rpcResponse) T {
	t.Helper()
	encoded, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatal(err)
	}
	var out T
	if err := json.Unmarshal(encoded, &out); err != nil {
		t.Fatalf("decode result: %v, raw = %s", err, encoded)
	}
	return out
}

func TestOpenTasksA2A_FullLifecycle(t *testing.T) {
	server, capabilities := newOpenTaskA2ATestServer(t)
	ownerToken, _ := openTaskA2AAccessToken(t, server.Auth, auth.ScopeOpenTasksRead, auth.ScopeOpenTasksWrite)
	providerToken, providerID := openTaskA2AAccessToken(t, server.Auth, auth.ScopeOpenTasksRead, auth.ScopeOpenTaskProposalsWrite)

	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "A2A OpenTask Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-a2a-open-task-cap",
	})
	if err != nil {
		t.Fatalf("Register capability: %v", err)
	}

	publishResp := callA2A(t, server, ownerToken, "openTasks/publish", map[string]any{
		"title": "A2A lifecycle test", "input": map[string]any{"x": 1},
		"expires_at":      time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
		"idempotency_key": "a2a-publish-1",
	})
	if publishResp.Error != nil {
		t.Fatalf("publish: %+v", publishResp.Error)
	}
	task := decodeResult[domain.OpenTask](t, publishResp)
	if task.Status != domain.OpenTaskOpen {
		t.Fatalf("status = %s, want open", task.Status)
	}

	searchResp := callA2A(t, server, ownerToken, "openTasks/search", map[string]any{"limit": 10})
	if searchResp.Error != nil {
		t.Fatalf("search: %+v", searchResp.Error)
	}
	searchResult := decodeResult[struct {
		OpenTasks []domain.OpenTask `json:"open_tasks"`
	}](t, searchResp)
	found := false
	for _, listed := range searchResult.OpenTasks {
		if listed.ID == task.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("published task %s not found in search results", task.ID)
	}

	getResp := callA2A(t, server, ownerToken, "openTasks/get", map[string]any{"task_id": task.ID})
	if getResp.Error != nil {
		t.Fatalf("get: %+v", getResp.Error)
	}

	proposeResp := callA2A(t, server, providerToken, "openTasks/proposals/submit", map[string]any{
		"task_id": task.ID, "capability_id": cap.ID, "idempotency_key": "a2a-propose-1",
	})
	if proposeResp.Error != nil {
		t.Fatalf("propose: %+v", proposeResp.Error)
	}
	proposal := decodeResult[domain.OpenTaskProposal](t, proposeResp)
	if proposal.ProviderID != providerID {
		t.Fatalf("proposal.provider_id = %s, want %s", proposal.ProviderID, providerID)
	}

	listResp := callA2A(t, server, ownerToken, "openTasks/proposals/list", map[string]any{"task_id": task.ID})
	if listResp.Error != nil {
		t.Fatalf("list proposals: %+v", listResp.Error)
	}
	listResult := decodeResult[struct {
		Proposals []domain.ProposalView `json:"proposals"`
	}](t, listResp)
	if len(listResult.Proposals) != 1 {
		t.Fatalf("proposals length = %d, want 1", len(listResult.Proposals))
	}

	acceptResp := callA2A(t, server, ownerToken, "openTasks/proposals/accept", map[string]any{
		"task_id": task.ID, "proposal_id": proposal.ID, "idempotency_key": "a2a-accept-1",
	})
	if acceptResp.Error != nil {
		t.Fatalf("accept: %+v", acceptResp.Error)
	}
	acceptResult := decodeResult[struct {
		OpenTask   domain.OpenTask            `json:"open_task"`
		Acceptance domain.AcceptanceOperation `json:"acceptance"`
	}](t, acceptResp)
	if acceptResult.Acceptance.Checkpoint != domain.AcceptanceCompleted {
		t.Fatalf("acceptance.checkpoint = %s, want completed: %+v", acceptResult.Acceptance.Checkpoint, acceptResult.Acceptance)
	}
	if acceptResult.OpenTask.Status != domain.OpenTaskFulfilled {
		t.Fatalf("open_task.status = %s, want fulfilled", acceptResult.OpenTask.Status)
	}

	withdrawResp := callA2A(t, server, providerToken, "openTasks/proposals/withdraw", map[string]any{"proposal_id": proposal.ID})
	if withdrawResp.Error == nil {
		t.Fatalf("expected withdrawing an already-accepted proposal to fail, got %+v", withdrawResp)
	}
}

func TestOpenTasksA2A_PublishRequiresWriteScope(t *testing.T) {
	server, _ := newOpenTaskA2ATestServer(t)
	readOnlyToken, _ := openTaskA2AAccessToken(t, server.Auth, auth.ScopeOpenTasksRead)

	resp := callA2A(t, server, readOnlyToken, "openTasks/publish", map[string]any{
		"title": "should be rejected", "expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "idempotency_key": "a2a-publish-reject",
	})
	if resp.Error == nil {
		t.Fatalf("expected a forbidden error for a read-only token, got %+v", resp)
	}
	if resp.Error.Code != codeForbidden {
		t.Fatalf("error code = %d, want codeForbidden (%d): %+v", resp.Error.Code, codeForbidden, resp.Error)
	}
}

func TestOpenTasksA2A_ProposeRequiresProposalsWriteScope(t *testing.T) {
	server, capabilities := newOpenTaskA2ATestServer(t)
	ownerToken, _ := openTaskA2AAccessToken(t, server.Auth, auth.ScopeOpenTasksRead, auth.ScopeOpenTasksWrite)
	// A provider holding only open_tasks:write (not open_task_proposals:write)
	// must not be able to submit a proposal -- the two scopes are
	// deliberately separate, never implied by one another.
	insufficientToken, insufficientID := openTaskA2AAccessToken(t, server.Auth, auth.ScopeOpenTasksRead, auth.ScopeOpenTasksWrite)

	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: insufficientID, Name: "Scope Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-a2a-scope-cap",
	})
	if err != nil {
		t.Fatalf("Register capability: %v", err)
	}

	publishResp := callA2A(t, server, ownerToken, "openTasks/publish", map[string]any{
		"title": "scope test task", "expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "idempotency_key": "a2a-publish-scope",
	})
	if publishResp.Error != nil {
		t.Fatalf("publish: %+v", publishResp.Error)
	}
	task := decodeResult[domain.OpenTask](t, publishResp)

	resp := callA2A(t, server, insufficientToken, "openTasks/proposals/submit", map[string]any{
		"task_id": task.ID, "capability_id": cap.ID, "idempotency_key": "a2a-propose-scope",
	})
	if resp.Error == nil {
		t.Fatalf("expected a forbidden error without open_task_proposals:write, got %+v", resp)
	}
	if resp.Error.Code != codeForbidden {
		t.Fatalf("error code = %d, want codeForbidden (%d): %+v", resp.Error.Code, codeForbidden, resp.Error)
	}
}

func TestOpenTasksA2A_SearchRequiresReadScope(t *testing.T) {
	server, _ := newOpenTaskA2ATestServer(t)
	noScopeToken, _ := openTaskA2AAccessToken(t, server.Auth, auth.ScopeAccountRead)

	resp := callA2A(t, server, noScopeToken, "openTasks/search", map[string]any{})
	if resp.Error == nil {
		t.Fatalf("expected a forbidden error without open_tasks:read, got %+v", resp)
	}
	if resp.Error.Code != codeForbidden {
		t.Fatalf("error code = %d, want codeForbidden (%d): %+v", resp.Error.Code, codeForbidden, resp.Error)
	}
}
