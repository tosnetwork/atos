package httpapi

import (
	"bytes"
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

func openTaskAccessToken(t *testing.T, svc *auth.Service, scopes ...auth.Scope) (token, principalID string) {
	t.Helper()
	raw := make([]string, len(scopes))
	for i, scope := range scopes {
		raw[i] = string(scope)
	}
	grant, err := svc.StartDevice("test", "REST Open Task Test", raw)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	return pair.AccessToken, pair.Principal.ID
}

func newOpenTaskTestServer(t *testing.T) *Server {
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
	return &Server{Auth: authorization, Capabilities: capabilities, OpenTasks: openTasks, Quotes: quotes, Jobs: jobs, Accounts: accounts}
}

func doJSON(t *testing.T, server *Server, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)
	return recorder
}

// TestOpenTasksREST_GoldenPath exercises publish -> propose -> accept
// through the actual REST routes end to end.
func TestOpenTasksREST_GoldenPath(t *testing.T) {
	server := newOpenTaskTestServer(t)
	ownerToken, ownerID := openTaskAccessToken(t, server.Auth, auth.ScopeOpenTasksRead, auth.ScopeOpenTasksWrite)
	providerToken, providerID := openTaskAccessToken(t, server.Auth, auth.ScopeOpenTaskProposalsWrite)

	cap, err := server.Capabilities.Register(t.Context(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "REST Open Task Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-rest-open-task",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	publishRecorder := doJSON(t, server, http.MethodPost, "/v1/open-tasks", ownerToken, map[string]any{
		"title": "rest task", "input": map[string]any{},
		"expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339), "idempotency_key": "rest-publish-1",
	})
	if publishRecorder.Code != http.StatusCreated {
		t.Fatalf("publish status = %d, body = %s", publishRecorder.Code, publishRecorder.Body.String())
	}
	var task domain.OpenTask
	if err := json.Unmarshal(publishRecorder.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode publish response: %v", err)
	}
	if task.PrincipalID != ownerID {
		t.Fatalf("published task principal = %q, want %q", task.PrincipalID, ownerID)
	}

	proposeRecorder := doJSON(t, server, http.MethodPost, "/v1/open-tasks/"+task.ID+"/proposals", providerToken, map[string]any{
		"capability_id": cap.ID, "idempotency_key": "rest-propose-1",
	})
	if proposeRecorder.Code != http.StatusCreated {
		t.Fatalf("propose status = %d, body = %s", proposeRecorder.Code, proposeRecorder.Body.String())
	}
	var proposal domain.OpenTaskProposal
	if err := json.Unmarshal(proposeRecorder.Body.Bytes(), &proposal); err != nil {
		t.Fatalf("decode propose response: %v", err)
	}

	acceptRecorder := doJSON(t, server, http.MethodPost, "/v1/open-tasks/"+task.ID+"/proposals/"+proposal.ID+"/accept", ownerToken, map[string]any{
		"idempotency_key": "rest-accept-1",
	})
	if acceptRecorder.Code != http.StatusOK {
		t.Fatalf("accept status = %d, body = %s", acceptRecorder.Code, acceptRecorder.Body.String())
	}
	var acceptResponse struct {
		OpenTask domain.OpenTask `json:"open_task"`
	}
	if err := json.Unmarshal(acceptRecorder.Body.Bytes(), &acceptResponse); err != nil {
		t.Fatalf("decode accept response: %v", err)
	}
	if acceptResponse.OpenTask.Status != domain.OpenTaskFulfilled {
		t.Fatalf("open_task.status = %s, want fulfilled", acceptResponse.OpenTask.Status)
	}
}

// TestOpenTasksREST_PublishRequiresWriteScope proves a read-only-scoped
// token cannot publish.
func TestOpenTasksREST_PublishRequiresWriteScope(t *testing.T) {
	server := newOpenTaskTestServer(t)
	readOnlyToken, _ := openTaskAccessToken(t, server.Auth, auth.ScopeOpenTasksRead)

	recorder := doJSON(t, server, http.MethodPost, "/v1/open-tasks", readOnlyToken, map[string]any{
		"title": "x", "expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339), "idempotency_key": "scope-test-1",
	})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %s", recorder.Code, recorder.Body.String())
	}
}

// TestOpenTasksREST_ProposeRequiresProviderScope proves an ordinary
// consumer token (open_tasks:read/write only, the default consumer grant)
// cannot submit a proposal -- that requires the separate, explicit-grant-
// only open_task_proposals:write scope.
func TestOpenTasksREST_ProposeRequiresProviderScope(t *testing.T) {
	server := newOpenTaskTestServer(t)
	ownerToken, _ := openTaskAccessToken(t, server.Auth, auth.ScopeOpenTasksRead, auth.ScopeOpenTasksWrite)

	publishRecorder := doJSON(t, server, http.MethodPost, "/v1/open-tasks", ownerToken, map[string]any{
		"title": "x", "expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339), "idempotency_key": "scope-test-2",
	})
	var task domain.OpenTask
	if err := json.Unmarshal(publishRecorder.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode publish response: %v", err)
	}

	recorder := doJSON(t, server, http.MethodPost, "/v1/open-tasks/"+task.ID+"/proposals", ownerToken, map[string]any{
		"capability_id": "cap_whatever", "idempotency_key": "scope-test-2b",
	})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %s", recorder.Code, recorder.Body.String())
	}
}

// TestOpenTasksREST_AcceptRejectsNonOwner proves a caller who is not the
// task's owner cannot accept, even with full open_tasks:write scope.
func TestOpenTasksREST_AcceptRejectsNonOwner(t *testing.T) {
	server := newOpenTaskTestServer(t)
	ownerToken, _ := openTaskAccessToken(t, server.Auth, auth.ScopeOpenTasksRead, auth.ScopeOpenTasksWrite)
	impostorToken, _ := openTaskAccessToken(t, server.Auth, auth.ScopeOpenTasksRead, auth.ScopeOpenTasksWrite)
	providerToken, providerID := openTaskAccessToken(t, server.Auth, auth.ScopeOpenTaskProposalsWrite)

	cap, err := server.Capabilities.Register(t.Context(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "REST Impostor Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-rest-impostor",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	publishRecorder := doJSON(t, server, http.MethodPost, "/v1/open-tasks", ownerToken, map[string]any{
		"title": "x", "expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339), "idempotency_key": "scope-test-3",
	})
	var task domain.OpenTask
	if err := json.Unmarshal(publishRecorder.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode publish response: %v", err)
	}
	proposeRecorder := doJSON(t, server, http.MethodPost, "/v1/open-tasks/"+task.ID+"/proposals", providerToken, map[string]any{
		"capability_id": cap.ID, "idempotency_key": "scope-test-3b",
	})
	var proposal domain.OpenTaskProposal
	if err := json.Unmarshal(proposeRecorder.Body.Bytes(), &proposal); err != nil {
		t.Fatalf("decode propose response: %v", err)
	}

	recorder := doJSON(t, server, http.MethodPost, "/v1/open-tasks/"+task.ID+"/proposals/"+proposal.ID+"/accept", impostorToken, map[string]any{
		"idempotency_key": "scope-test-3c",
	})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (permission_denied): %s", recorder.Code, recorder.Body.String())
	}
}

// TestOpenTasksREST_PublicListingRedactsInput proves GET /v1/open-tasks
// (public browse) never includes Input, even though the owning caller
// authenticated the request.
func TestOpenTasksREST_PublicListingRedactsInput(t *testing.T) {
	server := newOpenTaskTestServer(t)
	ownerToken, _ := openTaskAccessToken(t, server.Auth, auth.ScopeOpenTasksRead, auth.ScopeOpenTasksWrite)
	strangerToken, _ := openTaskAccessToken(t, server.Auth, auth.ScopeOpenTasksRead)

	publishRecorder := doJSON(t, server, http.MethodPost, "/v1/open-tasks", ownerToken, map[string]any{
		"title": "x", "input": map[string]any{"secret": "leak-me-not"},
		"expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339), "idempotency_key": "scope-test-4",
	})
	var task domain.OpenTask
	if err := json.Unmarshal(publishRecorder.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode publish response: %v", err)
	}

	recorder := doJSON(t, server, http.MethodGet, "/v1/open-tasks", strangerToken, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if bytes.Contains(recorder.Body.Bytes(), []byte("leak-me-not")) {
		t.Fatalf("public listing leaked task input: %s", recorder.Body.String())
	}
}
