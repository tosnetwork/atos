package mcp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/tosnetwork/atos/internal/auth"
)

func accessToken(t *testing.T, svc *auth.Service, scopes ...auth.Scope) string {
	t.Helper()
	raw := make([]string, len(scopes))
	for i, scope := range scopes {
		raw[i] = string(scope)
	}
	grant, err := svc.StartDevice(raw)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	return pair.AccessToken
}

type listResponse struct {
	JSONRPC string `json:"jsonrpc"`
	Result  struct {
		Tools      []map[string]any `json:"tools"`
		TTLMS      int              `json:"ttlMs"`
		CacheScope string           `json:"cacheScope"`
	} `json:"result"`
	Error *rpcError `json:"error"`
}

func listTools(t *testing.T, server *Server, token string) listResponse {
	t.Helper()
	body := bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	req := httptest.NewRequest(http.MethodPost, "/mcp", body)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	server.Handler()(recorder, req)
	var response listResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode tools/list: %v; body=%s", err, recorder.Body.String())
	}
	if response.Error != nil {
		t.Fatalf("tools/list error: %+v", response.Error)
	}
	return response
}

func names(tools []map[string]any) []string {
	out := make([]string, 0, len(tools))
	for _, tool := range tools {
		name, _ := tool["name"].(string)
		out = append(out, name)
	}
	return out
}

func TestOrdinaryConsumerSeesNineStableTools(t *testing.T) {
	authorization := auth.NewService()
	token := accessToken(t, authorization,
		auth.ScopeCapabilitiesRead,
		auth.ScopeQuotesRead,
		auth.ScopeInvocationsCreate,
		auth.ScopeJobsCreate,
		auth.ScopeJobsRead,
		auth.ScopeJobsCancel,
		auth.ScopeAccountRead,
	)
	server := &Server{Auth: authorization}
	first := listTools(t, server, token)
	second := listTools(t, server, token)

	want := []string{
		"atos_search", "atos_get_capability", "atos_quote", "atos_invoke",
		"atos_create_job", "atos_get_job", "atos_cancel_job", "atos_account",
		"atos_artifact",
	}
	if got := names(first.Result.Tools); !reflect.DeepEqual(got, want) {
		t.Fatalf("tools = %v, want %v", got, want)
	}
	if got := names(second.Result.Tools); !reflect.DeepEqual(got, want) {
		t.Fatalf("second tools/list order = %v, want %v", got, want)
	}
	if first.Result.TTLMS != toolsListTTLMS || first.Result.CacheScope != toolsCacheScope {
		t.Fatalf("cache hints = ttl %d scope %q", first.Result.TTLMS, first.Result.CacheScope)
	}
}

func TestProviderScopeAddsCapabilityManagementTools(t *testing.T) {
	authorization := auth.NewService()
	token := accessToken(t, authorization,
		auth.ScopeCapabilitiesRead,
		auth.ScopeQuotesRead,
		auth.ScopeInvocationsCreate,
		auth.ScopeJobsCreate,
		auth.ScopeJobsRead,
		auth.ScopeJobsCancel,
		auth.ScopeAccountRead,
		auth.ScopeCapabilitiesWrite,
	)
	got := names(listTools(t, &Server{Auth: authorization}, token).Result.Tools)
	wantSuffix := []string{
		"atos_register_capability", "atos_update_capability",
		"atos_list_my_capabilities", "atos_pause_capability",
	}
	if len(got) != 13 || !reflect.DeepEqual(got[len(got)-len(wantSuffix):], wantSuffix) {
		t.Fatalf("provider tools = %v", got)
	}
}

func TestScopeGatedToolIsUnreachableToConsumer(t *testing.T) {
	authorization := auth.NewService()
	token := accessToken(t, authorization, auth.ScopeCapabilitiesRead)
	body := bytes.NewBufferString(`{
	  "jsonrpc":"2.0",
	  "id":1,
	  "method":"tools/call",
	  "params":{"name":"atos_register_capability","arguments":{}}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/mcp", body)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	(&Server{Auth: authorization}).Handler()(recorder, req)
	var response rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != codeMethodNotFound {
		t.Fatalf("got response %s, want method-not-found for hidden tool", recorder.Body.String())
	}
}

func TestArtifactSchemaIsOperationDiscriminated(t *testing.T) {
	definition := artifactTool()
	input, ok := definition["inputSchema"].(map[string]any)
	if !ok {
		t.Fatal("artifact inputSchema is not an object")
	}
	alternatives, ok := input["oneOf"].([]any)
	if !ok || len(alternatives) != 3 {
		t.Fatalf("artifact inputSchema oneOf = %#v", input["oneOf"])
	}
	output, ok := definition["outputSchema"].(map[string]any)
	if !ok {
		t.Fatal("artifact outputSchema is not an object")
	}
	outAlternatives, ok := output["oneOf"].([]any)
	if !ok || len(outAlternatives) != 3 {
		t.Fatalf("artifact outputSchema oneOf = %#v", output["oneOf"])
	}
}
