package mcpadapter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/domain"
)

// testMCPServer is a real, in-process third-party MCP server implementing
// the same stateless Streamable HTTP JSON-RPC transport ATOS's own
// internal/mcp.Server uses -- not a mocked Go interface -- so these tests
// exercise the actual wire protocol.
type testMCPServer struct {
	tool string
	// handle, if set, computes the tools/call result for a given
	// arguments map. Missing it entirely errors the test.
	handle func(args map[string]any) toolCallResult
	calls  int
}

func newTestMCPServer(t *testing.T, tool string, handle func(args map[string]any) toolCallResult) *httptest.Server {
	t.Helper()
	s := &testMCPServer{tool: tool, handle: handle}
	return httptest.NewServer(http.HandlerFunc(s.serve))
}

func (s *testMCPServer) serve(w http.ResponseWriter, r *http.Request) {
	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch req.Method {
	case "tools/list":
		_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: jsonRPCVersion, ID: idJSON(req.ID), Result: rawJSON(map[string]any{"tools": []map[string]any{{"name": s.tool}}})})
	case "tools/call":
		s.calls++
		raw, _ := json.Marshal(req.Params)
		var params toolCallParams
		_ = json.Unmarshal(raw, &params)
		if params.Name != s.tool {
			_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: jsonRPCVersion, ID: idJSON(req.ID), Error: &rpcError{Code: codeMethodNotFound, Message: "unknown tool " + params.Name}})
			return
		}
		result := s.handle(params.Arguments)
		_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: jsonRPCVersion, ID: idJSON(req.ID), Result: rawJSON(result)})
	default:
		_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: jsonRPCVersion, ID: idJSON(req.ID), Error: &rpcError{Code: codeMethodNotFound, Message: "unknown method " + req.Method}})
	}
}

func idJSON(v int64) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func rawJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func TestInvoke_Success(t *testing.T) {
	srv := newTestMCPServer(t, "analyze", func(args map[string]any) toolCallResult {
		if args["job_id"] != "job_1" {
			t.Fatalf("job_id = %v", args["job_id"])
		}
		return toolCallResult{IsError: false, StructuredContent: map[string]any{
			"status": "completed", "output": map[string]any{"ok": true},
		}}
	})
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL + "#analyze", IdempotencyKey: "k1", Input: map[string]any{"x": 1},
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Status != provideradapter.InvokeCompleted {
		t.Fatalf("status = %s, want completed", result.Status)
	}
}

func TestInvoke_MissingTool(t *testing.T) {
	srv := newTestMCPServer(t, "analyze", func(args map[string]any) toolCallResult { return toolCallResult{} })
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL + "#does_not_exist", IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected an error for a missing tool")
	}
}

func TestInvoke_MalformedEndpointRef(t *testing.T) {
	a := New(Config{})
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: "https://example.com/no-fragment", IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected an error for an endpoint_ref without a #tool-name fragment")
	}
}

func TestInvoke_ToolLevelError(t *testing.T) {
	srv := newTestMCPServer(t, "analyze", func(args map[string]any) toolCallResult {
		return toolCallResult{IsError: true, StructuredContent: map[string]any{"error": map[string]any{"message": "bad input"}}}
	})
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL + "#analyze", IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Status != provideradapter.InvokeFailed {
		t.Fatalf("status = %s, want failed", result.Status)
	}
	if result.FailureReason != "bad input" {
		t.Fatalf("failure_reason = %q", result.FailureReason)
	}
}

func TestInvoke_MalformedStructuredContent(t *testing.T) {
	srv := newTestMCPServer(t, "analyze", func(args map[string]any) toolCallResult {
		return toolCallResult{IsError: false, StructuredContent: map[string]any{"status": "not_a_real_status"}}
	})
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL + "#analyze", IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected an error for an unrecognized status")
	}
}

func TestInvoke_TimeoutSurfacesAsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	a := New(Config{Timeout: 20 * time.Millisecond, Client: &http.Client{Timeout: 20 * time.Millisecond, Transport: srv.Client().Transport}})
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL + "#analyze", IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected a timeout error, not a fabricated result")
	}
}

func TestInvoke_PendingStatusHonored(t *testing.T) {
	srv := newTestMCPServer(t, "analyze", func(args map[string]any) toolCallResult {
		return toolCallResult{IsError: false, StructuredContent: map[string]any{"status": "pending"}}
	})
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL + "#analyze", IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Status != provideradapter.InvokePending {
		t.Fatalf("status = %s, want pending", result.Status)
	}
}

func TestInvoke_DuplicateInvocationIdentityPassedThrough(t *testing.T) {
	var seenKeys []string
	srv := newTestMCPServer(t, "analyze", func(args map[string]any) toolCallResult {
		seenKeys = append(seenKeys, args["idempotency_key"].(string))
		return toolCallResult{IsError: false, StructuredContent: map[string]any{"status": "completed"}}
	})
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	for i := 0; i < 2; i++ {
		if _, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
			JobID: "job_1", EndpointRef: srv.URL + "#analyze", IdempotencyKey: "k1",
		}); err != nil {
			t.Fatalf("Invoke attempt %d: %v", i, err)
		}
	}
	if len(seenKeys) != 2 || seenKeys[0] != "k1" || seenKeys[1] != "k1" {
		t.Fatalf("idempotency keys seen by provider = %v, want [k1 k1] (provider is responsible for dedup)", seenKeys)
	}
}

func TestInvoke_WrongResultBindingIgnored(t *testing.T) {
	// A response claiming to belong to a different job -- the adapter has
	// no way to enforce this itself (MCP tools/call has no job-scoped
	// result binding built in), so this documents that binding validation
	// is the DISPATCH layer's responsibility (it knows which JobID it
	// asked for), not the adapter's. The adapter must still decode
	// correctly.
	srv := newTestMCPServer(t, "analyze", func(args map[string]any) toolCallResult {
		return toolCallResult{IsError: false, StructuredContent: map[string]any{
			"status": "completed", "output": map[string]any{"for_job": "job_OTHER"},
		}}
	})
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL + "#analyze", IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Output["for_job"] != "job_OTHER" {
		t.Fatal("expected the adapter to decode the output as-is; binding validation is dispatch's job")
	}
}

func TestInvoke_ProviderMetadataCannotClaimStrongerTrustMode(t *testing.T) {
	// A malicious/confused provider stuffing trust-mode claims into its
	// response must have zero effect -- the adapter only ever returns
	// Output/Usage/Status, never anything that could be mistaken for
	// domain.ModeSupport or SupportedTrustModes.
	srv := newTestMCPServer(t, "analyze", func(args map[string]any) toolCallResult {
		return toolCallResult{IsError: false, StructuredContent: map[string]any{
			"status": "completed", "output": map[string]any{"trust_mode": "native", "verified": true, "supported_trust_modes": []string{"verified", "native"}},
		}}
	})
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL + "#analyze", IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	// The claim survives only as opaque Output data -- provideradapter.
	// InvokeResult has no trust-mode-shaped field for it to land in
	// (verified statically by the struct definition), so a caller that
	// only reads Status/Usage/FailureReason, as domain.ModeSupport
	// activation code must, can never observe it.
	if result.Status != provideradapter.InvokeCompleted {
		t.Fatalf("status = %s", result.Status)
	}
	if result.Output["verified"] != true {
		t.Fatal("expected the claim to still be present as opaque, inert Output data")
	}
}

func TestQuery_AlwaysReportsNotFound(t *testing.T) {
	a := New(Config{})
	_, found, err := a.Query(context.Background(), "http://example.invalid#analyze", "any-key")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if found {
		t.Fatal("MCP Query must always report found=false -- the protocol has no lookup-by-id operation")
	}
}

func TestCancel_ReturnsUnsupported(t *testing.T) {
	a := New(Config{})
	if err := a.Cancel(context.Background(), "http://example.invalid#analyze", "k1", "no longer needed"); err != provideradapter.ErrCancelUnsupported {
		t.Fatalf("got %v, want ErrCancelUnsupported", err)
	}
}

func TestHealth_ReachableServer(t *testing.T) {
	srv := newTestMCPServer(t, "analyze", func(args map[string]any) toolCallResult { return toolCallResult{} })
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	check := a.Health(context.Background(), srv.URL+"#analyze")
	if check.Status != domain.AdapterHealthHealthy {
		t.Fatalf("status = %s, want healthy: %+v", check.Status, check)
	}
	if check.Transport != domain.AdapterMCP {
		t.Fatalf("transport = %s", check.Transport)
	}
}

func TestHealth_HandshakeFailure(t *testing.T) {
	a := New(Config{Timeout: 200 * time.Millisecond})
	check := a.Health(context.Background(), "http://127.0.0.1:1#analyze")
	if check.Status != domain.AdapterHealthUnhealthy {
		t.Fatalf("status = %s, want unhealthy", check.Status)
	}
}

func TestHealth_MalformedEndpointRef(t *testing.T) {
	a := New(Config{})
	check := a.Health(context.Background(), "no-fragment-here")
	if check.Status != domain.AdapterHealthUnhealthy {
		t.Fatal("expected unhealthy for a malformed endpoint_ref")
	}
}
