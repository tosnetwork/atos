package a2aadapter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/a2a"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/domain"
)

// testA2AServer is a real, in-process third-party A2A agent implementing
// the same JSON-RPC methods (message/send, tasks/get, tasks/cancel)
// internal/a2a.Server implements for ATOS's own inbound surface -- not a
// mocked Go interface.
type testA2AServer struct {
	tasks map[string]a2a.Task
	// onSend, if set, computes the Task returned for a message/send call.
	onSend  func(msg a2a.Message) a2a.Task
	cancels []string
}

func newTestA2AServer(t *testing.T, onSend func(msg a2a.Message) a2a.Task) *httptest.Server {
	t.Helper()
	s := &testA2AServer{tasks: map[string]a2a.Task{}, onSend: onSend}
	return httptest.NewServer(http.HandlerFunc(s.serve))
}

func (s *testA2AServer) serve(w http.ResponseWriter, r *http.Request) {
	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch req.Method {
	case "message/send":
		raw, _ := json.Marshal(req.Params)
		var params messageSendParams
		_ = json.Unmarshal(raw, &params)
		task := s.onSend(params.Message)
		if task.ID == "" {
			task.ID = params.Message.TaskID
		}
		s.tasks[task.ID] = task
		_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: jsonRPCVersion, Result: rawJSON(task)})
	case "tasks/get":
		raw, _ := json.Marshal(req.Params)
		var params taskIDParams
		_ = json.Unmarshal(raw, &params)
		task, ok := s.tasks[params.ID]
		if !ok {
			_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: jsonRPCVersion, Error: &rpcError{Code: -32001, Message: "task not found"}})
			return
		}
		_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: jsonRPCVersion, Result: rawJSON(task)})
	case "tasks/cancel":
		raw, _ := json.Marshal(req.Params)
		var params taskIDParams
		_ = json.Unmarshal(raw, &params)
		s.cancels = append(s.cancels, params.ID)
		task := s.tasks[params.ID]
		task.Status.State = a2a.TaskCanceled
		s.tasks[params.ID] = task
		_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: jsonRPCVersion, Result: rawJSON(task)})
	default:
		_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: jsonRPCVersion, Error: &rpcError{Code: -32601, Message: "unknown method " + req.Method}})
	}
}

func rawJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func TestInvoke_ValidTaskCompleted(t *testing.T) {
	srv := newTestA2AServer(t, func(msg a2a.Message) a2a.Task {
		if msg.TaskID != "job:job_1:v1" {
			t.Fatalf("task id = %s", msg.TaskID)
		}
		return a2a.Task{
			ID:        msg.TaskID,
			Status:    a2a.TaskStatus{State: a2a.TaskCompleted, Timestamp: time.Now()},
			Artifacts: []a2a.Artifact{{ID: "a1", Name: "result", Content: map[string]any{"ok": true}}},
		}
	})
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", CapabilityID: "cap_1", QuoteID: "q_1",
		EndpointRef: srv.URL, IdempotencyKey: "job:job_1:v1", Input: map[string]any{"x": 1},
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Status != provideradapter.InvokeCompleted {
		t.Fatalf("status = %s, want completed", result.Status)
	}
	if result.Output["ok"] != true {
		t.Fatalf("output = %+v", result.Output)
	}
}

func TestInvoke_WrongTaskBinding(t *testing.T) {
	// A provider returning a Task for a different job/id than what was
	// requested must still decode correctly here -- job/provider binding
	// validation is the dispatch layer's responsibility (it knows what it
	// asked for), matching mcpadapter's equivalent test.
	srv := newTestA2AServer(t, func(msg a2a.Message) a2a.Task {
		return a2a.Task{ID: "some_other_task", Status: a2a.TaskStatus{State: a2a.TaskCompleted}}
	})
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Status != provideradapter.InvokeCompleted {
		t.Fatalf("status = %s", result.Status)
	}
}

func TestInvoke_TerminalFailureStates(t *testing.T) {
	for _, state := range []a2a.TaskState{a2a.TaskFailed, a2a.TaskCanceled, a2a.TaskRejected} {
		srv := newTestA2AServer(t, func(msg a2a.Message) a2a.Task {
			return a2a.Task{Status: a2a.TaskStatus{State: state}}
		})
		a := New(Config{Client: srv.Client()})
		result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
			JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
		})
		srv.Close()
		if err != nil {
			t.Fatalf("state %s: Invoke: %v", state, err)
		}
		if result.Status != provideradapter.InvokeFailed {
			t.Fatalf("state %s: status = %s, want failed", state, result.Status)
		}
	}
}

func TestInvoke_DelayedPendingStates(t *testing.T) {
	for _, state := range []a2a.TaskState{a2a.TaskSubmitted, a2a.TaskWorking, a2a.TaskInputRequired} {
		srv := newTestA2AServer(t, func(msg a2a.Message) a2a.Task {
			return a2a.Task{Status: a2a.TaskStatus{State: state}}
		})
		a := New(Config{Client: srv.Client()})
		result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
			JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
		})
		srv.Close()
		if err != nil {
			t.Fatalf("state %s: Invoke: %v", state, err)
		}
		if result.Status != provideradapter.InvokePending {
			t.Fatalf("state %s: status = %s, want pending", state, result.Status)
		}
	}
}

func TestInvoke_UnknownStateFailsClosed(t *testing.T) {
	srv := newTestA2AServer(t, func(msg a2a.Message) a2a.Task {
		return a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskState("some-future-state")}}
	})
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected an error for an unrecognized A2A task state (fail closed)")
	}
}

func TestInvoke_DuplicateResultConverges(t *testing.T) {
	calls := 0
	srv := newTestA2AServer(t, func(msg a2a.Message) a2a.Task {
		calls++
		return a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskCompleted}, Artifacts: []a2a.Artifact{{Name: "result", Content: map[string]any{"call": calls}}}}
	})
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	first, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	// A caller that instead recovers via Query (the intended lost-
	// response-recovery path) must see the SAME task, without triggering
	// a second message/send.
	recovered, found, err := a.Query(context.Background(), srv.URL, "k1")
	if err != nil || !found {
		t.Fatalf("Query: found=%v err=%v", found, err)
	}
	if recovered.Output["call"] != first.Output["call"] {
		t.Fatalf("recovered a different result: %+v vs %+v", recovered.Output, first.Output)
	}
	if calls != 1 {
		t.Fatalf("message/send called %d times, want 1 (Query must not trigger a duplicate send)", calls)
	}
}

func TestQuery_UnknownTaskIsNotFoundNotError(t *testing.T) {
	srv := newTestA2AServer(t, func(msg a2a.Message) a2a.Task { return a2a.Task{} })
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	_, found, err := a.Query(context.Background(), srv.URL, "never-sent")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if found {
		t.Fatal("expected found=false for an unknown task id")
	}
}

func TestQuery_RequiresEndpoint(t *testing.T) {
	a := New(Config{})
	_, found, err := a.Query(context.Background(), "", "k1")
	if found || err == nil {
		t.Fatal("Query with an empty endpoint must error")
	}
}

func TestCancel_Success(t *testing.T) {
	srv := newTestA2AServer(t, func(msg a2a.Message) a2a.Task {
		return a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskWorking}}
	})
	defer srv.Close()
	a := New(Config{Client: srv.Client()})
	if _, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Cancel(context.Background(), srv.URL, "k1", "no longer needed"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	result, found, err := a.Query(context.Background(), srv.URL, "k1")
	if err != nil || !found {
		t.Fatalf("Query after cancel: found=%v err=%v", found, err)
	}
	if result.Status != provideradapter.InvokeFailed {
		t.Fatalf("status after cancel = %s, want failed (canceled maps to failed)", result.Status)
	}
}

func TestHealth_ReachableAgent(t *testing.T) {
	srv := newTestA2AServer(t, func(msg a2a.Message) a2a.Task { return a2a.Task{} })
	defer srv.Close()

	a := New(Config{Client: srv.Client()})
	check := a.Health(context.Background(), srv.URL)
	if check.Status != domain.AdapterHealthHealthy {
		t.Fatalf("status = %s, want healthy", check.Status)
	}
	if check.Transport != domain.AdapterA2A {
		t.Fatalf("transport = %s", check.Transport)
	}
}

func TestHealth_Unreachable(t *testing.T) {
	a := New(Config{Timeout: 200 * time.Millisecond})
	check := a.Health(context.Background(), "http://127.0.0.1:1")
	if check.Status != domain.AdapterHealthUnhealthy {
		t.Fatalf("status = %s, want unhealthy", check.Status)
	}
}
