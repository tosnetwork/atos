package httpadapter

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/domain"
)

func testAdapter(t *testing.T, srv *httptest.Server) *Adapter {
	t.Helper()
	return New(Config{Timeout: 5 * time.Second, Client: srv.Client()})
}

func TestInvoke_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.Header.Get("Idempotency-Key") != "job:job_1:v1" {
			t.Fatalf("idempotency header = %s", r.Header.Get("Idempotency-Key"))
		}
		var body wireRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.JobID != "job_1" {
			t.Fatalf("job_id = %s", body.JobID)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(wireResponse{Status: "completed", Output: map[string]any{"ok": true}, Usage: domain.Usage{OutputTokens: 5}})
	}))
	defer srv.Close()

	a := testAdapter(t, srv)
	result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", CapabilityID: "cap_1", CapabilityVersion: "1.0.0",
		EndpointRef: srv.URL, IdempotencyKey: "job:job_1:v1", Input: map[string]any{"x": 1},
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Status != provideradapter.InvokeCompleted {
		t.Fatalf("status = %s, want completed", result.Status)
	}
	if result.Usage.OutputTokens != 5 {
		t.Fatalf("usage = %+v", result.Usage)
	}
}

func TestInvoke_MalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{not json"))
	}))
	defer srv.Close()

	a := testAdapter(t, srv)
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected an error for malformed JSON response")
	}
}

func TestInvoke_Non2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	a := testAdapter(t, srv)
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected an error for a 500 response")
	}
}

func TestInvoke_OversizedResponseRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"completed","output":{"blob":"` + strings.Repeat("x", 2000) + `"}}`))
	}))
	defer srv.Close()

	a := New(Config{Timeout: 5 * time.Second, Client: srv.Client(), MaxResponseBytes: 100})
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected an error for a response exceeding the byte limit")
	}
}

func TestInvoke_TimeoutSurfacesAsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(wireResponse{Status: "completed"})
	}))
	defer srv.Close()

	a := New(Config{Timeout: 20 * time.Millisecond, Client: &http.Client{Timeout: 20 * time.Millisecond, Transport: srv.Client().Transport}})
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected a timeout error, not a fabricated result")
	}
}

func TestInvoke_DelayedPastDeadlineNotTreatedAsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(wireResponse{Status: "completed"})
	}))
	defer srv.Close()

	a := testAdapter(t, srv)
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
		Deadline: time.Now().Add(10 * time.Millisecond),
	})
	if err == nil {
		t.Fatal("expected an error when the response arrives after the bound deadline")
	}
}

func TestInvoke_PendingStatusHonored(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(wireResponse{Status: "pending"})
	}))
	defer srv.Close()

	a := testAdapter(t, srv)
	result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Status != provideradapter.InvokePending {
		t.Fatalf("status = %s, want pending", result.Status)
	}
}

func TestInvoke_UnknownStatusRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"totally_made_up"}`))
	}))
	defer srv.Close()

	a := testAdapter(t, srv)
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected an error for an unrecognized status value")
	}
}

func TestInvoke_RedirectNotFollowed(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("redirect target must never be reached")
	}))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer srv.Close()

	a := testAdapter(t, srv)
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected a 3xx to surface as a non-2xx error, not be silently followed")
	}
}

func TestQueryAt_NotFoundIsNotAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	a := testAdapter(t, srv)
	result, found, err := a.QueryAt(context.Background(), srv.URL, "k1")
	if err != nil {
		t.Fatalf("QueryAt: %v", err)
	}
	if found {
		t.Fatalf("expected found=false for a 404, got result=%+v", result)
	}
}

func TestQueryAt_RecoversPriorResultWithoutNewSideEffect(t *testing.T) {
	invokeCalls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			invokeCalls++
		}
		if r.URL.Query().Get("idempotency_key") != "k1" {
			t.Fatalf("query param = %s", r.URL.Query().Get("idempotency_key"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(wireResponse{Status: "completed", Output: map[string]any{"done": true}})
	}))
	defer srv.Close()

	a := testAdapter(t, srv)
	result, found, err := a.QueryAt(context.Background(), srv.URL, "k1")
	if err != nil || !found {
		t.Fatalf("QueryAt: found=%v err=%v", found, err)
	}
	if result.Status != provideradapter.InvokeCompleted {
		t.Fatalf("status = %s", result.Status)
	}
	if invokeCalls != 0 {
		t.Fatalf("Query caused %d POST calls, want 0", invokeCalls)
	}
}

func TestCancel_ReturnsUnsupported(t *testing.T) {
	a := New(Config{})
	err := a.Cancel(context.Background(), "k1", "no longer needed")
	if err != provideradapter.ErrCancelUnsupported {
		t.Fatalf("got %v, want ErrCancelUnsupported", err)
	}
}

func TestHealth_ReachableEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := testAdapter(t, srv)
	check := a.Health(context.Background(), srv.URL)
	if check.Status != domain.AdapterHealthHealthy {
		t.Fatalf("status = %s, want healthy: %+v", check.Status, check)
	}
	if check.Transport != domain.AdapterHTTP {
		t.Fatalf("transport = %s", check.Transport)
	}
}

func TestHealth_UnreachableEndpoint(t *testing.T) {
	a := New(Config{Timeout: 200 * time.Millisecond})
	check := a.Health(context.Background(), "http://127.0.0.1:1") // nothing listens here
	if check.Status != domain.AdapterHealthUnhealthy {
		t.Fatalf("status = %s, want unhealthy", check.Status)
	}
	if check.FailureReason == "" {
		t.Fatal("expected a failure reason")
	}
}

func TestPolicy_RejectsPrivateNetworkDestinationByDefault(t *testing.T) {
	// 10.0.0.1 is RFC1918 private space; the default outbound policy must
	// reject it without ever attempting a real connection, regardless of
	// whether anything is actually listening there.
	a := New(Config{Timeout: 2 * time.Second})
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: "http://10.0.0.1/invoke", IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected the outbound policy to reject a private-network destination")
	}
}

func TestPolicy_RejectsLoopbackDestinationByDefault(t *testing.T) {
	a := New(Config{Timeout: 2 * time.Second})
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: "http://127.0.0.1:1/invoke", IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected the outbound policy to reject a loopback destination")
	}
}

func TestPolicy_EscapeHatchAllowsLoopbackWhenExplicitlySet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(wireResponse{Status: "completed"})
	}))
	defer srv.Close()

	t.Setenv(AllowPrivateNetworksEnv, "1")
	// A real (non-test-injected) client, exercising the actual DialContext
	// policy path against the httptest server's loopback address.
	a := New(Config{Timeout: 5 * time.Second})
	result, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Invoke with escape hatch set: %v", err)
	}
	if result.Status != provideradapter.InvokeCompleted {
		t.Fatalf("status = %s", result.Status)
	}
}

func TestPolicy_LoopbackBlockedWithoutEscapeHatchEvenAgainstRealServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler must never be reached when the outbound policy blocks the destination")
	}))
	defer srv.Close()

	a := New(Config{Timeout: 2 * time.Second})
	_, err := a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: srv.URL, IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected the real (non-test-injected) client to reject a loopback destination")
	}
}

func TestPolicy_ConnectionFailureNotConfusedWithSuccess(t *testing.T) {
	// Reserve a port, then close it immediately so nothing listens there.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	t.Setenv(AllowPrivateNetworksEnv, "1")
	a := New(Config{Timeout: 500 * time.Millisecond})
	_, err = a.Invoke(context.Background(), provideradapter.InvokeRequest{
		JobID: "job_1", EndpointRef: "http://" + addr + "/invoke", IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected a connection-refused error")
	}
}
