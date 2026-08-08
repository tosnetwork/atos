package toprotocol_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/adapters/tosprotocol"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
	"github.com/tosnetwork/tos-protocol/pkg/atosrpc"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
	"google.golang.org/protobuf/proto"
)

func TestATOSConnectRPCManagedLifecycle(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	capability, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_rpc_provider", Name: "RPC Echo",
		Description:         "executes through tos-protocol and the private Worker RPC",
		DeliveryMode:        domain.DeliveryInstant,
		InputSchema:         map[string]any{"type": "object"},
		OutputSchema:        map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged},
		Bindings: []domain.CapabilityBinding{{
			Transport: domain.AdapterTOSNative, EndpointRef: "tos-protocol:test",
			EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged},
		}},
		IdempotencyKey: "register-rpc-echo-v1",
	})
	if err != nil {
		t.Fatal(err)
	}

	workerSocket, stopWorker := startPrivateWorker(t)
	defer stopWorker()
	worker, err := localrpc.NewWorkerClient(localrpc.DefaultWorkerClientConfig(workerSocket))
	if err != nil {
		t.Fatal(err)
	}
	router, err := atosrpc.NewStaticRouter([]atosrpc.Route{{
		ProviderID: capability.ProviderID, CapabilityID: capability.ID,
		CapabilityVersion: capability.Version, ServiceID: "tos.ai.integration",
		Operation: "echo", Model: "deterministic-json-echo",
		MaxOutputBytes: 8 << 20, Priority: edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}})
	if err != nil {
		t.Fatal(err)
	}
	protocolServer, err := atosrpc.Open(atosrpc.Config{
		StatePath:   filepath.Join(t.TempDir(), "atos-rpc.db"),
		BearerToken: "integration-secret", Worker: worker, Router: router,
		Authority: atosrpc.NewLocalAuthority("tos-local"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer protocolServer.Close()
	httpServer := httptest.NewServer(protocolServer.Handler())
	defer httpServer.Close()

	client, err := toprotocol.New(toprotocol.Config{
		BaseURL: httpServer.URL, BearerToken: "integration-secret",
		Insecure: true, Timeout: 20 * time.Second, Store: st,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.CheckReady(ctx); err != nil {
		t.Fatal(err)
	}

	quotes := service.NewQuoteService(st, client)
	accounts := service.NewAccountService(st)
	jobs := service.NewJobService(st, client, client, accounts)
	input := map[string]any{"message": "hello over real RPC"}
	quote, err := quotes.Create(ctx, service.CreateQuoteInput{
		PrincipalID: "prn_rpc_client", CapabilityID: capability.ID,
		InputSummary: input, RequestedTrustMode: domain.RequestedTrustManaged,
	})
	if err != nil {
		t.Fatal(err)
	}
	if quote.ServiceQuoteID == "" || quote.UnderlyingServiceQuoteRef == "" {
		t.Fatalf("two-layer quote was not bound: %#v", quote)
	}

	result, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_rpc_client", CapabilityID: capability.ID,
		QuoteID: quote.ID, Input: input, IdempotencyKey: "rpc-lifecycle-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Type != service.ResultCompleted || result.Job.State != domain.JobCompleted {
		t.Fatalf("unexpected result: type=%s state=%s failure=%s", result.Type, result.Job.State, result.Job.FailureReason)
	}
	if got := result.Job.Output["message"]; got != "hello over real RPC" {
		t.Fatalf("worker output = %#v", result.Job.Output)
	}
	if result.Job.ServiceQuoteID != quote.ServiceQuoteID || result.Job.ExecutionDeadline.IsZero() {
		t.Fatalf("job lost service quote/deadline binding: %#v", result.Job)
	}

	receipt, err := st.ReceiptByJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != domain.ReceiptSettled || receipt.TrustMode != domain.TrustModeManaged {
		t.Fatalf("unexpected settlement receipt: %#v", receipt)
	}
	if receipt.NetworkProofRef != "" {
		t.Fatalf("Managed receipt fabricated a network proof: %q", receipt.NetworkProofRef)
	}
	account, err := accounts.Get(ctx, "prn_rpc_client")
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance.Amount != "23.95" {
		t.Fatalf("account balance = %s, want 23.95", account.Balance.Amount)
	}

	replay, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_rpc_client", CapabilityID: capability.ID,
		QuoteID: quote.ID, Input: input, IdempotencyKey: "rpc-lifecycle-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replay.Job.ID != result.Job.ID {
		t.Fatalf("idempotent replay created a new job: %s != %s", replay.Job.ID, result.Job.ID)
	}
	afterReplay, err := accounts.Get(ctx, "prn_rpc_client")
	if err != nil {
		t.Fatal(err)
	}
	if afterReplay.Balance.Amount != "23.95" {
		t.Fatalf("idempotent replay charged twice: %s", afterReplay.Balance.Amount)
	}
}

type integrationWorker struct {
	mu    sync.Mutex
	tasks map[string]*edgev1.GetTaskResponse
}

func newIntegrationWorker() *integrationWorker {
	return &integrationWorker{tasks: make(map[string]*edgev1.GetTaskResponse)}
}

func (w *integrationWorker) Health(context.Context, *connect.Request[edgev1.HealthRequest]) (*connect.Response[edgev1.HealthResponse], error) {
	components := make([]*edgev1.ReadinessComponent, 0, 6)
	for _, id := range []string{"worker", "admission", "resources", "runtimes", "model-binding", "task-store"} {
		components = append(components, &edgev1.ReadinessComponent{Id: id, Status: edgev1.ReadinessStatus_READINESS_STATUS_READY, Revision: "integration-v1"})
	}
	return connect.NewResponse(&edgev1.HealthResponse{Status: "ready", Version: "integration-v1", Readiness: components}), nil
}

func (w *integrationWorker) GetCapabilities(context.Context, *connect.Request[edgev1.GetCapabilitiesRequest]) (*connect.Response[edgev1.GetCapabilitiesResponse], error) {
	now := time.Now().UTC()
	return connect.NewResponse(&edgev1.GetCapabilitiesResponse{
		CapacityRevision: "capacity-v1", TerminalRevision: "terminal-v1",
		CollectedUnixMillis: now.Add(-time.Second).UnixMilli(), ExpiresUnixMillis: now.Add(time.Minute).UnixMilli(),
		Capabilities: []*edgev1.Capability{{
			ServiceId: "tos.ai.integration", Operation: "echo", Model: "deterministic-json-echo",
			ModelDigest: "sha256:" + strings.Repeat("a", 64), Runtime: "integration",
			RuntimeRevision: "runtime-v1", MaxInputBytes: 8 << 20, MaxOutputBytes: 8 << 20,
			AcceptedPriorities: []edgev1.Priority{edgev1.Priority_PRIORITY_EXTERNAL_SERVICE},
		}},
	}), nil
}

func (w *integrationWorker) Quote(_ context.Context, request *connect.Request[edgev1.QuoteRequest]) (*connect.Response[edgev1.QuoteResponse], error) {
	now := time.Now().UTC()
	expires := now.Add(time.Minute)
	deadline := time.UnixMilli(request.Msg.DeadlineUnixMillis)
	if !expires.Before(deadline) {
		expires = deadline.Add(-time.Second)
	}
	return connect.NewResponse(&edgev1.QuoteResponse{
		QuoteId: "worker-quote-0001", RequestId: request.Msg.RequestId,
		ExpiresUnixMillis: expires.UnixMilli(), PriceNanoTos: 100_000_000,
		CapacityRevision: "capacity-v1", ModelRevision: "model-v1", RuntimeRevision: "runtime-v1",
	}), nil
}

func (w *integrationWorker) Invoke(_ context.Context, request *connect.Request[edgev1.InvokeRequest]) (*connect.Response[edgev1.InvokeResponse], error) {
	now := time.Now().UTC()
	output := append([]byte(nil), request.Msg.Payload...)
	response := &edgev1.InvokeResponse{
		RequestId: request.Msg.RequestId, Output: output,
		Usage:         &edgev1.Usage{InputBytes: uint64(len(request.Msg.Payload)), OutputBytes: uint64(len(output)), ExecutionMillis: 1},
		ModelRevision: "model-v1", RuntimeRevision: "runtime-v1", CompletedUnixMillis: now.UnixMilli(),
	}
	w.mu.Lock()
	w.tasks[request.Msg.TaskId] = &edgev1.GetTaskResponse{
		RequestId: request.Msg.RequestId, TaskId: request.Msg.TaskId,
		RequestDigest: request.Msg.RequestDigest, Status: edgev1.TaskStatus_TASK_STATUS_SUCCEEDED,
		Result: response, CompletedUnixMillis: response.CompletedUnixMillis,
		RetainUntilUnixMillis: request.Msg.RetainUntilUnixMillis,
	}
	w.mu.Unlock()
	return connect.NewResponse(response), nil
}

func (w *integrationWorker) GetTask(_ context.Context, request *connect.Request[edgev1.GetTaskRequest]) (*connect.Response[edgev1.GetTaskResponse], error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	stored := w.tasks[request.Msg.TaskId]
	if stored == nil {
		return connect.NewResponse(&edgev1.GetTaskResponse{
			RequestId: request.Msg.RequestId, TaskId: request.Msg.TaskId,
			RequestDigest: request.Msg.RequestDigest, Status: edgev1.TaskStatus_TASK_STATUS_NOT_FOUND,
		}), nil
	}
	cloned := proto.Clone(stored).(*edgev1.GetTaskResponse)
	return connect.NewResponse(cloned), nil
}

func (w *integrationWorker) Cancel(_ context.Context, request *connect.Request[edgev1.CancelRequest]) (*connect.Response[edgev1.CancelResponse], error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, found := w.tasks[request.Msg.TaskId]; found {
		return connect.NewResponse(&edgev1.CancelResponse{
			RequestId: request.Msg.RequestId, TaskId: request.Msg.TaskId,
			RequestDigest: request.Msg.RequestDigest, Accepted: false,
		}), nil
	}
	return connect.NewResponse(&edgev1.CancelResponse{
		RequestId: request.Msg.RequestId, TaskId: request.Msg.TaskId,
		RequestDigest: request.Msg.RequestDigest, Accepted: true,
	}), nil
}

func startPrivateWorker(t *testing.T) (string, func()) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(directory, "worker.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	path, handler := edgev1connect.NewWorkerServiceHandler(newIntegrationWorker())
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := &http.Server{Handler: mux}
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			t.Errorf("private Worker server: %v", serveErr)
		}
	}()
	return socketPath, func() {
		_ = server.Close()
		_ = listener.Close()
		_ = os.Remove(socketPath)
	}
}
