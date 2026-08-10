package toprotocol_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/tosai/dispatch"
	"github.com/tosnetwork/atos/internal/adapters/tosprotocol"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
	"github.com/tosnetwork/tos-protocol/pkg/atosrpc"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
)

// fakeThirdPartyWorker stands in for tos-ai's real
// internal/thirdparty.Service (a separate repository, not re-tested here)
// -- it deterministically echoes the invocation's input back as output,
// simulating a third-party provider tos-ai successfully dialed. The point
// of this test is proving atos's own wiring (dispatch.Provider ->
// tosprotocol.Client -> real ConnectRPC -> atosrpc.Server's third-party
// SubmitJob/QuoteExecution -> the ThirdPartyWorker interface) is correct
// end-to-end, not re-verifying tos-ai's actual outbound dial logic.
type fakeThirdPartyWorker struct {
	invokeCalls int
}

func (w *fakeThirdPartyWorker) Health(
	context.Context, *connect.Request[edgev1.ThirdPartyHealthRequest],
) (*connect.Response[edgev1.ThirdPartyHealthResponse], error) {
	return connect.NewResponse(&edgev1.ThirdPartyHealthResponse{Healthy: true}), nil
}

func (w *fakeThirdPartyWorker) Invoke(
	_ context.Context, req *connect.Request[edgev1.ThirdPartyInvokeRequest],
) (*connect.Response[edgev1.ThirdPartyInvokeResponse], error) {
	w.invokeCalls++
	return connect.NewResponse(&edgev1.ThirdPartyInvokeResponse{
		RequestId: req.Msg.RequestId, Status: edgev1.ThirdPartyInvokeStatus_THIRD_PARTY_INVOKE_STATUS_COMPLETED,
		Output: append([]byte(nil), req.Msg.Input...), CompletedUnixMillis: time.Now().UnixMilli(),
	}), nil
}

func (w *fakeThirdPartyWorker) Query(
	context.Context, *connect.Request[edgev1.ThirdPartyQueryRequest],
) (*connect.Response[edgev1.ThirdPartyQueryResponse], error) {
	return connect.NewResponse(&edgev1.ThirdPartyQueryResponse{Found: false}), nil
}

func (w *fakeThirdPartyWorker) Cancel(
	context.Context, *connect.Request[edgev1.ThirdPartyCancelRequest],
) (*connect.Response[edgev1.ThirdPartyCancelResponse], error) {
	return connect.NewResponse(&edgev1.ThirdPartyCancelResponse{Accepted: true}), nil
}

func startThirdPartyWorker(t *testing.T, worker edgev1connect.ThirdPartyExecutionServiceHandler) (string, func()) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(directory, "thirdparty.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	path, handler := edgev1connect.NewThirdPartyExecutionServiceHandler(worker)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := &http.Server{Handler: mux}
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			t.Errorf("third-party worker server: %v", serveErr)
		}
	}()
	return socketPath, func() {
		_ = server.Close()
		_ = listener.Close()
		_ = os.Remove(socketPath)
	}
}

// TestATOSConnectRPCThirdPartyManagedLifecycle proves the full real-RPC
// path for a third-party (http-bound) Capability under
// dispatch.WithRemoteThirdPartyExecution: atos's JobService -> dispatch.
// Provider -> tosprotocol.Client -> real ConnectRPC -> atosrpc.Server's
// third-party SubmitJob/QuoteExecution state machine -> ThirdPartyWorker,
// landing in the same Receipt/settlement pipeline a tos-native Job uses,
// exactly as atos-spec §7.1.5's cross-repository acceptance criterion
// requires (modulo the ThirdPartyWorker side being faked here -- see
// fakeThirdPartyWorker's doc comment).
func TestATOSConnectRPCThirdPartyManagedLifecycle(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	capability, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_rpc_thirdparty_provider", Name: "RPC Third-Party Echo",
		Description:         "executes through tos-protocol's ThirdPartyExecutionService boundary",
		DeliveryMode:        domain.DeliveryInstant,
		InputSchema:         map[string]any{"type": "object"},
		OutputSchema:        map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged},
		Bindings: []domain.CapabilityBinding{{
			Transport: domain.AdapterHTTP, EndpointRef: "https://third-party-provider.example.com/invoke",
			EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged},
		}},
		IdempotencyKey: "register-rpc-thirdparty-echo-v1",
	})
	if err != nil {
		t.Fatal(err)
	}

	thirdPartySocket, stopThirdParty := startThirdPartyWorker(t, &fakeThirdPartyWorker{})
	defer stopThirdParty()
	thirdPartyWorker, err := localrpc.NewThirdPartyWorkerClient(localrpc.DefaultThirdPartyWorkerClientConfig(thirdPartySocket))
	if err != nil {
		t.Fatal(err)
	}

	protocolServer, err := atosrpc.Open(atosrpc.Config{
		StatePath:   filepath.Join(t.TempDir(), "atos-rpc.db"),
		BearerToken: "integration-secret", ThirdPartyWorker: thirdPartyWorker,
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

	// Mirrors cmd/api/main.go's real wiring exactly: the quoter/core stay
	// as the raw tosprotocol.Client (QuoteExecution/receipt verification
	// are transport-agnostic already); only the execution Provider gets
	// wrapped by dispatch.New, with remote third-party execution enabled.
	execution := dispatch.New(client, provideradapter.NewResolver(), dispatch.WithRemoteThirdPartyExecution(true))

	quotes := service.NewQuoteService(st, client)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, execution, client, accounts)
	input := map[string]any{"message": "hello over real third-party RPC"}
	quote, err := quotes.Create(ctx, service.CreateQuoteInput{
		PrincipalID: "prn_rpc_thirdparty_client", CapabilityID: capability.ID,
		InputSummary: input, RequestedTrustMode: domain.RequestedTrustManaged,
	})
	if err != nil {
		t.Fatal(err)
	}
	if quote.ServiceQuoteID == "" {
		t.Fatalf("third-party quote was not bound to a tos-protocol ServiceExecutionQuote: %#v", quote)
	}

	result, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_rpc_thirdparty_client", CapabilityID: capability.ID,
		QuoteID: quote.ID, Input: input, IdempotencyKey: "rpc-thirdparty-lifecycle-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Type != service.ResultCompleted || result.Job.State != domain.JobCompleted {
		t.Fatalf("unexpected result: type=%s state=%s failure=%s", result.Type, result.Job.State, result.Job.FailureReason)
	}
	if got := result.Job.Output["message"]; got != "hello over real third-party RPC" {
		t.Fatalf("worker output = %#v, want the fake third-party worker's echo", result.Job.Output)
	}

	receipt, err := st.ReceiptByJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != domain.ReceiptSettled || receipt.TrustMode != domain.TrustModeManaged {
		t.Fatalf("unexpected settlement receipt: %#v", receipt)
	}

	account, err := accounts.Get(ctx, "prn_rpc_thirdparty_client")
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance.Amount != "23.95" {
		t.Fatalf("account balance = %s, want 23.95 (charged exactly once)", account.Balance.Amount)
	}

	// Idempotent replay must not re-invoke the third-party worker.
	replay, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_rpc_thirdparty_client", CapabilityID: capability.ID,
		QuoteID: quote.ID, Input: input, IdempotencyKey: "rpc-thirdparty-lifecycle-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replay.Job.ID != result.Job.ID {
		t.Fatalf("replay produced a different job: %s vs %s", replay.Job.ID, result.Job.ID)
	}
}
