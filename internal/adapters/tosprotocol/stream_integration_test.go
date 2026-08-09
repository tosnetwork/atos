package toprotocol_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/adapters/tosprotocol"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/pkg/atosrpc"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
)

// TestATOSConnectRPCStreamJobIngestsIntoDurableJournal proves the real
// tos-protocol ExecutionGatewayService.StreamJob RPC (not the mock backend)
// can be consumed end to end: SubmitJob through the real RPC/private Worker
// path, StreamJobEvents over the real stream, and every received event
// durably appended to ATOS's own store without any validation rejection --
// including tos-protocol's current per-chunk digest quirk (see stream.go),
// which this test exercises against the real server rather than a synthetic
// substitute.
func TestATOSConnectRPCStreamJobIngestsIntoDurableJournal(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	capability, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_rpc_stream_provider", Name: "RPC Stream Echo",
		Description:         "exercises the real StreamJob RPC end to end",
		DeliveryMode:        domain.DeliveryInstant,
		InputSchema:         map[string]any{"type": "object"},
		OutputSchema:        map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged},
		Bindings: []domain.CapabilityBinding{{
			Transport: domain.AdapterTOSNative, EndpointRef: "tos-protocol:stream-test",
			EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged},
		}},
		IdempotencyKey: "register-rpc-stream-echo-v1",
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
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, client, client, accounts)
	input := map[string]any{"message": "hello streamed over real RPC"}
	quote, err := quotes.Create(ctx, service.CreateQuoteInput{
		PrincipalID: "prn_rpc_stream_client", CapabilityID: capability.ID,
		InputSummary: input, RequestedTrustMode: domain.RequestedTrustManaged,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_rpc_stream_client", CapabilityID: capability.ID,
		QuoteID: quote.ID, Input: input, IdempotencyKey: "rpc-stream-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Type != service.ResultCompleted || result.Job.State != domain.JobCompleted {
		t.Fatalf("unexpected result: type=%s state=%s failure=%s", result.Type, result.Job.State, result.Job.FailureReason)
	}

	var events []domain.JobEvent
	err = client.StreamJobEvents(ctx, tosai.StreamJobEventsRequest{JobID: result.Job.ID}, func(e domain.JobEvent) error {
		events = append(events, e)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamJobEvents over the real RPC failed: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("got %d events from the real StreamJob RPC, want at least STATE + TERMINAL", len(events))
	}
	if events[0].EventType != domain.JobEventState {
		t.Fatalf("first real-RPC event = %+v, want state", events[0])
	}
	last := events[len(events)-1]
	if last.EventType != domain.JobEventTerminal || !last.Terminal {
		t.Fatalf("last real-RPC event = %+v, want terminal", last)
	}

	// Every real-RPC event, appended in order, must be accepted by ATOS's
	// own durable journal without any rejection -- proving the adapter's
	// deliberate choice not to forward tos-protocol's current non-cumulative
	// stream_digest actually avoids the false-rejection it would otherwise
	// cause (see stream.go).
	for _, event := range events {
		if err := st.AppendJobStreamEvent(ctx, event); err != nil {
			t.Fatalf("appending a real-RPC event to the durable journal failed: %v (event=%+v)", err, event)
		}
	}
	cursor, found, err := st.JobStreamCursor(ctx, result.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || !cursor.Terminal {
		t.Fatalf("durable journal did not reach a terminal cursor after ingesting the real RPC stream: %+v found=%v", cursor, found)
	}

	// Reassembled OutputChunk bytes across the real stream must equal the
	// exact JSON the mock private Worker echoed back.
	var output []byte
	for _, event := range events {
		if event.EventType == domain.JobEventOutputChunk {
			output = append(output, event.Chunk...)
		}
	}
	encodedInput, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output, encodedInput) {
		t.Fatalf("reassembled streamed output = %s, want %s", output, encodedInput)
	}

	// The durable journal's own recomputed cumulative digest on the last
	// chunk must be the real sha256 of the full reassembled output -- this
	// is what ATOS actually relies on, since the raw RPC event's
	// stream_digest is not trustworthy (see stream.go).
	stored, err := st.JobStreamEvents(ctx, result.Job.ID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var storedLastChunkDigest string
	for _, event := range stored {
		if event.EventType == domain.JobEventOutputChunk {
			storedLastChunkDigest = event.StreamDigest
		}
	}
	sum := sha256.Sum256(output)
	want := "sha256:" + hex.EncodeToString(sum[:])
	if storedLastChunkDigest != want {
		t.Fatalf("durable journal's cumulative digest = %q, want %q", storedLastChunkDigest, want)
	}
}
