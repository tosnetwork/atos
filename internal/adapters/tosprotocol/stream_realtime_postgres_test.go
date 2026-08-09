package toprotocol_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/adapters/tosprotocol"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
	"github.com/tosnetwork/tos-protocol/pkg/atosrpc"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
)

// blockingWorker wraps integrationWorker but holds Invoke open until the
// test releases it, so a Job can be observed genuinely still "working"
// (both at the ATOS level and at tos-protocol's own internal JobRecord
// level) before it completes -- a real race, not a simulated one.
type blockingWorker struct {
	*integrationWorker
	release chan struct{}
	invoked chan struct{}
}

func (w *blockingWorker) Invoke(ctx context.Context, request *connect.Request[edgev1.InvokeRequest]) (*connect.Response[edgev1.InvokeResponse], error) {
	close(w.invoked)
	select {
	case <-w.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return w.integrationWorker.Invoke(ctx, request)
}

func startBlockingPrivateWorker(t *testing.T) (socketPath string, worker *blockingWorker, stop func()) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath = filepath.Join(directory, "worker.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	worker = &blockingWorker{integrationWorker: newIntegrationWorker(), release: make(chan struct{}), invoked: make(chan struct{})}
	path, handler := edgev1connect.NewWorkerServiceHandler(worker)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := &http.Server{Handler: mux}
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			t.Errorf("blocking private Worker server: %v", serveErr)
		}
	}()
	return socketPath, worker, func() {
		_ = server.Close()
		_ = listener.Close()
		_ = os.Remove(socketPath)
	}
}

// TestStreamServiceResumesAcrossGenuineWorkingToCompletedTransition is the
// long-running-Job regression test for the P1-1 finding: EnsureIngested
// must not re-pull a Job's stream from scratch once it has already captured
// a non-terminal STATE snapshot, because a legitimate later STATE(completed)
// at the same sequence number would otherwise be rejected as content
// substitution. This exercises the real bug mechanism end to end: real
// PostgreSQL 16, the real tos-protocol StreamJob RPC and SubmitJob path, and
// a genuinely still-executing Job (via ATOS's async CreateJob path and a
// private Worker whose Invoke is held open by the test) -- not a
// pre-seeded terminal journal.
func TestStreamServiceResumesAcrossGenuineWorkingToCompletedTransition(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres + real tos-protocol long-running Job test")
	}
	ctx := context.Background()
	st, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	// Suffix every identifier that participates in idempotency/uniqueness
	// checks: this test runs against a persistent database, so a prior
	// (e.g. failed) run's leftover rows must never collide with this run's.
	suffix := time.Now().UTC().Format("20060102T150405.000000000")
	principalID := "prn_realtime_stream_" + suffix

	capabilities := service.NewCapabilityService(st)
	capability, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_realtime_stream_provider_" + suffix, Name: "Realtime Stream Echo",
		Description:         "exercises a genuine working->completed transition against the real StreamJob RPC",
		DeliveryMode:        domain.DeliveryInstant,
		InputSchema:         map[string]any{"type": "object"},
		OutputSchema:        map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged},
		Bindings: []domain.CapabilityBinding{{
			Transport: domain.AdapterTOSNative, EndpointRef: "tos-protocol:realtime-stream-test",
			EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged},
		}},
		IdempotencyKey: "register-realtime-stream-echo-" + suffix,
	})
	if err != nil {
		t.Fatal(err)
	}

	workerSocket, worker, stopWorker := startBlockingPrivateWorker(t)
	defer stopWorker()
	privateWorker, err := localrpc.NewWorkerClient(localrpc.DefaultWorkerClientConfig(workerSocket))
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
		BearerToken: "realtime-secret", Worker: privateWorker, Router: router,
		Authority: atosrpc.NewLocalAuthority("tos-local"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer protocolServer.Close()
	httpServer := httptest.NewServer(protocolServer.Handler())
	defer httpServer.Close()

	client, err := toprotocol.New(toprotocol.Config{
		BaseURL: httpServer.URL, BearerToken: "realtime-secret",
		Insecure: true, Timeout: 30 * time.Second, Store: st,
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
	streams := service.NewStreamService(st, client)

	input := map[string]any{"message": "still working, please wait"}
	quote, err := quotes.Create(ctx, service.CreateQuoteInput{
		PrincipalID: principalID, CapabilityID: capability.ID,
		InputSummary: input, RequestedTrustMode: domain.RequestedTrustManaged,
	})
	if err != nil {
		t.Fatal(err)
	}

	// CreateJob is ATOS's asynchronous path: it returns as soon as the Job
	// is durably claimed for execution (State already transitioned to
	// Working) without waiting for the background goroutine to finish.
	submitted, err := jobs.CreateJob(ctx, service.SubmitInput{
		PrincipalID: principalID, CapabilityID: capability.ID,
		QuoteID: quote.ID, Input: input, IdempotencyKey: "realtime-stream-" + suffix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if submitted.Type != service.ResultAccepted || submitted.Job.State.Terminal() {
		t.Fatalf("expected an accepted, non-terminal Job, got type=%s state=%s", submitted.Type, submitted.Job.State)
	}
	jobID := submitted.Job.ID

	// Wait for the background execution goroutine to actually reach the
	// private Worker's Invoke -- at which point tos-protocol's own
	// JobRecord is durably committed as Working (invokeDurableJob commits
	// that state before calling Invoke), so streaming against it now is
	// guaranteed to observe a real, durable non-terminal record rather than
	// racing a job that doesn't exist yet on the tos-protocol side.
	select {
	case <-worker.invoked:
	case <-time.After(5 * time.Second):
		t.Fatal("background execution never reached the private Worker's Invoke")
	}

	// First stream read: captures a genuine non-terminal STATE snapshot
	// through the real StreamJob RPC while the Job is still executing.
	firstEvents, firstCursor, err := streams.Events(ctx, jobID, principalID, 0, 0, "", 0)
	if err != nil {
		t.Fatalf("first stream read while working failed: %v", err)
	}
	if firstCursor.Terminal {
		t.Fatalf("cursor must not be terminal yet: %+v", firstCursor)
	}
	if len(firstEvents) != 1 || firstEvents[0].EventType != domain.JobEventState || firstEvents[0].State.Terminal() {
		t.Fatalf("expected exactly one non-terminal STATE event, got %+v", firstEvents)
	}

	// A second read while still working must be a safe no-op (this is the
	// "don't spam redundant STATE ingestion on every poll" half of the
	// P1-1 fix), not an error.
	againEvents, againCursor, err := streams.Events(ctx, jobID, principalID, 0, 0, "", 0)
	if err != nil {
		t.Fatalf("second stream read while still working failed: %v", err)
	}
	if len(againEvents) != 1 || againCursor.Terminal {
		t.Fatalf("re-reading while still working must not change the journal, got %+v cursor=%+v", againEvents, againCursor)
	}

	// Now let execution actually complete.
	close(worker.release)
	deadline := time.Now().Add(10 * time.Second)
	for {
		job, err := st.GetJob(ctx, jobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.State.Terminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job did not reach a terminal state after Invoke was released")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// This is the exact call that reproduced the P1-1 bug: resuming
	// ingestion after the Job legitimately transitioned from working to
	// completed must not be rejected as a sequence substitution.
	finalEvents, finalCursor, err := streams.Events(ctx, jobID, principalID, 0, 0, "", 0)
	if err != nil {
		t.Fatalf("resuming the stream after the working->completed transition failed: %v", err)
	}
	if !finalCursor.Terminal {
		t.Fatalf("cursor must be terminal after resume completes, got %+v", finalCursor)
	}
	if len(finalEvents) < 3 {
		t.Fatalf("got %d events after resume, want at least: original working STATE, completed STATE, chunk(s), TERMINAL", len(finalEvents))
	}
	// Sequences must be contiguous and strictly increasing -- no gap, no
	// duplicate, no reordering across the resume boundary.
	for i, e := range finalEvents {
		if e.Sequence != uint64(i) {
			t.Fatalf("event %d has sequence %d, journal is not contiguous: %+v", i, e.Sequence, finalEvents)
		}
	}
	if finalEvents[0].EventType != domain.JobEventState || finalEvents[0].State.Terminal() {
		t.Fatalf("first event must be the original non-terminal snapshot, got %+v", finalEvents[0])
	}
	last := finalEvents[len(finalEvents)-1]
	if last.EventType != domain.JobEventTerminal || !last.Terminal {
		t.Fatalf("last event must be TERMINAL, got %+v", last)
	}
	var output []byte
	for _, e := range finalEvents {
		if e.EventType == domain.JobEventOutputChunk {
			output = append(output, e.Chunk...)
		}
	}
	encodedInput, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != string(encodedInput) {
		t.Fatalf("reassembled output = %s, want %s", output, encodedInput)
	}
}
