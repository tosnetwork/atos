package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

// streamTestChunks are three fixed-size synthetic OutputChunk events (STATE
// -> chunk*3 -> TERMINAL) used to exercise the durable journal and its
// resume cursor without having to drive multi-hundred-KB payloads through
// the full Quote/escrow economic pipeline just to get more than one chunk.
func streamTestEvents(jobID string) []domain.JobEvent {
	chunk := func(n int) []byte { return bytes.Repeat([]byte{byte('A' + n)}, 5000) }
	events := []domain.JobEvent{{JobID: jobID, Sequence: 0, EventType: domain.JobEventState, State: domain.JobWorking}}
	cumulative := []byte(nil)
	offset := uint64(0)
	for i := 0; i < 3; i++ {
		c := chunk(i)
		h := sha256.New()
		h.Write(cumulative)
		h.Write(c)
		digest := "sha256:" + hex.EncodeToString(h.Sum(nil))
		cumulative = append(cumulative, c...)
		events = append(events, domain.JobEvent{
			JobID: jobID, Sequence: uint64(i + 1), EventType: domain.JobEventOutputChunk,
			State: domain.JobWorking, Chunk: c, Offset: offset,
			TotalOutputBytes: offset + uint64(len(c)), StreamDigest: digest,
		})
		offset += uint64(len(c))
	}
	events = append(events, domain.JobEvent{JobID: jobID, Sequence: 4, EventType: domain.JobEventTerminal, State: domain.JobCompleted, Terminal: true})
	return events
}

// sseFrame is one parsed "id/event/data" Server-Sent Events frame.
type sseFrame struct {
	ID   string
	Kind string
	Data []byte
}

func parseSSE(t *testing.T, body []byte) []sseFrame {
	t.Helper()
	var frames []sseFrame
	for _, raw := range strings.Split(strings.TrimSpace(string(body)), "\n\n") {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		var frame sseFrame
		for _, line := range strings.Split(raw, "\n") {
			switch {
			case strings.HasPrefix(line, "id: "):
				frame.ID = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				frame.Kind = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				frame.Data = []byte(strings.TrimPrefix(line, "data: "))
			}
		}
		frames = append(frames, frame)
	}
	return frames
}

type streamTestFixture struct {
	server *Server
	http   *httptest.Server
	client *http.Client
	store  *postgres.Store
}

func newStreamTestFixture(t *testing.T, databaseURL string) *streamTestFixture {
	t.Helper()
	authorization, err := auth.Open(auth.Config{PollInterval: time.Second, AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authorization.Close() })
	return newStreamTestFixtureWithAuth(t, databaseURL, authorization)
}

// newStreamTestFixtureWithAuth builds an independent Store/JobService/
// StreamService/Server/httptest.Server against databaseURL but reuses the
// given auth.Service, standing in for a restarted ATOS process: the
// application layer is rebuilt from scratch, but identity (backed in
// production by durable auth persistence, see cmd/api's ATOS_AUTH_STATE_PATH)
// and the PostgreSQL database both survive the restart.
func newStreamTestFixtureWithAuth(t *testing.T, databaseURL string, authorization *auth.Service) *streamTestFixture {
	t.Helper()
	st, err := postgres.Open(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	provider := tosaimock.New()
	core := toscoremock.New(st)
	accounts := service.NewAccountService(st)
	jobs := service.NewJobService(st, provider, core, accounts)
	streams := service.NewStreamService(st, provider)

	server := &Server{
		Auth: authorization, Jobs: jobs, Streams: streams, Accounts: accounts,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	httpServer := httptest.NewServer(server.Mux())
	t.Cleanup(httpServer.Close)
	server.PublicBaseURL = httpServer.URL

	return &streamTestFixture{server: server, http: httpServer, client: httpServer.Client(), store: st}
}

// authorizePrincipal drives the AutoApprove device flow to mint a bearer
// token scoped to read Job streams.
func (f *streamTestFixture) authorizePrincipal(t *testing.T, suffix string) (token, principalID string) {
	t.Helper()
	start := phase01Request(t, f.client, http.MethodPost, f.http.URL+"/v1/auth/device", "", map[string]any{
		"client_type": "codex", "client_name": "stream test " + suffix,
		"requested_scopes": []string{"jobs:read"},
	}, nil)
	if start.Status != http.StatusOK {
		t.Fatalf("device start = %d %s", start.Status, start.Body)
	}
	grant := phase01Decode[struct {
		DeviceCode string `json:"device_code"`
	}](t, start)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		tokenResponse := phase01Request(t, f.client, http.MethodPost, f.http.URL+"/v1/auth/device/token", "", map[string]any{"device_code": grant.DeviceCode}, nil)
		if tokenResponse.Status == http.StatusOK {
			tokens := phase01Decode[struct {
				AccessToken string `json:"access_token"`
				PrincipalID string `json:"principal_id"`
			}](t, tokenResponse)
			return tokens.AccessToken, tokens.PrincipalID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("device authorization did not complete (AutoApprove)")
	return "", ""
}

// seedTerminalJob creates a minimal completed Job owned by principalID and
// seeds its durable stream journal directly through the store, ending in a
// Terminal event. Because the cursor is already terminal, StreamService's
// EnsureIngested will not attempt to re-pull from the mock provider, so this
// synthetic multi-chunk journal is exactly what a client reads back.
func (f *streamTestFixture) seedTerminalJob(t *testing.T, principalID string) string {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	jobID := "job_stream_http_" + randSuffix()
	job := domain.Job{
		ID: jobID, CapabilityID: "cap_stream_test", QuoteID: "q_stream_test",
		PrincipalID: principalID, TrustMode: domain.TrustModeManaged,
		State: domain.JobCompleted, Input: map[string]any{}, Artifacts: []domain.Artifact{},
		CreatedAt: now, UpdatedAt: now, CompletedAt: &now,
	}
	if err := f.store.PutJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	for _, event := range streamTestEvents(jobID) {
		if err := f.store.AppendJobStreamEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	return jobID
}

func (f *streamTestFixture) streamURL(jobID, query string) string {
	url := f.http.URL + "/v1/jobs/" + jobID + "/stream"
	if query != "" {
		url += "?" + query
	}
	return url
}

// TestJobStreamHTTPFullReadThenResumeAfterDisconnect proves a client that
// stops reading partway through a stream (simulating a dropped connection)
// can resume from its last acknowledged cursor and receive exactly the
// remaining events -- no gap, no duplicate, no misordering -- and that the
// reassembled output is byte-identical to a full, uninterrupted read.
func TestJobStreamHTTPFullReadThenResumeAfterDisconnect(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres stream HTTP acceptance test")
	}
	fixture := newStreamTestFixture(t, databaseURL)
	token, principalID := fixture.authorizePrincipal(t, "disconnect")
	jobID := fixture.seedTerminalJob(t, principalID)

	full := phase01Request(t, fixture.client, http.MethodGet, fixture.streamURL(jobID, ""), token, nil, nil)
	if full.Status != http.StatusOK {
		t.Fatalf("full stream read = %d %s", full.Status, full.Body)
	}
	fullFrames := parseSSE(t, full.Body)
	if len(fullFrames) != 5 {
		t.Fatalf("got %d SSE frames, want 5", len(fullFrames))
	}

	// Simulate a disconnect after the client has durably acknowledged the
	// first two events (STATE + first chunk): resume from sequence 2 using
	// the cursor derived from the last event it actually saw.
	var chunk1 domain.JobEvent
	if err := json.Unmarshal(fullFrames[1].Data, &chunk1); err != nil {
		t.Fatal(err)
	}
	resumeOffset := strconv.FormatUint(chunk1.Offset+uint64(len(chunk1.Chunk)), 10)
	resumeQuery := "next_sequence=2&next_offset=" + resumeOffset + "&expected_stream_digest=" + chunk1.StreamDigest

	resumed := phase01Request(t, fixture.client, http.MethodGet, fixture.streamURL(jobID, resumeQuery), token, nil, nil)
	if resumed.Status != http.StatusOK {
		t.Fatalf("resumed stream read = %d %s", resumed.Status, resumed.Body)
	}
	resumedFrames := parseSSE(t, resumed.Body)
	if len(resumedFrames) != 3 {
		t.Fatalf("got %d resumed SSE frames, want 3 (sequences 2,3,4)", len(resumedFrames))
	}
	for i, want := range []string{"2", "3", "4"} {
		if resumedFrames[i].ID != want {
			t.Fatalf("resumed frame %d has id %q, want %q", i, resumedFrames[i].ID, want)
		}
	}

	// Reassembling full-read chunks and resumed chunks over the same range
	// must be byte-identical -- resume must not have skipped, repeated, or
	// reordered any output bytes.
	fullOutput := reassembleOutput(t, fullFrames[1:4])
	resumedOutput := reassembleOutput(t, resumedFrames[0:2])
	if !bytes.Equal(fullOutput[5000:], resumedOutput) {
		t.Fatalf("resumed output diverges from the full read at the resume point")
	}
}

// TestJobStreamHTTPResumeCursorSubstitutionRejected proves the server
// detects a client (or attacker) claiming a fabricated resume cursor rather
// than trusting client-supplied next_offset/expected_stream_digest values.
func TestJobStreamHTTPResumeCursorSubstitutionRejected(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres stream HTTP acceptance test")
	}
	fixture := newStreamTestFixture(t, databaseURL)
	token, principalID := fixture.authorizePrincipal(t, "cursor-substitution")
	jobID := fixture.seedTerminalJob(t, principalID)

	bogusDigest := "sha256:" + strings.Repeat("00", 32)
	resp := phase01Request(t, fixture.client, http.MethodGet, fixture.streamURL(jobID, "next_sequence=2&next_offset=5000&expected_stream_digest="+bogusDigest), token, nil, nil)
	if resp.Status != http.StatusBadRequest {
		t.Fatalf("fabricated resume cursor = %d %s, want 400", resp.Status, resp.Body)
	}
	if !bytes.Contains(resp.Body, []byte("stream_cursor_mismatch")) {
		t.Fatalf("expected stream_cursor_mismatch error, got: %s", resp.Body)
	}
}

// TestJobStreamHTTPProcessRestartResumes proves a resume request served by a
// brand-new Server/Store (standing in for a restarted ATOS process) returns
// exactly the same events as the original process would have -- the durable
// PostgreSQL cursor, not any in-process state, is what makes resume correct.
func TestJobStreamHTTPProcessRestartResumes(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres stream HTTP acceptance test")
	}
	authorization, err := auth.Open(auth.Config{PollInterval: time.Second, AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authorization.Close() })

	first := newStreamTestFixtureWithAuth(t, databaseURL, authorization)
	token, principalID := first.authorizePrincipal(t, "restart")
	jobID := first.seedTerminalJob(t, principalID)

	// A brand-new fixture opens its own Store/Server/StreamService against
	// the same database and the same (durable, in production) identity
	// service -- a restarted process, not a shared in-process handle.
	second := newStreamTestFixtureWithAuth(t, databaseURL, authorization)

	offset, digest := chunkStateAt(t, first, jobID, 3)
	resp := second.callStream(t, jobID, token, "next_sequence=3&next_offset="+strconv.FormatUint(offset, 10)+"&expected_stream_digest="+digest)
	if resp.Status != http.StatusOK {
		t.Fatalf("post-restart resume = %d %s", resp.Status, resp.Body)
	}
	frames := parseSSE(t, resp.Body)
	if len(frames) != 2 || frames[0].ID != "3" || frames[1].ID != "4" {
		t.Fatalf("post-restart resume returned %d frames %+v, want sequences 3 and 4", len(frames), frames)
	}
}

// TestJobStreamHTTPConcurrentConsumers proves multiple simultaneous readers
// of the same Job's stream each receive the complete, correct event
// sequence without interfering with one another.
func TestJobStreamHTTPConcurrentConsumers(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres stream HTTP acceptance test")
	}
	fixture := newStreamTestFixture(t, databaseURL)
	token, principalID := fixture.authorizePrincipal(t, "concurrent")
	jobID := fixture.seedTerminalJob(t, principalID)

	const readers = 10
	var wg sync.WaitGroup
	results := make([]phase01HTTPResponse, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = phase01Request(t, fixture.client, http.MethodGet, fixture.streamURL(jobID, ""), token, nil, nil)
		}(i)
	}
	wg.Wait()

	for i, result := range results {
		if result.Status != http.StatusOK {
			t.Fatalf("reader %d: status = %d %s", i, result.Status, result.Body)
		}
		frames := parseSSE(t, result.Body)
		if len(frames) != 5 {
			t.Fatalf("reader %d: got %d frames, want 5", i, len(frames))
		}
		for j, want := range []string{"0", "1", "2", "3", "4"} {
			if frames[j].ID != want {
				t.Fatalf("reader %d frame %d has id %q, want %q", i, j, frames[j].ID, want)
			}
		}
	}
}

func (f *streamTestFixture) callStream(t *testing.T, jobID, token, query string) phase01HTTPResponse {
	t.Helper()
	return phase01Request(t, f.client, http.MethodGet, f.streamURL(jobID, query), token, nil, nil)
}

// chunkStateAt reads the durable (offset, stream_digest) recorded for a
// job's OutputChunk event at exactly sequence-1 -- the cumulative state a
// resume request at `sequence` must continue from -- directly through the
// store.
func chunkStateAt(t *testing.T, f *streamTestFixture, jobID string, sequence uint64) (uint64, string) {
	t.Helper()
	events, err := f.store.JobStreamEvents(context.Background(), jobID, sequence-1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatalf("no durable event at sequence %d", sequence-1)
	}
	event := events[0]
	return event.Offset + uint64(len(event.Chunk)), event.StreamDigest
}

func reassembleOutput(t *testing.T, frames []sseFrame) []byte {
	t.Helper()
	var out []byte
	for _, frame := range frames {
		var event domain.JobEvent
		if err := json.Unmarshal(frame.Data, &event); err != nil {
			t.Fatal(err)
		}
		out = append(out, event.Chunk...)
	}
	return out
}

var streamTestSuffix int64

// randSuffix returns a unique per-call suffix for durable test row IDs,
// mirroring internal/store/postgres's test helper of the same purpose
// (unexported and package-local, since Postgres data outlives the process).
func randSuffix() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10) + "_" + strconv.FormatInt(atomic.AddInt64(&streamTestSuffix, 1), 10)
}
