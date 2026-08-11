package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-webauthn/webauthn/webauthn"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/observability"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestPasskeyFinish_SlowBodyIsCutOffThroughRealMiddlewareChain is a
// regression test for a real P1 that a prior fix (http.MaxBytesReader +
// http.ResponseController.SetReadDeadline) turned out not to actually
// close: every earlier passkey HTTP test called server.Mux().ServeHTTP
// directly against an httptest.ResponseRecorder, which never goes through
// observability.Middleware -- the wrapper cmd/api/main.go actually puts in
// front of the whole mux in production. statusRecorder (that middleware's
// ResponseWriter wrapper) did not implement Unwrap(), so
// ResponseController could not see past it to the real deadline-capable
// writer, and SetReadDeadline silently failed with ErrNotSupported on
// every real request. This test is the only kind that can catch that: a
// genuine httptest.NewServer (a real TCP listener) with the exact same
// Middleware(logger, mux) wrapping production uses, fed a deliberately
// slow-drip body and asserted to be cut off near the (test-shrunk)
// deadline -- not hang indefinitely, and not fail suspiciously fast either.
func TestPasskeyFinish_SlowBodyIsCutOffThroughRealMiddlewareChain(t *testing.T) {
	original := passkeyFinishReadDeadline
	passkeyFinishReadDeadline = 300 * time.Millisecond
	t.Cleanup(func() { passkeyFinishReadDeadline = original })

	st := memory.New()
	authorization, err := auth.Open(auth.Config{})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := webauthn.New(&webauthn.Config{RPID: passkeyHTTPTestRPID, RPDisplayName: "Test ATOS", RPOrigins: []string{passkeyHTTPTestOrigin}})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	restServer := &Server{Auth: authorization, Passkeys: service.NewPasskeyService(st, instance, authorization), Logger: logger}

	// The exact same wrapping cmd/api/main.go applies in production --
	// this is the whole point of the test.
	wrapped := observability.Middleware(logger, restServer.Mux())
	httpServer := httptest.NewServer(wrapped)
	t.Cleanup(httpServer.Close)

	beginResp, err := http.Post(httpServer.URL+"/v1/auth/passkey/register/begin", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer beginResp.Body.Close()
	var begin ceremonyBeginResponse
	if err := json.NewDecoder(beginResp.Body).Decode(&begin); err != nil {
		t.Fatalf("decode register/begin response: %v", err)
	}

	// A body that trickles in far slower than the (test-shrunk) deadline,
	// and never finishes -- io.Pipe blocks the Read side until Write
	// supplies more data, exactly modeling a deliberately slow client.
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer pipeWriter.Close()
		for i := 0; i < 5; i++ {
			if _, err := pipeWriter.Write([]byte("x")); err != nil {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/auth/passkey/register/finish/"+begin.CeremonyID, pipeReader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, doErr := http.DefaultClient.Do(req)
	elapsed := time.Since(start)
	if resp != nil {
		defer resp.Body.Close()
	}

	// The client observes either a transport-level error (connection
	// reset/closed once the server's read deadline fires) or a completed
	// response the server sent after giving up reading the body -- either
	// way, this MUST happen close to the deadline, never after waiting out
	// all 5 slow writes (which would take >=1s) and never suspiciously
	// instantly (which would mean the deadline fired immediately/wrongly
	// or something else short-circuited before ever exercising it).
	if elapsed < 250*time.Millisecond {
		t.Fatalf("request ended after %v, suspiciously before the %v deadline even elapsed", elapsed, passkeyFinishReadDeadline)
	}
	if elapsed > 900*time.Millisecond {
		t.Fatalf("request ended after %v -- the slow body was not cut off near the %v deadline (it looks like all 5 slow writes completed instead)", elapsed, passkeyFinishReadDeadline)
	}
	if doErr == nil && resp.StatusCode < 400 {
		t.Fatalf("expected the slow-body request to fail or error, got status %d", resp.StatusCode)
	}
}
