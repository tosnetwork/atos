// Package observability implements the W3C trace context propagation
// required by ~/atos-spec/docs/ARCHITECTURE.md's "Observability" section:
// every REST/MCP/A2A request (they share one HTTP mux — see
// cmd/api/main.go) gets a trace_id and request_id, echoed back to the
// caller and attached to every structured log line, so an operator can
// correlate a client-visible failure back to the exact request that
// caused it.
package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

type contextKey string

const (
	traceIDKey   contextKey = "atos_trace_id"
	requestIDKey contextKey = "atos_request_id"
)

// TraceID returns the request's trace_id, or "" if the context wasn't
// produced by Middleware.
func TraceID(ctx context.Context) string {
	v, _ := ctx.Value(traceIDKey).(string)
	return v
}

// RequestID returns the request's request_id.
func RequestID(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}

// WithTrace injects trace/request IDs into a context — mainly useful for
// tests that need to exercise TraceID/RequestID without going through
// Middleware.
func WithTrace(ctx context.Context, traceID, requestID string) context.Context {
	ctx = context.WithValue(ctx, traceIDKey, traceID)
	ctx = context.WithValue(ctx, requestIDKey, requestID)
	return ctx
}

// traceIDFromTraceparent extracts the trace-id field from a W3C
// traceparent header ("00-<32 hex trace-id>-<16 hex parent-id>-<2 hex
// flags>"), returning "" if the header is absent or malformed rather than
// guessing — a malformed incoming trace should start a new one, not
// silently propagate garbage.
func traceIDFromTraceparent(header string) string {
	parts := strings.Split(header, "-")
	if len(parts) != 4 || len(parts[1]) != 32 {
		return ""
	}
	if _, err := hex.DecodeString(parts[1]); err != nil {
		return ""
	}
	return parts[1]
}

func newHexID(bytes int) string {
	b := make([]byte, bytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// statusRecorder captures the response status for logging, since
// http.ResponseWriter doesn't expose what was already written.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Middleware wraps the whole HTTP mux (REST + MCP + A2A all multiplex
// through one *http.ServeMux in cmd/api/main.go, so wrapping it once
// covers all three transports docs/ARCHITECTURE.md names). It reads an
// inbound W3C traceparent if present, otherwise starts a new trace,
// mints a fresh request_id per call regardless (a request_id identifies
// this one hop, not the end-to-end trace), injects both into the request
// context, echoes them as response headers, and logs one structured line
// per request correlated by both IDs.
func Middleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := traceIDFromTraceparent(r.Header.Get("Traceparent"))
		if traceID == "" {
			traceID = newHexID(16)
		}
		requestID := uuid.NewString()
		spanID := newHexID(8)

		ctx := WithTrace(r.Context(), traceID, requestID)

		w.Header().Set("Traceparent", "00-"+traceID+"-"+spanID+"-01")
		w.Header().Set("X-Request-Id", requestID)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r.WithContext(ctx))

		logger.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"trace_id", traceID,
			"request_id", requestID,
		)
	})
}
