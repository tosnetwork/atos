package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

const (
	streamPollInterval = 250 * time.Millisecond
	streamMaxWait      = 5 * time.Minute
)

// handleStreamJob serves the Job's durable event journal as Server-Sent
// Events, honoring next_sequence/next_offset/expected_stream_digest for
// resume. Delivery is bounded by writing straight to the response writer
// under Flush rather than buffering events in an application-level queue:
// a slow or absent consumer applies TCP backpressure to the write instead
// of growing unbounded server-side memory, and the connection's context is
// canceled the moment the client disconnects.
func (s *Server) handleStreamJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	principalID := principalFrom(r)

	fromSequence, err := parseUintQuery(r, "next_sequence")
	if err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "next_sequence must be a non-negative integer", false)
		return
	}
	expectedOffset, err := parseUintQuery(r, "next_offset")
	if err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "next_offset must be a non-negative integer", false)
		return
	}
	expectedDigest := r.URL.Query().Get("expected_stream_digest")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, domain.ErrProviderFailed, "streaming is not supported by this response writer", false)
		return
	}

	events, cursor, err := s.Streams.Events(r.Context(), id, principalID, fromSequence, expectedOffset, expectedDigest, 0)
	if err != nil {
		writeDomainErr(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	ctx, cancel := context.WithTimeout(r.Context(), streamMaxWait)
	defer cancel()

	nextSequence := fromSequence
	caughtUp := func(cursor domain.JobStreamCursor) bool {
		return cursor.Terminal && nextSequence >= cursor.NextSequence
	}
	writeBatch := func(batch []domain.JobEvent) bool {
		for _, event := range batch {
			if !writeSSEEvent(w, event) {
				return false
			}
			nextSequence = event.Sequence + 1
		}
		flusher.Flush()
		return true
	}

	if !writeBatch(events) {
		return
	}
	if caughtUp(cursor) {
		return
	}

	ticker := time.NewTicker(streamPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			more, cursor, err := s.Streams.Events(ctx, id, principalID, nextSequence, 0, "", 0)
			if err != nil {
				return
			}
			if !writeBatch(more) {
				return
			}
			if caughtUp(cursor) {
				return
			}
		}
	}
}

func writeSSEEvent(w http.ResponseWriter, event domain.JobEvent) bool {
	encoded, err := json.Marshal(event)
	if err != nil {
		return false
	}
	frame := "id: " + strconv.FormatUint(event.Sequence, 10) + "\n" +
		"event: " + string(event.EventType) + "\n" +
		"data: " + string(encoded) + "\n\n"
	_, err = w.Write([]byte(frame))
	return err == nil
}

func parseUintQuery(r *http.Request, key string) (uint64, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return 0, nil
	}
	return strconv.ParseUint(raw, 10, 64)
}
