package postgres

import (
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// maxJobStreamChunkBytes bounds a single OutputChunk event, matching the
// tos-protocol Edge default chunk size. It is enforced both here and by a
// CHECK constraint on job_stream_events.chunk so a malformed row can never
// be written even by a future caller that skips this Go path.
const maxJobStreamChunkBytes = 256 << 10

// eventContentHash summarizes the semantically meaningful fields of an event
// so a replayed append at the same sequence can be recognized as identical
// (idempotent no-op) versus a substitution attempt (rejected).
func eventContentHash(e domain.JobEvent) string {
	encoded, _ := json.Marshal(struct {
		EventType        domain.JobEventType `json:"event_type"`
		State            domain.JobState     `json:"state"`
		Chunk            []byte              `json:"chunk"`
		Offset           uint64              `json:"offset"`
		TotalOutputBytes uint64              `json:"total_output_bytes"`
		StreamDigest     string              `json:"stream_digest"`
		Terminal         bool                `json:"terminal"`
		Usage            *domain.Usage       `json:"usage"`
		ProofStatus      *domain.ProofStatus `json:"proof_status"`
		ErrorCode        domain.ErrorCode    `json:"error_code"`
	}{
		EventType: e.EventType, State: e.State, Chunk: e.Chunk, Offset: e.Offset,
		TotalOutputBytes: e.TotalOutputBytes, StreamDigest: e.StreamDigest,
		Terminal: e.Terminal, Usage: e.Usage, ProofStatus: e.ProofStatus, ErrorCode: e.ErrorCode,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

type streamCursorRow struct {
	nextSequence   uint64
	nextOffset     uint64
	streamDigest   string
	digestState    []byte
	terminal       bool
	upstreamDigest string
	exists         bool
}

func (s *Store) AppendJobStreamEvent(ctx context.Context, event domain.JobEvent) error {
	if event.JobID == "" {
		return domain.NewError(domain.ErrValidationFailed, "job_id is required", false)
	}
	if len(event.Chunk) > maxJobStreamChunkBytes {
		return domain.NewError(domain.ErrStreamChunkTooLarge, fmt.Sprintf("chunk exceeds %d bytes", maxJobStreamChunkBytes), false)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	// A PostgreSQL advisory transaction lock (not a process-local mutex)
	// serializes appends for one Job across every ATOS replica, so the
	// cursor read-then-write below is safe under concurrent ingestion.
	if err := lockTransactionKey(ctx, tx, "job-stream", event.JobID); err != nil {
		return err
	}

	cursor, err := loadStreamCursorTx(ctx, tx, event.JobID)
	if err != nil {
		return err
	}

	if event.Sequence < cursor.nextSequence {
		// Replay of an already-durable sequence: idempotent iff identical.
		var storedHash string
		err := tx.QueryRow(ctx, `SELECT content_hash FROM job_stream_events WHERE job_id=$1 AND sequence=$2`, event.JobID, event.Sequence).Scan(&storedHash)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.NewError(domain.ErrStreamSequenceConflict, "stream sequence is behind the durable cursor but has no matching row", false)
		}
		if err != nil {
			return err
		}
		if storedHash != eventContentHash(event) {
			return domain.NewError(domain.ErrStreamSequenceConflict, "stream sequence already holds different content", false)
		}
		return tx.Commit(ctx)
	}
	if event.Sequence > cursor.nextSequence {
		return store.ErrConflict
	}

	if cursor.terminal {
		return domain.NewError(domain.ErrStreamTerminal, "job stream already recorded a terminal event", false)
	}

	// The execution provider's retained-output identity digest is validated
	// and set in this same transaction as the event it arrived with -- never
	// as a separate, independently-committed write. A crash or a rejected
	// event (e.g. a bad offset/digest below) must not be able to leave the
	// identity digest durably set without the event itself ever landing;
	// tx.Rollback on any early return above/below undoes both together.
	// Only OutputChunk events carry a trustworthy identity digest: a Job
	// observed only via STATE has no output yet, so its provider identity
	// digest is not yet stable (see docs/TOS_RPC.md's StreamJob section).
	nextUpstreamDigest := cursor.upstreamDigest
	if event.EventType == domain.JobEventOutputChunk && event.UpstreamRetainedDigest != "" {
		switch {
		case cursor.upstreamDigest == "":
			nextUpstreamDigest = event.UpstreamRetainedDigest
		case cursor.upstreamDigest != event.UpstreamRetainedDigest:
			return domain.NewError(domain.ErrProviderFailed, "execution provider's retained-output identity digest changed for an existing job stream", false)
		}
	}

	// rowOffset/rowDigest are the values stored on the event row itself and
	// mirror tos-protocol's JobEvent semantics: offset is the start of this
	// chunk, stream_digest is the cumulative digest including this chunk.
	// nextOffset/nextDigest/nextDigestState are the *cursor's* forward-looking
	// state, i.e. what the next OutputChunk event must continue from.
	rowOffset := event.Offset
	rowDigest := event.StreamDigest
	nextOffset := cursor.nextOffset
	nextDigest := cursor.streamDigest
	nextDigestState := cursor.digestState
	if event.EventType == domain.JobEventOutputChunk {
		if event.Offset != cursor.nextOffset {
			return domain.NewError(domain.ErrStreamOffsetInvalid, fmt.Sprintf("chunk offset %d does not match expected offset %d", event.Offset, cursor.nextOffset), false)
		}
		hasher := sha256.New()
		if len(cursor.digestState) > 0 {
			unmarshaler, ok := hasher.(encoding.BinaryUnmarshaler)
			if !ok {
				return errors.New("postgres: sha256 hasher does not support resumable state")
			}
			if err := unmarshaler.UnmarshalBinary(cursor.digestState); err != nil {
				return fmt.Errorf("postgres: restore stream digest state: %w", err)
			}
		}
		hasher.Write(event.Chunk)
		computedDigest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
		if event.StreamDigest != "" && event.StreamDigest != computedDigest {
			return domain.NewError(domain.ErrStreamDigestInvalid, "cumulative stream digest does not match the recomputed digest", false)
		}
		marshaler, ok := hasher.(encoding.BinaryMarshaler)
		if !ok {
			return errors.New("postgres: sha256 hasher does not support resumable state")
		}
		state, err := marshaler.MarshalBinary()
		if err != nil {
			return fmt.Errorf("postgres: persist stream digest state: %w", err)
		}
		rowDigest = computedDigest
		nextOffset = event.Offset + uint64(len(event.Chunk))
		nextDigest = computedDigest
		nextDigestState = state
	}

	hash := eventContentHash(event)
	if _, err := tx.Exec(ctx, `
		INSERT INTO job_stream_events (
			job_id, sequence, event_type, state, chunk, byte_offset,
			total_output_bytes, stream_digest, terminal, usage, proof_status,
			error_code, content_hash
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
	`, event.JobID, event.Sequence, string(event.EventType), string(event.State),
		nullableBytes(event.Chunk), rowOffset, event.TotalOutputBytes, rowDigest,
		event.Terminal, nullableJSONAny(event.Usage), nullableJSONAny(event.ProofStatus),
		string(event.ErrorCode), hash); err != nil {
		return err
	}

	terminal := cursor.terminal || event.Terminal
	if _, err := tx.Exec(ctx, `
		INSERT INTO job_stream_cursors (job_id, next_sequence, next_offset, stream_digest, digest_state, terminal, upstream_digest, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7, now())
		ON CONFLICT (job_id) DO UPDATE SET
			next_sequence=$2, next_offset=$3, stream_digest=$4, digest_state=$5, terminal=$6, upstream_digest=$7, updated_at=now()
	`, event.JobID, event.Sequence+1, nextOffset, nextDigest, nullableBytes(nextDigestState), terminal, nextUpstreamDigest); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func loadStreamCursorTx(ctx context.Context, tx pgx.Tx, jobID string) (streamCursorRow, error) {
	var row streamCursorRow
	var digestState []byte
	err := tx.QueryRow(ctx, `
		SELECT next_sequence, next_offset, stream_digest, digest_state, terminal, upstream_digest
		FROM job_stream_cursors WHERE job_id=$1 FOR UPDATE
	`, jobID).Scan(&row.nextSequence, &row.nextOffset, &row.streamDigest, &digestState, &row.terminal, &row.upstreamDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return streamCursorRow{}, nil
	}
	if err != nil {
		return streamCursorRow{}, err
	}
	row.digestState = digestState
	row.exists = true
	return row, nil
}

func nullableBytes(v []byte) any {
	if len(v) == 0 {
		return nil
	}
	return v
}

func nullableJSONAny(v any) any {
	if v == nil {
		return nil
	}
	return mustMarshal(v)
}

func (s *Store) JobStreamEvents(ctx context.Context, jobID string, fromSequence uint64, limit int) ([]domain.JobEvent, error) {
	query := `
		SELECT job_id, sequence, event_type, state, chunk, byte_offset,
		       total_output_bytes, stream_digest, terminal, usage, proof_status,
		       error_code, created_at
		FROM job_stream_events
		WHERE job_id=$1 AND sequence >= $2
		ORDER BY sequence ASC
	`
	args := []any{jobID, fromSequence}
	if limit > 0 {
		query += ` LIMIT $3`
		args = append(args, limit)
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.JobEvent
	for rows.Next() {
		var e domain.JobEvent
		var eventType, state, errorCode string
		var chunk, usage, proofStatus []byte
		if err := rows.Scan(&e.JobID, &e.Sequence, &eventType, &state, &chunk, &e.Offset,
			&e.TotalOutputBytes, &e.StreamDigest, &e.Terminal, &usage, &proofStatus,
			&errorCode, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.EventType = domain.JobEventType(eventType)
		e.State = domain.JobState(state)
		e.ErrorCode = domain.ErrorCode(errorCode)
		e.Chunk = chunk
		if len(usage) > 0 {
			var u domain.Usage
			if err := json.Unmarshal(usage, &u); err != nil {
				return nil, fmt.Errorf("postgres: decode stream event usage: %w", err)
			}
			e.Usage = &u
		}
		if len(proofStatus) > 0 {
			var p domain.ProofStatus
			if err := json.Unmarshal(proofStatus, &p); err != nil {
				return nil, fmt.Errorf("postgres: decode stream event proof status: %w", err)
			}
			e.ProofStatus = &p
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) LastJobStreamChunkBefore(ctx context.Context, jobID string, beforeSequence uint64) (domain.JobEvent, bool, error) {
	var e domain.JobEvent
	var eventType, state, errorCode string
	var chunk, usage, proofStatus []byte
	err := s.pool.QueryRow(ctx, `
		SELECT job_id, sequence, event_type, state, chunk, byte_offset,
		       total_output_bytes, stream_digest, terminal, usage, proof_status,
		       error_code, created_at
		FROM job_stream_events
		WHERE job_id=$1 AND sequence < $2 AND event_type=$3
		ORDER BY sequence DESC
		LIMIT 1
	`, jobID, beforeSequence, string(domain.JobEventOutputChunk)).Scan(
		&e.JobID, &e.Sequence, &eventType, &state, &chunk, &e.Offset,
		&e.TotalOutputBytes, &e.StreamDigest, &e.Terminal, &usage, &proofStatus,
		&errorCode, &e.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.JobEvent{}, false, nil
	}
	if err != nil {
		return domain.JobEvent{}, false, err
	}
	e.EventType = domain.JobEventType(eventType)
	e.State = domain.JobState(state)
	e.ErrorCode = domain.ErrorCode(errorCode)
	e.Chunk = chunk
	if len(usage) > 0 {
		var u domain.Usage
		if err := json.Unmarshal(usage, &u); err != nil {
			return domain.JobEvent{}, false, fmt.Errorf("postgres: decode stream event usage: %w", err)
		}
		e.Usage = &u
	}
	if len(proofStatus) > 0 {
		var p domain.ProofStatus
		if err := json.Unmarshal(proofStatus, &p); err != nil {
			return domain.JobEvent{}, false, fmt.Errorf("postgres: decode stream event proof status: %w", err)
		}
		e.ProofStatus = &p
	}
	return e, true, nil
}

func (s *Store) JobStreamCursor(ctx context.Context, jobID string) (domain.JobStreamCursor, bool, error) {
	var c domain.JobStreamCursor
	c.JobID = jobID
	err := s.pool.QueryRow(ctx, `
		SELECT next_sequence, next_offset, stream_digest, terminal, upstream_digest
		FROM job_stream_cursors WHERE job_id=$1
	`, jobID).Scan(&c.NextSequence, &c.NextOffset, &c.StreamDigest, &c.Terminal, &c.UpstreamDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.JobStreamCursor{JobID: jobID}, false, nil
	}
	if err != nil {
		return domain.JobStreamCursor{}, false, err
	}
	return c, true, nil
}

var _ store.JobStream = (*Store)(nil)
