package financial

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool      *pgxpool.Pool
	gatewayID string
	networkID string
	now       func() time.Time
}

func NewRepository(pool *pgxpool.Pool, gatewayID, networkID string) (*Repository, error) {
	if pool == nil || gatewayID == "" || networkID == "" {
		return nil, errors.New("financial: repository requires pool, gateway and network")
	}
	return &Repository{pool: pool, gatewayID: gatewayID, networkID: networkID, now: time.Now}, nil
}

func (r *Repository) WithClock(now func() time.Time) *Repository {
	if now != nil {
		r.now = now
	}
	return r
}

const eventColumns = `SELECT commitment, commitment_digest, canonical_cbor, semantic_digest,
 ledger_transaction_id, source_indicator, destination_indicator, decimals, allow_overdraft, state, attempts,
 last_error, finalized_at FROM financial_events`

const eventSelect = eventColumns + ` WHERE idempotency_identity=$1`

func scanEvent(row pgx.Row) (Event, error) {
	var event Event
	var raw []byte
	err := row.Scan(&raw, &event.Digest, &event.CanonicalCBOR, &event.SemanticDigest,
		&event.LedgerTransactionID, &event.SourceIndicator, &event.DestinationIndicator,
		&event.Decimals, &event.AllowOverdraft, &event.State, &event.Attempts, &event.LastError, &event.FinalizedAt)
	if err != nil {
		return Event{}, err
	}
	if err := json.Unmarshal(raw, &event.Commitment); err != nil {
		return Event{}, fmt.Errorf("financial: decode stored commitment: %w", err)
	}
	return event, nil
}

func (r *Repository) Lookup(ctx context.Context, identity string) (Event, error) {
	return scanEvent(r.pool.QueryRow(ctx, eventSelect, identity))
}

func (r *Repository) LookupEventID(ctx context.Context, eventID string) (Event, error) {
	return scanEvent(r.pool.QueryRow(ctx, eventColumns+` WHERE event_id=$1`, eventID))
}

// LockEvent acquires a PostgreSQL session advisory lock held across the Blnk
// lookup/submit/observe window. It serializes replicas without holding a SQL
// transaction open across network I/O.
func (r *Repository) LockEvent(ctx context.Context, identity string) (func(), error) {
	const retryInterval = 10 * time.Millisecond
	lockName := "atos-financial-event:" + identity
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	for {
		conn, err := r.pool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		var acquired bool
		err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, lockName).Scan(&acquired)
		if err != nil {
			conn.Release()
			return nil, err
		}
		if acquired {
			return func() {
				releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, _ = conn.Exec(releaseCtx, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, lockName)
				conn.Release()
			}, nil
		}
		conn.Release()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Repository) OpenIntent(ctx context.Context, request TransferRequest) (Event, error) {
	semantic, err := SemanticDigest(request)
	if err != nil {
		return Event{}, err
	}
	if existing, err := r.Lookup(ctx, request.IdempotencyIdentity); err == nil {
		if existing.SemanticDigest != semantic {
			return Event{}, ErrIdempotencyConflict
		}
		return existing, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return Event{}, err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var safeMode bool
	if err := tx.QueryRow(ctx, `SELECT safe_mode FROM financial_integrity_state WHERE singleton=TRUE`).Scan(&safeMode); err != nil {
		return Event{}, err
	}
	if safeMode {
		return Event{}, ErrSafeMode
	}
	var sequence int64
	var previous string
	if err := tx.QueryRow(ctx, `SELECT next_sequence,last_commitment FROM financial_chain_state WHERE singleton=TRUE FOR UPDATE`).Scan(&sequence, &previous); err != nil {
		return Event{}, err
	}
	if existing, err := scanEvent(tx.QueryRow(ctx, eventSelect+` FOR UPDATE`, request.IdempotencyIdentity)); err == nil {
		if existing.SemanticDigest != semantic {
			return Event{}, ErrIdempotencyConflict
		}
		return existing, tx.Commit(ctx)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return Event{}, err
	}

	occurred := request.OccurredAt.UTC()
	if occurred.IsZero() {
		occurred = r.now().UTC()
	}
	event, err := BuildCommitment(r.gatewayID, r.networkID, sequence, previous, occurred.UnixMilli(), request)
	if err != nil {
		return Event{}, err
	}
	commitmentJSON, err := json.Marshal(event.Commitment)
	if err != nil {
		return Event{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO financial_events(
 idempotency_identity,semantic_digest,event_id,event_type,gateway_id,network_id,sequence,
 previous_commitment,commitment_digest,canonical_cbor,ledger_reference,ledger_transaction_id,
 source_indicator,destination_indicator,asset,decimals,atomic_amount,allow_overdraft,commitment,created_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`,
		request.IdempotencyIdentity, event.SemanticDigest, event.EventID, event.EventType,
		event.GatewayID, event.NetworkID, event.Sequence, event.PreviousCommitment, event.Digest,
		event.CanonicalCBOR, event.LedgerReference, event.LedgerTransactionID,
		event.SourceIndicator, event.DestinationIndicator, event.Asset, event.Decimals,
		event.AtomicAmount, event.AllowOverdraft, commitmentJSON, occurred)
	if err != nil {
		return Event{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE financial_chain_state SET next_sequence=$1,last_commitment=$2,updated_at=now() WHERE singleton=TRUE`, sequence+1, event.Digest); err != nil {
		return Event{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Event{}, err
	}
	return event, nil
}

func (r *Repository) MarkSubmitting(ctx context.Context, identity string) error {
	result, err := r.pool.Exec(ctx, `UPDATE financial_events SET state='submitting',attempts=attempts+1,last_error='',updated_at=now()
 WHERE idempotency_identity=$1 AND state IN ('intent','submitting')`, identity)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		event, lookupErr := r.Lookup(ctx, identity)
		if lookupErr == nil && event.State == "finalized" {
			return nil
		}
		return fmt.Errorf("financial: intent %q cannot be submitted", identity)
	}
	return nil
}

func (r *Repository) MarkUncertain(ctx context.Context, identity string, cause error) error {
	message := "ledger outcome uncertain"
	if cause != nil {
		message = cause.Error()
	}
	if len(message) > 2048 {
		message = message[:2048]
	}
	_, err := r.pool.Exec(ctx, `UPDATE financial_events SET state='submitting',last_error=$2,updated_at=now()
 WHERE idempotency_identity=$1 AND state <> 'finalized'`, identity, message)
	return err
}

func (r *Repository) MarkFinalized(ctx context.Context, identity string, ledgerResponse any) (Event, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	event, err := scanEvent(tx.QueryRow(ctx, eventSelect+` FOR UPDATE`, identity))
	if err != nil {
		return Event{}, err
	}
	if event.State == "finalized" {
		return event, tx.Commit(ctx)
	}
	raw, err := json.Marshal(ledgerResponse)
	if err != nil {
		return Event{}, err
	}
	for index, posting := range event.Postings {
		sign := "-"
		if posting.Direction == "credit" {
			sign = "+"
		}
		_, err = tx.Exec(ctx, `INSERT INTO financial_projections(account_code,account_owner_id,asset,atomic_balance,last_sequence)
 VALUES($1,$2,$3,`+sign+`$4::numeric,$5)
 ON CONFLICT(account_code,account_owner_id,asset) DO UPDATE SET
 atomic_balance=financial_projections.atomic_balance+EXCLUDED.atomic_balance,
 last_sequence=EXCLUDED.last_sequence,updated_at=now()
 WHERE financial_projections.last_sequence < EXCLUDED.last_sequence`,
			posting.AccountCode, posting.AccountOwnerID, event.Asset, posting.AtomicAmount, event.Sequence)
		if err != nil {
			return Event{}, fmt.Errorf("financial: apply projection posting %d: %w", index, err)
		}
	}
	var finalizedAt time.Time
	if err := tx.QueryRow(ctx, `UPDATE financial_events SET state='finalized',ledger_response=$2,finalized_at=now(),last_error='',updated_at=now()
 WHERE idempotency_identity=$1 RETURNING finalized_at`, identity, raw).Scan(&finalizedAt); err != nil {
		return Event{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Event{}, err
	}
	event.State = "finalized"
	event.FinalizedAt = &finalizedAt
	return event, nil
}

func (r *Repository) Pending(ctx context.Context, limit int) ([]Event, error) {
	if limit < 1 || limit > 1000 {
		return nil, errors.New("financial: pending limit must be 1-1000")
	}
	rows, err := r.pool.Query(ctx, `SELECT commitment,commitment_digest,canonical_cbor,semantic_digest,
 ledger_transaction_id,source_indicator,destination_indicator,decimals,allow_overdraft,state,attempts,last_error,finalized_at
 FROM financial_events WHERE state IN ('intent','submitting') ORDER BY sequence LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *Repository) EnterSafeMode(ctx context.Context, classification string, expected, observed any) (string, error) {
	incidentID, err := stableID("finc_", "tos.atos.financial.incident-id.v1", classification+":"+r.now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return "", err
	}
	expectedJSON, _ := json.Marshal(expected)
	observedJSON, _ := json.Marshal(observed)
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `INSERT INTO financial_integrity_incidents(incident_id,classification,expected,observed) VALUES($1,$2,$3,$4)`, incidentID, classification, expectedJSON, observedJSON); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE financial_integrity_state SET safe_mode=TRUE,reason=$1,incident_id=$2,entered_at=COALESCE(entered_at,now()),updated_at=now() WHERE singleton=TRUE`, classification, incidentID); err != nil {
		return "", err
	}
	return incidentID, tx.Commit(ctx)
}

func (r *Repository) SafeMode(ctx context.Context) (bool, string, error) {
	var enabled bool
	var reason string
	err := r.pool.QueryRow(ctx, `SELECT safe_mode,reason FROM financial_integrity_state WHERE singleton=TRUE`).Scan(&enabled, &reason)
	return enabled, reason, err
}
