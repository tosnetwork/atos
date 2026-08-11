package financial

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type repositoryDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type Repository struct {
	pool      *pgxpool.Pool
	gatewayID string
	networkID string
	now       func() time.Time
}

const (
	reconciliationLockName  = "atos-financial-global-reconciliation-v1"
	reconciliationLeaseName = "managed-financial-integrity-v1"
	sealingLockName         = "atos-financial-global-sealing-v1"
)

func NewRepository(pool *pgxpool.Pool, gatewayID, networkID string) (*Repository, error) {
	if pool == nil || !validGatewayNetworkIDs(gatewayID, networkID) {
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
	return r.lookupWith(ctx, r.pool, identity)
}

func (r *Repository) lookupWith(ctx context.Context, db repositoryDB, identity string) (Event, error) {
	return scanEvent(db.QueryRow(ctx, eventSelect, identity))
}

func (r *Repository) LookupEventID(ctx context.Context, eventID string) (Event, error) {
	return scanEvent(r.pool.QueryRow(ctx, eventColumns+` WHERE event_id=$1`, eventID))
}

// LockMutationEvent takes both the shared side of the global reconciliation
// lock and the exclusive per-event lock on one PostgreSQL session. Keeping the
// pair on one pooled connection avoids pool starvation when many replicas race
// the same event. It is held across durable-intent -> Blnk -> durable-outcome,
// so reconciliation cannot observe the ledger after submission but the ATOS
// projection before finalization.
func (r *Repository) LockMutationEvent(ctx context.Context, identity string) (*pgxpool.Conn, func(), error) {
	if identity == "" {
		return nil, nil, errors.New("financial: mutation lock identity is empty")
	}
	eventLockName := "atos-financial-event:" + identity
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		conn, err := r.pool.Acquire(ctx)
		if err != nil {
			return nil, nil, err
		}
		var acquired bool
		err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, eventLockName).Scan(&acquired)
		if err != nil {
			conn.Release()
			return nil, nil, err
		}
		if !acquired {
			conn.Release()
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-ticker.C:
			}
			continue
		}
		if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock_shared(hashtextextended($1,0))`, reconciliationLockName); err != nil {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = conn.Exec(releaseCtx, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, eventLockName)
			cancel()
			conn.Release()
			return nil, nil, err
		}
		unlock := func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = conn.Exec(releaseCtx, `SELECT pg_advisory_unlock_shared(hashtextextended($1,0))`, reconciliationLockName)
			_, _ = conn.Exec(releaseCtx, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, eventLockName)
			conn.Release()
		}
		return conn, unlock, nil
	}
}

// LockReconciliation takes the exclusive side of the same session-level lock.
// PostgreSQL releases it automatically if the worker crashes or loses its
// connection. Normal financial mutations use LockMutationEvent and may proceed
// concurrently with each other, but not through this full-snapshot audit.
func (r *Repository) LockReconciliation(ctx context.Context) (*pgxpool.Conn, func(), error) {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended($1,0))`, reconciliationLockName); err != nil {
		conn.Release()
		return nil, nil, err
	}
	return conn, func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(releaseCtx, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, reconciliationLockName)
		conn.Release()
	}, nil
}

// lockSealing serializes the complete batch create -> sign -> retain -> anchor
// state machine across API replicas. It is session-scoped so PostgreSQL
// releases it automatically when a crashed replica loses its connection. The
// dedicated pooled connection is also the exclusive database execution path
// until every external side effect and its durable outcome have completed. A
// lost lock session therefore fences the stale worker from later persistence.
func (r *Repository) lockSealing(ctx context.Context) (*pgxpool.Conn, func(), error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var conn *pgxpool.Conn
	for {
		var err error
		conn, err = r.pool.Acquire(ctx)
		if err != nil {
			return nil, nil, err
		}
		var acquired bool
		err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, sealingLockName).Scan(&acquired)
		if err != nil {
			// Acquisition may have reached PostgreSQL even if its response was
			// lost. Destroy the uncertain session instead of returning it pooled.
			hijacked := conn.Hijack()
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = hijacked.Close(closeCtx)
			closeCancel()
			return nil, nil, err
		}
		if acquired {
			break
		}
		conn.Release()
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-ticker.C:
		}
	}
	return conn, func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var unlocked bool
		unlockErr := conn.QueryRow(releaseCtx, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, sealingLockName).Scan(&unlocked)
		cancel()
		if unlockErr != nil || !unlocked {
			// Never return a possibly lock-bearing session to the pool. Closing a
			// hijacked connection also makes PostgreSQL release the session lock.
			hijacked := conn.Hijack()
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = hijacked.Close(closeCtx)
			closeCancel()
			return
		}
		conn.Release()
	}, nil
}

// ClaimReconciliation records durable worker ownership and returns the last
// successfully verified financial sequence. The caller must already hold the
// exclusive advisory lock; that makes a stale lease after a crash safe to
// replace immediately instead of delaying recovery until a timeout.
func (r *Repository) ClaimReconciliation(ctx context.Context, owner string, lease time.Duration) (int64, error) {
	return r.claimReconciliationWith(ctx, r.pool, owner, lease)
}

func (r *Repository) claimReconciliationWith(ctx context.Context, db repositoryDB, owner string, lease time.Duration) (int64, error) {
	if owner == "" || len(owner) > 128 || lease <= 0 || lease > time.Hour {
		return 0, errors.New("financial: invalid reconciliation lease")
	}
	var cursor int64
	err := db.QueryRow(ctx, `INSERT INTO financial_reconciler_leases(lease_name,owner_id,lease_until,cursor)
 VALUES($1,$2,now()+($3*interval '1 second'),0)
 ON CONFLICT(lease_name) DO UPDATE SET owner_id=EXCLUDED.owner_id,lease_until=EXCLUDED.lease_until,updated_at=now()
 RETURNING cursor`, reconciliationLeaseName, owner, lease.Seconds()).Scan(&cursor)
	return cursor, err
}

func (r *Repository) CompleteReconciliation(ctx context.Context, owner string, cursor int64) error {
	return r.completeReconciliationWith(ctx, r.pool, owner, cursor)
}

func (r *Repository) completeReconciliationWith(ctx context.Context, db repositoryDB, owner string, cursor int64) error {
	if cursor < 0 {
		return errors.New("financial: invalid reconciliation cursor")
	}
	result, err := db.Exec(ctx, `UPDATE financial_reconciler_leases
 SET cursor=GREATEST(cursor,$3),lease_until=now(),updated_at=now()
 WHERE lease_name=$1 AND owner_id=$2`, reconciliationLeaseName, owner, cursor)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New("financial: reconciliation lease ownership changed")
	}
	return nil
}

func (r *Repository) ReleaseReconciliation(ctx context.Context, owner string) error {
	return r.releaseReconciliationWith(ctx, r.pool, owner)
}

func (r *Repository) releaseReconciliationWith(ctx context.Context, db repositoryDB, owner string) error {
	_, err := db.Exec(ctx, `UPDATE financial_reconciler_leases SET lease_until=now(),updated_at=now()
 WHERE lease_name=$1 AND owner_id=$2`, reconciliationLeaseName, owner)
	return err
}

func (r *Repository) OpenIntent(ctx context.Context, request TransferRequest) (Event, error) {
	return r.openIntentWith(ctx, r.pool, request)
}

func (r *Repository) openIntentWith(ctx context.Context, db repositoryDB, request TransferRequest) (Event, error) {
	semantic, err := SemanticDigest(request)
	if err != nil {
		return Event{}, err
	}
	if existing, err := r.lookupWith(ctx, db, request.IdempotencyIdentity); err == nil {
		if existing.SemanticDigest != semantic {
			return Event{}, ErrIdempotencyConflict
		}
		if existing.State == "finalized" {
			return existing, nil
		}
		safeMode, _, safeErr := r.safeModeWith(ctx, db)
		if safeErr != nil {
			return Event{}, safeErr
		}
		if safeMode {
			return existing, ErrSafeMode
		}
		return existing, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return Event{}, err
	}
	safeMode, _, err := r.safeModeWith(ctx, db)
	if err != nil {
		return Event{}, err
	}
	if safeMode {
		return Event{}, ErrSafeMode
	}

	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var transactionSafeMode bool
	if err := tx.QueryRow(ctx, `SELECT safe_mode FROM financial_integrity_state WHERE singleton=TRUE`).Scan(&transactionSafeMode); err != nil {
		return Event{}, err
	}
	if transactionSafeMode {
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
	return r.markSubmittingWith(ctx, r.pool, identity)
}

func (r *Repository) markSubmittingWith(ctx context.Context, db repositoryDB, identity string) error {
	result, err := db.Exec(ctx, `UPDATE financial_events SET state='submitting',attempts=attempts+1,last_error='',updated_at=now()
 WHERE idempotency_identity=$1 AND state IN ('intent','submitting')`, identity)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		event, lookupErr := r.lookupWith(ctx, db, identity)
		if lookupErr == nil && event.State == "finalized" {
			return nil
		}
		return fmt.Errorf("financial: intent %q cannot be submitted", identity)
	}
	return nil
}

func (r *Repository) MarkUncertain(ctx context.Context, identity string, cause error) error {
	return r.markUncertainWith(ctx, r.pool, identity, cause)
}

func (r *Repository) markUncertainWith(ctx context.Context, db repositoryDB, identity string, cause error) error {
	message := "ledger outcome uncertain"
	if cause != nil {
		message = cause.Error()
	}
	if len(message) > 2048 {
		message = message[:2048]
	}
	_, err := db.Exec(ctx, `UPDATE financial_events SET state='submitting',last_error=$2,updated_at=now()
 WHERE idempotency_identity=$1 AND state <> 'finalized'`, identity, message)
	return err
}

func (r *Repository) MarkFinalized(ctx context.Context, identity string, ledgerResponse any) (Event, error) {
	return r.markFinalizedWith(ctx, r.pool, identity, ledgerResponse)
}

func (r *Repository) markFinalizedWith(ctx context.Context, db repositoryDB, identity string, ledgerResponse any) (Event, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
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
	return r.pendingWith(ctx, r.pool, limit)
}

func (r *Repository) pendingWith(ctx context.Context, db repositoryDB, limit int) ([]Event, error) {
	if limit < 1 || limit > 1000 {
		return nil, errors.New("financial: pending limit must be 1-1000")
	}
	rows, err := db.Query(ctx, `SELECT commitment,commitment_digest,canonical_cbor,semantic_digest,
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
	// Establish safe mode on the exclusive side of the mutation/audit lock.
	// This waits for any ledger submission already inside its critical section
	// and prevents a request that read the old flag from submitting afterward.
	db, unlock, err := r.LockReconciliation(ctx)
	if err != nil {
		return "", err
	}
	defer unlock()
	return r.enterSafeModeWith(ctx, db, classification, expected, observed)
}

func (r *Repository) enterSafeModeWith(ctx context.Context, db repositoryDB, classification string, expected, observed any) (string, error) {
	incidentID, err := stableID("finc_", "tos.atos.financial.incident-id.v1", classification+":"+r.now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return "", err
	}
	expectedJSON, _ := json.Marshal(expected)
	observedJSON, _ := json.Marshal(observed)
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `INSERT INTO financial_integrity_incidents(incident_id,classification,expected,observed) VALUES($1,$2,$3,$4)`, incidentID, classification, expectedJSON, observedJSON); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `SELECT enter_financial_safe_mode($1,$2)`, classification, incidentID); err != nil {
		return "", err
	}
	return incidentID, tx.Commit(ctx)
}

func (r *Repository) SafeMode(ctx context.Context) (bool, string, error) {
	return r.safeModeWith(ctx, r.pool)
}

func (r *Repository) safeModeWith(ctx context.Context, db repositoryDB) (bool, string, error) {
	var enabled bool
	var reason string
	err := db.QueryRow(ctx, `SELECT safe_mode,reason FROM financial_integrity_state WHERE singleton=TRUE`).Scan(&enabled, &reason)
	return enabled, reason, err
}
