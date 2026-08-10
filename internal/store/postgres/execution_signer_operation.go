package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

const signerOperationColumns = `id, provider_id, capability_id, capability_version, type, checkpoint, idempotency_key,
	new_authorization_id, new_execution_signer_id, new_signer_public_key, new_signature_algorithm, new_valid_from, new_valid_until, new_authorization_ref,
	old_authorization_id, old_execution_signer_id, revocation_reason_code, failure_reason, content_hash, created_at, completed_at, updated_at`

// signerOperationContentHash summarizes the identity fields that must
// never change once an operation is opened -- mirrors
// certificationContentHash's role for domain.SandboxCertification.
//
// NewAuthorizationID is deliberately EXCLUDED: it is generated fresh by
// the service on every call (like op.ID, also excluded), never supplied
// by the caller, so it must not be treated as caller-identity -- including
// it made every replay of the same logical request hash differently and
// spuriously conflict, since a second call generates a new random value
// even when everything the caller actually provided is identical.
//
// NewValidFrom/NewValidUntil are hashed as UnixMicro, not time.Time
// directly -- Postgres's timestamptz column only stores microsecond
// precision, so a round-tripped record's nanosecond-precision time.Time
// differs from the original in-memory value's, which would otherwise make
// a genuine same-content replay hash differently depending on whether the
// comparison side came fresh from a caller or back from a SELECT.
//
// RevocationReasonCode is caller-supplied content for Revoke/Rotate (it is
// forwarded verbatim into the RevokeExecutionSigner request tos-protocol
// hashes into its own commitment digest) and must be part of this
// operation's identity like every other caller-supplied field: an
// idempotency-key replay that supplies a different reason code is a
// different logical request and must conflict, not silently keep whatever
// reason the first call happened to persist.
func signerOperationContentHash(op domain.ExecutionSignerOperation) string {
	encoded, _ := json.Marshal(struct {
		ProviderID, CapabilityID, CapabilityVersion   string
		Type                                          domain.SignerOperationType
		IdempotencyKey                                string
		NewExecutionSignerID                          string
		NewSignerPublicKey                            []byte
		NewSignatureAlgorithm                         string
		NewValidFromUnixMicro, NewValidUntilUnixMicro int64
		OldAuthorizationID, OldExecutionSignerID      string
		RevocationReasonCode                          string
	}{
		op.ProviderID, op.CapabilityID, op.CapabilityVersion, op.Type,
		op.IdempotencyKey, op.NewExecutionSignerID,
		op.NewSignerPublicKey, op.NewSignatureAlgorithm, op.NewValidFrom.UnixMicro(), op.NewValidUntil.UnixMicro(),
		op.OldAuthorizationID, op.OldExecutionSignerID,
		op.RevocationReasonCode,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func signerOperationWriteArgs(op domain.ExecutionSignerOperation) []any {
	var newValidFrom, newValidUntil any
	if !op.NewValidFrom.IsZero() {
		newValidFrom = op.NewValidFrom
	}
	if !op.NewValidUntil.IsZero() {
		newValidUntil = op.NewValidUntil
	}
	return []any{
		op.ID, op.ProviderID, op.CapabilityID, op.CapabilityVersion, string(op.Type), string(op.Checkpoint), op.IdempotencyKey,
		op.NewAuthorizationID, op.NewExecutionSignerID, op.NewSignerPublicKey, op.NewSignatureAlgorithm, newValidFrom, newValidUntil, op.NewAuthorizationRef,
		op.OldAuthorizationID, op.OldExecutionSignerID, op.RevocationReasonCode, op.FailureReason,
		signerOperationContentHash(op), op.CreatedAt, op.CompletedAt, op.UpdatedAt,
	}
}

const insertSignerOperationSQL = `
	INSERT INTO execution_signer_operations (` + signerOperationColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
	ON CONFLICT (provider_id, idempotency_key) DO NOTHING
`

const upsertSignerOperationSQL = `
	INSERT INTO execution_signer_operations (` + signerOperationColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
	ON CONFLICT (id) DO UPDATE SET
		checkpoint=$6, new_authorization_ref=$14, revocation_reason_code=$17, failure_reason=$18, completed_at=$21, updated_at=$22
`

func scanSignerOperation(row pgx.Row) (domain.ExecutionSignerOperation, error) {
	var op domain.ExecutionSignerOperation
	var opType, checkpoint, contentHash string
	var newValidFrom, newValidUntil *time.Time
	if err := row.Scan(
		&op.ID, &op.ProviderID, &op.CapabilityID, &op.CapabilityVersion, &opType, &checkpoint, &op.IdempotencyKey,
		&op.NewAuthorizationID, &op.NewExecutionSignerID, &op.NewSignerPublicKey, &op.NewSignatureAlgorithm, &newValidFrom, &newValidUntil, &op.NewAuthorizationRef,
		&op.OldAuthorizationID, &op.OldExecutionSignerID, &op.RevocationReasonCode, &op.FailureReason,
		&contentHash, &op.CreatedAt, &op.CompletedAt, &op.UpdatedAt,
	); err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	op.Type = domain.SignerOperationType(opType)
	op.Checkpoint = domain.SignerOperationCheckpoint(checkpoint)
	if newValidFrom != nil {
		op.NewValidFrom = *newValidFrom
	}
	if newValidUntil != nil {
		op.NewValidUntil = *newValidUntil
	}
	return op, nil
}

// OpenSignerOperation mirrors OpenCertification's pattern exactly: an
// advisory transaction lock serializes concurrent first-writers for the
// same (provider_id, idempotency_key) before a row exists to
// SELECT...FOR UPDATE, and a UNIQUE(provider_id, idempotency_key)
// constraint (migration 011) makes "at most one operation per idempotency
// key" a database guarantee under concurrent openers or two independent
// ATOS replicas.
func (s *Store) OpenSignerOperation(ctx context.Context, providerID string, op domain.ExecutionSignerOperation) (domain.ExecutionSignerOperation, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := lockTransactionKey(ctx, tx, "signer-operation", providerID, op.IdempotencyKey); err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}
	return openSignerOperationTx(ctx, tx, providerID, op)
}

// openSignerOperationTx is OpenSignerOperation's body, factored out so
// OpenSignerOperationForCapability can run it inside a transaction that
// already holds a different advisory lock (the capability-scoped one) --
// it still takes out its own "signer-operation" lock itself, exactly like
// OpenSignerOperation does, since Postgres advisory xact locks are
// independent per lock key and holding one does not exempt a caller from
// taking another.
func openSignerOperationTx(ctx context.Context, tx pgx.Tx, providerID string, op domain.ExecutionSignerOperation) (domain.ExecutionSignerOperation, bool, error) {
	existing, err := scanSignerOperation(tx.QueryRow(ctx, `
		SELECT `+signerOperationColumns+` FROM execution_signer_operations
		WHERE provider_id=$1 AND idempotency_key=$2 FOR UPDATE
	`, providerID, op.IdempotencyKey))
	if err == nil {
		if signerOperationContentHash(existing) != signerOperationContentHash(op) {
			return domain.ExecutionSignerOperation{}, false, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with different execution-signer operation content", false)
		}
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.ExecutionSignerOperation{}, false, err
	}

	tag, err := tx.Exec(ctx, insertSignerOperationSQL, signerOperationWriteArgs(op)...)
	if err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}
	if tag.RowsAffected() == 0 {
		existing, err := scanSignerOperation(tx.QueryRow(ctx, `
			SELECT `+signerOperationColumns+` FROM execution_signer_operations WHERE provider_id=$1 AND idempotency_key=$2
		`, providerID, op.IdempotencyKey))
		if err != nil {
			return domain.ExecutionSignerOperation{}, false, err
		}
		return existing, false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}
	return op, true, nil
}

// currentSignerTx derives "the currently authorized execution signer" for
// (capabilityID, capabilityVersion) exactly like
// service.ExecutionSignerService.CurrentSigner does from
// LatestCompletedSignerOperationByCapability's result -- duplicated here
// (rather than shared across the service/store package boundary) because
// OpenSignerOperationForCapability needs this same derivation available
// INSIDE the transaction that holds its advisory lock, at a snapshot
// consistent with the operation it is about to open, not as a separate
// store call the service layer could only make before or after that lock
// is held.
func currentSignerTx(ctx context.Context, tx pgx.Tx, capabilityID, capabilityVersion string) (authorizationID, executionSignerID string, found bool, err error) {
	op, err := scanSignerOperation(tx.QueryRow(ctx, `
		SELECT `+signerOperationColumns+` FROM execution_signer_operations
		WHERE capability_id=$1 AND capability_version=$2 AND checkpoint='completed'
		ORDER BY updated_at DESC, id ASC
		LIMIT 1
	`, capabilityID, capabilityVersion))
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	switch op.Type {
	case domain.SignerOperationAuthorize, domain.SignerOperationRotate:
		return op.NewAuthorizationID, op.NewExecutionSignerID, true, nil
	default: // revoke
		return "", "", false, nil
	}
}

// hasNonTerminalSignerOperationTx reports whether a non-terminal
// (checkpoint <> 'completed') signer operation already exists for
// (capabilityID, capabilityVersion) -- the "at most one in-flight signer
// mutation per capability version" invariant OpenSignerOperationForCapability
// enforces. Scoped to the transaction so it observes the same snapshot
// currentSignerTx did, under the same advisory lock.
func hasNonTerminalSignerOperationTx(ctx context.Context, tx pgx.Tx, capabilityID, capabilityVersion string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM execution_signer_operations
			WHERE capability_id=$1 AND capability_version=$2 AND checkpoint <> 'completed'
		)
	`, capabilityID, capabilityVersion).Scan(&exists)
	return exists, err
}

// OpenSignerOperationForCapability -- see the interface doc comment
// (internal/store/store.go) for why this must be one atomic sequence
// rather than two separate store calls. The capability-scoped advisory
// lock is taken out BEFORE reading the current signer and held for the
// whole transaction, including the eventual insert, exactly mirroring
// OpenSignerOperation's own lock-before-read discipline one level up.
//
// Locking the read-then-open sequence alone is NOT sufficient on its
// own: it prevents two concurrent callers from reading current-signer at
// the exact same instant, but does nothing about two callers that read it
// in QUICK SUCCESSION, before the first one's operation has reached
// Completed -- the second would still see the same stale "current"
// signer and open a second, independently-completable operation against
// it. Rejecting a new open while ANY non-terminal operation already
// exists for this capability version is what actually closes that
// window: the second caller is forced to fail and retry AFTER the first
// either completes (and it will then correctly see the new signer as
// current) or is itself recovered by the reconciler.
func (s *Store) OpenSignerOperationForCapability(
	ctx context.Context, providerID, capabilityID, capabilityVersion string,
	build func(currentAuthorizationID, currentExecutionSignerID string, found bool) (domain.ExecutionSignerOperation, error),
) (domain.ExecutionSignerOperation, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := lockTransactionKey(ctx, tx, "signer-current", capabilityID, capabilityVersion); err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}

	inFlight, err := hasNonTerminalSignerOperationTx(ctx, tx, capabilityID, capabilityVersion)
	if err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}
	if inFlight {
		return domain.ExecutionSignerOperation{}, false, domain.NewError(domain.ErrSignerOperationInProgress,
			"a signer mutation is already in progress for this capability version", true)
	}

	currentAuthorizationID, currentExecutionSignerID, found, err := currentSignerTx(ctx, tx, capabilityID, capabilityVersion)
	if err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}

	op, err := build(currentAuthorizationID, currentExecutionSignerID, found)
	if err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}

	if err := lockTransactionKey(ctx, tx, "signer-operation", providerID, op.IdempotencyKey); err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}
	return openSignerOperationTx(ctx, tx, providerID, op)
}

func (s *Store) GetSignerOperation(ctx context.Context, id string) (domain.ExecutionSignerOperation, error) {
	op, err := scanSignerOperation(s.pool.QueryRow(ctx, `SELECT `+signerOperationColumns+` FROM execution_signer_operations WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ExecutionSignerOperation{}, store.ErrNotFound
	}
	return op, err
}

func (s *Store) SignerOperationByIdempotencyKey(ctx context.Context, providerID, key string) (domain.ExecutionSignerOperation, error) {
	op, err := scanSignerOperation(s.pool.QueryRow(ctx, `
		SELECT `+signerOperationColumns+` FROM execution_signer_operations WHERE provider_id=$1 AND idempotency_key=$2
	`, providerID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ExecutionSignerOperation{}, store.ErrNotFound
	}
	return op, err
}

func (s *Store) LatestSignerOperationByCapability(ctx context.Context, capabilityID string) (domain.ExecutionSignerOperation, bool, error) {
	op, err := scanSignerOperation(s.pool.QueryRow(ctx, `
		SELECT `+signerOperationColumns+` FROM execution_signer_operations
		WHERE capability_id=$1
		ORDER BY updated_at DESC, id ASC
		LIMIT 1
	`, capabilityID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ExecutionSignerOperation{}, false, nil
	}
	if err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}
	return op, true, nil
}

func (s *Store) LatestCompletedSignerOperationByCapability(ctx context.Context, capabilityID, capabilityVersion string) (domain.ExecutionSignerOperation, bool, error) {
	op, err := scanSignerOperation(s.pool.QueryRow(ctx, `
		SELECT `+signerOperationColumns+` FROM execution_signer_operations
		WHERE capability_id=$1 AND capability_version=$2 AND checkpoint='completed'
		ORDER BY updated_at DESC, id ASC
		LIMIT 1
	`, capabilityID, capabilityVersion))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ExecutionSignerOperation{}, false, nil
	}
	if err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}
	return op, true, nil
}

func (s *Store) StaleSignerOperations(ctx context.Context, cutoff time.Time, limit int) ([]domain.ExecutionSignerOperation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+signerOperationColumns+` FROM execution_signer_operations
		WHERE checkpoint <> 'completed' AND updated_at < $1
		ORDER BY updated_at ASC, id ASC
		LIMIT $2
	`, cutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ExecutionSignerOperation
	for rows.Next() {
		op, err := scanSignerOperation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

func (s *Store) UpdateSignerOperation(ctx context.Context, id string, fn func(domain.ExecutionSignerOperation, bool) (domain.ExecutionSignerOperation, error)) (domain.ExecutionSignerOperation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "signer-operation-id", id); err != nil {
		return domain.ExecutionSignerOperation{}, err
	}

	current, err := scanSignerOperation(tx.QueryRow(ctx, `SELECT `+signerOperationColumns+` FROM execution_signer_operations WHERE id=$1 FOR UPDATE`, id))
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		current = domain.ExecutionSignerOperation{}
		exists = false
		err = nil
	}
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	next, err := fn(current, exists)
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	if exists {
		if next.ID != current.ID {
			return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "execution-signer operation update must not change the operation id", false)
		}
		if signerOperationContentHash(current) != signerOperationContentHash(next) {
			return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "execution-signer operation update must not change identity fields", false)
		}
	}
	if _, err := tx.Exec(ctx, upsertSignerOperationSQL, signerOperationWriteArgs(next)...); err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	return next, nil
}
