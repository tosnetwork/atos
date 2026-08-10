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
	}{
		op.ProviderID, op.CapabilityID, op.CapabilityVersion, op.Type,
		op.IdempotencyKey, op.NewExecutionSignerID,
		op.NewSignerPublicKey, op.NewSignatureAlgorithm, op.NewValidFrom.UnixMicro(), op.NewValidUntil.UnixMicro(),
		op.OldAuthorizationID, op.OldExecutionSignerID,
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

func (s *Store) LatestCompletedSignerOperationByCapability(ctx context.Context, capabilityID string) (domain.ExecutionSignerOperation, bool, error) {
	op, err := scanSignerOperation(s.pool.QueryRow(ctx, `
		SELECT `+signerOperationColumns+` FROM execution_signer_operations
		WHERE capability_id=$1 AND checkpoint='completed'
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
