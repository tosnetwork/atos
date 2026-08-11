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

func (s *Store) CurrentPrincipalBinding(ctx context.Context, principalID string) (domain.PrincipalIdentityBinding, bool, error) {
	var b domain.PrincipalIdentityBinding
	err := s.pool.QueryRow(ctx, `
		SELECT principal_id, agent_id, network, binding_ref, bound_at, updated_at
		FROM principal_identity_bindings WHERE principal_id=$1
	`, principalID).Scan(&b.PrincipalID, &b.AgentID, &b.Network, &b.BindingRef, &b.BoundAt, &b.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.PrincipalIdentityBinding{}, false, nil
	}
	if err != nil {
		return domain.PrincipalIdentityBinding{}, false, err
	}
	return b, true, nil
}

func (s *Store) PutPrincipalBinding(ctx context.Context, b domain.PrincipalIdentityBinding) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO principal_identity_bindings (principal_id, agent_id, network, binding_ref, bound_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (principal_id) DO UPDATE SET
			agent_id=$2, network=$3, binding_ref=$4, bound_at=$5, updated_at=$6
	`, b.PrincipalID, b.AgentID, b.Network, b.BindingRef, b.BoundAt, b.UpdatedAt)
	return err
}

func (s *Store) DeletePrincipalBinding(ctx context.Context, principalID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM principal_identity_bindings WHERE principal_id=$1`, principalID)
	return err
}

const identityBindingOperationColumns = `id, principal_id, type, checkpoint, idempotency_key, agent_id, reason_code,
	binding_ref, ref_network, created, content_hash, failure_reason, created_at, completed_at, updated_at`

// identityBindingOperationContentHash mirrors signerOperationContentHash's
// role, simplified for identity binding's single-step operations. See
// internal/store/memory/identity_binding.go's identical function.
func identityBindingOperationContentHash(op domain.IdentityBindingOperation) string {
	encoded, _ := json.Marshal(struct {
		PrincipalID    string
		Type           domain.IdentityBindingOperationType
		IdempotencyKey string
		AgentID        string
		ReasonCode     string
	}{op.PrincipalID, op.Type, op.IdempotencyKey, op.AgentID, op.ReasonCode})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func identityBindingOperationWriteArgs(op domain.IdentityBindingOperation) []any {
	return []any{
		op.ID, op.PrincipalID, string(op.Type), string(op.Checkpoint), op.IdempotencyKey, op.AgentID, op.ReasonCode,
		op.BindingRef, op.RefNetwork, op.Created, identityBindingOperationContentHash(op), op.FailureReason,
		op.CreatedAt, op.CompletedAt, op.UpdatedAt,
	}
}

const insertIdentityBindingOperationSQL = `
	INSERT INTO identity_binding_operations (` + identityBindingOperationColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
	ON CONFLICT (principal_id, idempotency_key) DO NOTHING
`

const upsertIdentityBindingOperationSQL = `
	INSERT INTO identity_binding_operations (` + identityBindingOperationColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
	ON CONFLICT (id) DO UPDATE SET
		checkpoint=$4, binding_ref=$8, ref_network=$9, created=$10, failure_reason=$12, completed_at=$14, updated_at=$15
`

func scanIdentityBindingOperation(row pgx.Row) (domain.IdentityBindingOperation, error) {
	var op domain.IdentityBindingOperation
	var opType, checkpoint, contentHash string
	if err := row.Scan(
		&op.ID, &op.PrincipalID, &opType, &checkpoint, &op.IdempotencyKey, &op.AgentID, &op.ReasonCode,
		&op.BindingRef, &op.RefNetwork, &op.Created, &contentHash, &op.FailureReason, &op.CreatedAt, &op.CompletedAt, &op.UpdatedAt,
	); err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	op.Type = domain.IdentityBindingOperationType(opType)
	op.Checkpoint = domain.IdentityBindingCheckpoint(checkpoint)
	return op, nil
}

// OpenIdentityBindingOperation mirrors OpenSignerOperation's pattern
// exactly: an advisory transaction lock serializes concurrent
// first-writers for the same (principal_id, idempotency_key) before a row
// exists to SELECT...FOR UPDATE, and a UNIQUE(principal_id,
// idempotency_key) constraint (migration 014) makes "at most one operation
// per idempotency key" a database guarantee under concurrent openers or
// multiple ATOS replicas.
func (s *Store) OpenIdentityBindingOperation(ctx context.Context, principalID string, op domain.IdentityBindingOperation) (domain.IdentityBindingOperation, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.IdentityBindingOperation{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := lockTransactionKey(ctx, tx, "identity-binding-operation", principalID, op.IdempotencyKey); err != nil {
		return domain.IdentityBindingOperation{}, false, err
	}

	existing, err := scanIdentityBindingOperation(tx.QueryRow(ctx, `
		SELECT `+identityBindingOperationColumns+` FROM identity_binding_operations
		WHERE principal_id=$1 AND idempotency_key=$2 FOR UPDATE
	`, principalID, op.IdempotencyKey))
	if err == nil {
		if identityBindingOperationContentHash(existing) != identityBindingOperationContentHash(op) {
			return domain.IdentityBindingOperation{}, false, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with different identity-binding operation content", false)
		}
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.IdentityBindingOperation{}, false, err
	}

	tag, err := tx.Exec(ctx, insertIdentityBindingOperationSQL, identityBindingOperationWriteArgs(op)...)
	if err != nil {
		return domain.IdentityBindingOperation{}, false, err
	}
	if tag.RowsAffected() == 0 {
		existing, err := scanIdentityBindingOperation(tx.QueryRow(ctx, `
			SELECT `+identityBindingOperationColumns+` FROM identity_binding_operations WHERE principal_id=$1 AND idempotency_key=$2
		`, principalID, op.IdempotencyKey))
		if err != nil {
			return domain.IdentityBindingOperation{}, false, err
		}
		return existing, false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.IdentityBindingOperation{}, false, err
	}
	return op, true, nil
}

func (s *Store) GetIdentityBindingOperation(ctx context.Context, id string) (domain.IdentityBindingOperation, error) {
	op, err := scanIdentityBindingOperation(s.pool.QueryRow(ctx, `SELECT `+identityBindingOperationColumns+` FROM identity_binding_operations WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.IdentityBindingOperation{}, store.ErrNotFound
	}
	return op, err
}

func (s *Store) IdentityBindingOperationByIdempotencyKey(ctx context.Context, principalID, key string) (domain.IdentityBindingOperation, error) {
	op, err := scanIdentityBindingOperation(s.pool.QueryRow(ctx, `
		SELECT `+identityBindingOperationColumns+` FROM identity_binding_operations WHERE principal_id=$1 AND idempotency_key=$2
	`, principalID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.IdentityBindingOperation{}, store.ErrNotFound
	}
	return op, err
}

func (s *Store) StaleIdentityBindingOperations(ctx context.Context, cutoff time.Time, limit int) ([]domain.IdentityBindingOperation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+identityBindingOperationColumns+` FROM identity_binding_operations
		WHERE checkpoint <> 'completed' AND updated_at < $1
		ORDER BY updated_at ASC, id ASC
		LIMIT $2
	`, cutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.IdentityBindingOperation
	for rows.Next() {
		op, err := scanIdentityBindingOperation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

func (s *Store) UpdateIdentityBindingOperation(ctx context.Context, id string, fn func(domain.IdentityBindingOperation, bool) (domain.IdentityBindingOperation, error)) (domain.IdentityBindingOperation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "identity-binding-operation-id", id); err != nil {
		return domain.IdentityBindingOperation{}, err
	}

	current, err := scanIdentityBindingOperation(tx.QueryRow(ctx, `SELECT `+identityBindingOperationColumns+` FROM identity_binding_operations WHERE id=$1 FOR UPDATE`, id))
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		current = domain.IdentityBindingOperation{}
		exists = false
		err = nil
	}
	if err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	next, err := fn(current, exists)
	if err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	if exists {
		if next.ID != current.ID {
			return domain.IdentityBindingOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "identity-binding operation update must not change the operation id", false)
		}
		if identityBindingOperationContentHash(current) != identityBindingOperationContentHash(next) {
			return domain.IdentityBindingOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "identity-binding operation update must not change identity fields", false)
		}
	}
	if _, err := tx.Exec(ctx, upsertIdentityBindingOperationSQL, identityBindingOperationWriteArgs(next)...); err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	return next, nil
}

func (s *Store) PutCapabilityOwnershipCommitment(ctx context.Context, c domain.CapabilityOwnershipCommitment) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "capability-ownership-commitment", c.CapabilityID, c.Version); err != nil {
		return err
	}
	var existingProvider, existingManifest, existingOwnership, existingNetwork string
	err = tx.QueryRow(ctx, `
		SELECT provider_id, manifest_commitment, ownership_commitment, network
		FROM capability_ownership_commitments WHERE capability_id=$1 AND version=$2 FOR UPDATE
	`, c.CapabilityID, c.Version).Scan(&existingProvider, &existingManifest, &existingOwnership, &existingNetwork)
	if err == nil {
		if existingProvider != c.ProviderID || existingManifest != c.ManifestCommitment ||
			existingOwnership != c.OwnershipCommitment || existingNetwork != c.Network {
			return domain.NewError(domain.ErrIdempotencyConflict, "capability version is already committed with different ownership or manifest", false)
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO capability_ownership_commitments (capability_id, version, provider_id, network, manifest_commitment, ownership_commitment, committed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
	`, c.CapabilityID, c.Version, c.ProviderID, c.Network, c.ManifestCommitment, c.OwnershipCommitment, c.CommittedAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CapabilityOwnershipCommitmentByVersion(ctx context.Context, capabilityID, version string) (domain.CapabilityOwnershipCommitment, bool, error) {
	var c domain.CapabilityOwnershipCommitment
	err := s.pool.QueryRow(ctx, `
		SELECT capability_id, version, provider_id, network, manifest_commitment, ownership_commitment, committed_at
		FROM capability_ownership_commitments WHERE capability_id=$1 AND version=$2
	`, capabilityID, version).Scan(&c.CapabilityID, &c.Version, &c.ProviderID, &c.Network, &c.ManifestCommitment, &c.OwnershipCommitment, &c.CommittedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.CapabilityOwnershipCommitment{}, false, nil
	}
	if err != nil {
		return domain.CapabilityOwnershipCommitment{}, false, err
	}
	return c, true, nil
}
