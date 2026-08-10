package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

const healthCheckColumns = `capability_id, capability_version, transport, endpoint_ref, status, latency_ms, failure_reason, checked_at`

const upsertHealthCheckSQL = `
	INSERT INTO provider_health_checks (` + healthCheckColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	ON CONFLICT (capability_id, capability_version, transport) DO UPDATE SET
		endpoint_ref=$4, status=$5, latency_ms=$6, failure_reason=$7, checked_at=$8
`

func (s *Store) PutHealthCheck(ctx context.Context, check domain.AdapterHealthCheck) error {
	_, err := s.pool.Exec(ctx, upsertHealthCheckSQL,
		check.CapabilityID, check.CapabilityVersion, string(check.Transport), check.EndpointRef,
		string(check.Status), check.LatencyMS, check.FailureReason, check.CheckedAt,
	)
	return err
}

func scanHealthCheck(row pgx.Row) (domain.AdapterHealthCheck, error) {
	var check domain.AdapterHealthCheck
	var transport, status string
	if err := row.Scan(&check.CapabilityID, &check.CapabilityVersion, &transport, &check.EndpointRef, &status, &check.LatencyMS, &check.FailureReason, &check.CheckedAt); err != nil {
		return domain.AdapterHealthCheck{}, err
	}
	check.Transport = domain.EndpointAdapterType(transport)
	check.Status = domain.AdapterHealthStatus(status)
	return check, nil
}

func (s *Store) HealthCheck(ctx context.Context, capabilityID, capabilityVersion string, transport domain.EndpointAdapterType) (domain.AdapterHealthCheck, bool, error) {
	check, err := scanHealthCheck(s.pool.QueryRow(ctx, `
		SELECT `+healthCheckColumns+` FROM provider_health_checks
		WHERE capability_id=$1 AND capability_version=$2 AND transport=$3
	`, capabilityID, capabilityVersion, string(transport)))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AdapterHealthCheck{}, false, nil
	}
	if err != nil {
		return domain.AdapterHealthCheck{}, false, err
	}
	return check, true, nil
}

const certificationColumns = `id, provider_id, capability_id, capability_version, transport, endpoint_ref, status, idempotency_key, failure_reason, evidence, content_hash, created_at, completed_at, updated_at`

// certificationContentHash summarizes the identity fields that must never
// change once a certification is opened -- mirrors disputeContentHash's
// role for domain.Dispute.
func certificationContentHash(c domain.SandboxCertification) string {
	encoded, _ := json.Marshal(struct {
		ProviderID, CapabilityID, CapabilityVersion string
		Transport                                   domain.EndpointAdapterType
		EndpointRef, IdempotencyKey                 string
	}{
		c.ProviderID, c.CapabilityID, c.CapabilityVersion, c.Transport, c.EndpointRef, c.IdempotencyKey,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func certificationWriteArgs(c domain.SandboxCertification) []any {
	return []any{
		c.ID, c.ProviderID, c.CapabilityID, c.CapabilityVersion, string(c.Transport), c.EndpointRef,
		string(c.Status), c.IdempotencyKey, c.FailureReason, mustMarshal(c.Evidence),
		certificationContentHash(c), c.CreatedAt, c.CompletedAt, c.UpdatedAt,
	}
}

const insertCertificationSQL = `
	INSERT INTO sandbox_certifications (` + certificationColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	ON CONFLICT (provider_id, idempotency_key) DO NOTHING
`

const upsertCertificationSQL = `
	INSERT INTO sandbox_certifications (` + certificationColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	ON CONFLICT (id) DO UPDATE SET
		status=$7, failure_reason=$9, evidence=$10, completed_at=$13, updated_at=$14
`

func scanCertification(row pgx.Row) (domain.SandboxCertification, error) {
	var c domain.SandboxCertification
	var transport, status, contentHash string
	var evidence []byte
	if err := row.Scan(
		&c.ID, &c.ProviderID, &c.CapabilityID, &c.CapabilityVersion, &transport, &c.EndpointRef,
		&status, &c.IdempotencyKey, &c.FailureReason, &evidence, &contentHash, &c.CreatedAt, &c.CompletedAt, &c.UpdatedAt,
	); err != nil {
		return domain.SandboxCertification{}, err
	}
	c.Transport = domain.EndpointAdapterType(transport)
	c.Status = domain.CertificationStatus(status)
	_ = json.Unmarshal(evidence, &c.Evidence)
	return c, nil
}

// OpenCertification serializes concurrent first-writers for the same
// (provider_id, idempotency_key) via an advisory transaction lock (a row
// cannot be SELECT...FOR UPDATE locked before it exists), mirroring
// OpenDispute's own pattern exactly. A UNIQUE(provider_id, idempotency_key)
// constraint (migration 010) is what makes "at most one certification per
// idempotency key" a database guarantee under concurrent openers or two
// independent ATOS replicas, not a service-layer race.
func (s *Store) OpenCertification(ctx context.Context, providerID string, cert domain.SandboxCertification) (domain.SandboxCertification, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.SandboxCertification{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := lockTransactionKey(ctx, tx, "certification", providerID, cert.IdempotencyKey); err != nil {
		return domain.SandboxCertification{}, false, err
	}

	existing, err := scanCertification(tx.QueryRow(ctx, `
		SELECT `+certificationColumns+` FROM sandbox_certifications
		WHERE provider_id=$1 AND idempotency_key=$2 FOR UPDATE
	`, providerID, cert.IdempotencyKey))
	if err == nil {
		if certificationContentHash(existing) != certificationContentHash(cert) {
			return domain.SandboxCertification{}, false, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with different certification content", false)
		}
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.SandboxCertification{}, false, err
	}

	tag, err := tx.Exec(ctx, insertCertificationSQL, certificationWriteArgs(cert)...)
	if err != nil {
		return domain.SandboxCertification{}, false, err
	}
	if tag.RowsAffected() == 0 {
		existing, err := scanCertification(tx.QueryRow(ctx, `
			SELECT `+certificationColumns+` FROM sandbox_certifications WHERE provider_id=$1 AND idempotency_key=$2
		`, providerID, cert.IdempotencyKey))
		if err != nil {
			return domain.SandboxCertification{}, false, err
		}
		return existing, false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.SandboxCertification{}, false, err
	}
	return cert, true, nil
}

func (s *Store) GetCertification(ctx context.Context, id string) (domain.SandboxCertification, error) {
	c, err := scanCertification(s.pool.QueryRow(ctx, `SELECT `+certificationColumns+` FROM sandbox_certifications WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.SandboxCertification{}, store.ErrNotFound
	}
	return c, err
}

func (s *Store) CertificationByIdempotencyKey(ctx context.Context, providerID, key string) (domain.SandboxCertification, error) {
	c, err := scanCertification(s.pool.QueryRow(ctx, `
		SELECT `+certificationColumns+` FROM sandbox_certifications WHERE provider_id=$1 AND idempotency_key=$2
	`, providerID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.SandboxCertification{}, store.ErrNotFound
	}
	return c, err
}

func (s *Store) CertificationsByCapability(ctx context.Context, capabilityID string) ([]domain.SandboxCertification, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+certificationColumns+` FROM sandbox_certifications
		WHERE capability_id=$1
		ORDER BY created_at DESC, id ASC
	`, capabilityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.SandboxCertification
	for rows.Next() {
		c, err := scanCertification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) UpdateCertification(ctx context.Context, id string, fn func(domain.SandboxCertification, bool) (domain.SandboxCertification, error)) (domain.SandboxCertification, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.SandboxCertification{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "certification-id", id); err != nil {
		return domain.SandboxCertification{}, err
	}

	current, err := scanCertification(tx.QueryRow(ctx, `SELECT `+certificationColumns+` FROM sandbox_certifications WHERE id=$1 FOR UPDATE`, id))
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		current = domain.SandboxCertification{}
		exists = false
		err = nil
	}
	if err != nil {
		return domain.SandboxCertification{}, err
	}
	next, err := fn(current, exists)
	if err != nil {
		return domain.SandboxCertification{}, err
	}
	if exists {
		if next.ID != current.ID {
			return domain.SandboxCertification{}, domain.NewError(domain.ErrIdempotencyConflict, "certification update must not change the certification id", false)
		}
		if certificationContentHash(current) != certificationContentHash(next) {
			return domain.SandboxCertification{}, domain.NewError(domain.ErrIdempotencyConflict, "certification update must not change identity fields", false)
		}
	}
	if _, err := tx.Exec(ctx, upsertCertificationSQL, certificationWriteArgs(next)...); err != nil {
		return domain.SandboxCertification{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.SandboxCertification{}, err
	}
	return next, nil
}
