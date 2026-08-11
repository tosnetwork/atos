// Package postgres implements store.Store with indexed relational columns for
// gateway queries plus a complete JSONB payload for the evolving ATOS v0.2
// protocol object. The payload is authoritative for fields such as trust mode,
// proof profile, signer authorization and settlement evidence; legacy columns
// remain useful for indexes, constraints and rolling migration compatibility.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("postgres: marshal domain value: " + err.Error())
	}
	return b
}

func applyPayload(payload []byte, target any) error {
	if len(payload) == 0 || string(payload) == "{}" || string(payload) == "null" {
		return nil
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("postgres: decode v0.2 payload: %w", err)
	}
	return nil
}

func nullableJSON(v map[string]any) any {
	if v == nil {
		return nil
	}
	return mustMarshal(v)
}

// lockTransactionKey serializes a logical mutation even before its row
// exists. SELECT ... FOR UPDATE cannot lock a missing row, so relying on
// it alone permits concurrent first-writers to derive state from the same
// seed. PostgreSQL advisory transaction locks are shared by all gateway
// replicas and are released automatically on commit or rollback.
func lockTransactionKey(ctx context.Context, tx pgx.Tx, namespace string, parts ...string) error {
	if namespace == "" || len(parts) == 0 {
		return errors.New("postgres: advisory lock identity is empty")
	}
	identity := namespace
	for _, part := range parts {
		if part == "" {
			return errors.New("postgres: advisory lock identity contains an empty part")
		}
		identity += fmt.Sprintf(":%d:%s", len(part), part)
	}
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, identity)
	return err
}

// --- Capabilities ---

const capabilityColumns = `id, provider_id, name, description, version, tags, modalities, delivery_mode, input_schema, output_schema, pricing, sla, trust, status, updated_at, payload`

const upsertCapabilitySQL = `
	INSERT INTO capabilities (
		id, provider_id, name, description, version, tags, modalities,
		delivery_mode, input_schema, output_schema, pricing, sla, trust,
		status, updated_at, payload
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
	ON CONFLICT (id) DO UPDATE SET
		name=$3, description=$4, version=$5, tags=$6, modalities=$7,
		delivery_mode=$8, input_schema=$9, output_schema=$10, pricing=$11,
		sla=$12, trust=$13, status=$14, updated_at=$15, payload=$16
`

func capabilityWriteArgs(c domain.Capability) []any {
	return []any{
		c.ID, c.ProviderID, c.Name, c.Description, c.Version,
		mustMarshal(c.Tags), mustMarshal(c.Modalities), string(c.DeliveryMode),
		mustMarshal(c.InputSchema), mustMarshal(c.OutputSchema), mustMarshal(c.Pricing),
		mustMarshal(c.SLA), mustMarshal(c.Trust), string(c.Status), c.UpdatedAt,
		mustMarshal(c),
	}
}

func (s *Store) Put(ctx context.Context, c domain.Capability) error {
	_, err := s.pool.Exec(ctx, upsertCapabilitySQL, capabilityWriteArgs(c)...)
	return err
}

func scanCapability(row pgx.Row) (domain.Capability, error) {
	var c domain.Capability
	var tags, modalities, inputSchema, outputSchema, pricing, sla, trust, payload []byte
	var deliveryMode, status string
	if err := row.Scan(
		&c.ID, &c.ProviderID, &c.Name, &c.Description, &c.Version,
		&tags, &modalities, &deliveryMode, &inputSchema, &outputSchema,
		&pricing, &sla, &trust, &status, &c.UpdatedAt, &payload,
	); err != nil {
		return domain.Capability{}, err
	}
	c.DeliveryMode = domain.DeliveryMode(deliveryMode)
	c.Status = domain.CapabilityStatus(status)
	_ = json.Unmarshal(tags, &c.Tags)
	_ = json.Unmarshal(modalities, &c.Modalities)
	_ = json.Unmarshal(inputSchema, &c.InputSchema)
	_ = json.Unmarshal(outputSchema, &c.OutputSchema)
	_ = json.Unmarshal(pricing, &c.Pricing)
	_ = json.Unmarshal(sla, &c.SLA)
	_ = json.Unmarshal(trust, &c.Trust)
	if err := applyPayload(payload, &c); err != nil {
		return domain.Capability{}, err
	}
	return c, nil
}

func (s *Store) Get(ctx context.Context, id string) (domain.Capability, error) {
	c, err := scanCapability(s.pool.QueryRow(ctx, `SELECT `+capabilityColumns+` FROM capabilities WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Capability{}, store.ErrNotFound
	}
	return c, err
}

func (s *Store) UpdateCapability(ctx context.Context, id string, fn func(domain.Capability, bool) (domain.Capability, error)) (domain.Capability, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Capability{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "capability", id); err != nil {
		return domain.Capability{}, err
	}

	current, err := scanCapability(tx.QueryRow(ctx, `SELECT `+capabilityColumns+` FROM capabilities WHERE id=$1 FOR UPDATE`, id))
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		current = domain.Capability{}
		exists = false
		err = nil
	}
	if err != nil {
		return domain.Capability{}, err
	}
	next, err := fn(current, exists)
	if err != nil {
		return domain.Capability{}, err
	}
	if _, err := tx.Exec(ctx, upsertCapabilitySQL, capabilityWriteArgs(next)...); err != nil {
		return domain.Capability{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Capability{}, err
	}
	return next, nil
}

func (s *Store) Search(ctx context.Context, query string, limit int) ([]domain.Capability, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+capabilityColumns+` FROM capabilities
		WHERE status='active' AND (name ILIKE $1 OR description ILIKE $1 OR tags::text ILIKE $1)
		ORDER BY updated_at DESC, id ASC
		LIMIT $2
	`, "%"+query+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Capability
	for rows.Next() {
		c, err := scanCapability(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ActiveByMode uses the GIN index on (payload->'supported_trust_modes')
// migration 003 already created for exactly this containment query shape --
// no new index needed.
func (s *Store) ActiveByMode(ctx context.Context, mode domain.TrustMode, limit int) ([]domain.Capability, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+capabilityColumns+` FROM capabilities
		WHERE payload->'supported_trust_modes' @> $1::jsonb
		ORDER BY updated_at ASC, id ASC
		LIMIT $2
	`, mustMarshal([]domain.TrustMode{mode}), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Capability
	for rows.Next() {
		c, err := scanCapability(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) ByProvider(ctx context.Context, providerID string) ([]domain.Capability, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+capabilityColumns+` FROM capabilities WHERE provider_id=$1 ORDER BY updated_at DESC, id ASC`, providerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Capability
	for rows.Next() {
		c, err := scanCapability(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// --- Quotes ---

func (s *Store) PutQuote(ctx context.Context, q domain.Quote) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO quotes (
			id, capability_id, capability_version, price, expires_at,
			requires_confirmation, terms_hash, created_at, principal_id,
			idempotency_key, payload
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (id) DO NOTHING
	`, q.ID, q.CapabilityID, q.CapabilityVersion, mustMarshal(q.Price),
		q.ExpiresAt, q.RequiresConfirmation, q.TermsHash, q.CreatedAt,
		q.PrincipalID, q.IdempotencyKey, mustMarshal(q))
	return err
}

// QuoteByIdempotencyKey returns the Quote previously committed under
// (principalID, key), used by QuoteService.Create to recover from a crash
// between committing the Quote row and marking the idempotency record
// finished. Mirrors JobByIdempotencyKey below.
func (s *Store) QuoteByIdempotencyKey(ctx context.Context, principalID, key string) (domain.Quote, error) {
	var q domain.Quote
	var price, payload []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, capability_id, capability_version, price, expires_at,
		       requires_confirmation, terms_hash, created_at, payload
		FROM quotes
		WHERE principal_id=$1 AND idempotency_key=$2 AND idempotency_key <> ''
		ORDER BY created_at DESC
		LIMIT 1
	`, principalID, key).Scan(&q.ID, &q.CapabilityID, &q.CapabilityVersion, &price,
		&q.ExpiresAt, &q.RequiresConfirmation, &q.TermsHash, &q.CreatedAt, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Quote{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Quote{}, err
	}
	_ = json.Unmarshal(price, &q.Price)
	if err := applyPayload(payload, &q); err != nil {
		return domain.Quote{}, err
	}
	return q, nil
}

func (s *Store) GetQuote(ctx context.Context, id string) (domain.Quote, error) {
	var q domain.Quote
	var price, payload []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, capability_id, capability_version, price, expires_at,
		       requires_confirmation, terms_hash, created_at, payload
		FROM quotes WHERE id=$1
	`, id).Scan(&q.ID, &q.CapabilityID, &q.CapabilityVersion, &price,
		&q.ExpiresAt, &q.RequiresConfirmation, &q.TermsHash, &q.CreatedAt, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Quote{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Quote{}, err
	}
	_ = json.Unmarshal(price, &q.Price)
	if err := applyPayload(payload, &q); err != nil {
		return domain.Quote{}, err
	}
	return q, nil
}

// --- Escrows ---

const escrowColumns = `id, quote_id, job_id, principal_id, provider_id, capability_id, reserved, status, created_at, expires_at, settled_at, payload`

func (s *Store) PutEscrow(ctx context.Context, e domain.Escrow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO escrows (
			id, quote_id, job_id, principal_id, provider_id, capability_id, reserved,
			status, created_at, expires_at, settled_at, payload
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id) DO UPDATE SET
			job_id=$3, reserved=$7, status=$8, expires_at=$10, settled_at=$11, payload=$12
	`, e.ID, e.QuoteID, e.JobID, e.PrincipalID, e.ProviderID, e.CapabilityID,
		mustMarshal(e.Reserved), string(e.Status), e.CreatedAt, e.ExpiresAt,
		e.SettledAt, mustMarshal(e))
	return err
}

func scanEscrow(row pgx.Row) (domain.Escrow, error) {
	var e domain.Escrow
	var reserved, payload []byte
	var status string
	if err := row.Scan(
		&e.ID, &e.QuoteID, &e.JobID, &e.PrincipalID, &e.ProviderID, &e.CapabilityID,
		&reserved, &status, &e.CreatedAt, &e.ExpiresAt, &e.SettledAt, &payload,
	); err != nil {
		return domain.Escrow{}, err
	}
	e.Status = domain.EscrowStatus(status)
	_ = json.Unmarshal(reserved, &e.Reserved)
	if err := applyPayload(payload, &e); err != nil {
		return domain.Escrow{}, err
	}
	return e, nil
}

func (s *Store) GetEscrow(ctx context.Context, id string) (domain.Escrow, error) {
	e, err := scanEscrow(s.pool.QueryRow(ctx, `SELECT `+escrowColumns+` FROM escrows WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Escrow{}, store.ErrNotFound
	}
	return e, err
}

// --- Receipts ---

const receiptColumns = `id, quote_id, escrow_id, job_id, principal_id, charged, refunded, status, created_at, payload`

func (s *Store) EscrowByJob(ctx context.Context, jobID string) (domain.Escrow, error) {
	e, err := scanEscrow(s.pool.QueryRow(ctx, `SELECT `+escrowColumns+` FROM escrows WHERE job_id=$1`, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Escrow{}, store.ErrNotFound
	}
	return e, err
}

func (s *Store) PutReceipt(ctx context.Context, r domain.Receipt) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO receipts (
			id, quote_id, escrow_id, job_id, principal_id, charged, refunded,
			status, created_at, payload
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (id) DO NOTHING
	`, r.ID, r.QuoteID, r.EscrowID, r.JobID, r.PrincipalID,
		mustMarshal(r.Charged), mustMarshal(r.Refunded), string(r.Status),
		r.CreatedAt, mustMarshal(r))
	return err
}

func scanReceipt(row pgx.Row) (domain.Receipt, error) {
	var r domain.Receipt
	var charged, refunded, payload []byte
	var status string
	if err := row.Scan(
		&r.ID, &r.QuoteID, &r.EscrowID, &r.JobID, &r.PrincipalID,
		&charged, &refunded, &status, &r.CreatedAt, &payload,
	); err != nil {
		return domain.Receipt{}, err
	}
	r.Status = domain.ReceiptStatus(status)
	_ = json.Unmarshal(charged, &r.Charged)
	_ = json.Unmarshal(refunded, &r.Refunded)
	if err := applyPayload(payload, &r); err != nil {
		return domain.Receipt{}, err
	}
	return r, nil
}

func (s *Store) GetReceipt(ctx context.Context, id string) (domain.Receipt, error) {
	r, err := scanReceipt(s.pool.QueryRow(ctx, `SELECT `+receiptColumns+` FROM receipts WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Receipt{}, store.ErrNotFound
	}
	return r, err
}

func (s *Store) ReceiptByJob(ctx context.Context, jobID string) (domain.Receipt, error) {
	r, err := scanReceipt(s.pool.QueryRow(ctx, `SELECT `+receiptColumns+` FROM receipts WHERE job_id=$1`, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Receipt{}, store.ErrNotFound
	}
	return r, err
}

func (s *Store) ReceiptsByPrincipal(ctx context.Context, principalID string) ([]domain.Receipt, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+receiptColumns+` FROM receipts WHERE principal_id=$1 ORDER BY created_at DESC, id ASC`, principalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Receipt
	for rows.Next() {
		r, err := scanReceipt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// --- Jobs ---

const jobColumns = `id, capability_id, quote_id, escrow_id, principal_id, state, input, output, artifacts, idempotency_key, failure_reason, created_at, updated_at, estimated_completion_at, economic_state, execution_receipt, pending_credit, reconciliation_target, payload`

func (s *Store) PutJob(ctx context.Context, j domain.Job) error {
	_, err := s.pool.Exec(ctx, upsertJobSQL, jobWriteArgs(j)...)
	return err
}

const upsertJobSQL = `
	INSERT INTO jobs (
		id, capability_id, quote_id, escrow_id, principal_id, state, input,
		output, artifacts, idempotency_key, failure_reason, created_at,
		updated_at, estimated_completion_at, economic_state, execution_receipt,
		pending_credit, reconciliation_target, payload
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
	ON CONFLICT (id) DO UPDATE SET
		escrow_id=$4, state=$6, input=$7, output=$8, artifacts=$9,
		failure_reason=$11, updated_at=$13, estimated_completion_at=$14,
		economic_state=$15, execution_receipt=$16, pending_credit=$17,
		reconciliation_target=$18, payload=$19
`

func jobWriteArgs(j domain.Job) []any {
	var executionReceipt, pendingCredit any
	if j.ExecutionReceipt != nil {
		executionReceipt = mustMarshal(j.ExecutionReceipt)
	}
	if j.PendingCredit != nil {
		pendingCredit = mustMarshal(j.PendingCredit)
	}
	return []any{
		j.ID, j.CapabilityID, j.QuoteID, j.EscrowID, j.PrincipalID,
		string(j.State), mustMarshal(j.Input), nullableJSON(j.Output),
		mustMarshal(j.Artifacts), j.IdempotencyKey, j.FailureReason,
		j.CreatedAt, j.UpdatedAt, j.EstimatedCompletionAt, string(j.EconomicState),
		executionReceipt, pendingCredit, string(j.ReconciliationTarget), mustMarshal(j),
	}
}

func scanJob(row pgx.Row) (domain.Job, error) {
	var j domain.Job
	var input, output, artifacts, executionReceipt, pendingCredit, payload []byte
	var state, economicState, reconciliationTarget string
	if err := row.Scan(
		&j.ID, &j.CapabilityID, &j.QuoteID, &j.EscrowID, &j.PrincipalID,
		&state, &input, &output, &artifacts, &j.IdempotencyKey,
		&j.FailureReason, &j.CreatedAt, &j.UpdatedAt,
		&j.EstimatedCompletionAt, &economicState, &executionReceipt, &pendingCredit,
		&reconciliationTarget, &payload,
	); err != nil {
		return domain.Job{}, err
	}
	j.State = domain.JobState(state)
	j.EconomicState = domain.EconomicState(economicState)
	j.ReconciliationTarget = domain.JobState(reconciliationTarget)
	_ = json.Unmarshal(input, &j.Input)
	if output != nil {
		_ = json.Unmarshal(output, &j.Output)
	}
	_ = json.Unmarshal(artifacts, &j.Artifacts)
	if executionReceipt != nil {
		var receipt domain.ExecutionReceipt
		if err := json.Unmarshal(executionReceipt, &receipt); err != nil {
			return domain.Job{}, fmt.Errorf("postgres: decode execution receipt: %w", err)
		}
		j.ExecutionReceipt = &receipt
	}
	if pendingCredit != nil {
		var credit domain.Money
		if err := json.Unmarshal(pendingCredit, &credit); err != nil {
			return domain.Job{}, fmt.Errorf("postgres: decode pending credit: %w", err)
		}
		j.PendingCredit = &credit
	}
	if err := applyPayload(payload, &j); err != nil {
		return domain.Job{}, err
	}
	// Internal economic recovery fields are deliberately stored in dedicated
	// columns because they are not part of the public Job JSON contract.
	j.EconomicState = domain.EconomicState(economicState)
	j.ReconciliationTarget = domain.JobState(reconciliationTarget)
	if executionReceipt == nil {
		j.ExecutionReceipt = nil
	}
	if pendingCredit == nil {
		j.PendingCredit = nil
	}
	return j, nil
}

func (s *Store) GetJob(ctx context.Context, id string) (domain.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, store.ErrNotFound
	}
	return j, err
}

func (s *Store) JobsByPrincipal(ctx context.Context, principalID string) ([]domain.Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE principal_id=$1 ORDER BY created_at DESC, id ASC`, principalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// JobsByProvider filters on the JSONB payload, not a dedicated column --
// jobs has no provider_id column of its own; ProviderID lives only inside
// payload, exactly like trust_mode (see jobs_trust_mode_idx). Migration
// 010 creates a matching expression index.
func (s *Store) JobsByProvider(ctx context.Context, providerID string) ([]domain.Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE payload->>'provider_id'=$1 ORDER BY created_at DESC, id ASC`, providerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) JobsForRecovery(ctx context.Context, updatedBefore time.Time, limit int) ([]domain.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobColumns+` FROM jobs
		WHERE state IN ('submitted','working','canceling','reconciling')
		  AND updated_at <= $1
		ORDER BY updated_at ASC, id ASC
		LIMIT $2
	`, updatedBefore.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.Job, 0, limit)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) JobByConfirmationCode(ctx context.Context, userCode string) (domain.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `
		SELECT `+jobColumns+` FROM jobs
		WHERE payload #>> '{confirmation,user_code}' = $1
		ORDER BY created_at DESC
		LIMIT 1
	`, userCode))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, store.ErrNotFound
	}
	return j, err
}

func (s *Store) JobByIdempotencyKey(ctx context.Context, principalID, key string) (domain.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `
		SELECT `+jobColumns+` FROM jobs
		WHERE principal_id=$1 AND idempotency_key=$2
		ORDER BY created_at DESC
		LIMIT 1
	`, principalID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, store.ErrNotFound
	}
	return j, err
}

func (s *Store) UpdateJob(ctx context.Context, id string, fn func(domain.Job, bool) (domain.Job, error)) (domain.Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "job", id); err != nil {
		return domain.Job{}, err
	}

	current, err := scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1 FOR UPDATE`, id))
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		current = domain.Job{}
		exists = false
		err = nil
	}
	if err != nil {
		return domain.Job{}, err
	}
	next, err := fn(current, exists)
	if err != nil {
		return domain.Job{}, err
	}
	if _, err := tx.Exec(ctx, upsertJobSQL, jobWriteArgs(next)...); err != nil {
		return domain.Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, err
	}
	return next, nil
}

func (s *Store) UpdateJobAndAccount(
	ctx context.Context, jobID, principalID string, seed domain.Account,
	fn func(domain.Job, bool, domain.Account, bool) (domain.Job, domain.Account, error),
) (domain.Job, domain.Account, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, domain.Account{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	// All multi-object economic transactions lock Account first, then Job.
	// Single-object mutations take only one of these locks, so this order is
	// deadlock-free across concurrent jobs for the same principal.
	if err := lockTransactionKey(ctx, tx, "account", principalID); err != nil {
		return domain.Job{}, domain.Account{}, err
	}
	if err := lockTransactionKey(ctx, tx, "job", jobID); err != nil {
		return domain.Job{}, domain.Account{}, err
	}

	job, err := scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1 FOR UPDATE`, jobID))
	jobExists := true
	if errors.Is(err, pgx.ErrNoRows) {
		job = domain.Job{}
		jobExists = false
		err = nil
	}
	if err != nil {
		return domain.Job{}, domain.Account{}, err
	}

	var account domain.Account
	account.PrincipalID = principalID
	var balance, spendPolicy, payload []byte
	err = tx.QueryRow(ctx, `SELECT balance, spend_policy, payload FROM accounts WHERE principal_id=$1 FOR UPDATE`, principalID).
		Scan(&balance, &spendPolicy, &payload)
	accountExists := true
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		accountExists = false
		account = seed
	case err != nil:
		return domain.Job{}, domain.Account{}, err
	default:
		_ = json.Unmarshal(balance, &account.Balance)
		_ = json.Unmarshal(spendPolicy, &account.SpendPolicy)
		if err := applyPayload(payload, &account); err != nil {
			return domain.Job{}, domain.Account{}, err
		}
	}
	account.PrincipalID = principalID

	nextJob, nextAccount, err := fn(job, jobExists, account, accountExists)
	if err != nil {
		return domain.Job{}, domain.Account{}, err
	}
	if nextJob.ID != jobID || nextAccount.PrincipalID != principalID {
		return domain.Job{}, domain.Account{}, store.ErrConflict
	}
	if _, err := tx.Exec(ctx, upsertJobSQL, jobWriteArgs(nextJob)...); err != nil {
		return domain.Job{}, domain.Account{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO accounts (principal_id, balance, spend_policy, payload)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (principal_id) DO UPDATE SET
			balance=$2, spend_policy=$3, payload=$4
	`, principalID, mustMarshal(nextAccount.Balance), mustMarshal(nextAccount.SpendPolicy), mustMarshal(nextAccount)); err != nil {
		return domain.Job{}, domain.Account{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, domain.Account{}, err
	}
	return nextJob, nextAccount, nil
}

// --- Accounts ---

func (s *Store) GetAccount(ctx context.Context, principalID string) (domain.Account, error) {
	var a domain.Account
	a.PrincipalID = principalID
	var balance, spendPolicy, payload []byte
	err := s.pool.QueryRow(ctx, `SELECT balance, spend_policy, payload FROM accounts WHERE principal_id=$1`, principalID).
		Scan(&balance, &spendPolicy, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Account{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Account{}, err
	}
	_ = json.Unmarshal(balance, &a.Balance)
	_ = json.Unmarshal(spendPolicy, &a.SpendPolicy)
	if err := applyPayload(payload, &a); err != nil {
		return domain.Account{}, err
	}
	a.PrincipalID = principalID
	return a, nil
}

func (s *Store) PutAccount(ctx context.Context, a domain.Account) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO accounts (principal_id, balance, spend_policy, payload)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (principal_id) DO UPDATE SET
			balance=$2, spend_policy=$3, payload=$4
	`, a.PrincipalID, mustMarshal(a.Balance), mustMarshal(a.SpendPolicy), mustMarshal(a))
	return err
}

func (s *Store) UpdateAccount(ctx context.Context, principalID string, seed domain.Account, fn func(domain.Account, bool) (domain.Account, error)) (domain.Account, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Account{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "account", principalID); err != nil {
		return domain.Account{}, err
	}

	var current domain.Account
	current.PrincipalID = principalID
	var balance, spendPolicy, payload []byte
	err = tx.QueryRow(ctx, `SELECT balance, spend_policy, payload FROM accounts WHERE principal_id=$1 FOR UPDATE`, principalID).
		Scan(&balance, &spendPolicy, &payload)
	exists := true
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		exists = false
		current = seed
	case err != nil:
		return domain.Account{}, err
	default:
		_ = json.Unmarshal(balance, &current.Balance)
		_ = json.Unmarshal(spendPolicy, &current.SpendPolicy)
		if err := applyPayload(payload, &current); err != nil {
			return domain.Account{}, err
		}
	}
	current.PrincipalID = principalID
	next, err := fn(current, exists)
	if err != nil {
		return domain.Account{}, err
	}
	next.PrincipalID = principalID
	if _, err := tx.Exec(ctx, `
		INSERT INTO accounts (principal_id, balance, spend_policy, payload)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (principal_id) DO UPDATE SET
			balance=$2, spend_policy=$3, payload=$4
	`, principalID, mustMarshal(next.Balance), mustMarshal(next.SpendPolicy), mustMarshal(next)); err != nil {
		return domain.Account{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Account{}, err
	}
	return next, nil
}

// --- Artifacts ---

func (s *Store) PutArtifact(ctx context.Context, a domain.StoredArtifact) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO artifacts (
			id, owner_principal_id, content_type, size_bytes, sha256, status,
			created_at, expires_at, payload
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (id) DO UPDATE SET
			content_type=$3, size_bytes=$4, sha256=$5, status=$6,
			expires_at=$8, payload=$9
	`, a.ID, a.OwnerPrincipalID, a.ContentType, a.SizeBytes, a.SHA256,
		string(a.Status), a.CreatedAt, a.ExpiresAt, mustMarshal(a))
	return err
}

func (s *Store) GetArtifact(ctx context.Context, id string) (domain.StoredArtifact, error) {
	var a domain.StoredArtifact
	var status string
	var payload []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_principal_id, content_type, size_bytes, sha256,
		       status, created_at, expires_at, payload
		FROM artifacts WHERE id=$1
	`, id).Scan(&a.ID, &a.OwnerPrincipalID, &a.ContentType, &a.SizeBytes,
		&a.SHA256, &status, &a.CreatedAt, &a.ExpiresAt, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.StoredArtifact{}, store.ErrNotFound
	}
	if err != nil {
		return domain.StoredArtifact{}, err
	}
	a.Status = domain.ArtifactStatus(status)
	if err := applyPayload(payload, &a); err != nil {
		return domain.StoredArtifact{}, err
	}
	return a, nil
}

// --- Idempotency ---

func (s *Store) Reserve(ctx context.Context, principalID, key, requestHash string, leaseUntil time.Time) (store.IdempotencyRecord, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.IdempotencyRecord{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "idempotency", principalID, key); err != nil {
		return store.IdempotencyRecord{}, false, err
	}

	var rec store.IdempotencyRecord
	var status string
	err = tx.QueryRow(ctx, `
		SELECT request_hash, response_key, status, reserved_at, lease_expires_at
		FROM idempotency_records
		WHERE principal_id=$1 AND key=$2
		FOR UPDATE
	`, principalID, key).Scan(
		&rec.RequestHash, &rec.ResponseKey, &status,
		&rec.ReservedAt, &rec.LeaseExpires,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		now := time.Now().UTC()
		_, err = tx.Exec(ctx, `
			INSERT INTO idempotency_records (
				principal_id, key, request_hash, status, reserved_at, lease_expires_at
			) VALUES ($1,$2,$3,$4,$5,$6)
		`, principalID, key, requestHash, string(store.IdempotencyInProgress), now, leaseUntil.UTC())
		if err != nil {
			return store.IdempotencyRecord{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return store.IdempotencyRecord{}, false, err
		}
		return store.IdempotencyRecord{
			RequestHash:  requestHash,
			Status:       store.IdempotencyInProgress,
			ReservedAt:   now,
			LeaseExpires: leaseUntil.UTC(),
		}, true, nil
	}
	if err != nil {
		return store.IdempotencyRecord{}, false, err
	}
	rec.Status = store.IdempotencyStatus(status)
	now := time.Now().UTC()
	if rec.Status == store.IdempotencyInProgress &&
		rec.RequestHash == requestHash &&
		!rec.LeaseExpires.IsZero() && !now.Before(rec.LeaseExpires) {
		_, err = tx.Exec(ctx, `
			UPDATE idempotency_records
			SET reserved_at=$3, lease_expires_at=$4
			WHERE principal_id=$1 AND key=$2
		`, principalID, key, now, leaseUntil.UTC())
		if err != nil {
			return store.IdempotencyRecord{}, false, err
		}
		rec.ReservedAt = now
		rec.LeaseExpires = leaseUntil.UTC()
		if err := tx.Commit(ctx); err != nil {
			return store.IdempotencyRecord{}, false, err
		}
		return rec, true, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return store.IdempotencyRecord{}, false, err
	}
	return rec, false, nil
}

func (s *Store) Finish(ctx context.Context, principalID, key, responseKey string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE idempotency_records SET response_key=$3, status=$4
		WHERE principal_id=$1 AND key=$2
	`, principalID, key, responseKey, string(store.IdempotencyCompleted))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) Release(ctx context.Context, principalID, key string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM idempotency_records WHERE principal_id=$1 AND key=$2`, principalID, key)
	return err
}
