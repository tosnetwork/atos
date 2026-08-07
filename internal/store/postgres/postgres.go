// Package postgres implements store.Store against Postgres, the "Phase 1
// — centralized Postgres registry" deliverable from
// ~/atos-spec/docs/IMPLEMENTATION_ROADMAP.md. It satisfies the exact same
// interface internal/store/memory does, so internal/service never changes
// when this replaces the in-memory store.
//
// Atomicity: the in-memory store gets UpdateAccount/UpdateJob's
// compare-and-swap semantics from a single process-wide mutex. Here the
// same guarantee comes from `SELECT ... FOR UPDATE` inside a transaction —
// the row lock blocks a concurrent updater until the first transaction
// commits or rolls back, which is the same "only one caller wins" property
// the concurrency tests in internal/store/memory/memory_test.go assert.
package postgres

import (
	"context"
	"encoding/json"
	"errors"

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

func (s *Store) Close() {
	s.pool.Close()
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// Every value passed here is one of our own domain types with no
		// unmarshalable fields (channels, funcs) — a failure here is a
		// programming error, not a runtime condition callers can recover
		// from, so it is not worth threading an error return through every
		// call site for.
		panic("postgres: marshal domain value: " + err.Error())
	}
	return b
}

// --- Capabilities ---

func (s *Store) Put(ctx context.Context, c domain.Capability) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO capabilities (id, provider_id, name, description, version, tags, modalities, delivery_mode, input_schema, output_schema, pricing, sla, trust, status, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		ON CONFLICT (id) DO UPDATE SET
			name=$3, description=$4, version=$5, tags=$6, modalities=$7, delivery_mode=$8,
			input_schema=$9, output_schema=$10, pricing=$11, sla=$12, trust=$13, status=$14, updated_at=$15
	`, c.ID, c.ProviderID, c.Name, c.Description, c.Version, mustMarshal(c.Tags), mustMarshal(c.Modalities),
		string(c.DeliveryMode), mustMarshal(c.InputSchema), mustMarshal(c.OutputSchema), mustMarshal(c.Pricing),
		mustMarshal(c.SLA), mustMarshal(c.Trust), string(c.Status), c.UpdatedAt)
	return err
}

func scanCapability(row pgx.Row) (domain.Capability, error) {
	var c domain.Capability
	var tags, modalities, inputSchema, outputSchema, pricing, sla, trust []byte
	var deliveryMode, status string
	err := row.Scan(&c.ID, &c.ProviderID, &c.Name, &c.Description, &c.Version, &tags, &modalities,
		&deliveryMode, &inputSchema, &outputSchema, &pricing, &sla, &trust, &status, &c.UpdatedAt)
	if err != nil {
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
	return c, nil
}

const capabilityColumns = `id, provider_id, name, description, version, tags, modalities, delivery_mode, input_schema, output_schema, pricing, sla, trust, status, updated_at`

func (s *Store) Get(ctx context.Context, id string) (domain.Capability, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+capabilityColumns+` FROM capabilities WHERE id=$1`, id)
	c, err := scanCapability(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Capability{}, store.ErrNotFound
	}
	return c, err
}

// Search does a naive ILIKE match, same Phase 0/1 simplification as
// internal/store/memory — see docs/CAPABILITIES.md for the real semantic
// ranking this is a stand-in for.
func (s *Store) Search(ctx context.Context, query string, limit int) ([]domain.Capability, error) {
	pattern := "%" + query + "%"
	rows, err := s.pool.Query(ctx, `
		SELECT `+capabilityColumns+` FROM capabilities
		WHERE status='active' AND (name ILIKE $1 OR description ILIKE $1 OR tags::text ILIKE $1)
		LIMIT $2
	`, pattern, limit)
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
	rows, err := s.pool.Query(ctx, `SELECT `+capabilityColumns+` FROM capabilities WHERE provider_id=$1`, providerID)
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
		INSERT INTO quotes (id, capability_id, capability_version, price, expires_at, requires_confirmation, terms_hash, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (id) DO NOTHING
	`, q.ID, q.CapabilityID, q.CapabilityVersion, mustMarshal(q.Price), q.ExpiresAt, q.RequiresConfirmation, q.TermsHash, q.CreatedAt)
	return err
}

func (s *Store) GetQuote(ctx context.Context, id string) (domain.Quote, error) {
	var q domain.Quote
	var price []byte
	err := s.pool.QueryRow(ctx, `SELECT id, capability_id, capability_version, price, expires_at, requires_confirmation, terms_hash, created_at FROM quotes WHERE id=$1`, id).
		Scan(&q.ID, &q.CapabilityID, &q.CapabilityVersion, &price, &q.ExpiresAt, &q.RequiresConfirmation, &q.TermsHash, &q.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Quote{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Quote{}, err
	}
	_ = json.Unmarshal(price, &q.Price)
	return q, nil
}

// --- Escrows ---

func (s *Store) PutEscrow(ctx context.Context, e domain.Escrow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO escrows (id, quote_id, principal_id, provider_id, capability_id, reserved, status, created_at, expires_at, settled_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (id) DO UPDATE SET reserved=$6, status=$7, expires_at=$9, settled_at=$10
	`, e.ID, e.QuoteID, e.PrincipalID, e.ProviderID, e.CapabilityID, mustMarshal(e.Reserved), string(e.Status), e.CreatedAt, e.ExpiresAt, e.SettledAt)
	return err
}

func scanEscrow(row pgx.Row) (domain.Escrow, error) {
	var e domain.Escrow
	var reserved []byte
	var status string
	err := row.Scan(&e.ID, &e.QuoteID, &e.PrincipalID, &e.ProviderID, &e.CapabilityID, &reserved, &status, &e.CreatedAt, &e.ExpiresAt, &e.SettledAt)
	if err != nil {
		return domain.Escrow{}, err
	}
	e.Status = domain.EscrowStatus(status)
	_ = json.Unmarshal(reserved, &e.Reserved)
	return e, nil
}

const escrowColumns = `id, quote_id, principal_id, provider_id, capability_id, reserved, status, created_at, expires_at, settled_at`

func (s *Store) GetEscrow(ctx context.Context, id string) (domain.Escrow, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+escrowColumns+` FROM escrows WHERE id=$1`, id)
	e, err := scanEscrow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Escrow{}, store.ErrNotFound
	}
	return e, err
}

// --- Receipts ---

func (s *Store) PutReceipt(ctx context.Context, r domain.Receipt) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO receipts (id, quote_id, escrow_id, job_id, principal_id, charged, refunded, status, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (id) DO NOTHING
	`, r.ID, r.QuoteID, r.EscrowID, r.JobID, r.PrincipalID, mustMarshal(r.Charged), mustMarshal(r.Refunded), string(r.Status), r.CreatedAt)
	return err
}

func scanReceipt(row pgx.Row) (domain.Receipt, error) {
	var r domain.Receipt
	var charged, refunded []byte
	var status string
	err := row.Scan(&r.ID, &r.QuoteID, &r.EscrowID, &r.JobID, &r.PrincipalID, &charged, &refunded, &status, &r.CreatedAt)
	if err != nil {
		return domain.Receipt{}, err
	}
	r.Status = domain.ReceiptStatus(status)
	_ = json.Unmarshal(charged, &r.Charged)
	_ = json.Unmarshal(refunded, &r.Refunded)
	return r, nil
}

const receiptColumns = `id, quote_id, escrow_id, job_id, principal_id, charged, refunded, status, created_at`

func (s *Store) GetReceipt(ctx context.Context, id string) (domain.Receipt, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+receiptColumns+` FROM receipts WHERE id=$1`, id)
	r, err := scanReceipt(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Receipt{}, store.ErrNotFound
	}
	return r, err
}

func (s *Store) ReceiptByJob(ctx context.Context, jobID string) (domain.Receipt, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+receiptColumns+` FROM receipts WHERE job_id=$1`, jobID)
	r, err := scanReceipt(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Receipt{}, store.ErrNotFound
	}
	return r, err
}

func (s *Store) ReceiptsByPrincipal(ctx context.Context, principalID string) ([]domain.Receipt, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+receiptColumns+` FROM receipts WHERE principal_id=$1`, principalID)
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

func (s *Store) PutJob(ctx context.Context, j domain.Job) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jobs (id, capability_id, quote_id, escrow_id, principal_id, state, input, output, artifacts, idempotency_key, failure_reason, created_at, updated_at, estimated_completion_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (id) DO UPDATE SET
			escrow_id=$4, state=$6, input=$7, output=$8, artifacts=$9, failure_reason=$11, updated_at=$13, estimated_completion_at=$14
	`, j.ID, j.CapabilityID, j.QuoteID, j.EscrowID, j.PrincipalID, string(j.State), mustMarshal(j.Input),
		nullableJSON(j.Output), mustMarshal(j.Artifacts), j.IdempotencyKey, j.FailureReason, j.CreatedAt, j.UpdatedAt, j.EstimatedCompletionAt)
	return err
}

func nullableJSON(v map[string]any) any {
	if v == nil {
		return nil
	}
	return mustMarshal(v)
}

func scanJob(row pgx.Row) (domain.Job, error) {
	var j domain.Job
	var input, output, artifacts []byte
	var state string
	err := row.Scan(&j.ID, &j.CapabilityID, &j.QuoteID, &j.EscrowID, &j.PrincipalID, &state, &input, &output,
		&artifacts, &j.IdempotencyKey, &j.FailureReason, &j.CreatedAt, &j.UpdatedAt, &j.EstimatedCompletionAt)
	if err != nil {
		return domain.Job{}, err
	}
	j.State = domain.JobState(state)
	_ = json.Unmarshal(input, &j.Input)
	if output != nil {
		_ = json.Unmarshal(output, &j.Output)
	}
	_ = json.Unmarshal(artifacts, &j.Artifacts)
	return j, nil
}

const jobColumns = `id, capability_id, quote_id, escrow_id, principal_id, state, input, output, artifacts, idempotency_key, failure_reason, created_at, updated_at, estimated_completion_at`

func (s *Store) GetJob(ctx context.Context, id string) (domain.Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1`, id)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, store.ErrNotFound
	}
	return j, err
}

func (s *Store) JobsByPrincipal(ctx context.Context, principalID string) ([]domain.Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE principal_id=$1`, principalID)
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

// UpdateJob locks the row with SELECT ... FOR UPDATE inside a transaction,
// so a concurrent caller trying to update the same job blocks until this
// transaction commits or rolls back — the same exclusivity
// internal/store/memory's process-wide mutex gives claimForExecution and
// transitionIfNotTerminal in internal/service/job.go.
func (s *Store) UpdateJob(ctx context.Context, id string, fn func(j domain.Job, exists bool) (domain.Job, error)) (domain.Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op if already committed

	row := tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1 FOR UPDATE`, id)
	current, err := scanJob(row)
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
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

	_, err = tx.Exec(ctx, `
		INSERT INTO jobs (id, capability_id, quote_id, escrow_id, principal_id, state, input, output, artifacts, idempotency_key, failure_reason, created_at, updated_at, estimated_completion_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (id) DO UPDATE SET
			escrow_id=$4, state=$6, input=$7, output=$8, artifacts=$9, failure_reason=$11, updated_at=$13, estimated_completion_at=$14
	`, next.ID, next.CapabilityID, next.QuoteID, next.EscrowID, next.PrincipalID, string(next.State), mustMarshal(next.Input),
		nullableJSON(next.Output), mustMarshal(next.Artifacts), next.IdempotencyKey, next.FailureReason, next.CreatedAt, next.UpdatedAt, next.EstimatedCompletionAt)
	if err != nil {
		return domain.Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, err
	}
	return next, nil
}

// --- Accounts ---

func (s *Store) GetAccount(ctx context.Context, principalID string) (domain.Account, error) {
	var a domain.Account
	a.PrincipalID = principalID
	var balance, spendPolicy []byte
	err := s.pool.QueryRow(ctx, `SELECT balance, spend_policy FROM accounts WHERE principal_id=$1`, principalID).Scan(&balance, &spendPolicy)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Account{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Account{}, err
	}
	_ = json.Unmarshal(balance, &a.Balance)
	_ = json.Unmarshal(spendPolicy, &a.SpendPolicy)
	return a, nil
}

func (s *Store) PutAccount(ctx context.Context, a domain.Account) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO accounts (principal_id, balance, spend_policy) VALUES ($1,$2,$3)
		ON CONFLICT (principal_id) DO UPDATE SET balance=$2, spend_policy=$3
	`, a.PrincipalID, mustMarshal(a.Balance), mustMarshal(a.SpendPolicy))
	return err
}

// UpdateAccount mirrors UpdateJob's SELECT ... FOR UPDATE pattern — see
// that method's comment for why this gives the same atomicity guarantee
// the in-memory store's mutex does.
func (s *Store) UpdateAccount(ctx context.Context, principalID string, seed domain.Account, fn func(a domain.Account, exists bool) (domain.Account, error)) (domain.Account, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Account{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var current domain.Account
	current.PrincipalID = principalID
	var balance, spendPolicy []byte
	err = tx.QueryRow(ctx, `SELECT balance, spend_policy FROM accounts WHERE principal_id=$1 FOR UPDATE`, principalID).Scan(&balance, &spendPolicy)
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
	}

	next, err := fn(current, exists)
	if err != nil {
		return domain.Account{}, err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO accounts (principal_id, balance, spend_policy) VALUES ($1,$2,$3)
		ON CONFLICT (principal_id) DO UPDATE SET balance=$2, spend_policy=$3
	`, principalID, mustMarshal(next.Balance), mustMarshal(next.SpendPolicy))
	if err != nil {
		return domain.Account{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Account{}, err
	}
	next.PrincipalID = principalID
	return next, nil
}

// --- Artifacts ---

func (s *Store) PutArtifact(ctx context.Context, a domain.StoredArtifact) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO artifacts (id, owner_principal_id, content_type, size_bytes, sha256, status, created_at, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (id) DO UPDATE SET
			content_type=$3, size_bytes=$4, sha256=$5, status=$6, expires_at=$8
	`, a.ID, a.OwnerPrincipalID, a.ContentType, a.SizeBytes, a.SHA256, string(a.Status), a.CreatedAt, a.ExpiresAt)
	return err
}

func (s *Store) GetArtifact(ctx context.Context, id string) (domain.StoredArtifact, error) {
	var a domain.StoredArtifact
	var status string
	err := s.pool.QueryRow(ctx, `SELECT id, owner_principal_id, content_type, size_bytes, sha256, status, created_at, expires_at FROM artifacts WHERE id=$1`, id).
		Scan(&a.ID, &a.OwnerPrincipalID, &a.ContentType, &a.SizeBytes, &a.SHA256, &status, &a.CreatedAt, &a.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.StoredArtifact{}, store.ErrNotFound
	}
	if err != nil {
		return domain.StoredArtifact{}, err
	}
	a.Status = domain.ArtifactStatus(status)
	return a, nil
}

// --- Idempotency ---

func (s *Store) Reserve(ctx context.Context, principalID, key, requestHash string) (store.IdempotencyRecord, bool, error) {
	var rec store.IdempotencyRecord
	err := s.pool.QueryRow(ctx, `
		INSERT INTO idempotency_records (principal_id, key, request_hash, status)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (principal_id, key) DO NOTHING
		RETURNING request_hash, response_key, status
	`, principalID, key, requestHash, string(store.IdempotencyInProgress)).Scan(&rec.RequestHash, &rec.ResponseKey, &rec.Status)
	if err == nil {
		return rec, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.IdempotencyRecord{}, false, err
	}
	// Conflict: a record already exists. Fetch and return it.
	var status string
	err = s.pool.QueryRow(ctx, `SELECT request_hash, response_key, status FROM idempotency_records WHERE principal_id=$1 AND key=$2`, principalID, key).
		Scan(&rec.RequestHash, &rec.ResponseKey, &status)
	if err != nil {
		return store.IdempotencyRecord{}, false, err
	}
	rec.Status = store.IdempotencyStatus(status)
	return rec, false, nil
}

func (s *Store) Finish(ctx context.Context, principalID, key, responseKey string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE idempotency_records SET response_key=$3, status=$4 WHERE principal_id=$1 AND key=$2
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
