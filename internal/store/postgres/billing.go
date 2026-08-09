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

// --- Billing ---

const billingSnapshotColumns = `job_id, quote_id, receipt_id, provider_id, capability_id, capability_version, trust_mode, usage, usage_commitment, pricing_model, pricing_terms_hash, gross_charge, provider_gross, gateway_fee, principal_refund, calculated_at, payload`

// billingSnapshotContentHash summarizes the semantically meaningful
// economic fields of a BillingSnapshot -- everything except CalculatedAt,
// which legitimately differs between the original computation and a later
// idempotent recomputation -- so a replayed PutBillingSnapshot for the same
// JobID can be recognized as identical (safe no-op) versus a computation
// that produced different economic content for the same Job (rejected).
func billingSnapshotContentHash(snap domain.BillingSnapshot) string {
	encoded, _ := json.Marshal(struct {
		JobID, QuoteID, ReceiptID, ProviderID, CapabilityID, CapabilityVersion string
		TrustMode                                                              domain.TrustMode
		Usage                                                                  domain.Usage
		UsageCommitment                                                        string
		PricingModel                                                           domain.PricingModel
		PricingTermsHash                                                       string
		GrossCharge, ProviderGross, GatewayFee, PrincipalRefund                domain.Money
	}{
		snap.JobID, snap.QuoteID, snap.ReceiptID, snap.ProviderID, snap.CapabilityID, snap.CapabilityVersion,
		snap.TrustMode, snap.Usage, snap.UsageCommitment, snap.PricingModel, snap.PricingTermsHash,
		snap.GrossCharge, snap.ProviderGross, snap.GatewayFee, snap.PrincipalRefund,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func (s *Store) PutBillingSnapshot(ctx context.Context, snap domain.BillingSnapshot) (domain.BillingSnapshot, bool, error) {
	contentHash := billingSnapshotContentHash(snap)
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO billing_snapshots (
			job_id, quote_id, receipt_id, provider_id, capability_id, capability_version,
			trust_mode, usage, usage_commitment, pricing_model, pricing_terms_hash,
			gross_charge, provider_gross, gateway_fee, principal_refund, calculated_at, content_hash, payload
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
		ON CONFLICT (job_id) DO NOTHING
	`, snap.JobID, snap.QuoteID, snap.ReceiptID, snap.ProviderID, snap.CapabilityID, snap.CapabilityVersion,
		string(snap.TrustMode), mustMarshal(snap.Usage), snap.UsageCommitment, string(snap.PricingModel), snap.PricingTermsHash,
		mustMarshal(snap.GrossCharge), mustMarshal(snap.ProviderGross), mustMarshal(snap.GatewayFee), mustMarshal(snap.PrincipalRefund),
		snap.CalculatedAt, contentHash, mustMarshal(snap))
	if err != nil {
		return domain.BillingSnapshot{}, false, err
	}
	if tag.RowsAffected() > 0 {
		return snap, true, nil
	}
	var storedHash string
	if err := s.pool.QueryRow(ctx, `SELECT content_hash FROM billing_snapshots WHERE job_id=$1`, snap.JobID).Scan(&storedHash); err != nil {
		return domain.BillingSnapshot{}, false, err
	}
	if storedHash != contentHash {
		return domain.BillingSnapshot{}, false, domain.NewError(domain.ErrIdempotencyConflict, "billing snapshot already exists for this job with different economic content", false)
	}
	existing, err := s.BillingSnapshotByJob(ctx, snap.JobID)
	if err != nil {
		return domain.BillingSnapshot{}, false, err
	}
	return existing, false, nil
}

func scanBillingSnapshot(row pgx.Row) (domain.BillingSnapshot, error) {
	var snap domain.BillingSnapshot
	var usage, grossCharge, providerGross, gatewayFee, principalRefund, payload []byte
	var trustMode, pricingModel string
	if err := row.Scan(
		&snap.JobID, &snap.QuoteID, &snap.ReceiptID, &snap.ProviderID, &snap.CapabilityID, &snap.CapabilityVersion,
		&trustMode, &usage, &snap.UsageCommitment, &pricingModel, &snap.PricingTermsHash,
		&grossCharge, &providerGross, &gatewayFee, &principalRefund, &snap.CalculatedAt, &payload,
	); err != nil {
		return domain.BillingSnapshot{}, err
	}
	snap.TrustMode = domain.TrustMode(trustMode)
	snap.PricingModel = domain.PricingModel(pricingModel)
	_ = json.Unmarshal(usage, &snap.Usage)
	_ = json.Unmarshal(grossCharge, &snap.GrossCharge)
	_ = json.Unmarshal(providerGross, &snap.ProviderGross)
	_ = json.Unmarshal(gatewayFee, &snap.GatewayFee)
	_ = json.Unmarshal(principalRefund, &snap.PrincipalRefund)
	if err := applyPayload(payload, &snap); err != nil {
		return domain.BillingSnapshot{}, err
	}
	return snap, nil
}

func (s *Store) BillingSnapshotByJob(ctx context.Context, jobID string) (domain.BillingSnapshot, error) {
	snap, err := scanBillingSnapshot(s.pool.QueryRow(ctx, `SELECT `+billingSnapshotColumns+` FROM billing_snapshots WHERE job_id=$1`, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.BillingSnapshot{}, store.ErrNotFound
	}
	return snap, err
}

// --- Earnings ---

const earningColumns = `id, provider_id, job_id, quote_id, receipt_id, settlement_id, capability_id, capability_version, gross_amount, gateway_fee, net_amount, status, created_at, matures_at, available_at, payout_requested_at, payout_reference, paid_at, payout_idempotency_key, payout_attempts, payout_last_attempt_at, payout_failure_reason, content_hash, payload`

// earningContentHash summarizes the identity+economic fields of a
// ProviderEarning that must never differ for a given SettlementID --
// deliberately excluding lifecycle fields (Status, CreatedAt, MaturesAt,
// payout checkpoints, ...) which legitimately change over the earning's
// life and differ between the original create and a later retry.
func earningContentHash(e domain.ProviderEarning) string {
	encoded, _ := json.Marshal(struct {
		ProviderID, JobID, QuoteID, ReceiptID, SettlementID, CapabilityID, CapabilityVersion string
		GrossAmount, GatewayFee, NetAmount                                                   domain.Money
	}{
		e.ProviderID, e.JobID, e.QuoteID, e.ReceiptID, e.SettlementID, e.CapabilityID, e.CapabilityVersion,
		e.GrossAmount, e.GatewayFee, e.NetAmount,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func earningWriteArgs(e domain.ProviderEarning) []any {
	return []any{
		e.ID, e.ProviderID, e.JobID, e.QuoteID, e.ReceiptID, e.SettlementID, e.CapabilityID, e.CapabilityVersion,
		mustMarshal(e.GrossAmount), mustMarshal(e.GatewayFee), mustMarshal(e.NetAmount), string(e.Status),
		e.CreatedAt, e.MaturesAt, e.AvailableAt, e.PayoutRequestedAt, e.PayoutReference, e.PaidAt,
		e.PayoutIdempotencyKey, e.PayoutAttempts, nullableTime(e.PayoutLastAttemptAt), e.PayoutFailureReason,
		earningContentHash(e), mustMarshal(e),
	}
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

const upsertEarningSQL = `
	INSERT INTO provider_earnings (
		id, provider_id, job_id, quote_id, receipt_id, settlement_id, capability_id, capability_version,
		gross_amount, gateway_fee, net_amount, status, created_at, matures_at, available_at,
		payout_requested_at, payout_reference, paid_at, payout_idempotency_key, payout_attempts,
		payout_last_attempt_at, payout_failure_reason, content_hash, payload
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
	ON CONFLICT (id) DO UPDATE SET
		status=$12, available_at=$15, payout_requested_at=$16, payout_reference=$17, paid_at=$18,
		payout_idempotency_key=$19, payout_attempts=$20, payout_last_attempt_at=$21,
		payout_failure_reason=$22, payload=$24
`

func scanEarning(row pgx.Row) (domain.ProviderEarning, error) {
	var e domain.ProviderEarning
	var grossAmount, gatewayFee, netAmount, payload []byte
	var status, contentHash string
	var payoutLastAttemptAt *time.Time
	if err := row.Scan(
		&e.ID, &e.ProviderID, &e.JobID, &e.QuoteID, &e.ReceiptID, &e.SettlementID, &e.CapabilityID, &e.CapabilityVersion,
		&grossAmount, &gatewayFee, &netAmount, &status, &e.CreatedAt, &e.MaturesAt, &e.AvailableAt,
		&e.PayoutRequestedAt, &e.PayoutReference, &e.PaidAt, &e.PayoutIdempotencyKey, &e.PayoutAttempts,
		&payoutLastAttemptAt, &e.PayoutFailureReason, &contentHash, &payload,
	); err != nil {
		return domain.ProviderEarning{}, err
	}
	e.Status = domain.EarningStatus(status)
	_ = json.Unmarshal(grossAmount, &e.GrossAmount)
	_ = json.Unmarshal(gatewayFee, &e.GatewayFee)
	_ = json.Unmarshal(netAmount, &e.NetAmount)
	if payoutLastAttemptAt != nil {
		e.PayoutLastAttemptAt = *payoutLastAttemptAt
	}
	if err := applyPayload(payload, &e); err != nil {
		return domain.ProviderEarning{}, err
	}
	// PayoutIdempotencyKey/PayoutAttempts/PayoutLastAttemptAt/
	// PayoutFailureReason are tagged json:"-" on domain.ProviderEarning, so
	// they are never part of payload and applyPayload cannot touch them;
	// they already hold the values scanned from their dedicated columns
	// above. Status IS part of the public payload, so re-assert the
	// dedicated column's value defensively, mirroring scanJob's convention.
	e.Status = domain.EarningStatus(status)
	return e, nil
}

func (s *Store) CreateEarning(ctx context.Context, e domain.ProviderEarning) (domain.ProviderEarning, bool, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO provider_earnings (
			id, provider_id, job_id, quote_id, receipt_id, settlement_id, capability_id, capability_version,
			gross_amount, gateway_fee, net_amount, status, created_at, matures_at, available_at,
			payout_requested_at, payout_reference, paid_at, payout_idempotency_key, payout_attempts,
			payout_last_attempt_at, payout_failure_reason, content_hash, payload
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
		ON CONFLICT (settlement_id) DO NOTHING
	`, earningWriteArgs(e)...)
	if err != nil {
		return domain.ProviderEarning{}, false, err
	}
	if tag.RowsAffected() > 0 {
		return e, true, nil
	}
	var storedHash string
	if err := s.pool.QueryRow(ctx, `SELECT content_hash FROM provider_earnings WHERE settlement_id=$1`, e.SettlementID).Scan(&storedHash); err != nil {
		return domain.ProviderEarning{}, false, err
	}
	if storedHash != earningContentHash(e) {
		return domain.ProviderEarning{}, false, domain.NewError(domain.ErrIdempotencyConflict, "an earning already exists for this settlement with different identity/economic fields", false)
	}
	existing, err := s.EarningBySettlement(ctx, e.SettlementID)
	if err != nil {
		return domain.ProviderEarning{}, false, err
	}
	return existing, false, nil
}

func (s *Store) GetEarning(ctx context.Context, id string) (domain.ProviderEarning, error) {
	e, err := scanEarning(s.pool.QueryRow(ctx, `SELECT `+earningColumns+` FROM provider_earnings WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ProviderEarning{}, store.ErrNotFound
	}
	return e, err
}

func (s *Store) EarningBySettlement(ctx context.Context, settlementID string) (domain.ProviderEarning, error) {
	e, err := scanEarning(s.pool.QueryRow(ctx, `SELECT `+earningColumns+` FROM provider_earnings WHERE settlement_id=$1`, settlementID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ProviderEarning{}, store.ErrNotFound
	}
	return e, err
}

func (s *Store) EarningsByProvider(ctx context.Context, providerID string) ([]domain.ProviderEarning, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+earningColumns+` FROM provider_earnings WHERE provider_id=$1 ORDER BY created_at DESC, id ASC`, providerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ProviderEarning
	for rows.Next() {
		e, err := scanEarning(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) EarningsMaturing(ctx context.Context, before time.Time, limit int) ([]domain.ProviderEarning, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+earningColumns+` FROM provider_earnings
		WHERE status='maturing' AND matures_at <= $1
		ORDER BY matures_at ASC, id ASC
		LIMIT $2
	`, before.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.ProviderEarning, 0, limit)
	for rows.Next() {
		e, err := scanEarning(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) EarningsAvailableForPayout(ctx context.Context, limit int) ([]domain.ProviderEarning, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+earningColumns+` FROM provider_earnings
		WHERE status='available'
		ORDER BY created_at ASC, id ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.ProviderEarning, 0, limit)
	for rows.Next() {
		e, err := scanEarning(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) EarningsPayoutPending(ctx context.Context, before time.Time, limit int) ([]domain.ProviderEarning, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+earningColumns+` FROM provider_earnings
		WHERE status='payout_pending' AND payout_requested_at <= $1
		ORDER BY payout_requested_at ASC, id ASC
		LIMIT $2
	`, before.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.ProviderEarning, 0, limit)
	for rows.Next() {
		e, err := scanEarning(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) SettledJobsMissingEarning(ctx context.Context, limit int) ([]domain.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobColumns+` FROM jobs
		WHERE economic_state='settled'
		  AND NOT EXISTS (SELECT 1 FROM provider_earnings WHERE provider_earnings.job_id = jobs.id)
		ORDER BY updated_at ASC, id ASC
		LIMIT $1
	`, limit)
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

func (s *Store) UpdateEarning(ctx context.Context, id string, fn func(domain.ProviderEarning, bool) (domain.ProviderEarning, error)) (domain.ProviderEarning, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.ProviderEarning{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "earning", id); err != nil {
		return domain.ProviderEarning{}, err
	}

	current, err := scanEarning(tx.QueryRow(ctx, `SELECT `+earningColumns+` FROM provider_earnings WHERE id=$1 FOR UPDATE`, id))
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		current = domain.ProviderEarning{}
		exists = false
		err = nil
	}
	if err != nil {
		return domain.ProviderEarning{}, err
	}
	next, err := fn(current, exists)
	if err != nil {
		return domain.ProviderEarning{}, err
	}
	// Identity/economic fields (ProviderID, SettlementID, GrossAmount,
	// GatewayFee, NetAmount, ...) are immutable for the lifetime of an
	// earning once created -- only lifecycle fields (Status, timestamps,
	// payout checkpoints) may legitimately change through UpdateEarning. The
	// dedicated economic SQL columns are already excluded from
	// upsertEarningSQL's ON CONFLICT SET clause, but payload is not (it must
	// stay current for lifecycle fields it also carries), so without this
	// check a callback that changes economic content would still corrupt
	// what a later scanEarning reads back via applyPayload. A callback that
	// changes economic content is always a bug, not a valid state
	// transition, so it is rejected here rather than silently persisted.
	if exists && earningContentHash(current) != earningContentHash(next) {
		return domain.ProviderEarning{}, domain.NewError(domain.ErrIdempotencyConflict, "earning update must not change identity/economic fields", false)
	}
	if _, err := tx.Exec(ctx, upsertEarningSQL, earningWriteArgs(next)...); err != nil {
		return domain.ProviderEarning{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ProviderEarning{}, err
	}
	return next, nil
}
