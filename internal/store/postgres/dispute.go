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

const disputeColumns = `id, principal_id, provider_id, job_id, quote_id, capability_id, receipt_id, settlement_id, earning_id, charged_amount, original_refund_amount, reason, description, evidence, idempotency_key, review_status, economic_state, outcome, reviewer_id, reason_rejected, dispute_policy_hash, content_hash, opened_at, under_review_at, resolved_at, updated_at, payload`

// disputeContentHash summarizes the identity+economic fields of a Dispute
// that must never change once created -- deliberately excluding lifecycle
// fields (ReviewStatus, EconomicState, Outcome, ReviewerID,
// ReasonRejected, timestamps) which legitimately change over the
// dispute's life. Mirrors earningContentHash's role.
func disputeContentHash(d domain.Dispute) string {
	encoded, _ := json.Marshal(struct {
		PrincipalID, ProviderID, JobID, QuoteID, CapabilityID, ReceiptID, SettlementID, EarningID string
		ChargedAmount, OriginalRefundAmount                                                       domain.Money
		Reason, Description                                                                       string
		Evidence                                                                                  []domain.DisputeEvidence
		DisputePolicyHash                                                                         string
	}{
		d.PrincipalID, d.ProviderID, d.JobID, d.QuoteID, d.CapabilityID, d.ReceiptID, d.SettlementID, d.EarningID,
		d.ChargedAmount, d.OriginalRefundAmount, d.Reason, d.Description, d.Evidence, d.DisputePolicyHash,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func disputeWriteArgs(d domain.Dispute) []any {
	return []any{
		d.ID, d.PrincipalID, d.ProviderID, d.JobID, d.QuoteID, d.CapabilityID, d.ReceiptID, d.SettlementID, d.EarningID,
		mustMarshal(d.ChargedAmount), mustMarshal(d.OriginalRefundAmount), d.Reason, d.Description, mustMarshal(d.Evidence),
		d.IdempotencyKey, string(d.ReviewStatus), string(d.EconomicState), string(d.Outcome), d.ReviewerID, d.ReasonRejected,
		d.DisputePolicyHash, disputeContentHash(d), d.OpenedAt, d.UnderReviewAt, d.ResolvedAt, d.UpdatedAt, mustMarshal(d),
	}
}

const insertDisputeSQL = `
	INSERT INTO disputes (
		id, principal_id, provider_id, job_id, quote_id, capability_id, receipt_id, settlement_id, earning_id,
		charged_amount, original_refund_amount, reason, description, evidence, idempotency_key,
		review_status, economic_state, outcome, reviewer_id, reason_rejected, dispute_policy_hash,
		content_hash, opened_at, under_review_at, resolved_at, updated_at, payload
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)
	ON CONFLICT (job_id) DO NOTHING
`

const upsertDisputeSQL = `
	INSERT INTO disputes (
		id, principal_id, provider_id, job_id, quote_id, capability_id, receipt_id, settlement_id, earning_id,
		charged_amount, original_refund_amount, reason, description, evidence, idempotency_key,
		review_status, economic_state, outcome, reviewer_id, reason_rejected, dispute_policy_hash,
		content_hash, opened_at, under_review_at, resolved_at, updated_at, payload
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)
	ON CONFLICT (id) DO UPDATE SET
		review_status=$16, economic_state=$17, outcome=$18, reviewer_id=$19, reason_rejected=$20,
		under_review_at=$24, resolved_at=$25, updated_at=$26, payload=$27
`

func scanDispute(row pgx.Row) (domain.Dispute, error) {
	var d domain.Dispute
	var chargedAmount, originalRefundAmount, evidence, payload []byte
	var reviewStatus, economicState, outcome, contentHash string
	if err := row.Scan(
		&d.ID, &d.PrincipalID, &d.ProviderID, &d.JobID, &d.QuoteID, &d.CapabilityID, &d.ReceiptID, &d.SettlementID, &d.EarningID,
		&chargedAmount, &originalRefundAmount, &d.Reason, &d.Description, &evidence, &d.IdempotencyKey,
		&reviewStatus, &economicState, &outcome, &d.ReviewerID, &d.ReasonRejected, &d.DisputePolicyHash,
		&contentHash, &d.OpenedAt, &d.UnderReviewAt, &d.ResolvedAt, &d.UpdatedAt, &payload,
	); err != nil {
		return domain.Dispute{}, err
	}
	_ = json.Unmarshal(chargedAmount, &d.ChargedAmount)
	_ = json.Unmarshal(originalRefundAmount, &d.OriginalRefundAmount)
	_ = json.Unmarshal(evidence, &d.Evidence)
	if err := applyPayload(payload, &d); err != nil {
		return domain.Dispute{}, err
	}
	// review_status/economic_state/outcome are part of the public payload,
	// so re-assert the dedicated columns' values defensively, mirroring
	// scanEarning/scanJob's convention.
	d.ReviewStatus = domain.DisputeReviewStatus(reviewStatus)
	d.EconomicState = domain.DisputeEconomicState(economicState)
	d.Outcome = domain.DisputeOutcome(outcome)
	return d, nil
}

// OpenDispute serializes concurrent first-writers for the same job_id via
// an advisory transaction lock (a row cannot be SELECT...FOR UPDATE locked
// before it exists), then row-locks the disputed ProviderEarning -- found
// by settlementID, its real UNIQUE identity, never by job_id (a plain
// index with no uniqueness guarantee) -- before calling build, so the
// earning's status observed by build can never change concurrently
// underneath the freeze/defer decision. A plain SELECT...FOR UPDATE
// suffices here (no separate advisory lock, mirroring UpdateEarning's own
// convention) because the earning row is guaranteed to already exist by
// the time a Job can be disputed. Both the new dispute row and the
// earning's next state commit in the same transaction.
func (s *Store) OpenDispute(ctx context.Context, jobID, settlementID string, build func(domain.ProviderEarning, bool) (domain.Dispute, domain.ProviderEarning, error)) (domain.Dispute, domain.ProviderEarning, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := lockTransactionKey(ctx, tx, "dispute", jobID); err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}

	existing, err := scanDispute(tx.QueryRow(ctx, `SELECT `+disputeColumns+` FROM disputes WHERE job_id=$1 FOR UPDATE`, jobID))
	if err == nil {
		return existing, domain.ProviderEarning{}, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}

	earning, err := scanEarning(tx.QueryRow(ctx, `SELECT `+earningColumns+` FROM provider_earnings WHERE settlement_id=$1 FOR UPDATE`, settlementID))
	earningExists := true
	if errors.Is(err, pgx.ErrNoRows) {
		earning = domain.ProviderEarning{}
		earningExists = false
		err = nil
	}
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}

	dispute, nextEarning, err := build(earning, earningExists)
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}

	tag, err := tx.Exec(ctx, insertDisputeSQL, disputeWriteArgs(dispute)...)
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}
	if tag.RowsAffected() == 0 {
		// Lost an extremely unlikely race against the advisory lock (e.g.
		// a row inserted outside this code path). Behave exactly like the
		// already-exists branch above rather than erroring.
		existing, err := scanDispute(tx.QueryRow(ctx, `SELECT `+disputeColumns+` FROM disputes WHERE job_id=$1`, jobID))
		if err != nil {
			return domain.Dispute{}, domain.ProviderEarning{}, false, err
		}
		return existing, domain.ProviderEarning{}, false, nil
	}
	if earningExists {
		if nextEarning.ID != earning.ID {
			return domain.Dispute{}, domain.ProviderEarning{}, false, domain.NewError(domain.ErrIdempotencyConflict, "dispute open must not change the earning id", false)
		}
		if earningContentHash(earning) != earningContentHash(nextEarning) {
			return domain.Dispute{}, domain.ProviderEarning{}, false, domain.NewError(domain.ErrIdempotencyConflict, "dispute open must not change identity/economic earning fields", false)
		}
		if _, err := tx.Exec(ctx, upsertEarningSQL, earningWriteArgs(nextEarning)...); err != nil {
			return domain.Dispute{}, domain.ProviderEarning{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}
	return dispute, nextEarning, true, nil
}

func (s *Store) GetDispute(ctx context.Context, id string) (domain.Dispute, error) {
	d, err := scanDispute(s.pool.QueryRow(ctx, `SELECT `+disputeColumns+` FROM disputes WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Dispute{}, store.ErrNotFound
	}
	return d, err
}

// GetDisputeWithEarning reads the dispute and its earning as a single
// atomic snapshot. Plain READ COMMITTED (the default -- see ResolveDispute's
// own doc comment on this package's convention) takes a FRESH snapshot per
// statement, so two separate SELECTs on that isolation level could observe
// a concurrent ResolveDispute's write applied to one row but not yet to the
// other -- exactly the "no observable intermediate state" invariant this
// method exists to close. REPEATABLE READ fixes one snapshot for the whole
// transaction, so both SELECTs below see the same point in time regardless
// of what commits in between.
func (s *Store) GetDisputeWithEarning(ctx context.Context, id string) (domain.Dispute, domain.ProviderEarning, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	d, err := scanDispute(tx.QueryRow(ctx, `SELECT `+disputeColumns+` FROM disputes WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Dispute{}, domain.ProviderEarning{}, false, store.ErrNotFound
	}
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}
	e, err := scanEarning(tx.QueryRow(ctx, `SELECT `+earningColumns+` FROM provider_earnings WHERE id=$1`, d.EarningID))
	if errors.Is(err, pgx.ErrNoRows) {
		return d, domain.ProviderEarning{}, false, nil
	}
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}
	return d, e, true, nil
}

func (s *Store) DisputeByJob(ctx context.Context, jobID string) (domain.Dispute, error) {
	d, err := scanDispute(s.pool.QueryRow(ctx, `SELECT `+disputeColumns+` FROM disputes WHERE job_id=$1`, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Dispute{}, store.ErrNotFound
	}
	return d, err
}

func (s *Store) DisputeByIdempotencyKey(ctx context.Context, principalID, key string) (domain.Dispute, error) {
	d, err := scanDispute(s.pool.QueryRow(ctx, `
		SELECT `+disputeColumns+` FROM disputes
		WHERE principal_id=$1 AND idempotency_key=$2
		ORDER BY opened_at DESC
		LIMIT 1
	`, principalID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Dispute{}, store.ErrNotFound
	}
	return d, err
}

func (s *Store) DisputesByPrincipal(ctx context.Context, principalID string) ([]domain.Dispute, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+disputeColumns+` FROM disputes WHERE principal_id=$1 ORDER BY opened_at DESC, id ASC`, principalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.Dispute, 0)
	for rows.Next() {
		d, err := scanDispute(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) DisputesByProvider(ctx context.Context, providerID string) ([]domain.Dispute, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+disputeColumns+` FROM disputes WHERE provider_id=$1 ORDER BY opened_at DESC, id ASC`, providerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.Dispute, 0)
	for rows.Next() {
		d, err := scanDispute(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) DisputesUnderReview(ctx context.Context, limit int) ([]domain.Dispute, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+disputeColumns+` FROM disputes
		WHERE review_status IN ('opened','under_review')
		ORDER BY opened_at ASC, id ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.Dispute, 0, limit)
	for rows.Next() {
		d, err := scanDispute(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) DisputesForRecovery(ctx context.Context, updatedBefore time.Time, limit int) ([]domain.Dispute, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+disputeColumns+` FROM disputes
		WHERE economic_state = 'pending_payout_resolution' AND updated_at <= $1
		ORDER BY updated_at ASC, id ASC
		LIMIT $2
	`, updatedBefore.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.Dispute, 0, limit)
	for rows.Next() {
		d, err := scanDispute(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) UpdateDispute(ctx context.Context, id string, fn func(domain.Dispute, bool) (domain.Dispute, error)) (domain.Dispute, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Dispute{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "dispute", id); err != nil {
		return domain.Dispute{}, err
	}

	current, err := scanDispute(tx.QueryRow(ctx, `SELECT `+disputeColumns+` FROM disputes WHERE id=$1 FOR UPDATE`, id))
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		current = domain.Dispute{}
		exists = false
		err = nil
	}
	if err != nil {
		return domain.Dispute{}, err
	}
	next, err := fn(current, exists)
	if err != nil {
		return domain.Dispute{}, err
	}
	if exists {
		if next.ID != current.ID {
			return domain.Dispute{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change the dispute id", false)
		}
		if disputeContentHash(current) != disputeContentHash(next) {
			return domain.Dispute{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change identity/economic fields", false)
		}
	}
	if _, err := tx.Exec(ctx, upsertDisputeSQL, disputeWriteArgs(next)...); err != nil {
		return domain.Dispute{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Dispute{}, err
	}
	return next, nil
}

// UpdateDisputeAndEarning row-locks the dispute (by id) and the
// ProviderEarning it references (by the dispute's own immutable
// EarningID, never re-derived from job_id) in that order, then commits
// fn's result to both in one transaction.
func (s *Store) UpdateDisputeAndEarning(ctx context.Context, disputeID string, fn func(domain.Dispute, domain.ProviderEarning, bool) (domain.Dispute, domain.ProviderEarning, error)) (domain.Dispute, domain.ProviderEarning, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "dispute", disputeID); err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, err
	}

	currentDispute, err := scanDispute(tx.QueryRow(ctx, `SELECT `+disputeColumns+` FROM disputes WHERE id=$1 FOR UPDATE`, disputeID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Dispute{}, domain.ProviderEarning{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, err
	}

	// Locked by the dispute's own immutable EarningID, never re-derived
	// from job_id.
	currentEarning, err := scanEarning(tx.QueryRow(ctx, `SELECT `+earningColumns+` FROM provider_earnings WHERE id=$1 FOR UPDATE`, currentDispute.EarningID))
	earningExists := true
	if errors.Is(err, pgx.ErrNoRows) {
		currentEarning = domain.ProviderEarning{}
		earningExists = false
		err = nil
	}
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, err
	}

	nextDispute, nextEarning, err := fn(currentDispute, currentEarning, earningExists)
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, err
	}
	if nextDispute.ID != currentDispute.ID {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change the dispute id", false)
	}
	if disputeContentHash(currentDispute) != disputeContentHash(nextDispute) {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change identity/economic fields", false)
	}
	if _, err := tx.Exec(ctx, upsertDisputeSQL, disputeWriteArgs(nextDispute)...); err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, err
	}
	if earningExists {
		if nextEarning.ID != currentEarning.ID {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change the earning id", false)
		}
		if earningContentHash(currentEarning) != earningContentHash(nextEarning) {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change identity/economic earning fields", false)
		}
		if _, err := tx.Exec(ctx, upsertEarningSQL, earningWriteArgs(nextEarning)...); err != nil {
			return domain.Dispute{}, domain.ProviderEarning{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, err
	}
	return nextDispute, nextEarning, nil
}

// ResolveDispute row-locks the dispute (by id), the ProviderEarning it
// references (by the dispute's own immutable EarningID, never re-derived
// from job_id), and the principal's Account -- in that order (dispute,
// earning, account), consistent with UpdateDisputeAndEarning's (dispute,
// earning) order and never conflicting with UpdateJobAndAccount's
// (account, job) order since no code path locks both earning and job
// together -- then commits fn's result to all three in one transaction.
// A principal-win's earning reversal and account credit therefore can
// never be observed partially applied.
func (s *Store) ResolveDispute(ctx context.Context, disputeID, principalID string, seed domain.Account, fn func(domain.Dispute, domain.ProviderEarning, bool, domain.Account, bool) (domain.Dispute, domain.ProviderEarning, domain.Account, error)) (domain.Dispute, domain.ProviderEarning, domain.Account, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "dispute", disputeID); err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
	}

	currentDispute, err := scanDispute(tx.QueryRow(ctx, `SELECT `+disputeColumns+` FROM disputes WHERE id=$1 FOR UPDATE`, disputeID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
	}

	// Locked by the dispute's own immutable EarningID, never re-derived
	// from job_id.
	currentEarning, err := scanEarning(tx.QueryRow(ctx, `SELECT `+earningColumns+` FROM provider_earnings WHERE id=$1 FOR UPDATE`, currentDispute.EarningID))
	earningExists := true
	if errors.Is(err, pgx.ErrNoRows) {
		currentEarning = domain.ProviderEarning{}
		earningExists = false
		err = nil
	}
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
	}

	if err := lockTransactionKey(ctx, tx, "account", principalID); err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
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
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
	default:
		_ = json.Unmarshal(balance, &account.Balance)
		_ = json.Unmarshal(spendPolicy, &account.SpendPolicy)
		if err := applyPayload(payload, &account); err != nil {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
		}
	}
	account.PrincipalID = principalID

	nextDispute, nextEarning, nextAccount, err := fn(currentDispute, currentEarning, earningExists, account, accountExists)
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
	}
	if nextDispute.ID != currentDispute.ID {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change the dispute id", false)
	}
	if disputeContentHash(currentDispute) != disputeContentHash(nextDispute) {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change identity/economic fields", false)
	}
	if nextAccount.PrincipalID != principalID {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, store.ErrConflict
	}
	if _, err := tx.Exec(ctx, upsertDisputeSQL, disputeWriteArgs(nextDispute)...); err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
	}
	if earningExists {
		if nextEarning.ID != currentEarning.ID {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change the earning id", false)
		}
		if earningContentHash(currentEarning) != earningContentHash(nextEarning) {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change identity/economic earning fields", false)
		}
		if _, err := tx.Exec(ctx, upsertEarningSQL, earningWriteArgs(nextEarning)...); err != nil {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO accounts (principal_id, balance, spend_policy, payload)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (principal_id) DO UPDATE SET
			balance=$2, spend_policy=$3, payload=$4
	`, principalID, mustMarshal(nextAccount.Balance), mustMarshal(nextAccount.SpendPolicy), mustMarshal(nextAccount)); err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
	}
	return nextDispute, nextEarning, nextAccount, nil
}
