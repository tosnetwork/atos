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

// --- OpenTask ---

const openTaskColumns = `id, principal_id, title, status, expires_at, accepted_proposal_id, bound_quote_id, bound_job_id, idempotency_key, created_at, updated_at, payload`

const putOpenTaskSQL = `
	INSERT INTO open_tasks (` + openTaskColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	ON CONFLICT (id) DO NOTHING
`

const upsertOpenTaskSQL = `
	INSERT INTO open_tasks (` + openTaskColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	ON CONFLICT (id) DO UPDATE SET
		status=$4, expires_at=$5, accepted_proposal_id=$6, bound_quote_id=$7, bound_job_id=$8, updated_at=$11, payload=$12
`

func openTaskWriteArgs(t domain.OpenTask) []any {
	return []any{
		t.ID, t.PrincipalID, t.Title, string(t.Status), t.ExpiresAt,
		t.AcceptedProposalID, t.BoundQuoteID, t.BoundJobID, t.PublicationIdempotencyKey,
		t.CreatedAt, t.UpdatedAt, mustMarshal(t),
	}
}

// scanOpenTask scans the indexed columns then overlays the full jsonb
// payload, exactly like scanJob/GetQuote -- the payload is authoritative
// (it round-trips Description/Input/RequestedTrustMode/ProofRequirements/
// MaxTotal, none of which have their own column), the explicit columns
// exist only for indexing/filtering.
func scanOpenTask(row pgx.Row) (domain.OpenTask, error) {
	var t domain.OpenTask
	var status string
	var payload []byte
	if err := row.Scan(
		&t.ID, &t.PrincipalID, &t.Title, &status, &t.ExpiresAt,
		&t.AcceptedProposalID, &t.BoundQuoteID, &t.BoundJobID, &t.PublicationIdempotencyKey,
		&t.CreatedAt, &t.UpdatedAt, &payload,
	); err != nil {
		return domain.OpenTask{}, err
	}
	t.Status = domain.OpenTaskStatus(status)
	if err := applyPayload(payload, &t); err != nil {
		return domain.OpenTask{}, err
	}
	return t, nil
}

func (s *Store) PutOpenTask(ctx context.Context, t domain.OpenTask) error {
	_, err := s.pool.Exec(ctx, putOpenTaskSQL, openTaskWriteArgs(t)...)
	return err
}

func (s *Store) GetOpenTask(ctx context.Context, id string) (domain.OpenTask, error) {
	t, err := scanOpenTask(s.pool.QueryRow(ctx, `SELECT `+openTaskColumns+` FROM open_tasks WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.OpenTask{}, store.ErrNotFound
	}
	return t, err
}

func (s *Store) OpenTaskByIdempotencyKey(ctx context.Context, principalID, key string) (domain.OpenTask, error) {
	t, err := scanOpenTask(s.pool.QueryRow(ctx, `
		SELECT `+openTaskColumns+` FROM open_tasks
		WHERE principal_id=$1 AND idempotency_key=$2 AND idempotency_key <> ''
		ORDER BY created_at DESC
		LIMIT 1
	`, principalID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.OpenTask{}, store.ErrNotFound
	}
	return t, err
}

func (s *Store) OpenTasksByPrincipal(ctx context.Context, principalID string) ([]domain.OpenTask, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+openTaskColumns+` FROM open_tasks WHERE principal_id=$1 ORDER BY created_at DESC
	`, principalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.OpenTask
	for rows.Next() {
		t, err := scanOpenTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) ListPublicOpenTasks(ctx context.Context, limit int) ([]domain.OpenTask, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+openTaskColumns+` FROM open_tasks WHERE status='open' ORDER BY created_at DESC LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.OpenTask
	for rows.Next() {
		t, err := scanOpenTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) UpdateOpenTask(ctx context.Context, id string, fn func(domain.OpenTask, bool) (domain.OpenTask, error)) (domain.OpenTask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.OpenTask{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "open-task", id); err != nil {
		return domain.OpenTask{}, err
	}

	current, err := scanOpenTask(tx.QueryRow(ctx, `SELECT `+openTaskColumns+` FROM open_tasks WHERE id=$1 FOR UPDATE`, id))
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		current = domain.OpenTask{}
		exists = false
		err = nil
	}
	if err != nil {
		return domain.OpenTask{}, err
	}
	next, err := fn(current, exists)
	if err != nil {
		return domain.OpenTask{}, err
	}
	if _, err := tx.Exec(ctx, upsertOpenTaskSQL, openTaskWriteArgs(next)...); err != nil {
		return domain.OpenTask{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.OpenTask{}, err
	}
	return next, nil
}

// --- OpenTaskProposal ---

const proposalColumns = `id, task_id, provider_id, capability_id, capability_version, idempotency_key, withdrawn_at, created_at, updated_at, payload`

const putProposalSQL = `
	INSERT INTO open_task_proposals (` + proposalColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	ON CONFLICT (id) DO NOTHING
`

const upsertProposalSQL = `
	INSERT INTO open_task_proposals (` + proposalColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	ON CONFLICT (id) DO UPDATE SET withdrawn_at=$7, updated_at=$9, payload=$10
`

func proposalWriteArgs(p domain.OpenTaskProposal) []any {
	return []any{
		p.ID, p.TaskID, p.ProviderID, p.CapabilityID, p.CapabilityVersion,
		p.ProposalIdempotencyKey, p.WithdrawnAt, p.CreatedAt, p.UpdatedAt, mustMarshal(p),
	}
}

func scanProposal(row pgx.Row) (domain.OpenTaskProposal, error) {
	var p domain.OpenTaskProposal
	var payload []byte
	if err := row.Scan(
		&p.ID, &p.TaskID, &p.ProviderID, &p.CapabilityID, &p.CapabilityVersion,
		&p.ProposalIdempotencyKey, &p.WithdrawnAt, &p.CreatedAt, &p.UpdatedAt, &payload,
	); err != nil {
		return domain.OpenTaskProposal{}, err
	}
	if err := applyPayload(payload, &p); err != nil {
		return domain.OpenTaskProposal{}, err
	}
	return p, nil
}

func (s *Store) PutOpenTaskProposal(ctx context.Context, p domain.OpenTaskProposal) error {
	_, err := s.pool.Exec(ctx, putProposalSQL, proposalWriteArgs(p)...)
	return err
}

func (s *Store) GetOpenTaskProposal(ctx context.Context, id string) (domain.OpenTaskProposal, error) {
	p, err := scanProposal(s.pool.QueryRow(ctx, `SELECT `+proposalColumns+` FROM open_task_proposals WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.OpenTaskProposal{}, store.ErrNotFound
	}
	return p, err
}

func (s *Store) OpenTaskProposalByIdempotencyKey(ctx context.Context, providerID, key string) (domain.OpenTaskProposal, error) {
	p, err := scanProposal(s.pool.QueryRow(ctx, `
		SELECT `+proposalColumns+` FROM open_task_proposals
		WHERE provider_id=$1 AND idempotency_key=$2 AND idempotency_key <> ''
		ORDER BY created_at DESC
		LIMIT 1
	`, providerID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.OpenTaskProposal{}, store.ErrNotFound
	}
	return p, err
}

func (s *Store) ProposalsByTask(ctx context.Context, taskID string) ([]domain.OpenTaskProposal, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+proposalColumns+` FROM open_task_proposals WHERE task_id=$1 ORDER BY created_at DESC
	`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.OpenTaskProposal
	for rows.Next() {
		p, err := scanProposal(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) UpdateOpenTaskProposal(ctx context.Context, id string, fn func(domain.OpenTaskProposal, bool) (domain.OpenTaskProposal, error)) (domain.OpenTaskProposal, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.OpenTaskProposal{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "open-task-proposal", id); err != nil {
		return domain.OpenTaskProposal{}, err
	}

	current, err := scanProposal(tx.QueryRow(ctx, `SELECT `+proposalColumns+` FROM open_task_proposals WHERE id=$1 FOR UPDATE`, id))
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		current = domain.OpenTaskProposal{}
		exists = false
		err = nil
	}
	if err != nil {
		return domain.OpenTaskProposal{}, err
	}
	next, err := fn(current, exists)
	if err != nil {
		return domain.OpenTaskProposal{}, err
	}
	if exists {
		if next.ID != current.ID || next.TaskID != current.TaskID || next.ProviderID != current.ProviderID ||
			next.CapabilityID != current.CapabilityID || next.CapabilityVersion != current.CapabilityVersion ||
			next.ProposalIdempotencyKey != current.ProposalIdempotencyKey {
			return domain.OpenTaskProposal{}, domain.NewError(domain.ErrIdempotencyConflict, "proposal update must not change identity fields", false)
		}
	}
	if _, err := tx.Exec(ctx, upsertProposalSQL, proposalWriteArgs(next)...); err != nil {
		return domain.OpenTaskProposal{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.OpenTaskProposal{}, err
	}
	return next, nil
}

// WithdrawOpenTaskProposal -- see the interface doc comment
// (internal/store/store.go). Locks "open-task"+task_id BEFORE
// "open-task-proposal"+proposalID -- the SAME global lock order
// OpenAcceptanceOperation uses (task, then proposal), deliberately, not
// the reverse: two transactions that each acquire the same two advisory
// locks in opposite orders can deadlock each other in PostgreSQL (A holds
// task and waits on proposal while B holds proposal and waits on task),
// which an earlier version of this method did. task_id itself is not
// known until the proposal row is read, so an initial, LOCK-FREE preview
// read establishes it first -- safe because TaskID is one of the identity
// fields UpdateOpenTaskProposal enforces as immutable once a proposal
// exists, so it cannot change out from under this preview between the
// unlocked read and the locked one below.
func (s *Store) WithdrawOpenTaskProposal(ctx context.Context, proposalID, providerID string) (domain.OpenTaskProposal, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.OpenTaskProposal{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	preview, err := scanProposal(tx.QueryRow(ctx, `SELECT `+proposalColumns+` FROM open_task_proposals WHERE id=$1`, proposalID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.OpenTaskProposal{}, store.ErrNotFound
	}
	if err != nil {
		return domain.OpenTaskProposal{}, err
	}

	if err := lockTransactionKey(ctx, tx, "open-task", preview.TaskID); err != nil {
		return domain.OpenTaskProposal{}, err
	}
	if err := lockTransactionKey(ctx, tx, "open-task-proposal", proposalID); err != nil {
		return domain.OpenTaskProposal{}, err
	}

	p, err := scanProposal(tx.QueryRow(ctx, `SELECT `+proposalColumns+` FROM open_task_proposals WHERE id=$1 FOR UPDATE`, proposalID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.OpenTaskProposal{}, store.ErrNotFound
	}
	if err != nil {
		return domain.OpenTaskProposal{}, err
	}
	if p.ProviderID != providerID {
		return domain.OpenTaskProposal{}, domain.NewError(domain.ErrPermissionDenied, "not the submitting provider", false)
	}
	if p.WithdrawnAt != nil {
		if err := tx.Commit(ctx); err != nil {
			return domain.OpenTaskProposal{}, err
		}
		return p, nil
	}

	task, err := scanOpenTask(tx.QueryRow(ctx, `SELECT `+openTaskColumns+` FROM open_tasks WHERE id=$1 FOR UPDATE`, p.TaskID))
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return domain.OpenTaskProposal{}, err
	}
	if err == nil && task.AcceptedProposalID == proposalID {
		return domain.OpenTaskProposal{}, domain.NewError(domain.ErrOpenTaskNotOpen, "cannot withdraw a proposal that has already been accepted", false)
	}

	now := time.Now().UTC()
	p.WithdrawnAt = &now
	p.UpdatedAt = now
	if _, err := tx.Exec(ctx, upsertProposalSQL, proposalWriteArgs(p)...); err != nil {
		return domain.OpenTaskProposal{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.OpenTaskProposal{}, err
	}
	return p, nil
}

// --- AcceptanceOperation ---

const acceptanceColumns = `id, task_id, proposal_id, principal_id, provider_id, capability_id, capability_version, checkpoint, idempotency_key, quote_id, job_id, failure_reason, created_at, completed_at, updated_at`

const insertAcceptanceSQL = `
	INSERT INTO acceptance_operations (` + acceptanceColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
	ON CONFLICT (principal_id, idempotency_key) DO NOTHING
`

const upsertAcceptanceSQL = `
	INSERT INTO acceptance_operations (` + acceptanceColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
	ON CONFLICT (principal_id, idempotency_key) DO UPDATE SET
		checkpoint=$8, quote_id=$10, job_id=$11, failure_reason=$12, completed_at=$14, updated_at=$15
`

func acceptanceWriteArgs(op domain.AcceptanceOperation) []any {
	return []any{
		op.ID, op.TaskID, op.ProposalID, op.PrincipalID, op.ProviderID, op.CapabilityID, op.CapabilityVersion,
		string(op.Checkpoint), op.IdempotencyKey, op.QuoteID, op.JobID, op.FailureReason,
		op.CreatedAt, op.CompletedAt, op.UpdatedAt,
	}
}

func scanAcceptance(row pgx.Row) (domain.AcceptanceOperation, error) {
	var op domain.AcceptanceOperation
	var checkpoint string
	if err := row.Scan(
		&op.ID, &op.TaskID, &op.ProposalID, &op.PrincipalID, &op.ProviderID, &op.CapabilityID, &op.CapabilityVersion,
		&checkpoint, &op.IdempotencyKey, &op.QuoteID, &op.JobID, &op.FailureReason,
		&op.CreatedAt, &op.CompletedAt, &op.UpdatedAt,
	); err != nil {
		return domain.AcceptanceOperation{}, err
	}
	op.Checkpoint = domain.AcceptanceCheckpoint(checkpoint)
	return op, nil
}

// acceptanceOperationContentHash -- see the identical function in
// internal/store/memory/open_task.go's doc comment for why this is
// duplicated per-backend rather than shared across the service/store
// package boundary (mirrors signerOperationContentHash's own precedent).
func acceptanceOperationContentHash(op domain.AcceptanceOperation) string {
	encoded, _ := json.Marshal(struct {
		TaskID, ProposalID, PrincipalID, ProviderID string
		CapabilityID, CapabilityVersion             string
		IdempotencyKey                              string
	}{
		op.TaskID, op.ProposalID, op.PrincipalID, op.ProviderID,
		op.CapabilityID, op.CapabilityVersion,
		op.IdempotencyKey,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// hasNonTerminalAcceptanceOperationTx mirrors hasNonTerminalSignerOperationTx.
func hasNonTerminalAcceptanceOperationTx(ctx context.Context, tx pgx.Tx, taskID string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM acceptance_operations WHERE task_id=$1 AND checkpoint NOT IN ('completed','failed')
		)
	`, taskID).Scan(&exists)
	return exists, err
}

// OpenAcceptanceOperation -- see the interface doc comment
// (internal/store/store.go) and the memory implementation's doc comment for
// why the in-flight guard and build() must run BEFORE the idempotency-key
// dedup/conflict check, not after, and why a genuine sequential replay must
// instead be caught by the caller (service.OpenTaskService.Accept) via
// AcceptanceOperationByIdempotencyKey before this method is ever called.
//
// Three advisory locks are held for the whole transaction: the
// task-scoped "open-task-acceptance" in-flight guard (mirroring
// OpenSignerOperationForCapability's capability-scoped lock discipline),
// then "open-task"+taskID and "open-task-proposal"+proposalID -- the
// SAME two locks UpdateOpenTask/WithdrawOpenTaskProposal/
// CompleteAcceptance/FailAcceptance use, which is what makes this method
// race-free against a concurrent Cancel or Withdraw, not merely against a
// concurrent Accept. Both the task and proposal rows are read FOR UPDATE
// under those locks, not via a plain SELECT.
func (s *Store) OpenAcceptanceOperation(
	ctx context.Context, taskID, proposalID string,
	build func(task domain.OpenTask, proposal domain.OpenTaskProposal) (domain.AcceptanceOperation, error),
) (domain.AcceptanceOperation, domain.OpenTask, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := lockTransactionKey(ctx, tx, "open-task-acceptance", taskID); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}

	inFlight, err := hasNonTerminalAcceptanceOperationTx(ctx, tx, taskID)
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}
	if inFlight {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, domain.NewError(domain.ErrOpenTaskAcceptanceInProgress,
			"an acceptance is already in progress for this open task", true)
	}

	if err := lockTransactionKey(ctx, tx, "open-task", taskID); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}
	task, err := scanOpenTask(tx.QueryRow(ctx, `SELECT `+openTaskColumns+` FROM open_tasks WHERE id=$1 FOR UPDATE`, taskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, store.ErrNotFound
	}
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}

	if err := lockTransactionKey(ctx, tx, "open-task-proposal", proposalID); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}
	proposal, err := scanProposal(tx.QueryRow(ctx, `SELECT `+proposalColumns+` FROM open_task_proposals WHERE id=$1 FOR UPDATE`, proposalID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, store.ErrNotFound
	}
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}

	op, err := build(task, proposal)
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}

	if err := lockTransactionKey(ctx, tx, "open-task-acceptance-op", op.PrincipalID, op.IdempotencyKey); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}

	existing, err := scanAcceptance(tx.QueryRow(ctx, `
		SELECT `+acceptanceColumns+` FROM acceptance_operations
		WHERE principal_id=$1 AND idempotency_key=$2 FOR UPDATE
	`, op.PrincipalID, op.IdempotencyKey))
	if err == nil {
		if acceptanceOperationContentHash(existing) != acceptanceOperationContentHash(op) {
			return domain.AcceptanceOperation{}, domain.OpenTask{}, false, domain.NewError(domain.ErrIdempotencyConflict,
				"idempotency_key reused with different acceptance content", false)
		}
		return existing, task, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}

	tag, err := tx.Exec(ctx, insertAcceptanceSQL, acceptanceWriteArgs(op)...)
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}
	if tag.RowsAffected() == 0 {
		existing, err := scanAcceptance(tx.QueryRow(ctx, `
			SELECT `+acceptanceColumns+` FROM acceptance_operations WHERE principal_id=$1 AND idempotency_key=$2
		`, op.PrincipalID, op.IdempotencyKey))
		if err != nil {
			return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
		}
		return existing, task, false, nil
	}

	task.AcceptedProposalID = op.ProposalID
	task.Status = domain.OpenTaskAccepted
	task.UpdatedAt = op.CreatedAt
	if _, err := tx.Exec(ctx, upsertOpenTaskSQL, openTaskWriteArgs(task)...); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}
	return op, task, true, nil
}

func (s *Store) GetAcceptanceOperation(ctx context.Context, id string) (domain.AcceptanceOperation, error) {
	op, err := scanAcceptance(s.pool.QueryRow(ctx, `SELECT `+acceptanceColumns+` FROM acceptance_operations WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AcceptanceOperation{}, store.ErrNotFound
	}
	return op, err
}

func (s *Store) AcceptanceOperationByIdempotencyKey(ctx context.Context, principalID, key string) (domain.AcceptanceOperation, error) {
	op, err := scanAcceptance(s.pool.QueryRow(ctx, `
		SELECT `+acceptanceColumns+` FROM acceptance_operations WHERE principal_id=$1 AND idempotency_key=$2
	`, principalID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AcceptanceOperation{}, store.ErrNotFound
	}
	return op, err
}

func (s *Store) AcceptanceOperationByTask(ctx context.Context, taskID string) (domain.AcceptanceOperation, bool, error) {
	op, err := scanAcceptance(s.pool.QueryRow(ctx, `
		SELECT `+acceptanceColumns+` FROM acceptance_operations
		WHERE task_id=$1
		ORDER BY updated_at DESC, id ASC
		LIMIT 1
	`, taskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AcceptanceOperation{}, false, nil
	}
	if err != nil {
		return domain.AcceptanceOperation{}, false, err
	}
	return op, true, nil
}

func (s *Store) StaleAcceptanceOperations(ctx context.Context, cutoff time.Time, limit int) ([]domain.AcceptanceOperation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+acceptanceColumns+` FROM acceptance_operations
		WHERE checkpoint NOT IN ('completed','failed') AND updated_at < $1
		ORDER BY updated_at ASC, id ASC
		LIMIT $2
	`, cutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.AcceptanceOperation
	for rows.Next() {
		op, err := scanAcceptance(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

func (s *Store) UpdateAcceptanceOperation(ctx context.Context, id string, fn func(domain.AcceptanceOperation, bool) (domain.AcceptanceOperation, error)) (domain.AcceptanceOperation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.AcceptanceOperation{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockTransactionKey(ctx, tx, "acceptance-operation-id", id); err != nil {
		return domain.AcceptanceOperation{}, err
	}

	current, err := scanAcceptance(tx.QueryRow(ctx, `SELECT `+acceptanceColumns+` FROM acceptance_operations WHERE id=$1 FOR UPDATE`, id))
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		current = domain.AcceptanceOperation{}
		exists = false
		err = nil
	}
	if err != nil {
		return domain.AcceptanceOperation{}, err
	}
	next, err := fn(current, exists)
	if err != nil {
		return domain.AcceptanceOperation{}, err
	}
	if exists {
		if next.ID != current.ID {
			return domain.AcceptanceOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "acceptance operation update must not change the operation id", false)
		}
		if acceptanceOperationContentHash(current) != acceptanceOperationContentHash(next) {
			return domain.AcceptanceOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "acceptance operation update must not change identity fields", false)
		}
	}
	if _, err := tx.Exec(ctx, upsertAcceptanceSQL, acceptanceWriteArgs(next)...); err != nil {
		return domain.AcceptanceOperation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.AcceptanceOperation{}, err
	}
	return next, nil
}

// CompleteAcceptance -- see the interface doc comment
// (internal/store/store.go): the JobBound->Completed checkpoint write and
// the OpenTask->Fulfilled projection happen in this ONE transaction, never
// two separate commits.
func (s *Store) CompleteAcceptance(ctx context.Context, opID string) (domain.AcceptanceOperation, domain.OpenTask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := lockTransactionKey(ctx, tx, "acceptance-operation-id", opID); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}
	op, err := scanAcceptance(tx.QueryRow(ctx, `SELECT `+acceptanceColumns+` FROM acceptance_operations WHERE id=$1 FOR UPDATE`, opID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, store.ErrNotFound
	}
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}

	if op.Checkpoint == domain.AcceptanceJobBound {
		now := time.Now().UTC()
		op.Checkpoint = domain.AcceptanceCompleted
		op.UpdatedAt = now
		op.CompletedAt = &now
		if _, err := tx.Exec(ctx, upsertAcceptanceSQL, acceptanceWriteArgs(op)...); err != nil {
			return domain.AcceptanceOperation{}, domain.OpenTask{}, err
		}
	} else if op.Checkpoint != domain.AcceptanceCompleted {
		// Caller bug (called from a checkpoint that isn't JobBound or
		// already Completed) -- return the current state unchanged rather
		// than silently completing from an unexpected checkpoint.
		task, terr := scanOpenTask(tx.QueryRow(ctx, `SELECT `+openTaskColumns+` FROM open_tasks WHERE id=$1`, op.TaskID))
		if terr != nil && !errors.Is(terr, pgx.ErrNoRows) {
			return domain.AcceptanceOperation{}, domain.OpenTask{}, terr
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.AcceptanceOperation{}, domain.OpenTask{}, err
		}
		return op, task, nil
	}

	if err := lockTransactionKey(ctx, tx, "open-task", op.TaskID); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}
	task, err := scanOpenTask(tx.QueryRow(ctx, `SELECT `+openTaskColumns+` FROM open_tasks WHERE id=$1 FOR UPDATE`, op.TaskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, store.ErrNotFound
	}
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}
	if task.Status == domain.OpenTaskAccepted {
		task.BoundQuoteID = op.QuoteID
		task.BoundJobID = op.JobID
		task.Status = domain.OpenTaskFulfilled
		task.UpdatedAt = time.Now().UTC()
		if _, err := tx.Exec(ctx, upsertOpenTaskSQL, openTaskWriteArgs(task)...); err != nil {
			return domain.AcceptanceOperation{}, domain.OpenTask{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}
	return op, task, nil
}

// FailAcceptance -- see the interface doc comment
// (internal/store/store.go): the non-terminal->Failed checkpoint write and
// the OpenTask reopen happen in this ONE transaction, never two separate
// commits.
func (s *Store) FailAcceptance(ctx context.Context, opID, failureReason string) (domain.AcceptanceOperation, domain.OpenTask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := lockTransactionKey(ctx, tx, "acceptance-operation-id", opID); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}
	op, err := scanAcceptance(tx.QueryRow(ctx, `SELECT `+acceptanceColumns+` FROM acceptance_operations WHERE id=$1 FOR UPDATE`, opID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, store.ErrNotFound
	}
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}

	if !op.Checkpoint.Terminal() {
		op.Checkpoint = domain.AcceptanceFailed
		op.FailureReason = failureReason
		op.UpdatedAt = time.Now().UTC()
		if _, err := tx.Exec(ctx, upsertAcceptanceSQL, acceptanceWriteArgs(op)...); err != nil {
			return domain.AcceptanceOperation{}, domain.OpenTask{}, err
		}
	}

	if err := lockTransactionKey(ctx, tx, "open-task", op.TaskID); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}
	task, err := scanOpenTask(tx.QueryRow(ctx, `SELECT `+openTaskColumns+` FROM open_tasks WHERE id=$1 FOR UPDATE`, op.TaskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, store.ErrNotFound
	}
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}
	if op.Checkpoint == domain.AcceptanceFailed && task.Status == domain.OpenTaskAccepted && task.AcceptedProposalID == op.ProposalID {
		task.Status = domain.OpenTaskOpen
		task.AcceptedProposalID = ""
		task.UpdatedAt = time.Now().UTC()
		if _, err := tx.Exec(ctx, upsertOpenTaskSQL, openTaskWriteArgs(task)...); err != nil {
			return domain.AcceptanceOperation{}, domain.OpenTask{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, err
	}
	return op, task, nil
}
