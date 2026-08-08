from pathlib import Path


def read(path: str) -> str:
    return Path(path).read_text()


def write(path: str, text: str) -> None:
    Path(path).write_text(text)


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one occurrence, found {count}: {old[:80]!r}")
    write(path, text.replace(old, new, 1))


def insert_before(path: str, marker: str, addition: str) -> None:
    replace_once(path, marker, addition + marker)


# ---------------------------------------------------------------------------
# Domain: durable internal economic checkpoint.
# ---------------------------------------------------------------------------
replace_once(
    "internal/domain/job.go",
    '''func (s JobState) Terminal() bool {\n''',
    '''type EconomicState string\n\nconst (\n\tEconomicNone              EconomicState = ""\n\tEconomicDebited           EconomicState = "debited"\n\tEconomicEscrowPending     EconomicState = "escrow_pending"\n\tEconomicEscrowReserved    EconomicState = "escrow_reserved"\n\tEconomicSettlementPending EconomicState = "settlement_pending"\n\tEconomicReleasePending    EconomicState = "release_pending"\n\tEconomicSettled           EconomicState = "settled"\n\tEconomicReleased          EconomicState = "released"\n)\n\nfunc (s JobState) Terminal() bool {\n''',
)
replace_once(
    "internal/domain/job.go",
    '''\tErrorCode              ErrorCode          `json:"error_code,omitempty"`\n\tReconciliationRequired bool               `json:"reconciliation_required,omitempty"`\n\tPendingCredit          *Money             `json:"-"`\n\tReconciliationTarget   JobState           `json:"-"`\n''',
    '''\tErrorCode              ErrorCode          `json:"error_code,omitempty"`\n\tEconomicState          EconomicState      `json:"-"`\n\tReconciliationRequired bool               `json:"reconciliation_required,omitempty"`\n\tPendingCredit          *Money             `json:"-"`\n\tReconciliationTarget   JobState           `json:"-"`\n''',
)

# ---------------------------------------------------------------------------
# Store contract: escrow lookup, recovery scan, and atomic Job+Account mutation.
# ---------------------------------------------------------------------------
replace_once(
    "internal/store/store.go",
    '''type Escrows interface {\n\tPutEscrow(ctx context.Context, e domain.Escrow) error\n\tGetEscrow(ctx context.Context, id string) (domain.Escrow, error)\n}\n''',
    '''type Escrows interface {\n\tPutEscrow(ctx context.Context, e domain.Escrow) error\n\tGetEscrow(ctx context.Context, id string) (domain.Escrow, error)\n\tEscrowByJob(ctx context.Context, jobID string) (domain.Escrow, error)\n}\n''',
)
replace_once(
    "internal/store/store.go",
    '''\tJobsByPrincipal(ctx context.Context, principalID string) ([]domain.Job, error)\n\tJobByConfirmationCode(ctx context.Context, userCode string) (domain.Job, error)\n''',
    '''\tJobsByPrincipal(ctx context.Context, principalID string) ([]domain.Job, error)\n\tJobsForRecovery(ctx context.Context, updatedBefore time.Time, limit int) ([]domain.Job, error)\n\tJobByConfirmationCode(ctx context.Context, userCode string) (domain.Job, error)\n''',
)
replace_once(
    "internal/store/store.go",
    '''\tUpdateJob(ctx context.Context, id string, fn func(j domain.Job, exists bool) (domain.Job, error)) (domain.Job, error)\n}\n''',
    '''\tUpdateJob(ctx context.Context, id string, fn func(j domain.Job, exists bool) (domain.Job, error)) (domain.Job, error)\n\t// UpdateJobAndAccount commits one Job checkpoint and one Managed Account\n\t// mutation in the same storage transaction. This is the Phase 1 economic\n\t// atomicity boundary: a crash cannot persist a debit/credit without the\n\t// corresponding durable Job checkpoint, or vice versa.\n\tUpdateJobAndAccount(\n\t\tctx context.Context, jobID, principalID string, seed domain.Account,\n\t\tfn func(job domain.Job, jobExists bool, account domain.Account, accountExists bool) (domain.Job, domain.Account, error),\n\t) (domain.Job, domain.Account, error)\n}\n''',
)

# ---------------------------------------------------------------------------
# Memory implementation.
# ---------------------------------------------------------------------------
insert_before(
    "internal/store/memory/memory.go",
    '''func (s *Store) PutReceipt(ctx context.Context, r domain.Receipt) error {\n''',
    '''func (s *Store) EscrowByJob(ctx context.Context, jobID string) (domain.Escrow, error) {\n\ts.mu.Lock()\n\tdefer s.mu.Unlock()\n\tfor _, e := range s.escrows {\n\t\tif e.JobID == jobID {\n\t\t\treturn e, nil\n\t\t}\n\t}\n\treturn domain.Escrow{}, store.ErrNotFound\n}\n\n''',
)
insert_before(
    "internal/store/memory/memory.go",
    '''func (s *Store) JobByConfirmationCode(ctx context.Context, userCode string) (domain.Job, error) {\n''',
    '''func (s *Store) JobsForRecovery(ctx context.Context, updatedBefore time.Time, limit int) ([]domain.Job, error) {\n\ts.mu.Lock()\n\tdefer s.mu.Unlock()\n\tif limit <= 0 {\n\t\tlimit = 100\n\t}\n\tout := make([]domain.Job, 0, limit)\n\tfor _, j := range s.jobs {\n\t\tif j.State.Terminal() || j.State == domain.JobInputRequired {\n\t\t\tcontinue\n\t\t}\n\t\tif !j.UpdatedAt.IsZero() && j.UpdatedAt.After(updatedBefore) {\n\t\t\tcontinue\n\t\t}\n\t\tout = append(out, j)\n\t\tif len(out) >= limit {\n\t\t\tbreak\n\t\t}\n\t}\n\treturn out, nil\n}\n\n''',
)
insert_before(
    "internal/store/memory/memory.go",
    '''func (s *Store) PutArtifact(ctx context.Context, a domain.StoredArtifact) error {\n''',
    '''func (s *Store) UpdateJobAndAccount(\n\tctx context.Context, jobID, principalID string, seed domain.Account,\n\tfn func(domain.Job, bool, domain.Account, bool) (domain.Job, domain.Account, error),\n) (domain.Job, domain.Account, error) {\n\ts.mu.Lock()\n\tdefer s.mu.Unlock()\n\tjob, jobExists := s.jobs[jobID]\n\taccount, accountExists := s.accounts[principalID]\n\tif !accountExists {\n\t\taccount = seed\n\t}\n\tnextJob, nextAccount, err := fn(job, jobExists, account, accountExists)\n\tif err != nil {\n\t\treturn domain.Job{}, domain.Account{}, err\n\t}\n\tif nextJob.ID != jobID || nextAccount.PrincipalID != principalID {\n\t\treturn domain.Job{}, domain.Account{}, store.ErrConflict\n\t}\n\ts.jobs[jobID] = nextJob\n\ts.accounts[principalID] = nextAccount\n\treturn nextJob, nextAccount, nil\n}\n\n''',
)

# ---------------------------------------------------------------------------
# PostgreSQL implementation: persistence columns and atomic transaction.
# ---------------------------------------------------------------------------
insert_before(
    "internal/store/postgres/postgres.go",
    '''func (s *Store) PutReceipt(ctx context.Context, r domain.Receipt) error {\n''',
    '''func (s *Store) EscrowByJob(ctx context.Context, jobID string) (domain.Escrow, error) {\n\te, err := scanEscrow(s.pool.QueryRow(ctx, `SELECT `+escrowColumns+` FROM escrows WHERE job_id=$1`, jobID))\n\tif errors.Is(err, pgx.ErrNoRows) {\n\t\treturn domain.Escrow{}, store.ErrNotFound\n\t}\n\treturn e, err\n}\n\n''',
)
replace_once(
    "internal/store/postgres/postgres.go",
    '''const jobColumns = `id, capability_id, quote_id, escrow_id, principal_id, state, input, output, artifacts, idempotency_key, failure_reason, created_at, updated_at, estimated_completion_at, payload`\n''',
    '''const jobColumns = `id, capability_id, quote_id, escrow_id, principal_id, state, input, output, artifacts, idempotency_key, failure_reason, created_at, updated_at, estimated_completion_at, economic_state, pending_credit, reconciliation_target, payload`\n''',
)
replace_once(
    "internal/store/postgres/postgres.go",
    '''func (s *Store) PutJob(ctx context.Context, j domain.Job) error {\n\t_, err := s.pool.Exec(ctx, upsertJobSQL,\n\t\tj.ID, j.CapabilityID, j.QuoteID, j.EscrowID, j.PrincipalID,\n\t\tstring(j.State), mustMarshal(j.Input), nullableJSON(j.Output),\n\t\tmustMarshal(j.Artifacts), j.IdempotencyKey, j.FailureReason,\n\t\tj.CreatedAt, j.UpdatedAt, j.EstimatedCompletionAt, mustMarshal(j))\n\treturn err\n}\n\nconst upsertJobSQL = `\n\tINSERT INTO jobs (\n\t\tid, capability_id, quote_id, escrow_id, principal_id, state, input,\n\t\toutput, artifacts, idempotency_key, failure_reason, created_at,\n\t\tupdated_at, estimated_completion_at, payload\n\t) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)\n\tON CONFLICT (id) DO UPDATE SET\n\t\tescrow_id=$4, state=$6, input=$7, output=$8, artifacts=$9,\n\t\tfailure_reason=$11, updated_at=$13, estimated_completion_at=$14,\n\t\tpayload=$15\n`\n''',
    '''func (s *Store) PutJob(ctx context.Context, j domain.Job) error {\n\t_, err := s.pool.Exec(ctx, upsertJobSQL, jobWriteArgs(j)...)\n\treturn err\n}\n\nconst upsertJobSQL = `\n\tINSERT INTO jobs (\n\t\tid, capability_id, quote_id, escrow_id, principal_id, state, input,\n\t\toutput, artifacts, idempotency_key, failure_reason, created_at,\n\t\tupdated_at, estimated_completion_at, economic_state, pending_credit,\n\t\treconciliation_target, payload\n\t) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)\n\tON CONFLICT (id) DO UPDATE SET\n\t\tescrow_id=$4, state=$6, input=$7, output=$8, artifacts=$9,\n\t\tfailure_reason=$11, updated_at=$13, estimated_completion_at=$14,\n\t\teconomic_state=$15, pending_credit=$16, reconciliation_target=$17,\n\t\tpayload=$18\n`\n\nfunc jobWriteArgs(j domain.Job) []any {\n\tvar pendingCredit any\n\tif j.PendingCredit != nil {\n\t\tpendingCredit = mustMarshal(j.PendingCredit)\n\t}\n\treturn []any{\n\t\tj.ID, j.CapabilityID, j.QuoteID, j.EscrowID, j.PrincipalID,\n\t\tstring(j.State), mustMarshal(j.Input), nullableJSON(j.Output),\n\t\tmustMarshal(j.Artifacts), j.IdempotencyKey, j.FailureReason,\n\t\tj.CreatedAt, j.UpdatedAt, j.EstimatedCompletionAt, string(j.EconomicState),\n\t\tpendingCredit, string(j.ReconciliationTarget), mustMarshal(j),\n\t}\n}\n''',
)
# Replace the scanJob function as one block.
start = read("internal/store/postgres/postgres.go").index("func scanJob(row pgx.Row) (domain.Job, error) {")
end = read("internal/store/postgres/postgres.go").index("\nfunc (s *Store) GetJob", start)
text = read("internal/store/postgres/postgres.go")
scan = '''func scanJob(row pgx.Row) (domain.Job, error) {\n\tvar j domain.Job\n\tvar input, output, artifacts, pendingCredit, payload []byte\n\tvar state, economicState, reconciliationTarget string\n\tif err := row.Scan(\n\t\t&j.ID, &j.CapabilityID, &j.QuoteID, &j.EscrowID, &j.PrincipalID,\n\t\t&state, &input, &output, &artifacts, &j.IdempotencyKey,\n\t\t&j.FailureReason, &j.CreatedAt, &j.UpdatedAt,\n\t\t&j.EstimatedCompletionAt, &economicState, &pendingCredit,\n\t\t&reconciliationTarget, &payload,\n\t); err != nil {\n\t\treturn domain.Job{}, err\n\t}\n\tj.State = domain.JobState(state)\n\tj.EconomicState = domain.EconomicState(economicState)\n\tj.ReconciliationTarget = domain.JobState(reconciliationTarget)\n\t_ = json.Unmarshal(input, &j.Input)\n\tif output != nil {\n\t\t_ = json.Unmarshal(output, &j.Output)\n\t}\n\t_ = json.Unmarshal(artifacts, &j.Artifacts)\n\tif pendingCredit != nil {\n\t\tvar credit domain.Money\n\t\tif err := json.Unmarshal(pendingCredit, &credit); err != nil {\n\t\t\treturn domain.Job{}, fmt.Errorf("postgres: decode pending credit: %w", err)\n\t\t}\n\t\tj.PendingCredit = &credit\n\t}\n\tif err := applyPayload(payload, &j); err != nil {\n\t\treturn domain.Job{}, err\n\t}\n\t// Internal economic recovery fields are deliberately stored in dedicated\n\t// columns because they are not part of the public Job JSON contract.\n\tj.EconomicState = domain.EconomicState(economicState)\n\tj.ReconciliationTarget = domain.JobState(reconciliationTarget)\n\tif pendingCredit == nil {\n\t\tj.PendingCredit = nil\n\t}\n\treturn j, nil\n}\n'''
write("internal/store/postgres/postgres.go", text[:start] + scan + text[end:])
insert_before(
    "internal/store/postgres/postgres.go",
    '''func (s *Store) JobByConfirmationCode(ctx context.Context, userCode string) (domain.Job, error) {\n''',
    '''func (s *Store) JobsForRecovery(ctx context.Context, updatedBefore time.Time, limit int) ([]domain.Job, error) {\n\tif limit <= 0 {\n\t\tlimit = 100\n\t}\n\trows, err := s.pool.Query(ctx, `\n\t\tSELECT `+jobColumns+` FROM jobs\n\t\tWHERE state IN ('submitted','working','canceling','reconciling')\n\t\t  AND updated_at <= $1\n\t\tORDER BY updated_at ASC, id ASC\n\t\tLIMIT $2\n\t`, updatedBefore.UTC(), limit)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\tdefer rows.Close()\n\tout := make([]domain.Job, 0, limit)\n\tfor rows.Next() {\n\t\tj, err := scanJob(rows)\n\t\tif err != nil {\n\t\t\treturn nil, err\n\t\t}\n\t\tout = append(out, j)\n\t}\n\treturn out, rows.Err()\n}\n\n''',
)
replace_once(
    "internal/store/postgres/postgres.go",
    '''\tif _, err := tx.Exec(ctx, upsertJobSQL,\n\t\tnext.ID, next.CapabilityID, next.QuoteID, next.EscrowID,\n\t\tnext.PrincipalID, string(next.State), mustMarshal(next.Input),\n\t\tnullableJSON(next.Output), mustMarshal(next.Artifacts), next.IdempotencyKey,\n\t\tnext.FailureReason, next.CreatedAt, next.UpdatedAt,\n\t\tnext.EstimatedCompletionAt, mustMarshal(next)); err != nil {\n''',
    '''\tif _, err := tx.Exec(ctx, upsertJobSQL, jobWriteArgs(next)...); err != nil {\n''',
)
insert_before(
    "internal/store/postgres/postgres.go",
    '''// --- Accounts ---\n''',
    '''func (s *Store) UpdateJobAndAccount(\n\tctx context.Context, jobID, principalID string, seed domain.Account,\n\tfn func(domain.Job, bool, domain.Account, bool) (domain.Job, domain.Account, error),\n) (domain.Job, domain.Account, error) {\n\ttx, err := s.pool.Begin(ctx)\n\tif err != nil {\n\t\treturn domain.Job{}, domain.Account{}, err\n\t}\n\tdefer tx.Rollback(ctx) //nolint:errcheck\n\t// All multi-object economic transactions lock Account first, then Job.\n\t// Single-object mutations take only one of these locks, so this order is\n\t// deadlock-free across concurrent jobs for the same principal.\n\tif err := lockTransactionKey(ctx, tx, "account", principalID); err != nil {\n\t\treturn domain.Job{}, domain.Account{}, err\n\t}\n\tif err := lockTransactionKey(ctx, tx, "job", jobID); err != nil {\n\t\treturn domain.Job{}, domain.Account{}, err\n\t}\n\n\tjob, err := scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1 FOR UPDATE`, jobID))\n\tjobExists := true\n\tif errors.Is(err, pgx.ErrNoRows) {\n\t\tjob = domain.Job{}\n\t\tjobExists = false\n\t\terr = nil\n\t}\n\tif err != nil {\n\t\treturn domain.Job{}, domain.Account{}, err\n\t}\n\n\tvar account domain.Account\n\taccount.PrincipalID = principalID\n\tvar balance, spendPolicy, payload []byte\n\terr = tx.QueryRow(ctx, `SELECT balance, spend_policy, payload FROM accounts WHERE principal_id=$1 FOR UPDATE`, principalID).\n\t\tScan(&balance, &spendPolicy, &payload)\n\taccountExists := true\n\tswitch {\n\tcase errors.Is(err, pgx.ErrNoRows):\n\t\taccountExists = false\n\t\taccount = seed\n\tcase err != nil:\n\t\treturn domain.Job{}, domain.Account{}, err\n\tdefault:\n\t\t_ = json.Unmarshal(balance, &account.Balance)\n\t\t_ = json.Unmarshal(spendPolicy, &account.SpendPolicy)\n\t\tif err := applyPayload(payload, &account); err != nil {\n\t\t\treturn domain.Job{}, domain.Account{}, err\n\t\t}\n\t}\n\taccount.PrincipalID = principalID\n\n\tnextJob, nextAccount, err := fn(job, jobExists, account, accountExists)\n\tif err != nil {\n\t\treturn domain.Job{}, domain.Account{}, err\n\t}\n\tif nextJob.ID != jobID || nextAccount.PrincipalID != principalID {\n\t\treturn domain.Job{}, domain.Account{}, store.ErrConflict\n\t}\n\tif _, err := tx.Exec(ctx, upsertJobSQL, jobWriteArgs(nextJob)...); err != nil {\n\t\treturn domain.Job{}, domain.Account{}, err\n\t}\n\tif _, err := tx.Exec(ctx, `\n\t\tINSERT INTO accounts (principal_id, balance, spend_policy, payload)\n\t\tVALUES ($1,$2,$3,$4)\n\t\tON CONFLICT (principal_id) DO UPDATE SET\n\t\t\tbalance=$2, spend_policy=$3, payload=$4\n\t`, principalID, mustMarshal(nextAccount.Balance), mustMarshal(nextAccount.SpendPolicy), mustMarshal(nextAccount)); err != nil {\n\t\treturn domain.Job{}, domain.Account{}, err\n\t}\n\tif err := tx.Commit(ctx); err != nil {\n\t\treturn domain.Job{}, domain.Account{}, err\n\t}\n\treturn nextJob, nextAccount, nil\n}\n\n''',
)

# Migration for internal checkpoints + uniqueness/recovery index.
Path("migrations/005_phase1_crash_safety.sql").write_text('''-- Phase 1 crash-safe Managed economy.\n-- Internal checkpoints intentionally live outside the public Job JSON payload.\nALTER TABLE jobs ADD COLUMN IF NOT EXISTS economic_state TEXT NOT NULL DEFAULT '';\nALTER TABLE jobs ADD COLUMN IF NOT EXISTS pending_credit JSONB;\nALTER TABLE jobs ADD COLUMN IF NOT EXISTS reconciliation_target TEXT NOT NULL DEFAULT '';\n\nCREATE UNIQUE INDEX IF NOT EXISTS escrows_job_id_uidx\n  ON escrows (job_id) WHERE job_id <> '';\n\nCREATE INDEX IF NOT EXISTS jobs_recovery_scan_idx\n  ON jobs (updated_at, id)\n  WHERE state IN ('submitted','working','canceling','reconciling');\n''')

# ---------------------------------------------------------------------------
# Mock core: make external economic operations replayable by Job/Escrow identity.
# ---------------------------------------------------------------------------
insert_before(
    "internal/adapters/toscore/mock/mock.go",
    '''\tnow := time.Now().UTC()\n\te := domain.Escrow{\n''',
    '''\tif existing, err := c.store.EscrowByJob(ctx, req.JobID); err == nil {\n\t\tif existing.QuoteID != req.QuoteID || existing.PrincipalID != req.PrincipalID ||\n\t\t\texisting.ProviderID != req.ProviderID || existing.CapabilityID != req.CapabilityID ||\n\t\t\texisting.CapabilityVersion != req.CapabilityVersion || existing.TrustMode != req.TrustMode ||\n\t\t\texisting.ProofProfile != req.ProofProfile || existing.Reserved != req.Reserved {\n\t\t\treturn domain.Escrow{}, domain.NewError(domain.ErrIdempotencyConflict, "existing escrow does not match replayed request", false)\n\t\t}\n\t\treturn existing, nil\n\t} else if err != store.ErrNotFound {\n\t\treturn domain.Escrow{}, err\n\t}\n\n''',
)
replace_once(
    "internal/adapters/toscore/mock/mock.go",
    '''\tif e.Status.Terminal() {\n\t\treturn domain.Receipt{}, domain.NewError(domain.ErrSettlementFailed, "escrow already in a terminal state", false)\n\t}\n''',
    '''\tif e.Status == domain.EscrowReleased {\n\t\tif receipt, replayErr := c.store.ReceiptByJob(ctx, e.JobID); replayErr == nil && receipt.EscrowID == e.ID && receipt.Status == domain.ReceiptReleased {\n\t\t\treturn receipt, nil\n\t\t}\n\t}\n\tif e.Status.Terminal() {\n\t\treturn domain.Receipt{}, domain.NewError(domain.ErrSettlementFailed, "escrow already in a terminal state", false)\n\t}\n''',
)
replace_once(
    "internal/adapters/toscore/mock/mock.go",
    '''\t\tID:           "rcpt_" + uuid.NewString(),\n''',
    '''\t\tID:           "rcpt_release_" + e.ID,\n''',
)
insert_before(
    "internal/adapters/toscore/mock/mock.go",
    '''func (c *Core) SettleJob(ctx context.Context, req toscore.SettleJobRequest) (toscore.SettleJobResult, error) {\n''',
    '''// SettleJob is replayable: a lost response after a committed settlement\n// returns the original terminal receipt instead of requiring the caller to\n// guess whether the economic side effect happened.\n''',
)
replace_once(
    "internal/adapters/toscore/mock/mock.go",
    '''func (c *Core) SettleJob(ctx context.Context, req toscore.SettleJobRequest) (toscore.SettleJobResult, error) {\n\tc.mu.Lock()\n''',
    '''func (c *Core) SettleJob(ctx context.Context, req toscore.SettleJobRequest) (toscore.SettleJobResult, error) {\n\tif receipt, err := c.store.ReceiptByJob(ctx, req.JobID); err == nil && receipt.EscrowID == req.EscrowID && receipt.Status == domain.ReceiptSettled {\n\t\treturn toscore.SettleJobResult{Receipt: receipt}, nil\n\t}\n\tc.mu.Lock()\n''',
)
replace_once(
    "internal/adapters/toscore/mock/mock.go",
    '''\tsettlementReceiptID := "rcpt_" + uuid.NewString()\n''',
    '''\tsettlementReceiptID := "rcpt_settle_" + e.ID\n''',
)

# ---------------------------------------------------------------------------
# RPC core: local replay fast-paths; remote request IDs are already stable.
# ---------------------------------------------------------------------------
insert_before(
    "internal/adapters/tosprotocol/core.go",
    '''\treserve, err := networkAmount(req.Reserved)\n''',
    '''\tif existing, err := c.store.EscrowByJob(ctx, req.JobID); err == nil {\n\t\tif existing.QuoteID != req.QuoteID || existing.PrincipalID != req.PrincipalID ||\n\t\t\texisting.ProviderID != req.ProviderID || existing.CapabilityID != req.CapabilityID ||\n\t\t\texisting.CapabilityVersion != req.CapabilityVersion || existing.TrustMode != req.TrustMode ||\n\t\t\texisting.ProofProfile != req.ProofProfile || existing.Reserved != req.Reserved {\n\t\t\treturn domain.Escrow{}, domain.NewError(domain.ErrIdempotencyConflict, "stored escrow does not match replayed request", false)\n\t\t}\n\t\treturn existing, nil\n\t} else if err != store.ErrNotFound {\n\t\treturn domain.Escrow{}, err\n\t}\n''',
)
replace_once(
    "internal/adapters/tosprotocol/core.go",
    '''\tlocal, err := c.store.GetEscrow(ctx, escrowID)\n\tif err != nil {\n\t\treturn domain.Receipt{}, err\n\t}\n\tcallCtx, cancel := c.callContext(ctx, time.Time{})\n''',
    '''\tlocal, err := c.store.GetEscrow(ctx, escrowID)\n\tif err != nil {\n\t\treturn domain.Receipt{}, err\n\t}\n\tif local.Status == domain.EscrowReleased {\n\t\tif receipt, replayErr := c.store.ReceiptByJob(ctx, local.JobID); replayErr == nil && receipt.EscrowID == local.ID && receipt.Status == domain.ReceiptReleased {\n\t\t\treturn receipt, nil\n\t\t}\n\t}\n\tcallCtx, cancel := c.callContext(ctx, time.Time{})\n''',
)
replace_once(
    "internal/adapters/tosprotocol/core.go",
    '''\tlocal, err := c.store.GetEscrow(ctx, req.EscrowID)\n\tif err != nil {\n\t\treturn toscore.SettleJobResult{}, err\n\t}\n\tcharge, err := networkAmount(req.ActualCost)\n''',
    '''\tlocal, err := c.store.GetEscrow(ctx, req.EscrowID)\n\tif err != nil {\n\t\treturn toscore.SettleJobResult{}, err\n\t}\n\tif local.Status == domain.EscrowSettled {\n\t\tif receipt, replayErr := c.store.ReceiptByJob(ctx, req.JobID); replayErr == nil && receipt.EscrowID == local.ID && receipt.Status == domain.ReceiptSettled {\n\t\t\treturn toscore.SettleJobResult{Receipt: receipt}, nil\n\t\t}\n\t}\n\tcharge, err := networkAmount(req.ActualCost)\n''',
)

# ---------------------------------------------------------------------------
# P2 auth hardening: principal comes only from trusted authenticated boundary.
# ---------------------------------------------------------------------------
replace_once(
    "internal/httpapi/auth.go",
    '''\tvar req struct {\n\t\tUserCode    string `json:"user_code"`\n\t\tPrincipalID string `json:"principal_id,omitempty"`\n\t\tDecision    string `json:"decision"`\n\t}\n''',
    '''\tprincipalID := strings.TrimSpace(r.Header.Get("X-ATOS-Principal-ID"))\n\tif principalID == "" {\n\t\twriteError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, "authenticated consent principal is required", false)\n\t\treturn\n\t}\n\tvar req struct {\n\t\tUserCode string `json:"user_code"`\n\t\tDecision string `json:"decision"`\n\t}\n''',
)
replace_once(
    "internal/httpapi/auth.go",
    '''\tgrant, err := s.Auth.DecideDevice(req.UserCode, req.PrincipalID, approve)\n''',
    '''\tgrant, err := s.Auth.DecideDevice(req.UserCode, principalID, approve)\n''',
)
replace_once(
    "internal/httpapi/phase01_postgres_acceptance_test.go",
    '''\tdecision := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/auth/device/decision", "", map[string]any{\n\t\t"user_code": grant.UserCode, "principal_id": principalID, "decision": "approve",\n\t}, map[string]string{"X-ATOS-Approval-Token": approvalToken})\n''',
    '''\tdecision := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/auth/device/decision", "", map[string]any{\n\t\t"user_code": grant.UserCode, "decision": "approve",\n\t}, map[string]string{"X-ATOS-Approval-Token": approvalToken, "X-ATOS-Principal-ID": principalID})\n''',
)
# OpenAPI removes caller-supplied principal_id and requires trusted header.
openapi = read("api/openapi.yaml")
openapi = openapi.replace(
    '''      parameters:\n        - name: X-ATOS-Approval-Token\n          in: header\n          required: true\n          schema: {type: string}\n      requestBody:\n''',
    '''      parameters:\n        - name: X-ATOS-Approval-Token\n          in: header\n          required: true\n          schema: {type: string}\n        - name: X-ATOS-Principal-ID\n          in: header\n          required: true\n          schema: {type: string}\n      requestBody:\n''',
    1,
)
openapi = openapi.replace('''                principal_id: {type: string}\n''', '', 1)
write("api/openapi.yaml", openapi)

# Add a targeted auth regression test in the existing package.
Path("internal/httpapi/auth_decision_security_test.go").write_text(r'''package httpapi

import (
    "context"
    "io"
    "log/slog"
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"

    "github.com/tosnetwork/atos/internal/auth"
)

func TestDeviceDecisionRequiresTrustedPrincipalHeader(t *testing.T) {
    authorization := auth.NewService()
    grant, err := authorization.StartDevice("codex", "security-test", nil)
    if err != nil {
        t.Fatal(err)
    }
    approvalToken := strings.Repeat("s", 32)
    server := &Server{Auth: authorization, ApprovalToken: approvalToken, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
    httpServer := httptest.NewServer(server.Mux())
    defer httpServer.Close()

    body := map[string]any{"user_code": grant.UserCode, "decision": "approve"}
    missing := phase01Request(t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/auth/device/decision", "", body,
        map[string]string{"X-ATOS-Approval-Token": approvalToken})
    if missing.Status != http.StatusUnauthorized {
        t.Fatalf("missing trusted principal header = %d %s", missing.Status, missing.Body)
    }

    approved := phase01Request(t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/auth/device/decision", "", body,
        map[string]string{"X-ATOS-Approval-Token": approvalToken, "X-ATOS-Principal-ID": "prn_security"})
    if approved.Status != http.StatusOK {
        t.Fatalf("trusted principal decision = %d %s", approved.Status, approved.Body)
    }
    resolved, err := authorization.GrantByUserCode(grant.UserCode)
    if err != nil {
        t.Fatal(err)
    }
    if resolved.PrincipalID != "prn_security" {
        t.Fatalf("approved principal = %q", resolved.PrincipalID)
    }
    _ = context.Background()
}
''')

# Store-level transaction regression tests live in the Postgres package.
Path("internal/store/postgres/crash_safety_test.go").write_text(r'''package postgres_test

import (
    "context"
    "errors"
    "testing"
    "time"

    "github.com/tosnetwork/atos/internal/domain"
    "github.com/tosnetwork/atos/internal/store"
)

func TestUpdateJobAndAccountRollsBackTogether(t *testing.T) {
    ctx := context.Background()
    s := openTestStore(t)
    suffix := randSuffix()
    principalID := "prn_atomic_" + suffix
    jobID := "job_atomic_" + suffix
    now := time.Now().UTC()
    job := domain.Job{ID: jobID, CapabilityID: "cap_" + suffix, QuoteID: "q_" + suffix, PrincipalID: principalID, State: domain.JobSubmitted, Input: map[string]any{}, Artifacts: []domain.Artifact{}, CreatedAt: now, UpdatedAt: now}
    if err := s.PutJob(ctx, job); err != nil { t.Fatal(err) }
    seed := domain.Account{PrincipalID: principalID, Balance: domain.Money{Amount: "10", Currency: "USD"}}
    if err := s.PutAccount(ctx, seed); err != nil { t.Fatal(err) }

    wantErr := errors.New("inject rollback")
    _, _, err := s.UpdateJobAndAccount(ctx, jobID, principalID, seed, func(j domain.Job, _ bool, a domain.Account, _ bool) (domain.Job, domain.Account, error) {
        j.EconomicState = domain.EconomicDebited
        a.Balance.Amount = "9"
        return j, a, wantErr
    })
    if !errors.Is(err, wantErr) { t.Fatalf("UpdateJobAndAccount error = %v", err) }
    gotJob, err := s.GetJob(ctx, jobID); if err != nil { t.Fatal(err) }
    gotAccount, err := s.GetAccount(ctx, principalID); if err != nil { t.Fatal(err) }
    if gotJob.EconomicState != domain.EconomicNone || gotAccount.Balance.Amount != "10" {
        t.Fatalf("partial transaction persisted: job=%q balance=%q", gotJob.EconomicState, gotAccount.Balance.Amount)
    }
}

func TestEscrowByJobIsUniqueAndRecoverable(t *testing.T) {
    ctx := context.Background()
    s := openTestStore(t)
    suffix := randSuffix()
    jobID := "job_escrow_lookup_" + suffix
    now := time.Now().UTC()
    e := domain.Escrow{ID: "esc_" + suffix, QuoteID: "q_" + suffix, JobID: jobID, PrincipalID: "p_" + suffix, ProviderID: "a_" + suffix, CapabilityID: "c_" + suffix, CapabilityVersion: "1", TrustMode: domain.TrustModeManaged, Reserved: domain.Money{Amount: "1.00", Currency: "USD"}, Status: domain.EscrowReserved, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
    if err := s.PutEscrow(ctx, e); err != nil { t.Fatal(err) }
    got, err := s.EscrowByJob(ctx, jobID); if err != nil { t.Fatal(err) }
    if got.ID != e.ID { t.Fatalf("EscrowByJob = %s, want %s", got.ID, e.ID) }
    duplicate := e; duplicate.ID = "esc_duplicate_" + suffix
    if err := s.PutEscrow(ctx, duplicate); err == nil {
        t.Fatal("duplicate escrow for one job unexpectedly succeeded")
    }
    if _, err := s.EscrowByJob(ctx, "missing_"+suffix); !errors.Is(err, store.ErrNotFound) {
        t.Fatalf("missing EscrowByJob error = %v", err)
    }
}
''')

print("stage1 crash-safety primitives materialized")
