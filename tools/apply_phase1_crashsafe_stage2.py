from pathlib import Path


def read(path: str) -> str:
    return Path(path).read_text()


def write(path: str, text: str) -> None:
    Path(path).write_text(text)


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement target, found {count}: {old[:100]!r}")
    write(path, text.replace(old, new, 1))


def replace_func(path: str, name: str, next_name: str, replacement: str) -> None:
    text = read(path)
    start_marker = f"func (s *JobService) {name}"
    end_marker = f"func (s *JobService) {next_name}"
    start = text.find(start_marker)
    end = text.find(end_marker, start + 1)
    if start < 0 or end < 0:
        raise SystemExit(f"{path}: cannot locate {name} -> {next_name}")
    write(path, text[:start] + replacement.rstrip() + "\n\n" + text[end:])


# ---------------------------------------------------------------------------
# Persist the exact execution receipt privately for crash-safe settlement replay.
# ---------------------------------------------------------------------------
replace_once(
    "internal/domain/job.go",
    '''\tEconomicState          EconomicState      `json:"-"`\n\tReconciliationRequired bool               `json:"reconciliation_required,omitempty"`\n''',
    '''\tEconomicState          EconomicState      `json:"-"`\n\tExecutionReceipt       *ExecutionReceipt  `json:"-"`\n\tReconciliationRequired bool               `json:"reconciliation_required,omitempty"`\n''',
)

replace_once(
    "internal/store/postgres/postgres.go",
    'const jobColumns = `id, capability_id, quote_id, escrow_id, principal_id, state, input, output, artifacts, idempotency_key, failure_reason, created_at, updated_at, estimated_completion_at, economic_state, pending_credit, reconciliation_target, payload`',
    'const jobColumns = `id, capability_id, quote_id, escrow_id, principal_id, state, input, output, artifacts, idempotency_key, failure_reason, created_at, updated_at, estimated_completion_at, economic_state, execution_receipt, pending_credit, reconciliation_target, payload`',
)
replace_once(
    "internal/store/postgres/postgres.go",
    '''\t\tupdated_at, estimated_completion_at, economic_state, pending_credit,\n\t\treconciliation_target, payload\n\t) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)\n\tON CONFLICT (id) DO UPDATE SET\n\t\tescrow_id=$4, state=$6, input=$7, output=$8, artifacts=$9,\n\t\tfailure_reason=$11, updated_at=$13, estimated_completion_at=$14,\n\t\teconomic_state=$15, pending_credit=$16, reconciliation_target=$17,\n\t\tpayload=$18\n''',
    '''\t\tupdated_at, estimated_completion_at, economic_state, execution_receipt,\n\t\tpending_credit, reconciliation_target, payload\n\t) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)\n\tON CONFLICT (id) DO UPDATE SET\n\t\tescrow_id=$4, state=$6, input=$7, output=$8, artifacts=$9,\n\t\tfailure_reason=$11, updated_at=$13, estimated_completion_at=$14,\n\t\teconomic_state=$15, execution_receipt=$16, pending_credit=$17,\n\t\treconciliation_target=$18, payload=$19\n''',
)
replace_once(
    "internal/store/postgres/postgres.go",
    '''func jobWriteArgs(j domain.Job) []any {\n\tvar pendingCredit any\n\tif j.PendingCredit != nil {\n\t\tpendingCredit = mustMarshal(j.PendingCredit)\n\t}\n\treturn []any{\n\t\tj.ID, j.CapabilityID, j.QuoteID, j.EscrowID, j.PrincipalID,\n\t\tstring(j.State), mustMarshal(j.Input), nullableJSON(j.Output),\n\t\tmustMarshal(j.Artifacts), j.IdempotencyKey, j.FailureReason,\n\t\tj.CreatedAt, j.UpdatedAt, j.EstimatedCompletionAt, string(j.EconomicState),\n\t\tpendingCredit, string(j.ReconciliationTarget), mustMarshal(j),\n\t}\n}\n''',
    '''func jobWriteArgs(j domain.Job) []any {\n\tvar executionReceipt, pendingCredit any\n\tif j.ExecutionReceipt != nil {\n\t\texecutionReceipt = mustMarshal(j.ExecutionReceipt)\n\t}\n\tif j.PendingCredit != nil {\n\t\tpendingCredit = mustMarshal(j.PendingCredit)\n\t}\n\treturn []any{\n\t\tj.ID, j.CapabilityID, j.QuoteID, j.EscrowID, j.PrincipalID,\n\t\tstring(j.State), mustMarshal(j.Input), nullableJSON(j.Output),\n\t\tmustMarshal(j.Artifacts), j.IdempotencyKey, j.FailureReason,\n\t\tj.CreatedAt, j.UpdatedAt, j.EstimatedCompletionAt, string(j.EconomicState),\n\t\texecutionReceipt, pendingCredit, string(j.ReconciliationTarget), mustMarshal(j),\n\t}\n}\n''',
)
replace_once(
    "internal/store/postgres/postgres.go",
    '''\tvar input, output, artifacts, pendingCredit, payload []byte\n\tvar state, economicState, reconciliationTarget string\n''',
    '''\tvar input, output, artifacts, executionReceipt, pendingCredit, payload []byte\n\tvar state, economicState, reconciliationTarget string\n''',
)
replace_once(
    "internal/store/postgres/postgres.go",
    '''\t\t&j.EstimatedCompletionAt, &economicState, &pendingCredit,\n\t\t&reconciliationTarget, &payload,\n''',
    '''\t\t&j.EstimatedCompletionAt, &economicState, &executionReceipt, &pendingCredit,\n\t\t&reconciliationTarget, &payload,\n''',
)
replace_once(
    "internal/store/postgres/postgres.go",
    '''\t_ = json.Unmarshal(artifacts, &j.Artifacts)\n\tif pendingCredit != nil {\n''',
    '''\t_ = json.Unmarshal(artifacts, &j.Artifacts)\n\tif executionReceipt != nil {\n\t\tvar receipt domain.ExecutionReceipt\n\t\tif err := json.Unmarshal(executionReceipt, &receipt); err != nil {\n\t\t\treturn domain.Job{}, fmt.Errorf("postgres: decode execution receipt: %w", err)\n\t\t}\n\t\tj.ExecutionReceipt = &receipt\n\t}\n\tif pendingCredit != nil {\n''',
)
replace_once(
    "internal/store/postgres/postgres.go",
    '''\tif pendingCredit == nil {\n\t\tj.PendingCredit = nil\n\t}\n''',
    '''\tif executionReceipt == nil {\n\t\tj.ExecutionReceipt = nil\n\t}\n\tif pendingCredit == nil {\n\t\tj.PendingCredit = nil\n\t}\n''',
)
Path("migrations/006_phase1_execution_receipt_recovery.sql").write_text('''-- Persist private execution evidence required to replay an ambiguous settlement.\nALTER TABLE jobs ADD COLUMN IF NOT EXISTS execution_receipt JSONB;\n''')

# ---------------------------------------------------------------------------
# Account value helpers used inside the atomic Job+Account transaction.
# ---------------------------------------------------------------------------
insert = '''func (s *AccountService) debitAccountValue(a domain.Account, amountStr, currency string) (domain.Account, error) {\n\tamount, err := money.Parse(amountStr, currency, accountDecimals)\n\tif err != nil {\n\t\treturn domain.Account{}, domain.NewError(domain.ErrValidationFailed, "invalid amount", false)\n\t}\n\ta = s.normalizeAccount(a)\n\tbalance, err := money.Parse(a.Balance.Amount, a.Balance.Currency, accountDecimals)\n\tif err != nil {\n\t\treturn domain.Account{}, err\n\t}\n\tremaining, err := money.Parse(a.SpendPolicy.RemainingToday.Amount, a.SpendPolicy.RemainingToday.Currency, accountDecimals)\n\tif err != nil {\n\t\treturn domain.Account{}, err\n\t}\n\tif amount.Currency != balance.Currency {\n\t\treturn domain.Account{}, domain.NewError(domain.ErrValidationFailed, "currency mismatch with account balance", false)\n\t}\n\tif amount.Cmp(remaining) > 0 {\n\t\treturn domain.Account{}, domain.NewError(domain.ErrSpendLimitExceeded, "daily autonomous spend limit exceeded", false)\n\t}\n\tnewBalance, err := balance.Sub(amount)\n\tif err != nil {\n\t\treturn domain.Account{}, domain.NewError(domain.ErrInsufficientBalance, "insufficient balance", false)\n\t}\n\tnewRemaining, err := remaining.Sub(amount)\n\tif err != nil {\n\t\treturn domain.Account{}, domain.NewError(domain.ErrSpendLimitExceeded, "daily autonomous spend limit exceeded", false)\n\t}\n\ta.Balance = domain.Money{Amount: newBalance.String(), Currency: newBalance.Currency}\n\ta.SpendPolicy.RemainingToday = domain.Money{Amount: newRemaining.String(), Currency: newRemaining.Currency}\n\treturn a, nil\n}\n\nfunc (s *AccountService) creditAccountValue(a domain.Account, amountStr, currency string) (domain.Account, error) {\n\tamount, err := money.Parse(amountStr, currency, accountDecimals)\n\tif err != nil {\n\t\treturn domain.Account{}, domain.NewError(domain.ErrValidationFailed, "invalid amount", false)\n\t}\n\ta = s.normalizeAccount(a)\n\tif amount.IsZero() {\n\t\treturn a, nil\n\t}\n\tbalance, err := money.Parse(a.Balance.Amount, a.Balance.Currency, accountDecimals)\n\tif err != nil {\n\t\treturn domain.Account{}, err\n\t}\n\tremaining, err := money.Parse(a.SpendPolicy.RemainingToday.Amount, a.SpendPolicy.RemainingToday.Currency, accountDecimals)\n\tif err != nil {\n\t\treturn domain.Account{}, err\n\t}\n\tdailyLimit, err := money.Parse(a.SpendPolicy.DailyLimit.Amount, a.SpendPolicy.DailyLimit.Currency, accountDecimals)\n\tif err != nil {\n\t\treturn domain.Account{}, err\n\t}\n\tif amount.Currency != balance.Currency {\n\t\treturn domain.Account{}, domain.NewError(domain.ErrValidationFailed, "currency mismatch with account balance", false)\n\t}\n\tnewBalance, err := balance.Add(amount)\n\tif err != nil {\n\t\treturn domain.Account{}, err\n\t}\n\tnewRemaining, err := remaining.Add(amount)\n\tif err != nil {\n\t\treturn domain.Account{}, err\n\t}\n\tif newRemaining.Cmp(dailyLimit) > 0 {\n\t\tnewRemaining = dailyLimit\n\t}\n\ta.Balance = domain.Money{Amount: newBalance.String(), Currency: newBalance.Currency}\n\ta.SpendPolicy.RemainingToday = domain.Money{Amount: newRemaining.String(), Currency: newRemaining.Currency}\n\treturn a, nil\n}\n\n'''
replace_once("internal/service/account.go", "func (s *AccountService) Debit(ctx context.Context, principalID, amountStr, currency string) error {\n", insert + "func (s *AccountService) Debit(ctx context.Context, principalID, amountStr, currency string) error {\n")

# ---------------------------------------------------------------------------
# Provider mock must preserve the same Job result on replay and expose NotFound.
# ---------------------------------------------------------------------------
replace_once(
    "internal/adapters/tosai/mock/mock.go",
    '''func (p *Provider) SubmitJob(ctx context.Context, req tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {\n\tif !p.modes[req.TrustMode] {\n''',
    '''func (p *Provider) SubmitJob(ctx context.Context, req tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {\n\tp.mu.Lock()\n\tif existing, ok := p.jobs[req.JobID]; ok {\n\t\tp.mu.Unlock()\n\t\tif existing.Receipt != nil && (existing.Receipt.QuoteID != req.QuoteID || existing.Receipt.EscrowID != req.EscrowID || existing.Receipt.PrincipalID != req.PrincipalID || existing.Receipt.CapabilityID != req.CapabilityID) {\n\t\t\treturn tosai.SubmitJobResult{}, domain.NewError(domain.ErrIdempotencyConflict, "job replay does not match original execution", false)\n\t\t}\n\t\treturn existing, nil\n\t}\n\tp.mu.Unlock()\n\tif !p.modes[req.TrustMode] {\n''',
)
replace_once(
    "internal/adapters/tosai/mock/mock.go",
    '''\tif !ok {\n\t\treturn tosai.SubmitJobResult{}, fmt.Errorf("mock tosai: unknown job %q", jobID)\n\t}\n''',
    '''\tif !ok {\n\t\treturn tosai.SubmitJobResult{}, domain.NewError(domain.ErrNotFound, fmt.Sprintf("mock tosai: unknown job %q", jobID), false)\n\t}\n''',
)

# ---------------------------------------------------------------------------
# Crash-safe economic orchestration and reconciliation.
# ---------------------------------------------------------------------------
Path("internal/service/economic_recovery.go").write_text(r'''package service

import (
    "context"
    "errors"
    "fmt"
    "time"

    "github.com/tosnetwork/atos/internal/adapters/tosai"
    "github.com/tosnetwork/atos/internal/adapters/toscore"
    "github.com/tosnetwork/atos/internal/domain"
    "github.com/tosnetwork/atos/internal/store"
)

const (
    defaultReconcileInterval = 15 * time.Second
    defaultReconcileStaleAfter = 30 * time.Second
    defaultReconcileBatch = 100
)

func domainErrorIs(err error, code domain.ErrorCode) bool {
    var typed *domain.Error
    return errors.As(err, &typed) && typed.Code == code
}

func (s *JobService) atomicDebitCheckpoint(ctx context.Context, job domain.Job, quote domain.Quote) (domain.Job, error) {
    updated, _, err := s.store.UpdateJobAndAccount(ctx, job.ID, job.PrincipalID, s.accounts.defaultAccount(job.PrincipalID), func(current domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
        if !exists {
            return domain.Job{}, domain.Account{}, domain.NewError(domain.ErrNotFound, "job not found during economic debit", false)
        }
        if current.EconomicState != domain.EconomicNone {
            return current, account, nil
        }
        if current.State != domain.JobSubmitted {
            return domain.Job{}, domain.Account{}, store.ErrConflict
        }
        nextAccount, err := s.accounts.debitAccountValue(account, quote.Price.TotalMax, quote.Price.Currency)
        if err != nil {
            return domain.Job{}, domain.Account{}, err
        }
        current.EconomicState = domain.EconomicDebited
        current.ProofStatus.Quote = proofCheckpointForCommittedQuote(current.TrustMode)
        current.UpdatedAt = time.Now().UTC()
        return current, nextAccount, nil
    })
    return updated, err
}

func (s *JobService) markEconomicReconciliationUnderLock(ctx context.Context, jobID string, economic domain.EconomicState, target domain.JobState, code domain.ErrorCode, reason string) domain.Job {
    updated, err := s.store.UpdateJob(ctx, jobID, func(job domain.Job, exists bool) (domain.Job, error) {
        if !exists {
            return domain.Job{}, domain.NewError(domain.ErrNotFound, "job not found during reconciliation checkpoint", false)
        }
        job.State = domain.JobReconciling
        if economic != domain.EconomicNone {
            job.EconomicState = economic
        }
        job.ReconciliationRequired = true
        job.ReconciliationTarget = target
        job.ErrorCode = code
        job.FailureReason = reason
        job.UpdatedAt = time.Now().UTC()
        job.CompletedAt = nil
        return job, nil
    })
    if err != nil {
        current, _ := s.store.GetJob(ctx, jobID)
        return current
    }
    return updated
}

func validateRecoveredEscrow(job domain.Job, quote domain.Quote, capability domain.Capability, escrow domain.Escrow) error {
    expectedReserve := domain.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency}
    if escrow.JobID != job.ID || escrow.QuoteID != quote.ID || escrow.PrincipalID != job.PrincipalID ||
        escrow.ProviderID != capability.ProviderID || escrow.CapabilityID != capability.ID ||
        escrow.CapabilityVersion != capability.Version || escrow.TrustMode != quote.TrustMode ||
        escrow.ProofProfile != quote.ProofProfile || escrow.Reserved != expectedReserve {
        return domain.NewError(domain.ErrSettlementFailed, "recovered escrow does not match the committed Job/Quote", false)
    }
    return nil
}

func (s *JobService) recoverOrCreateEscrowUnderLock(ctx context.Context, job domain.Job, quote domain.Quote, capability domain.Capability) (domain.Escrow, error) {
    if existing, err := s.store.EscrowByJob(ctx, job.ID); err == nil {
        if err := validateRecoveredEscrow(job, quote, capability, existing); err != nil {
            return domain.Escrow{}, err
        }
        return existing, nil
    } else if err != store.ErrNotFound {
        return domain.Escrow{}, err
    }
    return s.core.CreateEscrow(ctx, toscore.CreateEscrowRequest{
        QuoteID: quote.ID, JobID: job.ID,
        CapabilityID: capability.ID, CapabilityVersion: capability.Version,
        PrincipalID: job.PrincipalID, ProviderID: capability.ProviderID,
        TrustMode: quote.TrustMode, ProofProfile: quote.ProofProfile,
        Settlement: quote.Settlement,
        Reserved: domain.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency},
    })
}

func (s *JobService) prepareExecutionUnderLock(ctx context.Context, jobID string) (domain.Job, domain.Capability, error) {
    job, err := s.store.GetJob(ctx, jobID)
    if err != nil {
        return domain.Job{}, domain.Capability{}, err
    }
    if job.State.Terminal() || job.State == domain.JobInputRequired || job.State == domain.JobCanceling {
        return job, domain.Capability{}, nil
    }
    if job.State == domain.JobReconciling && job.ReconciliationTarget != "" && job.ReconciliationTarget != domain.JobWorking {
        return job, domain.Capability{}, nil
    }
    quote, err := s.getQuote(ctx, job.QuoteID)
    if err != nil {
        if job.EconomicState == domain.EconomicNone {
            failed := s.finalizeNoEconomyUnderLock(ctx, job, domain.JobFailed, domain.ErrQuoteExpired, "quote unavailable before economic reservation")
            return failed, domain.Capability{}, nil
        }
        return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, domain.JobFailed, domain.ErrQuoteExpired, "quote unavailable while economic recovery is required"), domain.Capability{}, err
    }
    now := time.Now().UTC()
    if job.EconomicState == domain.EconomicNone && (quote.Expired(now) || (!quote.ExecutionDeadline.IsZero() && !now.Before(quote.ExecutionDeadline))) {
        failed := s.finalizeNoEconomyUnderLock(ctx, job, domain.JobFailed, domain.ErrQuoteExpired, "quote expired before economic reservation")
        return failed, domain.Capability{}, nil
    }
    capability, err := s.store.Get(ctx, job.CapabilityID)
    if err != nil {
        if job.EconomicState == domain.EconomicNone {
            failed := s.finalizeNoEconomyUnderLock(ctx, job, domain.JobFailed, domain.ErrCapabilityUnavailable, "capability unavailable before economic reservation")
            return failed, domain.Capability{}, nil
        }
        return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, domain.JobFailed, domain.ErrCapabilityUnavailable, "capability unavailable while economic recovery is required"), domain.Capability{}, err
    }
    capability = normalizeCapability(capability)
    if capability.Version != quote.CapabilityVersion || capability.ProviderID != quote.ProviderID || job.TrustMode != quote.TrustMode || job.ProofProfile != quote.ProofProfile {
        if job.EconomicState == domain.EconomicNone {
            failed := s.finalizeNoEconomyUnderLock(ctx, job, domain.JobFailed, domain.ErrQuoteMismatch, "execution contract no longer matches quote")
            return failed, capability, nil
        }
        return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, domain.JobFailed, domain.ErrQuoteMismatch, "execution contract drifted after funds were reserved"), capability, domain.NewError(domain.ErrQuoteMismatch, "execution contract drifted after funds were reserved", false)
    }
    if job.EconomicState == domain.EconomicNone {
        if quote.PrincipalID == "" {
            quote.PrincipalID = job.PrincipalID
        }
        if _, err := s.core.CommitQuote(ctx, quote); err != nil {
            failed := s.finalizeNoEconomyUnderLock(ctx, job, domain.JobFailed, errCode(err), "quote commitment failed: "+err.Error())
            return failed, capability, nil
        }
        job, err = s.atomicDebitCheckpoint(ctx, job, quote)
        if err != nil {
            failed := s.finalizeNoEconomyUnderLock(ctx, job, domain.JobFailed, errCode(err), err.Error())
            return failed, capability, nil
        }
    }
    if job.EconomicState == domain.EconomicDebited {
        job, err = s.store.UpdateJob(ctx, job.ID, func(current domain.Job, exists bool) (domain.Job, error) {
            if !exists || current.EconomicState != domain.EconomicDebited {
                return domain.Job{}, store.ErrConflict
            }
            current.EconomicState = domain.EconomicEscrowPending
            current.UpdatedAt = time.Now().UTC()
            return current, nil
        })
        if err != nil {
            return job, capability, err
        }
    }
    if job.EconomicState == domain.EconomicEscrowPending {
        escrow, createErr := s.recoverOrCreateEscrowUnderLock(ctx, job, quote, capability)
        if createErr != nil {
            pending := s.markEconomicReconciliationUnderLock(ctx, job.ID, domain.EconomicEscrowPending, domain.JobWorking, domain.ErrSettlementFailed, "escrow outcome requires idempotent recovery: "+createErr.Error())
            return pending, capability, createErr
        }
        job, err = s.store.UpdateJob(ctx, job.ID, func(current domain.Job, exists bool) (domain.Job, error) {
            if !exists {
                return domain.Job{}, store.ErrNotFound
            }
            current.EscrowID = escrow.ID
            current.EconomicState = domain.EconomicEscrowReserved
            current.ProofStatus.Escrow = domain.ProofReserved
            current.State = domain.JobWorking
            current.ReconciliationRequired = false
            current.ReconciliationTarget = ""
            current.ErrorCode = ""
            current.FailureReason = ""
            current.UpdatedAt = time.Now().UTC()
            return current, nil
        })
        if err != nil {
            return job, capability, err
        }
    }
    if job.EconomicState == domain.EconomicEscrowReserved && job.EscrowID != "" && job.State != domain.JobWorking {
        job, err = s.store.UpdateJob(ctx, job.ID, func(current domain.Job, exists bool) (domain.Job, error) {
            if !exists { return domain.Job{}, store.ErrNotFound }
            current.State = domain.JobWorking
            current.ReconciliationRequired = false
            current.ReconciliationTarget = ""
            current.ErrorCode = ""
            current.FailureReason = ""
            current.UpdatedAt = time.Now().UTC()
            return current, nil
        })
    }
    return job, capability, err
}

func (s *JobService) finalizeNoEconomyUnderLock(ctx context.Context, job domain.Job, target domain.JobState, code domain.ErrorCode, reason string) domain.Job {
    updated, err := s.store.UpdateJob(ctx, job.ID, func(current domain.Job, exists bool) (domain.Job, error) {
        if !exists { return domain.Job{}, store.ErrNotFound }
        if current.EconomicState != domain.EconomicNone { return domain.Job{}, store.ErrConflict }
        return finalizeTerminalJob(current, target, code, reason, domain.EconomicNone), nil
    })
    if err != nil { return job }
    return updated
}

func finalizeTerminalJob(job domain.Job, target domain.JobState, code domain.ErrorCode, reason string, economic domain.EconomicState) domain.Job {
    now := time.Now().UTC()
    job.State = target
    job.EconomicState = economic
    job.ReconciliationRequired = false
    job.PendingCredit = nil
    job.ReconciliationTarget = ""
    job.ErrorCode = code
    job.FailureReason = reason
    if target == domain.JobCanceled {
        job.ErrorCode = ""
    }
    job.UpdatedAt = now
    job.CompletedAt = &now
    return job
}

func (s *JobService) refundDebitedWithoutEscrowUnderLock(ctx context.Context, job domain.Job, target domain.JobState, code domain.ErrorCode, reason string) (domain.Job, error) {
    quote, err := s.getQuote(ctx, job.QuoteID)
    if err != nil { return job, err }
    updated, _, err := s.store.UpdateJobAndAccount(ctx, job.ID, job.PrincipalID, s.accounts.defaultAccount(job.PrincipalID), func(current domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
        if !exists { return domain.Job{}, domain.Account{}, store.ErrNotFound }
        if current.EconomicState == domain.EconomicReleased && current.State.Terminal() { return current, account, nil }
        if current.EconomicState != domain.EconomicDebited { return domain.Job{}, domain.Account{}, store.ErrConflict }
        nextAccount, err := s.accounts.creditAccountValue(account, quote.Price.TotalMax, quote.Price.Currency)
        if err != nil { return domain.Job{}, domain.Account{}, err }
        return finalizeTerminalJob(current, target, code, reason, domain.EconomicReleased), nextAccount, nil
    })
    return updated, err
}

func (s *JobService) releaseForTerminalUnderLock(ctx context.Context, job domain.Job, target domain.JobState, code domain.ErrorCode, reason string) (domain.Job, error) {
    if job.EconomicState == domain.EconomicNone {
        return s.finalizeNoEconomyUnderLock(ctx, job, target, code, reason), nil
    }
    if job.EconomicState == domain.EconomicDebited {
        return s.refundDebitedWithoutEscrowUnderLock(ctx, job, target, code, reason)
    }
    quote, err := s.getQuote(ctx, job.QuoteID)
    if err != nil { return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, target, code, reason+"; quote recovery failed"), err }
    capability, err := s.store.Get(ctx, job.CapabilityID)
    if err != nil { return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, target, code, reason+"; capability recovery failed"), err }
    capability = normalizeCapability(capability)
    if job.EconomicState == domain.EconomicEscrowPending {
        escrow, recoverErr := s.recoverOrCreateEscrowUnderLock(ctx, job, quote, capability)
        if recoverErr != nil {
            return s.markEconomicReconciliationUnderLock(ctx, job.ID, domain.EconomicEscrowPending, target, code, reason+"; escrow outcome remains ambiguous: "+recoverErr.Error()), recoverErr
        }
        job, err = s.store.UpdateJob(ctx, job.ID, func(current domain.Job, exists bool) (domain.Job, error) {
            if !exists { return domain.Job{}, store.ErrNotFound }
            current.EscrowID = escrow.ID
            current.EconomicState = domain.EconomicEscrowReserved
            current.ProofStatus.Escrow = domain.ProofReserved
            current.State = domain.JobReconciling
            current.ReconciliationRequired = true
            current.ReconciliationTarget = target
            current.ErrorCode = code
            current.FailureReason = reason
            current.UpdatedAt = time.Now().UTC()
            return current, nil
        })
        if err != nil { return job, err }
    }
    if job.EconomicState == domain.EconomicSettlementPending || job.EconomicState == domain.EconomicSettled {
        return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, domain.JobCompleted, domain.ErrSettlementFailed, "settlement outcome must be recovered before release"), domain.NewError(domain.ErrSettlementFailed, "settlement outcome must be recovered before release", true)
    }
    if job.EscrowID == "" {
        return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, target, code, reason+"; escrow reference missing"), domain.NewError(domain.ErrSettlementFailed, "escrow reference missing", true)
    }
    if job.EconomicState != domain.EconomicReleasePending {
        job, err = s.store.UpdateJob(ctx, job.ID, func(current domain.Job, exists bool) (domain.Job, error) {
            if !exists { return domain.Job{}, store.ErrNotFound }
            current.State = domain.JobReconciling
            current.EconomicState = domain.EconomicReleasePending
            current.ReconciliationRequired = true
            current.ReconciliationTarget = target
            current.ErrorCode = code
            current.FailureReason = reason
            current.UpdatedAt = time.Now().UTC()
            return current, nil
        })
        if err != nil { return job, err }
    }
    receipt, releaseErr := s.core.ReleaseEscrow(ctx, job.EscrowID)
    if releaseErr != nil {
        return s.markEconomicReconciliationUnderLock(ctx, job.ID, domain.EconomicReleasePending, target, code, reason+"; escrow release requires replay: "+releaseErr.Error()), releaseErr
    }
    updated, _, err := s.store.UpdateJobAndAccount(ctx, job.ID, job.PrincipalID, s.accounts.defaultAccount(job.PrincipalID), func(current domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
        if !exists { return domain.Job{}, domain.Account{}, store.ErrNotFound }
        if current.EconomicState == domain.EconomicReleased && current.State.Terminal() { return current, account, nil }
        if current.EconomicState != domain.EconomicReleasePending { return domain.Job{}, domain.Account{}, store.ErrConflict }
        nextAccount := account
        var creditErr error
        if nonZeroMoney(receipt.Refunded) {
            nextAccount, creditErr = s.accounts.creditAccountValue(account, receipt.Refunded.Amount, receipt.Refunded.Currency)
            if creditErr != nil { return domain.Job{}, domain.Account{}, creditErr }
        }
        current.ProofStatus.Escrow = domain.ProofReleased
        current.ProofStatus.Settlement = domain.ProofReleased
        return finalizeTerminalJob(current, target, code, reason, domain.EconomicReleased), nextAccount, nil
    })
    return updated, err
}

func (s *JobService) settleProviderResultUnderLock(ctx context.Context, current domain.Job, result tosai.SubmitJobResult) domain.Job {
    if current.ExecutionReceipt != nil && current.EconomicState == domain.EconomicSettlementPending {
        copied := *current.ExecutionReceipt
        result.Receipt = &copied
        if result.Output == nil { result.Output = cloneMap(current.Output) }
        if len(result.Artifacts) == 0 { result.Artifacts = append([]domain.Artifact(nil), current.Artifacts...) }
    }
    if result.Receipt == nil {
        failed, _ := s.releaseForTerminalUnderLock(ctx, current, domain.JobFailed, domain.ErrProviderFailed, "execution completed without a receipt")
        return failed
    }
    quote, err := s.getQuote(ctx, current.QuoteID)
    if err != nil {
        return s.markEconomicReconciliationUnderLock(ctx, current.ID, current.EconomicState, domain.JobCompleted, domain.ErrSettlementFailed, "quote unavailable during settlement recovery")
    }
    receipt := *result.Receipt
    receipt.Cost = domain.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency}
    if current.EconomicState != domain.EconomicSettlementPending {
        current.ProofStatus.Receipt = domain.ProofSigned
        proofRef, commitErr := s.core.CommitExecutionReceipt(ctx, receipt)
        if commitErr != nil {
            return s.markEconomicReconciliationUnderLock(ctx, current.ID, current.EconomicState, domain.JobWorking, errCode(commitErr), "execution receipt commitment requires replay: "+commitErr.Error())
        }
        if proofRef != "" { receipt.NetworkProofRef = proofRef }
        verify, verifyErr := s.core.VerifyExecutionReceipt(ctx, current.EscrowID, receipt)
        if verifyErr != nil {
            return s.markEconomicReconciliationUnderLock(ctx, current.ID, current.EconomicState, domain.JobWorking, domain.ErrSettlementFailed, "execution receipt verification unavailable: "+verifyErr.Error())
        }
        if !verify.Valid {
            failed, _ := s.releaseForTerminalUnderLock(ctx, current, domain.JobFailed, domain.ErrSettlementFailed, "execution receipt failed verification: "+verify.Reason)
            return failed
        }
        if verify.ProofRef != "" { receipt.NetworkProofRef = verify.ProofRef }
        current, err = s.store.UpdateJob(ctx, current.ID, func(job domain.Job, exists bool) (domain.Job, error) {
            if !exists { return domain.Job{}, store.ErrNotFound }
            job.Output = cloneMap(result.Output)
            job.Artifacts = append([]domain.Artifact(nil), result.Artifacts...)
            copied := receipt
            job.ExecutionReceipt = &copied
            job.ProofStatus.Receipt = domain.ProofVerified
            job.EconomicState = domain.EconomicSettlementPending
            job.State = domain.JobReconciling
            job.ReconciliationRequired = true
            job.ReconciliationTarget = domain.JobCompleted
            job.ErrorCode = domain.ErrSettlementFailed
            job.FailureReason = "settlement pending durable confirmation"
            job.UpdatedAt = time.Now().UTC()
            return job, nil
        })
        if err != nil { return current }
    } else if current.ExecutionReceipt != nil {
        receipt = *current.ExecutionReceipt
    }
    settled, settleErr := s.core.SettleJob(ctx, toscore.SettleJobRequest{
        EscrowID: current.EscrowID, JobID: current.ID, ReceiptID: receipt.ID, ActualCost: receipt.Cost,
    })
    if settleErr != nil {
        return s.markEconomicReconciliationUnderLock(ctx, current.ID, domain.EconomicSettlementPending, domain.JobCompleted, domain.ErrSettlementFailed, "settlement outcome requires idempotent replay: "+settleErr.Error())
    }
    final, _, finalErr := s.store.UpdateJobAndAccount(ctx, current.ID, current.PrincipalID, s.accounts.defaultAccount(current.PrincipalID), func(job domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
        if !exists { return domain.Job{}, domain.Account{}, store.ErrNotFound }
        if job.EconomicState == domain.EconomicSettled && job.State == domain.JobCompleted { return job, account, nil }
        if job.EconomicState != domain.EconomicSettlementPending { return domain.Job{}, domain.Account{}, store.ErrConflict }
        nextAccount := account
        var creditErr error
        if nonZeroMoney(settled.Receipt.Refunded) {
            nextAccount, creditErr = s.accounts.creditAccountValue(account, settled.Receipt.Refunded.Amount, settled.Receipt.Refunded.Currency)
            if creditErr != nil { return domain.Job{}, domain.Account{}, creditErr }
        }
        job.Output = cloneMap(current.Output)
        job.Artifacts = append([]domain.Artifact(nil), current.Artifacts...)
        job.ProofStatus.Receipt = domain.ProofVerified
        job.ProofStatus.Settlement = domain.ProofSettled
        job.EconomicState = domain.EconomicSettled
        job.State = domain.JobCompleted
        job.ReconciliationRequired = false
        job.ReconciliationTarget = ""
        job.ErrorCode = ""
        job.FailureReason = ""
        job.PendingCredit = nil
        now := time.Now().UTC()
        job.UpdatedAt = now
        job.CompletedAt = &now
        return job, nextAccount, nil
    })
    if finalErr != nil {
        return s.markEconomicReconciliationUnderLock(ctx, current.ID, domain.EconomicSettlementPending, domain.JobCompleted, domain.ErrSettlementFailed, "settlement finalized remotely but local atomic finalization must be retried: "+finalErr.Error())
    }
    _, _ = s.core.CommitProofOfServiceEvidence(ctx, receipt)
    return final
}

func (s *JobService) recoverProviderExecution(ctx context.Context, jobID string, allowSubmit bool) (domain.Job, error) {
    lock := s.jobLock(jobID)
    lock.Lock()
    defer lock.Unlock()
    job, err := s.store.GetJob(ctx, jobID)
    if err != nil { return domain.Job{}, err }
    if job.State.Terminal() { return job, nil }
    if job.EconomicState == domain.EconomicSettlementPending && job.ExecutionReceipt != nil {
        result := tosai.SubmitJobResult{State: domain.JobCompleted, Output: cloneMap(job.Output), Artifacts: append([]domain.Artifact(nil), job.Artifacts...), Receipt: job.ExecutionReceipt}
        return s.settleProviderResultUnderLock(ctx, job, result), nil
    }
    capability, err := s.store.Get(ctx, job.CapabilityID)
    if err != nil { return job, err }
    capability = normalizeCapability(capability)
    result, getErr := s.provider.GetJob(ctx, job.ID)
    if getErr != nil {
        if !domainErrorIs(getErr, domain.ErrNotFound) {
            pending := s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, domain.JobWorking, domain.ErrProviderFailed, "provider execution status unavailable: "+getErr.Error())
            return pending, getErr
        }
        if !allowSubmit {
            return job, getErr
        }
        if !job.ExecutionDeadline.IsZero() && !time.Now().UTC().Before(job.ExecutionDeadline) {
            released, releaseErr := s.releaseForTerminalUnderLock(ctx, job, domain.JobFailed, domain.ErrProviderFailed, "execution was never admitted before its deadline")
            return released, releaseErr
        }
        result, getErr = s.provider.SubmitJob(ctx, tosai.SubmitJobRequest{
            JobID: job.ID, InvocationID: job.InvocationID,
            QuoteID: job.QuoteID, ServiceQuoteID: job.ServiceQuoteID,
            EscrowID: job.EscrowID, PrincipalID: job.PrincipalID,
            CapabilityID: job.CapabilityID, CapabilityVersion: job.CapabilityVersion,
            ProviderID: job.ProviderID, TrustMode: job.TrustMode, ProofProfile: job.ProofProfile,
            Input: job.Input, InputCommitment: hashCommitment(job.Input),
            ExecutionDeadline: job.ExecutionDeadline, RetainUntil: time.Now().UTC().Add(executionRetention),
        })
        if getErr != nil {
            pending := s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, domain.JobWorking, domain.ErrProviderFailed, "provider submission outcome requires recovery: "+getErr.Error())
            return pending, getErr
        }
    }
    if result.State == domain.JobCompleted {
        return s.settleProviderResultUnderLock(ctx, job, result), nil
    }
    if result.State.Terminal() {
        released, releaseErr := s.releaseForTerminalUnderLock(ctx, job, domain.JobFailed, domain.ErrProviderFailed, fmt.Sprintf("provider execution ended in %s", result.State))
        return released, releaseErr
    }
    if !job.ExecutionDeadline.IsZero() && !time.Now().UTC().Before(job.ExecutionDeadline) {
        if cancelErr := s.provider.CancelJob(ctx, job.ID, "execution deadline exceeded during recovery"); cancelErr != nil {
            pending := s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, domain.JobFailed, domain.ErrProviderFailed, "provider cancellation outcome requires recovery: "+cancelErr.Error())
            return pending, cancelErr
        }
        released, releaseErr := s.releaseForTerminalUnderLock(ctx, job, domain.JobFailed, domain.ErrProviderFailed, "execution deadline exceeded")
        return released, releaseErr
    }
    return job, nil
}

func (s *JobService) ReconcileJob(ctx context.Context, jobID string) (domain.Job, error) {
    job, err := s.store.GetJob(ctx, jobID)
    if err != nil { return domain.Job{}, err }
    if job.State.Terminal() || job.State == domain.JobInputRequired { return job, nil }
    if job.PendingCredit != nil {
        return s.reconcileCredit(ctx, jobID)
    }
    switch job.EconomicState {
    case domain.EconomicNone:
        result, execErr := s.executeJob(ctx, jobID, false, 0)
        return result.Job, execErr
    case domain.EconomicDebited:
        if quote, quoteErr := s.getQuote(ctx, job.QuoteID); quoteErr == nil && (quote.Expired(time.Now().UTC()) || (!quote.ExecutionDeadline.IsZero() && !time.Now().UTC().Before(quote.ExecutionDeadline))) {
            lock := s.jobLock(jobID); lock.Lock(); defer lock.Unlock()
            return s.refundDebitedWithoutEscrowUnderLock(ctx, job, domain.JobFailed, domain.ErrQuoteExpired, "quote expired after debit but before escrow creation")
        }
        result, execErr := s.executeJob(ctx, jobID, false, 0)
        return result.Job, execErr
    case domain.EconomicEscrowPending:
        if job.ReconciliationTarget == domain.JobFailed || job.ReconciliationTarget == domain.JobCanceled {
            lock := s.jobLock(jobID); lock.Lock(); defer lock.Unlock()
            return s.releaseForTerminalUnderLock(ctx, job, job.ReconciliationTarget, job.ErrorCode, job.FailureReason)
        }
        result, execErr := s.executeJob(ctx, jobID, false, 0)
        return result.Job, execErr
    case domain.EconomicEscrowReserved:
        if job.ReconciliationTarget == domain.JobFailed || job.ReconciliationTarget == domain.JobCanceled {
            lock := s.jobLock(jobID); lock.Lock(); defer lock.Unlock()
            return s.releaseForTerminalUnderLock(ctx, job, job.ReconciliationTarget, job.ErrorCode, job.FailureReason)
        }
        return s.recoverProviderExecution(ctx, jobID, true)
    case domain.EconomicSettlementPending:
        return s.recoverProviderExecution(ctx, jobID, false)
    case domain.EconomicReleasePending:
        lock := s.jobLock(jobID); lock.Lock(); defer lock.Unlock()
        target := job.ReconciliationTarget
        if target == "" { target = domain.JobFailed }
        return s.releaseForTerminalUnderLock(ctx, job, target, job.ErrorCode, job.FailureReason)
    case domain.EconomicSettled, domain.EconomicReleased:
        return job, nil
    default:
        return job, domain.NewError(domain.ErrSettlementFailed, "unknown economic recovery checkpoint", false)
    }
}

func (s *JobService) ReconcileStaleJobs(ctx context.Context, updatedBefore time.Time, limit int) (int, error) {
    jobs, err := s.store.JobsForRecovery(ctx, updatedBefore, limit)
    if err != nil { return 0, err }
    var joined error
    for _, job := range jobs {
        if _, err := s.ReconcileJob(ctx, job.ID); err != nil {
            joined = errors.Join(joined, fmt.Errorf("reconcile %s: %w", job.ID, err))
        }
    }
    return len(jobs), joined
}

func (s *JobService) RunReconciler(ctx context.Context, interval, staleAfter time.Duration, limit int, report func(error)) {
    if interval <= 0 { interval = defaultReconcileInterval }
    if staleAfter <= 0 { staleAfter = defaultReconcileStaleAfter }
    if limit <= 0 { limit = defaultReconcileBatch }
    sweep := func() {
        _, err := s.ReconcileStaleJobs(ctx, time.Now().UTC().Add(-staleAfter), limit)
        if err != nil && report != nil { report(err) }
    }
    sweep()
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case <-ticker.C: sweep()
        }
    }
}
''')

# ---------------------------------------------------------------------------
# Route the existing JobService through the new durable state machine.
# ---------------------------------------------------------------------------
replace_func(
    "internal/service/job.go",
    "executeJob(ctx context.Context, jobID string, waitInline bool, maxWaitMS int64) (SubmitResult, error) {",
    "runToCompletion(ctx context.Context, snapshot domain.Job, capability domain.Capability) domain.Job {",
    r'''func (s *JobService) executeJob(ctx context.Context, jobID string, waitInline bool, maxWaitMS int64) (SubmitResult, error) {
    lock := s.jobLock(jobID)
    lock.Lock()
    job, capability, err := s.prepareExecutionUnderLock(ctx, jobID)
    lock.Unlock()
    if err != nil {
        current, getErr := s.store.GetJob(ctx, jobID)
        if getErr == nil {
            return SubmitResult{Type: resultTypeFor(current), Job: current}, err
        }
        return SubmitResult{}, err
    }
    if job.State != domain.JobWorking || job.EconomicState != domain.EconomicEscrowReserved {
        return SubmitResult{Type: resultTypeFor(job), Job: job}, nil
    }

    done := make(chan domain.Job, 1)
    go func(snapshot domain.Job, capability domain.Capability) {
        runCtx := context.Background()
        cancel := func() {}
        if !snapshot.ExecutionDeadline.IsZero() {
            runCtx, cancel = context.WithDeadline(runCtx, snapshot.ExecutionDeadline)
        }
        defer cancel()
        done <- s.runToCompletion(runCtx, snapshot, capability)
    }(job, capability)

    if !waitInline {
        return SubmitResult{Type: ResultAccepted, Job: job}, nil
    }
    wait := time.Duration(maxWaitMS) * time.Millisecond
    if maxWaitMS <= 0 { wait = inlineWaitDefault }
    timer := time.NewTimer(wait)
    defer timer.Stop()
    select {
    case finished := <-done:
        return SubmitResult{Type: resultTypeFor(finished), Job: finished}, nil
    case <-timer.C:
        current, err := s.store.GetJob(ctx, job.ID)
        if err != nil { return SubmitResult{}, err }
        return SubmitResult{Type: resultTypeFor(current), Job: current}, nil
    case <-ctx.Done():
        current, err := s.store.GetJob(context.Background(), job.ID)
        if err != nil { return SubmitResult{}, ctx.Err() }
        return SubmitResult{Type: resultTypeFor(current), Job: current}, nil
    }
}''',
)
replace_func(
    "internal/service/job.go",
    "runToCompletion(ctx context.Context, snapshot domain.Job, capability domain.Capability) domain.Job {",
    "claimForExecution(ctx context.Context, jobID string) (domain.Job, bool, error) {",
    r'''func (s *JobService) runToCompletion(ctx context.Context, snapshot domain.Job, capability domain.Capability) domain.Job {
    _ = capability
    recovered, err := s.recoverProviderExecution(ctx, snapshot.ID, true)
    if err != nil {
        if current, getErr := s.store.GetJob(context.Background(), snapshot.ID); getErr == nil {
            return current
        }
        return snapshot
    }
    return recovered
}''',
)
replace_func(
    "internal/service/job.go",
    "failUnderLock(ctx context.Context, jobID string, code domain.ErrorCode, reason string) SubmitResult {",
    "Get(ctx context.Context, jobID string) (domain.Job, error) {",
    r'''func (s *JobService) failUnderLock(ctx context.Context, jobID string, code domain.ErrorCode, reason string) SubmitResult {
    job, err := s.store.GetJob(ctx, jobID)
    if err != nil {
        return SubmitResult{Type: ResultFailed, Job: domain.Job{ID: jobID}}
    }
    terminal, releaseErr := s.releaseForTerminalUnderLock(ctx, job, domain.JobFailed, code, reason)
    if releaseErr != nil {
        return SubmitResult{Type: ResultAccepted, Job: terminal}
    }
    return SubmitResult{Type: resultTypeFor(terminal), Job: terminal}
}''',
)
# Replace Get so reconciliation is driven on explicit reads too.
replace_func(
    "internal/service/job.go",
    "Get(ctx context.Context, jobID string) (domain.Job, error) {",
    "Cancel(ctx context.Context, jobID, principalID, reason, idempotencyKey string) (domain.Job, error) {",
    r'''func (s *JobService) Get(ctx context.Context, jobID string) (domain.Job, error) {
    job, err := s.store.GetJob(ctx, jobID)
    if err != nil {
        if err == store.ErrNotFound { return domain.Job{}, domain.NewError(domain.ErrNotFound, "job not found", false) }
        return domain.Job{}, err
    }
    if job.State == domain.JobReconciling {
        reconciled, reconcileErr := s.ReconcileJob(ctx, jobID)
        if reconcileErr == nil { return reconciled, nil }
        return reconciled, domain.NewError(domain.ErrSettlementFailed, "job economic reconciliation is still pending: "+reconcileErr.Error(), true)
    }
    return job, nil
}''',
)
replace_func(
    "internal/service/job.go",
    "Cancel(ctx context.Context, jobID, principalID, reason, idempotencyKey string) (domain.Job, error) {",
    "reconcileCredit(ctx context.Context, jobID string) (domain.Job, error) {",
    r'''func (s *JobService) Cancel(ctx context.Context, jobID, principalID, reason, idempotencyKey string) (domain.Job, error) {
    if idempotencyKey == "" { return domain.Job{}, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false) }
    now := time.Now().UTC()
    requestHash := hashRequest("atos-cancel-v1", jobID, reason)
    rec, reserved, err := s.store.Reserve(ctx, principalID, idempotencyKey, requestHash, now.Add(idempotencyLease))
    if err != nil { return domain.Job{}, err }
    if !reserved {
        if rec.RequestHash != requestHash { return domain.Job{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false) }
        if rec.Status != store.IdempotencyCompleted { return domain.Job{}, domain.NewError(domain.ErrIdempotencyConflict, "a cancellation with this idempotency_key is still in progress", true) }
        return s.store.GetJob(ctx, rec.ResponseKey)
    }
    committed := false
    defer func() { if !committed { _ = s.store.Release(context.Background(), principalID, idempotencyKey) } }()

    lock := s.jobLock(jobID)
    lock.Lock()
    defer lock.Unlock()
    job, err := s.store.GetJob(ctx, jobID)
    if err != nil { return domain.Job{}, domain.NewError(domain.ErrNotFound, "job not found", false) }
    if job.PrincipalID != principalID { return domain.Job{}, domain.NewError(domain.ErrPermissionDenied, "not the job's owning principal", false) }
    if job.State.Terminal() { return domain.Job{}, domain.NewError(domain.ErrJobNotCancelable, "job is already terminal", false) }

    if job.State == domain.JobInputRequired && job.EconomicState == domain.EconomicNone {
        job = finalizeCanceled(job, reason, now)
        if err := s.store.PutJob(ctx, job); err != nil { return domain.Job{}, err }
    } else {
        if job.State == domain.JobWorking || job.State == domain.JobCanceling {
            job.State = domain.JobCanceling
            job.ReconciliationRequired = true
            job.ReconciliationTarget = domain.JobCanceled
            job.FailureReason = reason
            job.UpdatedAt = now
            if err := s.store.PutJob(ctx, job); err != nil { return domain.Job{}, err }
            if cancelErr := s.provider.CancelJob(ctx, job.ID, reason); cancelErr != nil {
                providerResult, statusErr := s.provider.GetJob(ctx, job.ID)
                if statusErr == nil && providerResult.State == domain.JobCompleted {
                    settled := s.settleProviderResultUnderLock(ctx, job, providerResult)
                    _ = s.store.Finish(ctx, principalID, idempotencyKey, jobID)
                    committed = true
                    return settled, domain.NewError(domain.ErrJobNotCancelable, "job completed before cancellation could be established", false)
                }
                if statusErr != nil && !domainErrorIs(statusErr, domain.ErrNotFound) {
                    pending := s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, domain.JobCanceled, domain.ErrProviderFailed, reason+"; provider cancellation outcome requires recovery: "+cancelErr.Error())
                    if finishErr := s.store.Finish(ctx, principalID, idempotencyKey, jobID); finishErr != nil { return domain.Job{}, finishErr }
                    committed = true
                    return pending, domain.NewError(domain.ErrProviderFailed, "provider cancellation requires reconciliation", true)
                }
            }
        }
        terminal, releaseErr := s.releaseForTerminalUnderLock(ctx, job, domain.JobCanceled, "", reason)
        job = terminal
        if releaseErr != nil {
            if finishErr := s.store.Finish(ctx, principalID, idempotencyKey, jobID); finishErr != nil { return domain.Job{}, finishErr }
            committed = true
            return job, domain.NewError(domain.ErrSettlementFailed, "cancellation economic release requires reconciliation", true)
        }
    }
    if err := s.store.Finish(ctx, principalID, idempotencyKey, jobID); err != nil { return domain.Job{}, err }
    committed = true
    return job, nil
}''',
)
replace_func(
    "internal/service/job.go",
    "reconcileCredit(ctx context.Context, jobID string) (domain.Job, error) {",
    "markReconciliationUnderLock(ctx context.Context, job domain.Job, credit domain.Money, target domain.JobState, reason string) domain.Job {",
    r'''func (s *JobService) reconcileCredit(ctx context.Context, jobID string) (domain.Job, error) {
    lock := s.jobLock(jobID)
    lock.Lock()
    defer lock.Unlock()
    job, err := s.store.GetJob(ctx, jobID)
    if err != nil { return domain.Job{}, err }
    if job.State != domain.JobReconciling || job.PendingCredit == nil { return job, nil }
    credit := *job.PendingCredit
    updated, _, err := s.store.UpdateJobAndAccount(ctx, job.ID, job.PrincipalID, s.accounts.defaultAccount(job.PrincipalID), func(current domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
        if !exists || current.PendingCredit == nil { return domain.Job{}, domain.Account{}, store.ErrConflict }
        nextAccount, err := s.accounts.creditAccountValue(account, credit.Amount, credit.Currency)
        if err != nil { return domain.Job{}, domain.Account{}, err }
        target := current.ReconciliationTarget
        if target == "" { target = domain.JobFailed }
        current = finalizeTerminalJob(current, target, current.ErrorCode, current.FailureReason, current.EconomicState)
        return current, nextAccount, nil
    })
    return updated, err
}''',
)

# Main starts an immediate + periodic recovery sweep before serving traffic.
replace_once(
    "cmd/api/main.go",
    '''\tjobs := service.NewJobService(st, execution, core, accounts)\n\treceipts := service.NewReceiptService(st, core)\n''',
    '''\tjobs := service.NewJobService(st, execution, core, accounts)\n\treceipts := service.NewReceiptService(st, core)\n\treconcileCtx, reconcileCancel := context.WithCancel(context.Background())\n\tdefer reconcileCancel()\n\tgo jobs.RunReconciler(reconcileCtx, 15*time.Second, 30*time.Second, 100, func(reconcileErr error) {\n\t\tlogger.Error("managed economic reconciliation pending", "error", reconcileErr)\n\t})\n''',
)

# ---------------------------------------------------------------------------
# Failure-injection tests for the two P1 findings and ambiguous external effects.
# ---------------------------------------------------------------------------
Path("internal/service/economic_crash_safety_test.go").write_text(r'''package service_test

import (
    "context"
    "testing"
    "time"

    tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
    "github.com/tosnetwork/atos/internal/adapters/toscore"
    toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
    "github.com/tosnetwork/atos/internal/domain"
    "github.com/tosnetwork/atos/internal/service"
    "github.com/tosnetwork/atos/internal/store"
    "github.com/tosnetwork/atos/internal/store/memory"
)

type loseCreateEscrowResponseCore struct {
    toscore.Core
    lost bool
}
func (c *loseCreateEscrowResponseCore) CreateEscrow(ctx context.Context, req toscore.CreateEscrowRequest) (domain.Escrow, error) {
    escrow, err := c.Core.CreateEscrow(ctx, req)
    if err == nil && !c.lost {
        c.lost = true
        return domain.Escrow{}, domain.NewError(domain.ErrNetworkUnavailable, "injected lost CreateEscrow response", true)
    }
    return escrow, err
}

type loseSettleResponseCore struct {
    toscore.Core
    lost bool
}
func (c *loseSettleResponseCore) SettleJob(ctx context.Context, req toscore.SettleJobRequest) (toscore.SettleJobResult, error) {
    result, err := c.Core.SettleJob(ctx, req)
    if err == nil && !c.lost {
        c.lost = true
        return toscore.SettleJobResult{}, domain.NewError(domain.ErrNetworkUnavailable, "injected lost SettleJob response", true)
    }
    return result, err
}

func crashHarness(coreWrapper func(toscore.Core) toscore.Core) harness {
    st := memory.New()
    provider := tosaimock.New()
    base := toscoremock.New(st)
    core := toscore.Core(base)
    if coreWrapper != nil { core = coreWrapper(core) }
    capabilities := service.NewCapabilityService(st)
    quotes := service.NewQuoteService(st)
    accounts := service.NewAccountService(st)
    quotes.WithAccountService(accounts)
    jobs := service.NewJobService(st, provider, core, accounts)
    return harness{capabilities: capabilities, quotes: quotes, accounts: accounts, jobs: jobs, st: st}
}

func TestCrashRecoveryLostCreateEscrowResponseDoesNotDoubleDebit(t *testing.T) {
    ctx := context.Background()
    h := crashHarness(func(core toscore.Core) toscore.Core { return &loseCreateEscrowResponseCore{Core: core} })
    cap := registerCapability(t, h, "agt_crash_create", "1.00")
    quote := createQuote(t, h, cap.ID)
    result, err := h.jobs.Invoke(ctx, service.SubmitInput{PrincipalID: "prn_crash_create", CapabilityID: cap.ID, QuoteID: quote.ID, Input: map[string]any{"x": 1}, IdempotencyKey: "crash-create"})
    if err == nil { t.Fatal("injected lost CreateEscrow response unexpectedly returned success") }
    if result.Job.State != domain.JobReconciling || result.Job.EconomicState != domain.EconomicEscrowPending {
        t.Fatalf("checkpoint after lost CreateEscrow = state %q economic %q", result.Job.State, result.Job.EconomicState)
    }
    account, err := h.accounts.Get(ctx, "prn_crash_create"); if err != nil { t.Fatal(err) }
    if account.Balance.Amount != "23.95" { t.Fatalf("balance after ambiguous create = %s", account.Balance.Amount) }
    if _, err := h.st.EscrowByJob(ctx, result.Job.ID); err != nil { t.Fatalf("external escrow side effect was not recoverable: %v", err) }

    recovered, err := h.jobs.ReconcileJob(ctx, result.Job.ID)
    if err != nil { t.Fatalf("ReconcileJob: %v", err) }
    if recovered.State != domain.JobCompleted || recovered.EconomicState != domain.EconomicSettled { t.Fatalf("recovered job = %+v", recovered) }
    account, err = h.accounts.Get(ctx, "prn_crash_create"); if err != nil { t.Fatal(err) }
    if account.Balance.Amount != "23.95" { t.Fatalf("recovery double-debited or refunded: %s", account.Balance.Amount) }
}

func TestCrashRecoveryLostSettlementResponseFinalizesExactlyOnce(t *testing.T) {
    ctx := context.Background()
    h := crashHarness(func(core toscore.Core) toscore.Core { return &loseSettleResponseCore{Core: core} })
    cap := registerCapability(t, h, "agt_crash_settle", "1.00")
    quote := createQuote(t, h, cap.ID)
    result, err := h.jobs.Invoke(ctx, service.SubmitInput{PrincipalID: "prn_crash_settle", CapabilityID: cap.ID, QuoteID: quote.ID, Input: map[string]any{"x": 2}, IdempotencyKey: "crash-settle"})
    if err != nil { t.Fatalf("Invoke should surface a durable reconciling job, got %v", err) }
    if result.Job.State != domain.JobReconciling || result.Job.EconomicState != domain.EconomicSettlementPending || result.Job.ExecutionReceipt == nil {
        t.Fatalf("settlement checkpoint missing after lost response: %+v", result.Job)
    }
    recovered, err := h.jobs.ReconcileJob(ctx, result.Job.ID)
    if err != nil { t.Fatalf("ReconcileJob: %v", err) }
    if recovered.State != domain.JobCompleted || recovered.EconomicState != domain.EconomicSettled { t.Fatalf("recovered job = %+v", recovered) }
    for i := 0; i < 3; i++ {
        again, err := h.jobs.ReconcileJob(ctx, result.Job.ID); if err != nil { t.Fatal(err) }
        if again.State != domain.JobCompleted { t.Fatalf("terminal replay changed state: %s", again.State) }
    }
    account, err := h.accounts.Get(ctx, "prn_crash_settle"); if err != nil { t.Fatal(err) }
    if account.Balance.Amount != "23.95" { t.Fatalf("settlement replay mutated balance twice: %s", account.Balance.Amount) }
}

func TestReconcilerRestoresDebitedJobBeforeEscrow(t *testing.T) {
    ctx := context.Background()
    h := crashHarness(nil)
    cap := registerCapability(t, h, "agt_crash_debit", "1.00")
    quote := createQuote(t, h, cap.ID)
    account, err := h.accounts.Get(ctx, "prn_crash_debit"); if err != nil { t.Fatal(err) }
    account.Balance.Amount = "23.95"
    account.SpendPolicy.RemainingToday.Amount = "18.95"
    if err := h.st.PutAccount(ctx, account); err != nil { t.Fatal(err) }
    now := time.Now().UTC().Add(-time.Minute)
    job := domain.Job{ID: "job_crash_debit", CapabilityID: cap.ID, CapabilityVersion: cap.Version, ProviderID: cap.ProviderID, QuoteID: quote.ID, PrincipalID: "prn_crash_debit", TrustMode: quote.TrustMode, ProofProfile: quote.ProofProfile, State: domain.JobSubmitted, EconomicState: domain.EconomicDebited, Input: map[string]any{"x": 3}, IdempotencyKey: "crash-debit", CreatedAt: now, UpdatedAt: now, ExecutionDeadline: quote.ExecutionDeadline, ServiceQuoteID: quote.ServiceQuoteID, Artifacts: []domain.Artifact{}}
    if err := h.st.PutJob(ctx, job); err != nil { t.Fatal(err) }
    recovered, err := h.jobs.ReconcileJob(ctx, job.ID); if err != nil { t.Fatal(err) }
    if recovered.State != domain.JobCompleted || recovered.EconomicState != domain.EconomicSettled { t.Fatalf("recovered job = %+v", recovered) }
    account, err = h.accounts.Get(ctx, job.PrincipalID); if err != nil { t.Fatal(err) }
    if account.Balance.Amount != "23.95" { t.Fatalf("recovery repeated debit: %s", account.Balance.Amount) }
}

func TestReconcileScanFindsStaleEconomicJob(t *testing.T) {
    ctx := context.Background()
    h := crashHarness(nil)
    cap := registerCapability(t, h, "agt_scan", "1.00")
    quote := createQuote(t, h, cap.ID)
    account, _ := h.accounts.Get(ctx, "prn_scan")
    account.Balance.Amount = "23.95"
    account.SpendPolicy.RemainingToday.Amount = "18.95"
    _ = h.st.PutAccount(ctx, account)
    old := time.Now().UTC().Add(-2 * time.Minute)
    job := domain.Job{ID: "job_scan", CapabilityID: cap.ID, CapabilityVersion: cap.Version, ProviderID: cap.ProviderID, QuoteID: quote.ID, PrincipalID: "prn_scan", TrustMode: quote.TrustMode, ProofProfile: quote.ProofProfile, State: domain.JobSubmitted, EconomicState: domain.EconomicDebited, Input: map[string]any{}, IdempotencyKey: "scan", CreatedAt: old, UpdatedAt: old, ExecutionDeadline: quote.ExecutionDeadline, ServiceQuoteID: quote.ServiceQuoteID, Artifacts: []domain.Artifact{}}
    if err := h.st.PutJob(ctx, job); err != nil { t.Fatal(err) }
    count, err := h.jobs.ReconcileStaleJobs(ctx, time.Now().UTC().Add(-30*time.Second), 10)
    if err != nil { t.Fatal(err) }
    if count != 1 { t.Fatalf("reconciled count = %d, want 1", count) }
    got, err := h.st.GetJob(ctx, job.ID); if err != nil { t.Fatal(err) }
    if got.State != domain.JobCompleted { t.Fatalf("stale job state = %s", got.State) }
}

var _ store.Store
''')

print("stage2 crash-safe economic state machine materialized")
