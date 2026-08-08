// JobService implements the Quote -> Escrow -> Execute -> Verify -> Settle
// pipeline. Invocation and Job share one durable record. Spend confirmation is
// a separate, server-issued challenge bound to the immutable Quote/request;
// callers cannot self-assert authorization with a boolean parameter.
package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

const (
	inlineWaitDefault  = 30 * time.Second
	inlineWaitMaximum  = 2 * time.Minute
	idempotencyLease   = 2 * time.Minute
	confirmationTTL    = 10 * time.Minute
	executionRetention = 48 * time.Hour
)

type JobService struct {
	store    store.Store
	provider tosai.Provider
	core     toscore.Core
	accounts *AccountService
	jobLocks sync.Map // job_id -> *sync.Mutex; process-local lifecycle serialization
}

func NewJobService(s store.Store, provider tosai.Provider, core toscore.Core, accounts *AccountService) *JobService {
	return &JobService{store: s, provider: provider, core: core, accounts: accounts}
}

type SubmitInput struct {
	PrincipalID    string
	CapabilityID   string
	QuoteID        string
	Input          map[string]any
	IdempotencyKey string
	MaxWaitMS      int64
}

type SubmitResultType string

const (
	ResultCompleted     SubmitResultType = "completed"
	ResultAccepted      SubmitResultType = "accepted"
	ResultInputRequired SubmitResultType = "input_required"
	ResultFailed        SubmitResultType = "failed"
)

type SubmitResult struct {
	Type SubmitResultType
	Job  domain.Job
}

func (s *JobService) Invoke(ctx context.Context, in SubmitInput) (SubmitResult, error) {
	return s.submit(ctx, in, true, "inv_")
}

func (s *JobService) CreateJob(ctx context.Context, in SubmitInput) (SubmitResult, error) {
	in.MaxWaitMS = 0
	return s.submit(ctx, in, false, "job_")
}

func (s *JobService) submit(ctx context.Context, in SubmitInput, waitInline bool, idPrefix string) (SubmitResult, error) {
	if in.PrincipalID == "" {
		return SubmitResult{}, domain.NewError(domain.ErrAuthenticationRequired, "principal is required", false)
	}
	if in.IdempotencyKey == "" {
		return SubmitResult{}, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}
	if in.MaxWaitMS < 0 || time.Duration(in.MaxWaitMS)*time.Millisecond > inlineWaitMaximum {
		return SubmitResult{}, domain.NewError(domain.ErrValidationFailed, "max_wait_ms is outside the allowed range", false)
	}

	requestHash := hashRequest("atos-submit-v1", in.CapabilityID, in.QuoteID, in.Input)
	now := time.Now().UTC()
	rec, reserved, err := s.store.Reserve(ctx, in.PrincipalID, in.IdempotencyKey, requestHash, now.Add(idempotencyLease))
	if err != nil {
		return SubmitResult{}, err
	}
	if !reserved {
		return s.replaySubmit(ctx, in, rec, requestHash, waitInline)
	}

	committed := false
	defer func() {
		if !committed {
			_ = s.store.Release(context.Background(), in.PrincipalID, in.IdempotencyKey)
		}
	}()

	// Recover a process crash after the Job commit but before Finish. The
	// unique (principal_id,idempotency_key) index makes this unambiguous.
	if existing, lookupErr := s.store.JobByIdempotencyKey(ctx, in.PrincipalID, in.IdempotencyKey); lookupErr == nil {
		if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, existing.ID); err != nil {
			return SubmitResult{}, err
		}
		committed = true
		return s.resumeOrReturn(ctx, existing, waitInline, in.MaxWaitMS)
	} else if lookupErr != store.ErrNotFound {
		return SubmitResult{}, lookupErr
	}

	quote, err := s.getQuote(ctx, in.QuoteID)
	if err != nil {
		return SubmitResult{}, err
	}
	if quote.CapabilityID != in.CapabilityID {
		return SubmitResult{}, domain.NewError(domain.ErrQuoteMismatch, "quote does not match capability_id", false)
	}
	if quote.PrincipalID != "" && quote.PrincipalID != in.PrincipalID {
		return SubmitResult{}, domain.NewError(domain.ErrQuoteMismatch, "quote belongs to a different principal", false)
	}
	if quote.Expired(now) {
		return SubmitResult{}, domain.NewError(domain.ErrQuoteExpired, "quote has expired", false)
	}
	if err := domain.ValidateCommittedTrust(quote.TrustMode, quote.ProofProfile); err != nil {
		return SubmitResult{}, domain.NewError(domain.ErrQuoteModeMismatch, err.Error(), false)
	}
	capability, err := s.store.Get(ctx, in.CapabilityID)
	if err != nil {
		return SubmitResult{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability not found", false)
	}
	capability = normalizeCapability(capability)
	if capability.Version != quote.CapabilityVersion || capability.ProviderID != quote.ProviderID {
		return SubmitResult{}, domain.NewError(domain.ErrQuoteMismatch, "capability/provider changed after quote issuance", false)
	}
	if !capability.Supports(quote.TrustMode) {
		return SubmitResult{}, domain.NewError(domain.ErrTrustModeUnavailable, "quoted trust mode is no longer active", true)
	}
	if quote.ProofProfile != domain.StandardProofProfile(quote.TrustMode) && quote.TrustMode != domain.TrustModeManaged {
		return SubmitResult{}, domain.NewError(domain.ErrQuoteModeMismatch, "quote proof profile does not match trust mode", false)
	}

	needsConfirmation, err := s.accounts.RequiresConfirmation(ctx, in.PrincipalID, quote.Price.TotalMax, quote.Price.Currency)
	if err != nil {
		return SubmitResult{}, err
	}
	state := domain.JobSubmitted
	proofStatus := domain.InitialProofStatus(quote.TrustMode)
	proofStatus.Escrow = domain.ProofPending
	if quote.TrustMode != domain.TrustModeManaged {
		proofStatus.Quote = domain.ProofPending
	}
	job := domain.Job{
		ID: idPrefix + uuid.NewString(), CapabilityID: capability.ID,
		CapabilityVersion: capability.Version, ProviderID: capability.ProviderID,
		QuoteID: quote.ID, ServiceQuoteID: quote.ServiceQuoteID,
		PrincipalID: in.PrincipalID, TrustMode: quote.TrustMode,
		ProofProfile: quote.ProofProfile, ProofStatus: proofStatus,
		State: state, Input: cloneMap(in.Input), IdempotencyKey: in.IdempotencyKey,
		CreatedAt: now, UpdatedAt: now, ExecutionDeadline: quote.ExecutionDeadline,
	}
	if idPrefix == "inv_" {
		job.InvocationID = job.ID
	}
	if needsConfirmation {
		job.State = domain.JobInputRequired
		job.Confirmation = newSpendConfirmation(job, quote, now)
	}
	if err := s.store.PutJob(ctx, job); err != nil {
		return SubmitResult{}, err
	}
	if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, job.ID); err != nil {
		return SubmitResult{}, err
	}
	committed = true
	if job.State == domain.JobInputRequired {
		return SubmitResult{Type: ResultInputRequired, Job: job}, nil
	}
	return s.executeJob(ctx, job.ID, waitInline, in.MaxWaitMS)
}

func (s *JobService) replaySubmit(ctx context.Context, in SubmitInput, rec store.IdempotencyRecord, requestHash string, waitInline bool) (SubmitResult, error) {
	if rec.RequestHash != requestHash {
		return SubmitResult{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false)
	}
	if rec.Status != store.IdempotencyCompleted {
		return SubmitResult{}, domain.NewError(domain.ErrIdempotencyConflict, "a request with this idempotency_key is still in progress; retry shortly", true)
	}
	job, err := s.store.GetJob(ctx, rec.ResponseKey)
	if err != nil {
		return SubmitResult{}, err
	}
	return s.resumeOrReturn(ctx, job, waitInline, in.MaxWaitMS)
}

func (s *JobService) resumeOrReturn(ctx context.Context, job domain.Job, waitInline bool, maxWaitMS int64) (SubmitResult, error) {
	if job.State == domain.JobInputRequired {
		return s.resumeConfirmedJob(ctx, job.ID, waitInline, maxWaitMS)
	}
	if job.State == domain.JobReconciling {
		reconciled, err := s.reconcileCredit(ctx, job.ID)
		if err != nil {
			return SubmitResult{Type: ResultAccepted, Job: reconciled}, err
		}
		job = reconciled
	}
	return SubmitResult{Type: resultTypeFor(job), Job: job}, nil
}

// DecideConfirmation records an authenticated principal's approval or denial.
// Approval does not execute immediately; the original idempotent submission is
// replayed by the client and consumes the challenge exactly once.
func (s *JobService) DecideConfirmation(ctx context.Context, userCode, principalID string, approve bool) (domain.Job, error) {
	userCode = normalizeConfirmationCode(userCode)
	job, err := s.store.JobByConfirmationCode(ctx, userCode)
	if err != nil {
		return domain.Job{}, domain.NewError(domain.ErrNotFound, "spend confirmation not found", false)
	}
	if job.PrincipalID != principalID {
		return domain.Job{}, domain.NewError(domain.ErrPermissionDenied, "confirmation belongs to another principal", false)
	}
	lock := s.jobLock(job.ID)
	lock.Lock()
	defer lock.Unlock()
	now := time.Now().UTC()
	updated, err := s.store.UpdateJob(ctx, job.ID, func(current domain.Job, exists bool) (domain.Job, error) {
		if !exists || current.Confirmation == nil || current.Confirmation.UserCode != userCode {
			return domain.Job{}, domain.NewError(domain.ErrNotFound, "spend confirmation not found", false)
		}
		if current.PrincipalID != principalID {
			return domain.Job{}, domain.NewError(domain.ErrPermissionDenied, "confirmation belongs to another principal", false)
		}
		confirmation := *current.Confirmation
		if !now.Before(confirmation.ExpiresAt) && confirmation.Status == domain.ConfirmationPending {
			confirmation.Status = domain.ConfirmationExpired
			confirmation.DecidedAt = &now
			current.Confirmation = &confirmation
			current.State = domain.JobRejected
			current.ErrorCode = domain.ErrSpendConfirmationExpired
			current.FailureReason = "spend confirmation expired"
			current.UpdatedAt = now
			current.CompletedAt = &now
			return current, nil
		}
		if confirmation.Status != domain.ConfirmationPending {
			return domain.Job{}, domain.NewError(domain.ErrIdempotencyConflict, "spend confirmation was already decided", false)
		}
		confirmation.DecidedAt = &now
		if approve {
			confirmation.Status = domain.ConfirmationApproved
		} else {
			confirmation.Status = domain.ConfirmationDenied
			current.State = domain.JobRejected
			current.ErrorCode = domain.ErrSpendConfirmationDenied
			current.FailureReason = "spend confirmation denied"
			current.CompletedAt = &now
		}
		current.Confirmation = &confirmation
		current.UpdatedAt = now
		return current, nil
	})
	if err != nil {
		return domain.Job{}, err
	}
	if updated.Confirmation != nil && updated.Confirmation.Status == domain.ConfirmationExpired {
		return updated, domain.NewError(domain.ErrSpendConfirmationExpired, "spend confirmation expired", false)
	}
	return updated, nil
}

func (s *JobService) Confirmation(ctx context.Context, userCode, principalID string) (domain.Job, error) {
	job, err := s.store.JobByConfirmationCode(ctx, normalizeConfirmationCode(userCode))
	if err != nil {
		return domain.Job{}, domain.NewError(domain.ErrNotFound, "spend confirmation not found", false)
	}
	if job.PrincipalID != principalID {
		return domain.Job{}, domain.NewError(domain.ErrPermissionDenied, "confirmation belongs to another principal", false)
	}
	return job, nil
}

func (s *JobService) resumeConfirmedJob(ctx context.Context, jobID string, waitInline bool, maxWaitMS int64) (SubmitResult, error) {
	lock := s.jobLock(jobID)
	lock.Lock()
	now := time.Now().UTC()
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		lock.Unlock()
		return SubmitResult{}, err
	}
	if job.State != domain.JobInputRequired {
		lock.Unlock()
		return SubmitResult{Type: resultTypeFor(job), Job: job}, nil
	}
	if job.Confirmation == nil {
		lock.Unlock()
		return SubmitResult{Type: ResultInputRequired, Job: job}, nil
	}
	confirmation := *job.Confirmation
	if !now.Before(confirmation.ExpiresAt) && confirmation.Status == domain.ConfirmationPending {
		confirmation.Status = domain.ConfirmationExpired
		confirmation.DecidedAt = &now
		job.Confirmation = &confirmation
		job.State = domain.JobRejected
		job.ErrorCode = domain.ErrSpendConfirmationExpired
		job.FailureReason = "spend confirmation expired"
		job.UpdatedAt = now
		job.CompletedAt = &now
		_ = s.store.PutJob(ctx, job)
		lock.Unlock()
		return SubmitResult{Type: ResultFailed, Job: job}, nil
	}
	switch confirmation.Status {
	case domain.ConfirmationPending:
		lock.Unlock()
		return SubmitResult{Type: ResultInputRequired, Job: job}, nil
	case domain.ConfirmationDenied, domain.ConfirmationExpired:
		lock.Unlock()
		return SubmitResult{Type: ResultFailed, Job: job}, nil
	case domain.ConfirmationApproved, domain.ConfirmationConsumed:
		quote, quoteErr := s.getQuote(ctx, job.QuoteID)
		if quoteErr != nil || confirmation.BindingHash != spendConfirmationBinding(job, quote) {
			job.State = domain.JobRejected
			job.ErrorCode = domain.ErrQuoteMismatch
			job.FailureReason = "spend confirmation no longer matches the committed request"
			job.UpdatedAt = now
			job.CompletedAt = &now
			_ = s.store.PutJob(ctx, job)
			lock.Unlock()
			return SubmitResult{Type: ResultFailed, Job: job}, nil
		}
		confirmation.Status = domain.ConfirmationConsumed
		confirmation.ConsumedAt = &now
		job.Confirmation = &confirmation
		job.State = domain.JobSubmitted
		job.UpdatedAt = now
		job.ErrorCode = ""
		job.FailureReason = ""
		if err := s.store.PutJob(ctx, job); err != nil {
			lock.Unlock()
			return SubmitResult{}, err
		}
		lock.Unlock()
		return s.executeJob(ctx, job.ID, waitInline, maxWaitMS)
	default:
		lock.Unlock()
		return SubmitResult{}, domain.NewError(domain.ErrValidationFailed, "invalid spend confirmation state", false)
	}
}

func (s *JobService) executeJob(ctx context.Context, jobID string, waitInline bool, maxWaitMS int64) (SubmitResult, error) {
	job, claimed, err := s.claimForExecution(ctx, jobID)
	if err != nil {
		return SubmitResult{}, err
	}
	if !claimed {
		return SubmitResult{Type: resultTypeFor(job), Job: job}, nil
	}

	lock := s.jobLock(jobID)
	lock.Lock()
	quote, err := s.getQuote(ctx, job.QuoteID)
	now := time.Now().UTC()
	if err != nil || quote.Expired(now) || (!quote.ExecutionDeadline.IsZero() && !now.Before(quote.ExecutionDeadline)) {
		failed := s.failUnderLock(ctx, job.ID, domain.ErrQuoteExpired, "quote unavailable or expired before execution")
		lock.Unlock()
		return failed, nil
	}
	capability, err := s.store.Get(ctx, job.CapabilityID)
	if err != nil {
		failed := s.failUnderLock(ctx, job.ID, domain.ErrCapabilityUnavailable, "capability lookup failed during execution")
		lock.Unlock()
		return failed, nil
	}
	capability = normalizeCapability(capability)
	if capability.Version != quote.CapabilityVersion || capability.ProviderID != quote.ProviderID ||
		job.TrustMode != quote.TrustMode || job.ProofProfile != quote.ProofProfile {
		failed := s.failUnderLock(ctx, job.ID, domain.ErrQuoteMismatch, "execution contract no longer matches quote")
		lock.Unlock()
		return failed, nil
	}
	if quote.PrincipalID == "" {
		quote.PrincipalID = job.PrincipalID
	}
	if _, err := s.core.CommitQuote(ctx, quote); err != nil {
		failed := s.failUnderLock(ctx, job.ID, errCode(err), "quote commitment failed: "+err.Error())
		lock.Unlock()
		return failed, nil
	}
	job.ProofStatus.Quote = proofCheckpointForCommittedQuote(job.TrustMode)
	if err := s.accounts.Debit(ctx, job.PrincipalID, quote.Price.TotalMax, quote.Price.Currency); err != nil {
		failed := s.failUnderLock(ctx, job.ID, errCode(err), err.Error())
		lock.Unlock()
		return failed, nil
	}
	escrow, err := s.core.CreateEscrow(ctx, toscore.CreateEscrowRequest{
		QuoteID: quote.ID, JobID: job.ID,
		CapabilityID: capability.ID, CapabilityVersion: capability.Version,
		PrincipalID: job.PrincipalID, ProviderID: capability.ProviderID,
		TrustMode: quote.TrustMode, ProofProfile: quote.ProofProfile,
		Settlement: quote.Settlement,
		Reserved:   domain.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency},
	})
	if err != nil {
		_ = s.accounts.Credit(ctx, job.PrincipalID, quote.Price.TotalMax, quote.Price.Currency)
		failed := s.failUnderLock(ctx, job.ID, errCode(err), "escrow creation failed: "+err.Error())
		lock.Unlock()
		return failed, nil
	}
	job.EscrowID = escrow.ID
	job.ProofStatus.Escrow = domain.ProofReserved
	job.UpdatedAt = time.Now().UTC()
	if err := s.store.PutJob(ctx, job); err != nil {
		releaseReceipt, releaseErr := s.core.ReleaseEscrow(context.Background(), escrow.ID)
		creditErr := error(nil)
		if releaseErr == nil {
			creditErr = s.accounts.Credit(context.Background(), job.PrincipalID, releaseReceipt.Refunded.Amount, releaseReceipt.Refunded.Currency)
		}
		lock.Unlock()
		if releaseErr != nil || creditErr != nil {
			return SubmitResult{}, domain.NewError(domain.ErrSettlementFailed, "job persistence failed after escrow creation and compensation was incomplete", true)
		}
		return SubmitResult{}, err
	}
	lock.Unlock()

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
	if maxWaitMS <= 0 {
		wait = inlineWaitDefault
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case finished := <-done:
		return SubmitResult{Type: resultTypeFor(finished), Job: finished}, nil
	case <-timer.C:
		current, err := s.store.GetJob(ctx, job.ID)
		if err != nil {
			return SubmitResult{}, err
		}
		return SubmitResult{Type: ResultAccepted, Job: current}, nil
	case <-ctx.Done():
		current, err := s.store.GetJob(context.Background(), job.ID)
		if err != nil {
			return SubmitResult{}, ctx.Err()
		}
		return SubmitResult{Type: ResultAccepted, Job: current}, nil
	}
}

func (s *JobService) runToCompletion(ctx context.Context, snapshot domain.Job, capability domain.Capability) domain.Job {
	result, err := s.provider.SubmitJob(ctx, tosai.SubmitJobRequest{
		JobID: snapshot.ID, InvocationID: snapshot.InvocationID,
		QuoteID: snapshot.QuoteID, ServiceQuoteID: snapshot.ServiceQuoteID,
		EscrowID: snapshot.EscrowID, PrincipalID: snapshot.PrincipalID,
		CapabilityID: snapshot.CapabilityID, CapabilityVersion: snapshot.CapabilityVersion,
		ProviderID: snapshot.ProviderID, TrustMode: snapshot.TrustMode,
		ProofProfile: snapshot.ProofProfile, Input: snapshot.Input,
		InputCommitment:   hashCommitment(snapshot.Input),
		ExecutionDeadline: snapshot.ExecutionDeadline,
		RetainUntil:       time.Now().UTC().Add(executionRetention),
	})
	if err != nil {
		return s.fail(ctx, snapshot.ID, errCode(err), "execution gateway failed: "+err.Error()).Job
	}
	if result.Receipt == nil {
		return s.fail(ctx, snapshot.ID, domain.ErrProviderFailed, "execution gateway returned no receipt").Job
	}

	lock := s.jobLock(snapshot.ID)
	lock.Lock()
	defer lock.Unlock()
	current, err := s.store.GetJob(ctx, snapshot.ID)
	if err != nil {
		return snapshot
	}
	if current.State != domain.JobWorking {
		return current
	}
	quote, err := s.getQuote(ctx, current.QuoteID)
	if err != nil {
		return s.failUnderLock(ctx, current.ID, domain.ErrQuoteMismatch, "quote lookup failed during settlement").Job
	}

	execReceipt := *result.Receipt
	execReceipt.Cost = domain.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency}
	current.ProofStatus.Receipt = domain.ProofSigned
	current.UpdatedAt = time.Now().UTC()
	if err := s.store.PutJob(ctx, current); err != nil {
		return current
	}
	proofRef, err := s.core.CommitExecutionReceipt(ctx, execReceipt)
	if err != nil {
		return s.failUnderLock(ctx, current.ID, errCode(err), "execution receipt commitment failed: "+err.Error()).Job
	}
	if proofRef != "" {
		execReceipt.NetworkProofRef = proofRef
	}
	verify, err := s.core.VerifyExecutionReceipt(ctx, current.EscrowID, execReceipt)
	if err != nil || !verify.Valid {
		reason := "execution receipt failed verification"
		if err != nil {
			reason += ": " + err.Error()
		} else if verify.Reason != "" {
			reason += ": " + verify.Reason
		}
		return s.failUnderLock(ctx, current.ID, domain.ErrSettlementFailed, reason).Job
	}
	if verify.ProofRef != "" {
		execReceipt.NetworkProofRef = verify.ProofRef
	}
	current.ProofStatus.Receipt = domain.ProofVerified
	current.UpdatedAt = time.Now().UTC()
	if err := s.store.PutJob(ctx, current); err != nil {
		return current
	}

	settled, err := s.core.SettleJob(ctx, toscore.SettleJobRequest{
		EscrowID: current.EscrowID, JobID: current.ID, ReceiptID: execReceipt.ID,
		ActualCost: execReceipt.Cost,
	})
	if err != nil {
		return s.failUnderLock(ctx, current.ID, domain.ErrSettlementFailed, "settlement failed: "+err.Error()).Job
	}
	current.Output = cloneMap(result.Output)
	current.Artifacts = append([]domain.Artifact(nil), result.Artifacts...)
	current.ProofStatus.Receipt = domain.ProofVerified
	current.ProofStatus.Settlement = domain.ProofSettled
	if nonZeroMoney(settled.Receipt.Refunded) {
		if err := s.accounts.Credit(ctx, current.PrincipalID, settled.Receipt.Refunded.Amount, settled.Receipt.Refunded.Currency); err != nil {
			return s.markReconciliationUnderLock(ctx, current, settled.Receipt.Refunded, domain.JobCompleted, "settlement completed but account refund reconciliation is pending")
		}
	}
	_, _ = s.core.CommitProofOfServiceEvidence(ctx, execReceipt)

	now := time.Now().UTC()
	current.State = domain.JobCompleted
	current.ErrorCode = ""
	current.FailureReason = ""
	current.ReconciliationRequired = false
	current.PendingCredit = nil
	current.ReconciliationTarget = ""
	current.UpdatedAt = now
	current.CompletedAt = &now
	if err := s.store.PutJob(ctx, current); err != nil {
		return current
	}
	return current
}

func (s *JobService) claimForExecution(ctx context.Context, jobID string) (domain.Job, bool, error) {
	result, err := s.store.UpdateJob(ctx, jobID, func(job domain.Job, exists bool) (domain.Job, error) {
		if !exists {
			return domain.Job{}, domain.NewError(domain.ErrNotFound, "job not found", false)
		}
		if job.State != domain.JobSubmitted {
			return domain.Job{}, store.ErrConflict
		}
		job.State = domain.JobWorking
		job.UpdatedAt = time.Now().UTC()
		return job, nil
	})
	if err == store.ErrConflict {
		current, getErr := s.store.GetJob(ctx, jobID)
		return current, false, getErr
	}
	if err != nil {
		return domain.Job{}, false, err
	}
	return result, true, nil
}

func (s *JobService) transitionIfActive(ctx context.Context, jobID string, mutate func(domain.Job) domain.Job) (domain.Job, bool, error) {
	result, err := s.store.UpdateJob(ctx, jobID, func(job domain.Job, exists bool) (domain.Job, error) {
		if !exists {
			return domain.Job{}, domain.NewError(domain.ErrNotFound, "job not found", false)
		}
		if job.State.Terminal() || job.State == domain.JobCanceling || job.State == domain.JobReconciling {
			return domain.Job{}, store.ErrConflict
		}
		return mutate(job), nil
	})
	if err == store.ErrConflict {
		current, getErr := s.store.GetJob(ctx, jobID)
		return current, false, getErr
	}
	if err != nil {
		return domain.Job{}, false, err
	}
	return result, true, nil
}

func (s *JobService) fail(ctx context.Context, jobID string, code domain.ErrorCode, reason string) SubmitResult {
	lock := s.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	return s.failUnderLock(ctx, jobID, code, reason)
}

func (s *JobService) failUnderLock(ctx context.Context, jobID string, code domain.ErrorCode, reason string) SubmitResult {
	job, applied, err := s.transitionIfActive(ctx, jobID, func(job domain.Job) domain.Job {
		now := time.Now().UTC()
		job.State = domain.JobFailed
		job.FailureReason = reason
		job.ErrorCode = code
		job.ProofStatus.Receipt = domain.ProofFailed
		job.UpdatedAt = now
		job.CompletedAt = &now
		return job
	})
	if err != nil || !applied {
		return SubmitResult{Type: resultTypeFor(job), Job: job}
	}
	if job.EscrowID != "" {
		receipt, releaseErr := s.core.ReleaseEscrow(ctx, job.EscrowID)
		if releaseErr != nil {
			job.ProofStatus.Settlement = domain.ProofFailed
			job.ReconciliationRequired = true
			job.FailureReason += "; escrow release requires reconciliation: " + releaseErr.Error()
			_ = s.store.PutJob(ctx, job)
			return SubmitResult{Type: ResultFailed, Job: job}
		}
		if nonZeroMoney(receipt.Refunded) {
			if creditErr := s.accounts.Credit(ctx, job.PrincipalID, receipt.Refunded.Amount, receipt.Refunded.Currency); creditErr != nil {
				job = s.markReconciliationUnderLock(ctx, job, receipt.Refunded, domain.JobFailed, reason+"; refund reconciliation is pending")
				return SubmitResult{Type: ResultAccepted, Job: job}
			}
		}
		job.ProofStatus.Escrow = domain.ProofReleased
		job.ProofStatus.Settlement = domain.ProofReleased
		_ = s.store.PutJob(ctx, job)
	}
	return SubmitResult{Type: resultTypeFor(job), Job: job}
}

func (s *JobService) Get(ctx context.Context, jobID string) (domain.Job, error) {
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Job{}, domain.NewError(domain.ErrNotFound, "job not found", false)
		}
		return domain.Job{}, err
	}
	if job.State == domain.JobReconciling {
		return s.reconcileCredit(ctx, jobID)
	}
	return job, nil
}

func (s *JobService) Cancel(ctx context.Context, jobID, principalID, reason, idempotencyKey string) (domain.Job, error) {
	if idempotencyKey == "" {
		return domain.Job{}, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}
	now := time.Now().UTC()
	requestHash := hashRequest("atos-cancel-v1", jobID, reason)
	rec, reserved, err := s.store.Reserve(ctx, principalID, idempotencyKey, requestHash, now.Add(idempotencyLease))
	if err != nil {
		return domain.Job{}, err
	}
	if !reserved {
		if rec.RequestHash != requestHash {
			return domain.Job{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false)
		}
		if rec.Status != store.IdempotencyCompleted {
			return domain.Job{}, domain.NewError(domain.ErrIdempotencyConflict, "a cancellation with this idempotency_key is still in progress", true)
		}
		return s.store.GetJob(ctx, rec.ResponseKey)
	}
	committed := false
	defer func() {
		if !committed {
			_ = s.store.Release(context.Background(), principalID, idempotencyKey)
		}
	}()

	lock := s.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return domain.Job{}, domain.NewError(domain.ErrNotFound, "job not found", false)
	}
	if job.PrincipalID != principalID {
		return domain.Job{}, domain.NewError(domain.ErrPermissionDenied, "not the job's owning principal", false)
	}
	if job.State.Terminal() || job.State == domain.JobReconciling {
		return domain.Job{}, domain.NewError(domain.ErrJobNotCancelable, "job is already terminal or reconciling", false)
	}
	if job.State == domain.JobInputRequired || job.State == domain.JobSubmitted {
		if job.Confirmation != nil && job.Confirmation.Status == domain.ConfirmationPending {
			confirmation := *job.Confirmation
			confirmation.Status = domain.ConfirmationDenied
			confirmation.DecidedAt = &now
			job.Confirmation = &confirmation
		}
		job = finalizeCanceled(job, reason, now)
		if err := s.store.PutJob(ctx, job); err != nil {
			return domain.Job{}, err
		}
		if err := s.store.Finish(ctx, principalID, idempotencyKey, jobID); err != nil {
			return domain.Job{}, err
		}
		committed = true
		return job, nil
	}

	originalState := job.State
	if job.State != domain.JobCanceling {
		job.State = domain.JobCanceling
		job.UpdatedAt = now
		job.ErrorCode = ""
		job.FailureReason = reason
		if err := s.store.PutJob(ctx, job); err != nil {
			return domain.Job{}, err
		}
	}
	if err := s.provider.CancelJob(ctx, jobID, reason); err != nil {
		if originalState != domain.JobCanceling {
			job.State = originalState
			job.UpdatedAt = time.Now().UTC()
			_ = s.store.PutJob(ctx, job)
		}
		return domain.Job{}, domain.NewError(domain.ErrProviderFailed, "provider cancellation failed: "+err.Error(), true)
	}
	if job.EscrowID != "" {
		receipt, releaseErr := s.core.ReleaseEscrow(ctx, job.EscrowID)
		if releaseErr != nil {
			return domain.Job{}, domain.NewError(domain.ErrSettlementFailed, "provider canceled but escrow release failed: "+releaseErr.Error(), true)
		}
		job.ProofStatus.Escrow = domain.ProofReleased
		job.ProofStatus.Settlement = domain.ProofReleased
		if nonZeroMoney(receipt.Refunded) {
			if creditErr := s.accounts.Credit(ctx, job.PrincipalID, receipt.Refunded.Amount, receipt.Refunded.Currency); creditErr != nil {
				job = s.markReconciliationUnderLock(ctx, job, receipt.Refunded, domain.JobCanceled, "provider canceled and escrow released; refund reconciliation is pending")
				if err := s.store.Finish(ctx, principalID, idempotencyKey, jobID); err != nil {
					return domain.Job{}, err
				}
				committed = true
				return job, domain.NewError(domain.ErrSettlementFailed, "refund reconciliation is pending", true)
			}
		}
	}
	job = finalizeCanceled(job, reason, time.Now().UTC())
	if err := s.store.PutJob(ctx, job); err != nil {
		return domain.Job{}, err
	}
	if err := s.store.Finish(ctx, principalID, idempotencyKey, jobID); err != nil {
		return domain.Job{}, err
	}
	committed = true
	return job, nil
}

func (s *JobService) reconcileCredit(ctx context.Context, jobID string) (domain.Job, error) {
	lock := s.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return domain.Job{}, err
	}
	if job.State != domain.JobReconciling || job.PendingCredit == nil {
		return job, nil
	}
	if err := s.accounts.Credit(ctx, job.PrincipalID, job.PendingCredit.Amount, job.PendingCredit.Currency); err != nil {
		return job, domain.NewError(domain.ErrSettlementFailed, "account reconciliation is still pending: "+err.Error(), true)
	}
	now := time.Now().UTC()
	job.State = job.ReconciliationTarget
	job.PendingCredit = nil
	job.ReconciliationTarget = ""
	job.ReconciliationRequired = false
	job.ErrorCode = ""
	if job.State == domain.JobCompleted || job.State == domain.JobCanceled || job.State == domain.JobFailed {
		job.CompletedAt = &now
	}
	job.UpdatedAt = now
	if job.State == domain.JobCompleted || job.State == domain.JobCanceled {
		job.FailureReason = ""
	}
	if err := s.store.PutJob(ctx, job); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

func (s *JobService) markReconciliationUnderLock(ctx context.Context, job domain.Job, credit domain.Money, target domain.JobState, reason string) domain.Job {
	job.State = domain.JobReconciling
	job.PendingCredit = &domain.Money{Amount: credit.Amount, Currency: credit.Currency}
	job.ReconciliationTarget = target
	job.ReconciliationRequired = true
	job.ErrorCode = domain.ErrSettlementFailed
	job.FailureReason = reason
	job.UpdatedAt = time.Now().UTC()
	job.CompletedAt = nil
	_ = s.store.PutJob(ctx, job)
	return job
}

func (s *JobService) getQuote(ctx context.Context, quoteID string) (domain.Quote, error) {
	quote, err := s.store.GetQuote(ctx, quoteID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Quote{}, domain.NewError(domain.ErrQuoteExpired, "quote not found", false)
		}
		return domain.Quote{}, err
	}
	if quote.TrustMode == "" {
		quote.RequestedTrustMode = domain.RequestedTrustManaged
		quote.TrustMode = domain.TrustModeManaged
		quote.Settlement, quote.Proof = quoteGuarantees(quote.TrustMode, quote.Price.Currency)
	}
	return quote, nil
}

func (s *JobService) jobLock(jobID string) *sync.Mutex {
	value, _ := s.jobLocks.LoadOrStore(jobID, &sync.Mutex{})
	return value.(*sync.Mutex)
}

func newSpendConfirmation(job domain.Job, quote domain.Quote, now time.Time) *domain.SpendConfirmation {
	return &domain.SpendConfirmation{
		ID: "cnf_" + uuid.NewString(), UserCode: confirmationCode(),
		Status:      domain.ConfirmationPending,
		Maximum:     domain.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency},
		BindingHash: spendConfirmationBinding(job, quote),
		CreatedAt:   now, ExpiresAt: minTime(now.Add(confirmationTTL), quote.ExpiresAt),
	}
}

func spendConfirmationBinding(job domain.Job, quote domain.Quote) string {
	return hashRequest(
		"atos-spend-confirmation-v1", job.PrincipalID, job.IdempotencyKey,
		job.CapabilityID, job.CapabilityVersion, job.ProviderID,
		quote.ID, quote.TrustMode, quote.ProofProfile,
		quote.Price.TotalMax, quote.Price.Currency, hashCommitment(job.Input),
	)
}

func confirmationCode() string {
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		random = []byte(strings.ReplaceAll(uuid.NewString(), "-", ""))[:8]
	}
	out := make([]byte, 8)
	for i := range out {
		out[i] = alphabet[int(random[i])%len(alphabet)]
	}
	return string(out[:4]) + "-" + string(out[4:])
}

func normalizeConfirmationCode(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, " ", "")
	if len(value) == 8 && !strings.Contains(value, "-") {
		value = value[:4] + "-" + value[4:]
	}
	return value
}

func finalizeCanceled(job domain.Job, reason string, now time.Time) domain.Job {
	job.State = domain.JobCanceled
	job.FailureReason = reason
	job.ErrorCode = ""
	job.ProofStatus.Receipt = domain.ProofNotRequired
	job.ReconciliationRequired = false
	job.PendingCredit = nil
	job.ReconciliationTarget = ""
	job.UpdatedAt = now
	job.CompletedAt = &now
	return job
}

func proofCheckpointForCommittedQuote(mode domain.TrustMode) domain.ProofCheckpoint {
	if mode == domain.TrustModeManaged {
		return domain.ProofNotRequired
	}
	return domain.ProofCommitted
}

func resultTypeFor(job domain.Job) SubmitResultType {
	switch job.State {
	case domain.JobCompleted:
		return ResultCompleted
	case domain.JobFailed, domain.JobCanceled, domain.JobRejected:
		return ResultFailed
	case domain.JobInputRequired:
		return ResultInputRequired
	default:
		return ResultAccepted
	}
}

func errCode(err error) domain.ErrorCode {
	if typed, ok := err.(*domain.Error); ok {
		return typed.Code
	}
	return domain.ErrProviderFailed
}

func hashRequest(parts ...any) string {
	encoded, _ := json.Marshal(parts)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func hashCommitment(value any) string {
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	encoded, _ := json.Marshal(value)
	var out map[string]any
	_ = json.Unmarshal(encoded, &out)
	return out
}

func nonZeroMoney(value domain.Money) bool {
	return value.Amount != "" && value.Amount != "0" && value.Amount != "0.00"
}

func minTime(a, b time.Time) time.Time {
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}
