// JobService implements the Quote -> Escrow -> Execute -> Verify -> Settle
// pipeline. Invocation and Job share the same durable record in Phase 0.
package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

const inlineWaitDefault = 30 * time.Second

type JobService struct {
	store    store.Store
	provider tosai.Provider
	core     toscore.Core
	accounts *AccountService
	jobLocks sync.Map // job_id -> *sync.Mutex; Phase 0 process-local lifecycle serialization
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
	Confirmed      bool
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
	requestHash := hashRequest(in.CapabilityID, in.QuoteID, in.Input)
	rec, reserved, err := s.store.Reserve(ctx, in.PrincipalID, in.IdempotencyKey, requestHash)
	if err != nil {
		return SubmitResult{}, err
	}
	if !reserved {
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
		if job.State == domain.JobInputRequired && in.Confirmed {
			return s.executeJob(ctx, job.ID, waitInline, in.MaxWaitMS)
		}
		return SubmitResult{Type: resultTypeFor(job), Job: job}, nil
	}

	committed := false
	defer func() {
		if !committed {
			_ = s.store.Release(ctx, in.PrincipalID, in.IdempotencyKey)
		}
	}()

	quote, err := s.getQuote(ctx, in.QuoteID)
	if err != nil {
		return SubmitResult{}, err
	}
	if quote.CapabilityID != in.CapabilityID {
		return SubmitResult{}, domain.NewError(domain.ErrQuoteMismatch, "quote does not match capability_id", false)
	}
	if quote.Expired(time.Now().UTC()) {
		return SubmitResult{}, domain.NewError(domain.ErrQuoteExpired, "quote has expired", false)
	}
	cap, err := s.store.Get(ctx, in.CapabilityID)
	if err != nil {
		return SubmitResult{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability not found", false)
	}
	cap = normalizeCapability(cap)
	if cap.Version != quote.CapabilityVersion || cap.ProviderID != quote.ProviderID {
		return SubmitResult{}, domain.NewError(domain.ErrQuoteMismatch, "capability/provider changed after quote issuance", false)
	}
	if !cap.Supports(quote.TrustMode) {
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
	if needsConfirmation && !in.Confirmed {
		state = domain.JobInputRequired
	}
	now := time.Now().UTC()
	proofStatus := domain.InitialProofStatus(quote.TrustMode)
	proofStatus.Escrow = domain.ProofPending
	if quote.TrustMode != domain.TrustModeManaged {
		proofStatus.Quote = domain.ProofPending
	}
	job := domain.Job{
		ID:                idPrefix + uuid.NewString(),
		CapabilityID:      cap.ID,
		CapabilityVersion: cap.Version,
		ProviderID:        cap.ProviderID,
		QuoteID:           quote.ID,
		PrincipalID:       in.PrincipalID,
		TrustMode:         quote.TrustMode,
		ProofProfile:      quote.ProofProfile,
		ProofStatus:       proofStatus,
		State:             state,
		Input:             in.Input,
		IdempotencyKey:    in.IdempotencyKey,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if idPrefix == "inv_" {
		job.InvocationID = job.ID
	}
	if err := s.store.PutJob(ctx, job); err != nil {
		return SubmitResult{}, err
	}
	if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, job.ID); err != nil {
		return SubmitResult{}, err
	}
	committed = true
	if state == domain.JobInputRequired {
		return SubmitResult{Type: ResultInputRequired, Job: job}, nil
	}
	return s.executeJob(ctx, job.ID, waitInline, in.MaxWaitMS)
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
	if err != nil || quote.Expired(time.Now().UTC()) {
		failed := s.failUnderLock(ctx, job.ID, domain.ErrQuoteExpired, "quote unavailable or expired before execution")
		lock.Unlock()
		return failed, nil
	}
	cap, err := s.store.Get(ctx, job.CapabilityID)
	if err != nil {
		failed := s.failUnderLock(ctx, job.ID, domain.ErrCapabilityUnavailable, "capability lookup failed during execution")
		lock.Unlock()
		return failed, nil
	}
	cap = normalizeCapability(cap)
	if cap.Version != quote.CapabilityVersion || cap.ProviderID != quote.ProviderID || job.TrustMode != quote.TrustMode || job.ProofProfile != quote.ProofProfile {
		failed := s.failUnderLock(ctx, job.ID, domain.ErrQuoteMismatch, "execution contract no longer matches quote")
		lock.Unlock()
		return failed, nil
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
		CapabilityID: cap.ID, CapabilityVersion: cap.Version,
		PrincipalID: job.PrincipalID, ProviderID: cap.ProviderID,
		TrustMode: quote.TrustMode, ProofProfile: quote.ProofProfile,
		Settlement: quote.Settlement,
		Reserved: domain.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency},
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
		lock.Unlock()
		return SubmitResult{}, err
	}
	lock.Unlock()

	done := make(chan domain.Job, 1)
	go func(snapshot domain.Job, capability domain.Capability) {
		done <- s.runToCompletion(context.Background(), snapshot, capability)
	}(job, cap)

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

func (s *JobService) runToCompletion(ctx context.Context, snapshot domain.Job, cap domain.Capability) domain.Job {
	result, err := s.provider.SubmitJob(ctx, tosai.SubmitJobRequest{
		JobID: snapshot.ID, QuoteID: snapshot.QuoteID, EscrowID: snapshot.EscrowID,
		PrincipalID: snapshot.PrincipalID,
		CapabilityID: snapshot.CapabilityID, CapabilityVersion: snapshot.CapabilityVersion,
		ProviderID: snapshot.ProviderID, TrustMode: snapshot.TrustMode,
		ProofProfile: snapshot.ProofProfile, Input: snapshot.Input,
		InputCommitment: hashCommitment(snapshot.Input),
	})
	if err != nil || result.Receipt == nil {
		return s.fail(ctx, snapshot.ID, domain.ErrProviderFailed, "tos-ai execution failed").Job
	}

	lock := s.jobLock(snapshot.ID)
	lock.Lock()
	defer lock.Unlock()
	current, err := s.store.GetJob(ctx, snapshot.ID)
	if err != nil {
		return snapshot
	}
	if current.State.Terminal() {
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
	_ = s.store.PutJob(ctx, current)
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
	_ = s.store.PutJob(ctx, current)

	settled, err := s.core.SettleJob(ctx, toscore.SettleJobRequest{
		EscrowID: current.EscrowID, JobID: current.ID, ReceiptID: execReceipt.ID,
		ActualCost: execReceipt.Cost,
	})
	if err != nil {
		return s.failUnderLock(ctx, current.ID, domain.ErrSettlementFailed, "settlement failed: "+err.Error()).Job
	}
	if settled.Receipt.Refunded.Amount != "" && settled.Receipt.Refunded.Amount != "0" && settled.Receipt.Refunded.Amount != "0.00" {
		_ = s.accounts.Credit(ctx, current.PrincipalID, settled.Receipt.Refunded.Amount, settled.Receipt.Refunded.Currency)
	}
	_, _ = s.core.CommitProofOfServiceEvidence(ctx, execReceipt)

	finished, applied, err := s.transitionIfNotTerminal(ctx, current.ID, func(j domain.Job) domain.Job {
		now := time.Now().UTC()
		j.State = domain.JobCompleted
		j.Output = result.Output
		j.Artifacts = append([]domain.Artifact(nil), result.Artifacts...)
		j.ProofStatus.Receipt = domain.ProofVerified
		j.ProofStatus.Settlement = domain.ProofSettled
		j.UpdatedAt = now
		j.CompletedAt = &now
		return j
	})
	if err != nil || !applied {
		return finished
	}
	return finished
}

func (s *JobService) claimForExecution(ctx context.Context, jobID string) (domain.Job, bool, error) {
	result, err := s.store.UpdateJob(ctx, jobID, func(j domain.Job, exists bool) (domain.Job, error) {
		if !exists {
			return domain.Job{}, domain.NewError(domain.ErrNotFound, "job not found", false)
		}
		if j.State != domain.JobSubmitted && j.State != domain.JobInputRequired {
			return domain.Job{}, store.ErrConflict
		}
		j.State = domain.JobWorking
		j.UpdatedAt = time.Now().UTC()
		return j, nil
	})
	if err == store.ErrConflict {
		current, gerr := s.store.GetJob(ctx, jobID)
		return current, false, gerr
	}
	if err != nil {
		return domain.Job{}, false, err
	}
	return result, true, nil
}

func (s *JobService) transitionIfNotTerminal(ctx context.Context, jobID string, mutate func(domain.Job) domain.Job) (domain.Job, bool, error) {
	result, err := s.store.UpdateJob(ctx, jobID, func(j domain.Job, exists bool) (domain.Job, error) {
		if !exists {
			return domain.Job{}, domain.NewError(domain.ErrNotFound, "job not found", false)
		}
		if j.State.Terminal() {
			return domain.Job{}, store.ErrConflict
		}
		return mutate(j), nil
	})
	if err == store.ErrConflict {
		current, gerr := s.store.GetJob(ctx, jobID)
		return current, false, gerr
	}
	if err != nil {
		return domain.Job{}, false, err
	}
	return result, true, nil
}

func (s *JobService) failTerminal(ctx context.Context, jobID, reason string) SubmitResult {
	return s.fail(ctx, jobID, domain.ErrProviderFailed, reason)
}

func (s *JobService) failWithCode(ctx context.Context, jobID string, code domain.ErrorCode, reason string) SubmitResult {
	return s.fail(ctx, jobID, code, reason)
}

func (s *JobService) fail(ctx context.Context, jobID string, code domain.ErrorCode, reason string) SubmitResult {
	lock := s.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	return s.failUnderLock(ctx, jobID, code, reason)
}

func (s *JobService) failUnderLock(ctx context.Context, jobID string, code domain.ErrorCode, reason string) SubmitResult {
	job, applied, err := s.transitionIfNotTerminal(ctx, jobID, func(j domain.Job) domain.Job {
		now := time.Now().UTC()
		j.State = domain.JobFailed
		j.FailureReason = reason
		j.ErrorCode = code
		j.ProofStatus.Receipt = domain.ProofFailed
		j.UpdatedAt = now
		j.CompletedAt = &now
		return j
	})
	if err != nil {
		return SubmitResult{Type: ResultFailed, Job: job}
	}
	if applied && job.EscrowID != "" {
		if receipt, releaseErr := s.core.ReleaseEscrow(ctx, job.EscrowID); releaseErr == nil {
			_ = s.accounts.Credit(ctx, job.PrincipalID, receipt.Refunded.Amount, receipt.Refunded.Currency)
			job.ProofStatus.Escrow = domain.ProofReleased
			job.ProofStatus.Settlement = domain.ProofReleased
			_ = s.store.PutJob(ctx, job)
		} else {
			job.ProofStatus.Settlement = domain.ProofFailed
			_ = s.store.PutJob(ctx, job)
		}
	}
	return SubmitResult{Type: resultTypeFor(job), Job: job}
}

func (s *JobService) Get(ctx context.Context, jobID string) (domain.Job, error) {
	j, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Job{}, domain.NewError(domain.ErrNotFound, "job not found", false)
		}
		return domain.Job{}, err
	}
	return j, nil
}

func (s *JobService) Cancel(ctx context.Context, jobID, principalID, reason, idempotencyKey string) (domain.Job, error) {
	if idempotencyKey == "" {
		return domain.Job{}, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}
	requestHash := hashRequest(jobID, reason)
	rec, reserved, err := s.store.Reserve(ctx, principalID, idempotencyKey, requestHash)
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
			_ = s.store.Release(ctx, principalID, idempotencyKey)
		}
	}()

	lock := s.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	existing, err := s.Get(ctx, jobID)
	if err != nil {
		return domain.Job{}, err
	}
	if existing.PrincipalID != principalID {
		return domain.Job{}, domain.NewError(domain.ErrPermissionDenied, "not the job's owning principal", false)
	}
	job, applied, err := s.transitionIfNotTerminal(ctx, jobID, func(j domain.Job) domain.Job {
		now := time.Now().UTC()
		j.State = domain.JobCanceled
		j.FailureReason = reason
		j.ErrorCode = domain.ErrJobNotCancelable
		j.ProofStatus.Receipt = domain.ProofFailed
		j.UpdatedAt = now
		j.CompletedAt = &now
		return j
	})
	if err != nil {
		return domain.Job{}, err
	}
	if !applied {
		return domain.Job{}, domain.NewError(domain.ErrJobNotCancelable, "job is already in a terminal state", false)
	}
	_ = s.provider.CancelJob(ctx, jobID, reason)
	if job.EscrowID != "" {
		receipt, err := s.core.ReleaseEscrow(ctx, job.EscrowID)
		if err != nil {
			return domain.Job{}, domain.NewError(domain.ErrSettlementFailed, "job canceled but escrow release failed: "+err.Error(), true)
		}
		if err := s.accounts.Credit(ctx, job.PrincipalID, receipt.Refunded.Amount, receipt.Refunded.Currency); err != nil {
			return domain.Job{}, domain.NewError(domain.ErrSettlementFailed, "job canceled but refund credit failed: "+err.Error(), true)
		}
		job.ProofStatus.Escrow = domain.ProofReleased
		job.ProofStatus.Settlement = domain.ProofReleased
		_ = s.store.PutJob(ctx, job)
	}
	if err := s.store.Finish(ctx, principalID, idempotencyKey, jobID); err != nil {
		return domain.Job{}, err
	}
	committed = true
	return job, nil
}

func (s *JobService) getQuote(ctx context.Context, quoteID string) (domain.Quote, error) {
	q, err := s.store.GetQuote(ctx, quoteID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Quote{}, domain.NewError(domain.ErrQuoteExpired, "quote not found", false)
		}
		return domain.Quote{}, err
	}
	if q.TrustMode == "" {
		q.RequestedTrustMode = domain.RequestedTrustManaged
		q.TrustMode = domain.TrustModeManaged
		q.Settlement, q.Proof = quoteGuarantees(q.TrustMode, q.Price.Currency)
	}
	return q, nil
}

func (s *JobService) jobLock(jobID string) *sync.Mutex {
	value, _ := s.jobLocks.LoadOrStore(jobID, &sync.Mutex{})
	return value.(*sync.Mutex)
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
	if de, ok := err.(*domain.Error); ok {
		return de.Code
	}
	return domain.ErrProviderFailed
}

func hashRequest(parts ...any) string {
	b, _ := json.Marshal(parts)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hashCommitment(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
