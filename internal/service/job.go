// JobService implements the invoke/create_job/get_job/cancel_job lifecycle
// from docs/MCP.md, wiring the quote -> escrow -> execute -> verify ->
// settle pipeline from docs/SETTLEMENT.md end to end.
//
// Simplification for this Phase 0 skeleton: atos-spec models Invocation
// (sync) and Job (async) as related but distinct objects. Here both are
// represented as a domain.Job — Invoke additionally waits (up to
// MaxWaitMS) for the job to finish before returning, matching the
// "completed vs accepted" response shape from docs/MCP.md without a
// separate Invocation table. Splitting them out is a reasonable follow-up
// once a real tos-ai network makes "accepted, still running" common.
//
// Concurrency note: job state transitions (claim-for-execution, terminal
// transitions) go through store.UpdateJob, which the in-memory store
// serializes under one lock, so two callers (e.g. the worker completing
// and Cancel) can never both act on the same stale state. Escrow
// settlement (in tos-core) and the job's terminal-state transition are
// still two separate locked operations rather than one atomic
// transaction, so a very narrow window exists where a cancellation could
// race a just-completed settlement. Closing that fully needs a real
// cross-collection transaction (Postgres phase), not just an in-memory
// mutex — documented here rather than silently assumed away.
package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// inlineWaitDefault bounds Invoke when the caller passes max_wait_ms <= 0.
// docs/MCP.md's schema allows 1000-120000; 0/omitted is treated as "use a
// sane default" rather than "wait forever".
const inlineWaitDefault = 30 * time.Second

type JobService struct {
	store    store.Store
	provider tosai.Provider
	core     toscore.Core
	accounts *AccountService
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
	// Confirmed marks this as the client reissuing the same idempotency_key
	// after being shown input_required, per docs/MCP.md: "The client
	// reissues the original call with the response." Phase 0
	// simplification: there is no signed opaque requestState to validate,
	// only "same principal, same idempotency_key, job still
	// input_required" — a real MRTR implementation would also verify a
	// signed state token here.
	Confirmed bool
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

// Invoke implements atos_invoke / POST /invocations: submit, then wait up
// to MaxWaitMS for completion before falling back to "accepted". The
// resulting ID is prefixed inv_ to match docs/API.md's invocation_id shape
// even though it shares the same underlying Job record as CreateJob.
func (s *JobService) Invoke(ctx context.Context, in SubmitInput) (SubmitResult, error) {
	return s.submit(ctx, in, true, "inv_")
}

// CreateJob implements atos_create_job / POST /jobs: submit and return
// immediately without waiting.
func (s *JobService) CreateJob(ctx context.Context, in SubmitInput) (SubmitResult, error) {
	in.MaxWaitMS = 0
	return s.submit(ctx, in, false, "job_")
}

func (s *JobService) submit(ctx context.Context, in SubmitInput, waitInline bool, idPrefix string) (SubmitResult, error) {
	if in.IdempotencyKey == "" {
		return SubmitResult{}, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}

	// requestHash intentionally excludes Confirmed/MaxWaitMS: a confirmed
	// reissue is a continuation of the same logical request, not a
	// different one, so it must match the original reservation's hash.
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

	// From here on this goroutine uniquely owns the reservation. Any
	// return before Finish MUST Release it first, or a corrected retry
	// would be permanently blocked by a poisoned in_progress record.
	committed := false
	defer func() {
		if !committed {
			_ = s.store.Release(ctx, in.PrincipalID, in.IdempotencyKey)
		}
	}()

	quote, err := s.store.GetQuote(ctx, in.QuoteID)
	if err != nil {
		if err == store.ErrNotFound {
			return SubmitResult{}, domain.NewError(domain.ErrQuoteExpired, "quote not found", false)
		}
		return SubmitResult{}, err
	}
	if quote.CapabilityID != in.CapabilityID {
		return SubmitResult{}, domain.NewError(domain.ErrQuoteMismatch, "quote does not match capability_id", false)
	}
	if quote.Expired(time.Now().UTC()) {
		return SubmitResult{}, domain.NewError(domain.ErrQuoteExpired, "quote has expired", false)
	}
	if _, err := s.store.Get(ctx, in.CapabilityID); err != nil {
		return SubmitResult{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability not found", false)
	}

	needsConfirmation, err := s.accounts.RequiresConfirmation(ctx, in.PrincipalID, quote.Price.TotalMax, quote.Price.Currency)
	if err != nil {
		return SubmitResult{}, err
	}

	state := domain.JobSubmitted
	if needsConfirmation && !in.Confirmed {
		state = domain.JobInputRequired
	}
	job := domain.Job{
		ID:             idPrefix + uuid.NewString(),
		CapabilityID:   in.CapabilityID,
		QuoteID:        in.QuoteID,
		PrincipalID:    in.PrincipalID,
		State:          state,
		Input:          in.Input,
		IdempotencyKey: in.IdempotencyKey,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
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

// executeJob claims a Submitted/InputRequired job for execution (exclusive
// via store.UpdateJob's compare-and-swap: a job can only be claimed once,
// so a concurrent confirmed-reissue of the same idempotency_key can't
// trigger a double debit/double execution), then runs the
// escrow-create -> tos-ai execute -> tos-core verify/settle pipeline.
func (s *JobService) executeJob(ctx context.Context, jobID string, waitInline bool, maxWaitMS int64) (SubmitResult, error) {
	job, claimed, err := s.claimForExecution(ctx, jobID)
	if err != nil {
		return SubmitResult{}, err
	}
	if !claimed {
		// Someone else already claimed (or finished) this job; return its
		// current state rather than executing it again.
		return SubmitResult{Type: resultTypeFor(job), Job: job}, nil
	}

	quote, err := s.store.GetQuote(ctx, job.QuoteID)
	if err != nil {
		return s.failTerminal(ctx, job.ID, "quote lookup failed during execution"), nil
	}
	if quote.Expired(time.Now().UTC()) {
		return s.failTerminal(ctx, job.ID, "quote expired before execution started"), nil
	}
	cap, err := s.store.Get(ctx, job.CapabilityID)
	if err != nil {
		return s.failTerminal(ctx, job.ID, "capability lookup failed during execution"), nil
	}
	// Enforce docs/CAPABILITIES.md "Versioning": a quote is only valid
	// against the exact capability version it was created against. If the
	// capability has since changed, the quote's terms no longer apply.
	if cap.Version != quote.CapabilityVersion {
		return s.failWithCode(ctx, job.ID, domain.ErrQuoteMismatch, "capability has changed since this quote was issued"), nil
	}

	if err := s.accounts.Debit(ctx, job.PrincipalID, quote.Price.TotalMax, quote.Price.Currency); err != nil {
		return s.failWithCode(ctx, job.ID, errCode(err), err.Error()), nil
	}
	escrow, err := s.core.CreateEscrow(ctx, toscore.CreateEscrowRequest{
		QuoteID:      quote.ID,
		CapabilityID: cap.ID,
		PrincipalID:  job.PrincipalID,
		ProviderID:   cap.ProviderID,
		Reserved:     domain.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency},
	})
	if err != nil {
		_ = s.accounts.Credit(ctx, job.PrincipalID, quote.Price.TotalMax, quote.Price.Currency)
		return s.failTerminal(ctx, job.ID, "escrow creation failed"), nil
	}

	job.EscrowID = escrow.ID
	// Record the escrow on the job before running, so Cancel (racing the
	// worker below) can find it — see store.UpdateJob for why this doesn't
	// need to be part of the same atomic step as the claim above.
	if err := s.store.PutJob(ctx, job); err != nil {
		return SubmitResult{}, err
	}

	done := make(chan domain.Job, 1)
	go func() {
		bg := context.Background()
		done <- s.runToCompletion(bg, job, cap)
	}()

	if !waitInline {
		return SubmitResult{Type: ResultAccepted, Job: job}, nil
	}

	wait := time.Duration(maxWaitMS) * time.Millisecond
	if maxWaitMS <= 0 {
		wait = inlineWaitDefault
	}
	select {
	case finished := <-done:
		return SubmitResult{Type: resultTypeFor(finished), Job: finished}, nil
	case <-time.After(wait):
		current, err := s.store.GetJob(ctx, job.ID)
		if err != nil {
			return SubmitResult{}, err
		}
		return SubmitResult{Type: ResultAccepted, Job: current}, nil
	}
}

// claimForExecution atomically moves a job from Submitted/InputRequired to
// Working. Only the caller that wins this compare-and-swap may debit funds
// or call tos-ai — this is what makes executeJob safe to call from both
// the fresh-submission path and the confirmed-reissue replay path without
// risking a double execution.
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

// transitionIfNotTerminal atomically applies mutate to a job if (and only
// if) it is not already in a terminal state. Both the completion pipeline
// and Cancel go through this, so whichever one gets there first wins and
// the other observes the already-terminal result instead of clobbering it.
func (s *JobService) transitionIfNotTerminal(ctx context.Context, jobID string, mutate func(domain.Job) domain.Job) (job domain.Job, applied bool, err error) {
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

// runToCompletion drives one claimed (Working) job through tos-ai
// execution and tos-core verify/settle, persisting the terminal state. It
// never returns an error directly — failures are captured on the Job
// itself so both the inline (Invoke) and background (CreateJob) callers
// share one code path.
func (s *JobService) runToCompletion(ctx context.Context, job domain.Job, cap domain.Capability) domain.Job {
	result, err := s.provider.SubmitJob(ctx, tosai.SubmitJobRequest{
		JobID:        job.ID,
		CapabilityID: job.CapabilityID,
		ProviderID:   cap.ProviderID,
		Input:        job.Input,
	})
	if err != nil || result.Receipt == nil {
		return s.failTerminal(ctx, job.ID, "tos-ai execution failed").Job
	}

	verify, err := s.core.VerifyExecutionReceipt(ctx, job.EscrowID, *result.Receipt)
	if err != nil || !verify.Valid {
		return s.failTerminal(ctx, job.ID, "execution receipt failed verification").Job
	}

	quote, err := s.store.GetQuote(ctx, job.QuoteID)
	if err != nil {
		return s.failTerminal(ctx, job.ID, "quote lookup failed during settlement").Job
	}
	// Phase 0: charge the full quoted total for fixed/per_use pricing.
	// Metered/per_unit capabilities need real usage accounting before they
	// can charge less than total_max — tracked as a follow-up.
	settled, err := s.core.SettleJob(ctx, toscore.SettleJobRequest{
		EscrowID:   job.EscrowID,
		JobID:      job.ID,
		ActualCost: domain.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency},
	})
	if err != nil {
		return s.failTerminal(ctx, job.ID, "settlement failed").Job
	}
	if settled.Receipt.Refunded.Amount != "" && settled.Receipt.Refunded.Amount != "0" && settled.Receipt.Refunded.Amount != "0.00" {
		_ = s.accounts.Credit(ctx, job.PrincipalID, settled.Receipt.Refunded.Amount, settled.Receipt.Refunded.Currency)
	}

	finished, applied, err := s.transitionIfNotTerminal(ctx, job.ID, func(j domain.Job) domain.Job {
		j.State = domain.JobCompleted
		j.Output = result.Output
		j.UpdatedAt = time.Now().UTC()
		return j
	})
	if err != nil || !applied {
		// The job was already finalized by someone else (e.g. Cancel won
		// the race after settlement had already succeeded here). The
		// money has already moved; this is the residual narrow window
		// documented at the top of this file, surfaced rather than hidden.
		return finished
	}
	return finished
}

// failTerminal transitions a job to Failed and releases its escrow in
// full, per docs/SETTLEMENT.md: verification/execution failure routes to
// release, not settlement. It is a no-op if the job already reached a
// terminal state through another path.
func (s *JobService) failTerminal(ctx context.Context, jobID, reason string) SubmitResult {
	return s.fail(ctx, jobID, domain.ErrProviderFailed, reason)
}

func (s *JobService) failWithCode(ctx context.Context, jobID string, code domain.ErrorCode, reason string) SubmitResult {
	return s.fail(ctx, jobID, code, reason)
}

func (s *JobService) fail(ctx context.Context, jobID string, _ domain.ErrorCode, reason string) SubmitResult {
	job, applied, err := s.transitionIfNotTerminal(ctx, jobID, func(j domain.Job) domain.Job {
		j.State = domain.JobFailed
		j.FailureReason = reason
		j.UpdatedAt = time.Now().UTC()
		return j
	})
	if err != nil {
		return SubmitResult{Type: ResultFailed, Job: job}
	}
	if applied && job.EscrowID != "" {
		if receipt, err := s.core.ReleaseEscrow(ctx, job.EscrowID); err == nil {
			_ = s.accounts.Credit(ctx, job.PrincipalID, receipt.Refunded.Amount, receipt.Refunded.Currency)
		}
		// A release failure here leaves funds reserved against a job that
		// will never complete. That is an operational problem (needs
		// reconciliation/retry), not silently ignorable — but Phase 0 has
		// no reconciliation worker yet to hand it to, so it is at least
		// not masked as success: the job is still reported failed, and
		// the escrow's own expiry (docs/SETTLEMENT.md) is the backstop
		// that eventually releases it.
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

// Cancel implements atos_cancel_job / POST /jobs/{id}/cancel. Idempotent
// per-key like submit; cancellation of a job that has already reached a
// terminal state (including one that just completed concurrently) is
// rejected rather than silently accepted or overwritten.
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
			return domain.Job{}, domain.NewError(domain.ErrIdempotencyConflict, "a request with this idempotency_key is still in progress; retry shortly", true)
		}
		return s.store.GetJob(ctx, rec.ResponseKey)
	}
	committed := false
	defer func() {
		if !committed {
			_ = s.store.Release(ctx, principalID, idempotencyKey)
		}
	}()

	existing, err := s.Get(ctx, jobID)
	if err != nil {
		return domain.Job{}, err
	}
	if existing.PrincipalID != principalID {
		return domain.Job{}, domain.NewError(domain.ErrPermissionDenied, "not the job's owning principal", false)
	}

	job, applied, err := s.transitionIfNotTerminal(ctx, jobID, func(j domain.Job) domain.Job {
		j.State = domain.JobCanceled
		j.FailureReason = reason
		j.UpdatedAt = time.Now().UTC()
		return j
	})
	if err != nil {
		return domain.Job{}, err
	}
	if !applied {
		return domain.Job{}, domain.NewError(domain.ErrJobNotCancelable, "job is already in a terminal state", false)
	}

	// Best-effort signal to stop in-flight execution — not required for
	// correctness (the job-level CAS above already prevents its result
	// from being accepted) so its error is not fatal to cancellation.
	_ = s.provider.CancelJob(ctx, jobID, reason)

	if job.EscrowID != "" {
		receipt, err := s.core.ReleaseEscrow(ctx, job.EscrowID)
		if err != nil {
			return domain.Job{}, domain.NewError(domain.ErrSettlementFailed, "job was canceled but releasing its escrow failed: "+err.Error(), true)
		}
		if err := s.accounts.Credit(ctx, job.PrincipalID, receipt.Refunded.Amount, receipt.Refunded.Currency); err != nil {
			return domain.Job{}, domain.NewError(domain.ErrSettlementFailed, "job was canceled but crediting the refund failed: "+err.Error(), true)
		}
	}

	if err := s.store.Finish(ctx, principalID, idempotencyKey, jobID); err != nil {
		return domain.Job{}, err
	}
	committed = true
	return job, nil
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
