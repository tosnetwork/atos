package service

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

const (
	defaultOpenTaskReconcileInterval   = 15 * time.Second
	defaultOpenTaskReconcileStaleAfter = 30 * time.Second
	defaultOpenTaskReconcileBatch      = 100
)

// OpenTaskService implements Phase 3C's Open Task Marketplace (atos-spec
// docs/IMPLEMENTATION_ROADMAP.md §7.3). An OpenTask is a demand-side
// marketplace object, never a parallel commercial contract: Publish/Propose
// are simple idempotent creates (the same store.Idempotency Reserve/Finish/
// Release pattern JobService.submit already uses), but Accept drives a
// durable, crash-recoverable checkpoint sequence -- winner claim -> Quote
// bind -> Job bind -- exactly mirroring ExecutionSignerService's
// authorize/rotate/revoke journal (internal/service/execution_signer.go),
// down to the resumeOrConflict-before-Open ordering and the ambiguous-vs-
// definitive failure handling. See domain.AcceptanceOperation's doc comment
// for the full checkpoint sequence.
//
// Quote/Job creation are NEVER reimplemented here: Accept calls
// s.quotes.Create and s.jobs.CreateJob exactly as a direct caller would,
// using the task owner as principal and the winning proposal's Capability/
// version as the target -- pricing, trust-mode resolution and proof
// requirements are entirely QuoteService's decision, never recomputed or
// overridden here.
type OpenTaskService struct {
	store  store.Store
	quotes *QuoteService
	jobs   *JobService
}

func NewOpenTaskService(s store.Store, quotes *QuoteService, jobs *JobService) *OpenTaskService {
	return &OpenTaskService{store: s, quotes: quotes, jobs: jobs}
}

// view returns t with Status lazily promoted to Expired if ExpiresAt has
// passed while still Open -- see domain.OpenTaskExpired's doc comment: there
// is no background sweep that mutates the stored row, every read path
// (Get/ListPublic/ListMine) must apply this before returning a task to a
// caller, or an expired task would misleadingly still report "open".
func (s *OpenTaskService) view(t domain.OpenTask, now time.Time) domain.OpenTask {
	if t.Status == domain.OpenTaskOpen && t.Expired(now) {
		t.Status = domain.OpenTaskExpired
	}
	return t
}

// --- Publish ---

type PublishOpenTaskInput struct {
	PrincipalID        string
	Title              string
	Description        string
	Input              map[string]any
	RequestedTrustMode domain.RequestedTrustMode
	ProofRequirements  domain.ProofRequirements
	MaxTotal           *domain.Money
	ExpiresAt          time.Time
	IdempotencyKey     string
}

func (in PublishOpenTaskInput) validate() error {
	if in.PrincipalID == "" {
		return domain.NewError(domain.ErrAuthenticationRequired, "principal is required", false)
	}
	if in.Title == "" || in.IdempotencyKey == "" {
		return domain.NewError(domain.ErrValidationFailed, "title and idempotency_key are required", false)
	}
	if in.ExpiresAt.IsZero() {
		return domain.NewError(domain.ErrValidationFailed, "expires_at is required", false)
	}
	return nil
}

// Publish creates a new OpenTask. Idempotent exactly like JobService.submit:
// Reserve/replay-or-conflict/crash-recovery-lookup/Finish, using the generic
// store.Idempotency primitive rather than a bespoke journal -- Publish is a
// single durable create with no multi-step remote side effect, unlike
// Accept.
func (s *OpenTaskService) Publish(ctx context.Context, in PublishOpenTaskInput) (domain.OpenTask, error) {
	if err := in.validate(); err != nil {
		return domain.OpenTask{}, err
	}
	if !in.ExpiresAt.After(time.Now().UTC()) {
		return domain.OpenTask{}, domain.NewError(domain.ErrValidationFailed, "expires_at must be in the future", false)
	}

	requestHash := hashRequest("atos-open-task-publish-v1", in.Title, in.Description, in.Input,
		string(in.RequestedTrustMode), in.ProofRequirements, in.MaxTotal, in.ExpiresAt)
	now := time.Now().UTC()
	rec, reserved, err := s.store.Reserve(ctx, in.PrincipalID, in.IdempotencyKey, requestHash, now.Add(idempotencyLease))
	if err != nil {
		return domain.OpenTask{}, err
	}
	if !reserved {
		if rec.RequestHash != requestHash {
			return domain.OpenTask{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false)
		}
		if rec.Status != store.IdempotencyCompleted {
			return domain.OpenTask{}, domain.NewError(domain.ErrIdempotencyConflict, "a request with this idempotency_key is still in progress; retry shortly", true)
		}
		return s.store.GetOpenTask(ctx, rec.ResponseKey)
	}

	committed := false
	defer func() {
		if !committed {
			_ = s.store.Release(context.Background(), in.PrincipalID, in.IdempotencyKey)
		}
	}()

	if existing, lookupErr := s.store.OpenTaskByIdempotencyKey(ctx, in.PrincipalID, in.IdempotencyKey); lookupErr == nil {
		if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, existing.ID); err != nil {
			return domain.OpenTask{}, err
		}
		committed = true
		return existing, nil
	} else if lookupErr != store.ErrNotFound {
		return domain.OpenTask{}, lookupErr
	}

	task := domain.OpenTask{
		ID: "otask_" + uuid.NewString(), PrincipalID: in.PrincipalID,
		Title: in.Title, Description: in.Description, Input: cloneMap(in.Input),
		RequestedTrustMode: in.RequestedTrustMode, ProofRequirements: in.ProofRequirements,
		MaxTotal: in.MaxTotal, ExpiresAt: in.ExpiresAt, Status: domain.OpenTaskOpen,
		PublicationIdempotencyKey: in.IdempotencyKey, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.store.PutOpenTask(ctx, task); err != nil {
		return domain.OpenTask{}, err
	}
	if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, task.ID); err != nil {
		return domain.OpenTask{}, err
	}
	committed = true
	return task, nil
}

// --- Read paths ---

// Get returns taskID, with Input and every other owner-only field visible
// only to the task's own owner or (once a winner is claimed) the accepted
// proposal's provider -- every other caller gets domain.OpenTask.Public().
// requestingPrincipalID is the authenticated caller, or "" for an
// unauthenticated/public request.
func (s *OpenTaskService) Get(ctx context.Context, requestingPrincipalID, taskID string) (domain.OpenTask, error) {
	t, err := s.store.GetOpenTask(ctx, taskID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.OpenTask{}, domain.NewError(domain.ErrNotFound, "open task not found", false)
		}
		return domain.OpenTask{}, err
	}
	t = s.view(t, time.Now().UTC())
	if requestingPrincipalID != "" {
		if t.PrincipalID == requestingPrincipalID {
			return t, nil
		}
		if t.AcceptedProposalID != "" {
			if proposal, err := s.store.GetOpenTaskProposal(ctx, t.AcceptedProposalID); err == nil && proposal.ProviderID == requestingPrincipalID {
				return t, nil
			}
		}
	}
	return t.Public(), nil
}

// ListPublic is the marketplace browse/search view: every currently-Open,
// non-expired task, redacted to domain.OpenTask.Public() -- Input and every
// other owner-only field is never included here.
func (s *OpenTaskService) ListPublic(ctx context.Context, limit int) ([]domain.OpenTask, error) {
	tasks, err := s.store.ListPublicOpenTasks(ctx, limit)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]domain.OpenTask, 0, len(tasks))
	for _, t := range tasks {
		t = s.view(t, now)
		if t.Status != domain.OpenTaskOpen {
			continue
		}
		out = append(out, t.Public())
	}
	return out, nil
}

// ListMine returns every OpenTask principalID owns (any status), full
// detail -- the owner's "my tasks" view.
func (s *OpenTaskService) ListMine(ctx context.Context, principalID string) ([]domain.OpenTask, error) {
	tasks, err := s.store.OpenTasksByPrincipal(ctx, principalID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	for i := range tasks {
		tasks[i] = s.view(tasks[i], now)
	}
	return tasks, nil
}

// effectiveProposalStatus derives submitted/withdrawn/accepted/rejected --
// see domain.OpenTaskProposal's doc comment for why accepted/rejected are
// never stored.
func effectiveProposalStatus(p domain.OpenTaskProposal, task domain.OpenTask) domain.OpenTaskProposalStatus {
	if task.AcceptedProposalID == p.ID {
		return domain.OpenTaskProposalAccepted
	}
	if p.WithdrawnAt != nil {
		return domain.OpenTaskProposalWithdrawn
	}
	if task.AcceptedProposalID != "" || task.Status.Terminal() {
		return domain.OpenTaskProposalRejected
	}
	return domain.OpenTaskProposalSubmitted
}

// ListProposals returns every proposal for taskID with the derived Status
// (see effectiveProposalStatus), redacting Message/ProposedPrice via
// domain.OpenTaskProposal.Public() for every proposal except the task
// owner's own view (sees everything) and each provider's view of their OWN
// proposal (sees their own Message in full, every other proposal
// redacted).
func (s *OpenTaskService) ListProposals(ctx context.Context, requestingPrincipalID, taskID string) ([]domain.ProposalView, error) {
	task, err := s.store.GetOpenTask(ctx, taskID)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, domain.NewError(domain.ErrNotFound, "open task not found", false)
		}
		return nil, err
	}
	proposals, err := s.store.ProposalsByTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	isOwner := requestingPrincipalID != "" && requestingPrincipalID == task.PrincipalID
	out := make([]domain.ProposalView, 0, len(proposals))
	for _, p := range proposals {
		visible := p
		if !isOwner && p.ProviderID != requestingPrincipalID {
			visible = p.Public()
		}
		out = append(out, domain.ProposalView{OpenTaskProposal: visible, Status: effectiveProposalStatus(p, task)})
	}
	return out, nil
}

// --- Propose ---

type ProposeInput struct {
	ProviderID     string
	TaskID         string
	CapabilityID   string
	Message        string
	ProposedPrice  *domain.Money
	IdempotencyKey string
}

func (in ProposeInput) validate() error {
	if in.ProviderID == "" {
		return domain.NewError(domain.ErrAuthenticationRequired, "provider principal is required", false)
	}
	if in.TaskID == "" || in.CapabilityID == "" || in.IdempotencyKey == "" {
		return domain.NewError(domain.ErrValidationFailed, "task_id, capability_id and idempotency_key are required", false)
	}
	return nil
}

// Propose submits a provider's application to fulfill taskID. CapabilityVersion
// is NEVER caller-supplied -- it is frozen here from the Capability's own
// CURRENT version at propose time (exactly like QuoteService.Create freezes
// CapabilityVersion from the live Capability, never trusting a caller-
// supplied version), so a later Accept re-verifying "is this still the
// current version" is checking against something this service itself
// derived, not something a caller could have lied about upfront.
func (s *OpenTaskService) Propose(ctx context.Context, in ProposeInput) (domain.OpenTaskProposal, error) {
	if err := in.validate(); err != nil {
		return domain.OpenTaskProposal{}, err
	}
	task, err := s.store.GetOpenTask(ctx, in.TaskID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.OpenTaskProposal{}, domain.NewError(domain.ErrNotFound, "open task not found", false)
		}
		return domain.OpenTaskProposal{}, err
	}
	now := time.Now().UTC()
	if task.Status != domain.OpenTaskOpen || task.Expired(now) {
		return domain.OpenTaskProposal{}, domain.NewError(domain.ErrOpenTaskNotOpen, "open task is not accepting proposals", false)
	}
	cap, err := s.store.Get(ctx, in.CapabilityID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.OpenTaskProposal{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability not found", false)
		}
		return domain.OpenTaskProposal{}, err
	}
	cap = normalizeCapability(cap)
	if cap.ProviderID != in.ProviderID {
		return domain.OpenTaskProposal{}, domain.NewError(domain.ErrPermissionDenied, "not the owning provider", false)
	}
	if cap.Status != domain.CapabilityActive {
		return domain.OpenTaskProposal{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability is not active", false)
	}

	requestHash := hashRequest("atos-open-task-propose-v1", in.TaskID, in.CapabilityID, cap.Version, in.Message, in.ProposedPrice)
	rec, reserved, err := s.store.Reserve(ctx, in.ProviderID, in.IdempotencyKey, requestHash, now.Add(idempotencyLease))
	if err != nil {
		return domain.OpenTaskProposal{}, err
	}
	if !reserved {
		if rec.RequestHash != requestHash {
			return domain.OpenTaskProposal{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false)
		}
		if rec.Status != store.IdempotencyCompleted {
			return domain.OpenTaskProposal{}, domain.NewError(domain.ErrIdempotencyConflict, "a request with this idempotency_key is still in progress; retry shortly", true)
		}
		return s.store.GetOpenTaskProposal(ctx, rec.ResponseKey)
	}

	committed := false
	defer func() {
		if !committed {
			_ = s.store.Release(context.Background(), in.ProviderID, in.IdempotencyKey)
		}
	}()

	if existing, lookupErr := s.store.OpenTaskProposalByIdempotencyKey(ctx, in.ProviderID, in.IdempotencyKey); lookupErr == nil {
		if err := s.store.Finish(ctx, in.ProviderID, in.IdempotencyKey, existing.ID); err != nil {
			return domain.OpenTaskProposal{}, err
		}
		committed = true
		return existing, nil
	} else if lookupErr != store.ErrNotFound {
		return domain.OpenTaskProposal{}, lookupErr
	}

	proposal := domain.OpenTaskProposal{
		ID: "otprop_" + uuid.NewString(), TaskID: in.TaskID, ProviderID: in.ProviderID,
		CapabilityID: cap.ID, CapabilityVersion: cap.Version,
		Message: in.Message, ProposedPrice: in.ProposedPrice,
		ProposalIdempotencyKey: in.IdempotencyKey, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.store.PutOpenTaskProposal(ctx, proposal); err != nil {
		return domain.OpenTaskProposal{}, err
	}
	if err := s.store.Finish(ctx, in.ProviderID, in.IdempotencyKey, proposal.ID); err != nil {
		return domain.OpenTaskProposal{}, err
	}
	committed = true
	return proposal, nil
}

// Withdraw marks a provider's own proposal withdrawn. There is a small,
// deliberately-accepted TOCTOU race between the pre-check below and the
// CAS update: a concurrent Accept can still claim this proposal as winner
// in between. That is safe, not just tolerated -- the winner-claim and
// Quote/Job binding are already durably committed by
// store.OpenTasks.OpenAcceptanceOperation before this method could ever
// observe it, so a Withdraw landing microseconds later never unwinds a
// binding, it only leaves WithdrawnAt set on a proposal that has ALSO been
// accepted, which effectiveProposalStatus resolves in favor of Accepted
// (checked first). Not retryable failures here are permanent for this
// call, matching every other ownership check in this codebase.
type WithdrawProposalInput struct {
	ProviderID string
	ProposalID string
}

func (s *OpenTaskService) Withdraw(ctx context.Context, in WithdrawProposalInput) (domain.OpenTaskProposal, error) {
	if in.ProviderID == "" || in.ProposalID == "" {
		return domain.OpenTaskProposal{}, domain.NewError(domain.ErrValidationFailed, "provider_id and proposal_id are required", false)
	}
	existing, err := s.store.GetOpenTaskProposal(ctx, in.ProposalID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.OpenTaskProposal{}, domain.NewError(domain.ErrNotFound, "proposal not found", false)
		}
		return domain.OpenTaskProposal{}, err
	}
	if existing.ProviderID != in.ProviderID {
		return domain.OpenTaskProposal{}, domain.NewError(domain.ErrPermissionDenied, "not the submitting provider", false)
	}
	if task, err := s.store.GetOpenTask(ctx, existing.TaskID); err == nil && task.AcceptedProposalID == in.ProposalID {
		return domain.OpenTaskProposal{}, domain.NewError(domain.ErrOpenTaskNotOpen, "cannot withdraw a proposal that has already been accepted", false)
	}
	return s.store.UpdateOpenTaskProposal(ctx, in.ProposalID, func(p domain.OpenTaskProposal, exists bool) (domain.OpenTaskProposal, error) {
		if !exists {
			return p, domain.NewError(domain.ErrNotFound, "proposal not found", false)
		}
		if p.ProviderID != in.ProviderID {
			return p, domain.NewError(domain.ErrPermissionDenied, "not the submitting provider", false)
		}
		if p.WithdrawnAt != nil {
			return p, nil
		}
		now := time.Now().UTC()
		p.WithdrawnAt = &now
		p.UpdatedAt = now
		return p, nil
	})
}

// --- Cancel ---

type CancelOpenTaskInput struct {
	PrincipalID string
	TaskID      string
}

// Cancel refuses once Status has moved past Open (Accepted or terminal) --
// an accepted winner is never silently discarded by a racing cancel; the
// CAS inside UpdateOpenTask is what makes this race-free against a
// concurrent Accept, whichever of the two transactions commits first wins,
// and the loser sees the other's already-persisted Status and is rejected
// cleanly.
func (s *OpenTaskService) Cancel(ctx context.Context, in CancelOpenTaskInput) (domain.OpenTask, error) {
	if in.PrincipalID == "" || in.TaskID == "" {
		return domain.OpenTask{}, domain.NewError(domain.ErrValidationFailed, "task_id is required", false)
	}
	return s.store.UpdateOpenTask(ctx, in.TaskID, func(t domain.OpenTask, exists bool) (domain.OpenTask, error) {
		if !exists {
			return t, domain.NewError(domain.ErrNotFound, "open task not found", false)
		}
		if t.PrincipalID != in.PrincipalID {
			return t, domain.NewError(domain.ErrPermissionDenied, "not the task owner", false)
		}
		if t.Status != domain.OpenTaskOpen {
			return t, domain.NewError(domain.ErrOpenTaskNotOpen, "open task can only be cancelled while open", false)
		}
		t.Status = domain.OpenTaskCancelled
		t.UpdatedAt = time.Now().UTC()
		return t, nil
	})
}

// --- Accept ---

type AcceptProposalInput struct {
	PrincipalID    string
	TaskID         string
	ProposalID     string
	IdempotencyKey string
}

func (in AcceptProposalInput) validate() error {
	if in.PrincipalID == "" {
		return domain.NewError(domain.ErrAuthenticationRequired, "principal is required", false)
	}
	if in.TaskID == "" || in.ProposalID == "" || in.IdempotencyKey == "" {
		return domain.NewError(domain.ErrValidationFailed, "task_id, proposal_id and idempotency_key are required", false)
	}
	return nil
}

// Accept claims proposalID as taskID's winner and drives the durable
// winner-claim -> Quote-bind -> Job-bind sequence to completion (or to a
// definitive Failed, which reopens the task). See
// store.OpenTasks.OpenAcceptanceOperation's doc comment for the store-level
// atomicity contract this method depends on, and the package doc comment
// above for how this whole method mirrors ExecutionSignerService.Authorize.
//
// A prior attempt under in.IdempotencyKey is looked up and resumed FIRST
// (AcceptanceOperationByIdempotencyKey), exactly mirroring
// ExecutionSignerService.resumeOrConflict -- this is not a style choice,
// see OpenAcceptanceOperation's own doc comment for why calling its build()
// callback against a task a prior, already-completed call already moved
// past Open would be wrong.
func (s *OpenTaskService) Accept(ctx context.Context, in AcceptProposalInput) (domain.OpenTask, domain.AcceptanceOperation, error) {
	if err := in.validate(); err != nil {
		return domain.OpenTask{}, domain.AcceptanceOperation{}, err
	}
	task, err := s.store.GetOpenTask(ctx, in.TaskID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrNotFound, "open task not found", false)
		}
		return domain.OpenTask{}, domain.AcceptanceOperation{}, err
	}
	if task.PrincipalID != in.PrincipalID {
		return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrPermissionDenied, "not the task owner", false)
	}

	if existing, err := s.store.AcceptanceOperationByIdempotencyKey(ctx, in.PrincipalID, in.IdempotencyKey); err == nil {
		if existing.TaskID != in.TaskID || existing.ProposalID != in.ProposalID {
			return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different accept request", false)
		}
		return s.driveAcceptance(ctx, existing)
	} else if err != store.ErrNotFound {
		return domain.OpenTask{}, domain.AcceptanceOperation{}, err
	}

	// Proposal/Capability are fetched and validated BEFORE calling
	// OpenAcceptanceOperation, never from inside its build() callback:
	// the in-memory store holds a single non-reentrant mutex for its
	// entire OpenAcceptanceOperation call, so a nested store call from
	// within build would deadlock; the Postgres store's build() runs
	// inside the acceptance transaction but on a callback that has no
	// access to that transaction's connection, so a nested store call
	// there would read a separate, non-transactional snapshot instead of
	// participating in the same transaction. This narrows the
	// consistency window slightly (a concurrent Withdraw or Capability
	// update landing between this fetch and the lock below is possible)
	// but is the same class of residual race Withdraw's own doc comment
	// already accepts, and is far preferable to a deadlock or a broken
	// isolation guarantee.
	proposal, perr := s.store.GetOpenTaskProposal(ctx, in.ProposalID)
	if perr != nil {
		if perr == store.ErrNotFound {
			return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrNotFound, "proposal not found", false)
		}
		return domain.OpenTask{}, domain.AcceptanceOperation{}, perr
	}
	if proposal.TaskID != in.TaskID {
		return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrValidationFailed, "proposal does not belong to this open task", false)
	}
	if proposal.WithdrawnAt != nil {
		return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskProposalWithdrawn, "proposal has been withdrawn", false)
	}
	cap, cerr := s.store.Get(ctx, proposal.CapabilityID)
	if cerr != nil {
		if cerr == store.ErrNotFound {
			return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskProposalStale, "proposal's capability no longer exists", false)
		}
		return domain.OpenTask{}, domain.AcceptanceOperation{}, cerr
	}
	cap = normalizeCapability(cap)
	// The proposal is refused outright, never silently rebound to
	// whatever the capability's CURRENT version happens to be -- see
	// domain.OpenTaskProposal.CapabilityVersion's doc comment.
	if cap.ProviderID != proposal.ProviderID || cap.Version != proposal.CapabilityVersion || cap.Status != domain.CapabilityActive {
		return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskProposalStale,
			"proposal's capability version is stale or no longer active; the provider must submit a fresh proposal", false)
	}

	op, _, _, err := s.store.OpenAcceptanceOperation(ctx, in.TaskID, func(snapshot domain.OpenTask) (domain.AcceptanceOperation, error) {
		now := time.Now().UTC()
		if snapshot.Status != domain.OpenTaskOpen || snapshot.Expired(now) {
			return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskNotOpen, "open task is not open", false)
		}
		return domain.AcceptanceOperation{
			ID: "accop_" + uuid.NewString(), TaskID: in.TaskID, ProposalID: in.ProposalID,
			PrincipalID: in.PrincipalID, ProviderID: proposal.ProviderID,
			CapabilityID: proposal.CapabilityID, CapabilityVersion: proposal.CapabilityVersion,
			// winner_claimed, not intent_persisted -- the OpenAcceptanceOperation
			// transaction this build() runs inside of durably claims the
			// winner (task.AcceptedProposalID) in the SAME commit as this
			// operation row, so by the time this row is observable at all,
			// the winner claim has already happened alongside it. See
			// store.OpenTasks.OpenAcceptanceOperation's doc comment.
			Checkpoint: domain.AcceptanceWinnerClaimed, IdempotencyKey: in.IdempotencyKey,
			CreatedAt: now, UpdatedAt: now,
		}, nil
	})
	if err != nil {
		return domain.OpenTask{}, domain.AcceptanceOperation{}, err
	}
	return s.driveAcceptance(ctx, op)
}

// advanceAcceptance is domain.AcceptanceOperation's compare-and-swap
// checkpoint helper, exactly mirroring ExecutionSignerService.advance:
// expectedFrom is a CAS guard (a stale/duplicate drive pass silently no-ops
// instead of clobbering newer state), never unconditionally overwriting.
func (s *OpenTaskService) advanceAcceptance(ctx context.Context, id string, expectedFrom, checkpoint domain.AcceptanceCheckpoint, quoteID, jobID, failureReason string) (domain.AcceptanceOperation, error) {
	return s.store.UpdateAcceptanceOperation(ctx, id, func(current domain.AcceptanceOperation, exists bool) (domain.AcceptanceOperation, error) {
		if !exists {
			return domain.AcceptanceOperation{}, domain.NewError(domain.ErrNotFound, "acceptance operation not found", false)
		}
		if current.Checkpoint.Terminal() || current.Checkpoint != expectedFrom {
			return current, nil
		}
		current.Checkpoint = checkpoint
		if quoteID != "" {
			current.QuoteID = quoteID
		}
		if jobID != "" {
			current.JobID = jobID
		}
		current.FailureReason = failureReason
		current.UpdatedAt = time.Now().UTC()
		if checkpoint == domain.AcceptanceCompleted {
			completedAt := current.UpdatedAt
			current.CompletedAt = &completedAt
		}
		return current, nil
	})
}

// handleAcceptanceFailure is Accept/driveAcceptance's sole error path for
// the Quote/Job creation calls, exactly mirroring
// ExecutionSignerService.handleAmbiguousOrFail -- isAmbiguousSignerFailure
// is reused as-is (it inspects only domain.Error.Retryable, nothing
// signer-specific) rather than duplicated under a new name.
func (s *OpenTaskService) handleAcceptanceFailure(ctx context.Context, op domain.AcceptanceOperation, callErr error) (domain.OpenTask, domain.AcceptanceOperation, error) {
	if isAmbiguousSignerFailure(callErr) {
		updated, err := s.advanceAcceptance(ctx, op.ID, op.Checkpoint, domain.AcceptanceReconciling, "", "", callErr.Error())
		if err != nil {
			return domain.OpenTask{}, updated, err
		}
		task, taskErr := s.store.GetOpenTask(ctx, op.TaskID)
		if taskErr != nil {
			return domain.OpenTask{}, updated, taskErr
		}
		return task, updated, domain.NewError(domain.ErrNetworkUnavailable, "open task acceptance outcome is uncertain, retry", true)
	}
	updated, err := s.advanceAcceptance(ctx, op.ID, op.Checkpoint, domain.AcceptanceFailed, "", "", callErr.Error())
	if err != nil {
		return domain.OpenTask{}, updated, err
	}
	task, taskErr := s.reopenTask(ctx, op.TaskID, op.ProposalID)
	if taskErr != nil {
		return domain.OpenTask{}, updated, taskErr
	}
	return task, updated, callErr
}

// reopenTask reverses a winner claim after a definitive (non-retryable)
// acceptance failure -- atos-spec §7.3's "an accepted winner must never be
// permanently stranded" rule. The CAS guard (only reopening while still
// Accepted with THIS proposal as the claimed winner) makes it a safe no-op
// if the task has already moved on through some other path by the time
// this runs.
func (s *OpenTaskService) reopenTask(ctx context.Context, taskID, proposalID string) (domain.OpenTask, error) {
	return s.store.UpdateOpenTask(ctx, taskID, func(t domain.OpenTask, exists bool) (domain.OpenTask, error) {
		if !exists {
			return t, domain.NewError(domain.ErrNotFound, "open task not found", false)
		}
		if t.Status != domain.OpenTaskAccepted || t.AcceptedProposalID != proposalID {
			return t, nil
		}
		t.Status = domain.OpenTaskOpen
		t.AcceptedProposalID = ""
		t.UpdatedAt = time.Now().UTC()
		return t, nil
	})
}

// driveAcceptance walks op from wherever its persisted Checkpoint is to
// Completed: winner_claimed -> quote_binding_pending -> quote_bound ->
// job_binding_pending -> job_bound -> completed. Reconciling is ambiguous
// between "the Quote call was in flight" and "the Job call was in flight" --
// op.QuoteID being durably set disambiguates, exactly like
// ExecutionSignerService.driveRotate's NewAuthorizationRef check.
func (s *OpenTaskService) driveAcceptance(ctx context.Context, op domain.AcceptanceOperation) (domain.OpenTask, domain.AcceptanceOperation, error) {
	if op.Checkpoint == domain.AcceptanceCompleted {
		task, err := s.store.GetOpenTask(ctx, op.TaskID)
		return task, op, err
	}
	if op.Checkpoint == domain.AcceptanceFailed {
		task, err := s.store.GetOpenTask(ctx, op.TaskID)
		if err != nil {
			return domain.OpenTask{}, op, err
		}
		return task, op, domain.NewError(domain.ErrValidationFailed, "acceptance previously failed: "+op.FailureReason, false)
	}

	var err error
	if op.Checkpoint == domain.AcceptanceWinnerClaimed {
		op, err = s.advanceAcceptance(ctx, op.ID, op.Checkpoint, domain.AcceptanceQuoteBindingPending, "", "", "")
		if err != nil {
			return domain.OpenTask{}, op, err
		}
	}

	creatingQuote := op.Checkpoint == domain.AcceptanceQuoteBindingPending ||
		(op.Checkpoint == domain.AcceptanceReconciling && op.QuoteID == "")
	if creatingQuote {
		task, taskErr := s.store.GetOpenTask(ctx, op.TaskID)
		if taskErr != nil {
			return domain.OpenTask{}, op, taskErr
		}
		quote, quoteErr := s.quotes.Create(ctx, CreateQuoteInput{
			PrincipalID: op.PrincipalID, CapabilityID: op.CapabilityID,
			InputSummary: task.Input, RequestedTrustMode: task.RequestedTrustMode,
			ProofRequirements: task.ProofRequirements, MaxTotal: task.MaxTotal,
			IdempotencyKey: op.NewQuoteIdempotencyKey(),
		})
		if quoteErr != nil {
			return s.handleAcceptanceFailure(ctx, op, quoteErr)
		}
		op, err = s.advanceAcceptance(ctx, op.ID, op.Checkpoint, domain.AcceptanceQuoteBound, quote.ID, "", "")
		if err != nil {
			return domain.OpenTask{}, op, err
		}
	}

	if op.Checkpoint == domain.AcceptanceQuoteBound {
		op, err = s.advanceAcceptance(ctx, op.ID, op.Checkpoint, domain.AcceptanceJobBindingPending, "", "", "")
		if err != nil {
			return domain.OpenTask{}, op, err
		}
	}

	creatingJob := op.Checkpoint == domain.AcceptanceJobBindingPending ||
		(op.Checkpoint == domain.AcceptanceReconciling && op.QuoteID != "" && op.JobID == "")
	if creatingJob {
		task, taskErr := s.store.GetOpenTask(ctx, op.TaskID)
		if taskErr != nil {
			return domain.OpenTask{}, op, taskErr
		}
		result, jobErr := s.jobs.CreateJob(ctx, SubmitInput{
			PrincipalID: op.PrincipalID, CapabilityID: op.CapabilityID, QuoteID: op.QuoteID,
			Input: task.Input, IdempotencyKey: op.NewJobIdempotencyKey(),
		})
		if jobErr != nil {
			return s.handleAcceptanceFailure(ctx, op, jobErr)
		}
		op, err = s.advanceAcceptance(ctx, op.ID, op.Checkpoint, domain.AcceptanceJobBound, "", result.Job.ID, "")
		if err != nil {
			return domain.OpenTask{}, op, err
		}
	}

	if op.Checkpoint == domain.AcceptanceJobBound {
		op, err = s.advanceAcceptance(ctx, op.ID, op.Checkpoint, domain.AcceptanceCompleted, "", "", "")
		if err != nil {
			return domain.OpenTask{}, op, err
		}
		if _, err := s.store.UpdateOpenTask(ctx, op.TaskID, func(t domain.OpenTask, exists bool) (domain.OpenTask, error) {
			if !exists {
				return t, domain.NewError(domain.ErrNotFound, "open task not found", false)
			}
			if t.Status != domain.OpenTaskAccepted {
				return t, nil
			}
			t.BoundQuoteID = op.QuoteID
			t.BoundJobID = op.JobID
			t.Status = domain.OpenTaskFulfilled
			t.UpdatedAt = time.Now().UTC()
			return t, nil
		}); err != nil {
			return domain.OpenTask{}, op, err
		}
	}

	task, err := s.store.GetOpenTask(ctx, op.TaskID)
	return task, op, err
}

// --- Reconciler ---

// RunReconciler mirrors ExecutionSignerService.RunReconciler's shape
// exactly.
func (s *OpenTaskService) RunReconciler(ctx context.Context, interval, staleAfter time.Duration, limit int, report func(error)) {
	if interval <= 0 {
		interval = defaultOpenTaskReconcileInterval
	}
	if staleAfter <= 0 {
		staleAfter = defaultOpenTaskReconcileStaleAfter
	}
	if limit <= 0 {
		limit = defaultOpenTaskReconcileBatch
	}
	sweep := func() {
		if err := s.ReconcileStaleOperations(ctx, time.Now().UTC().Add(-staleAfter), limit); err != nil && report != nil {
			report(err)
		}
	}
	sweep()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

// ReconcileStaleOperations drives forward every non-terminal
// AcceptanceOperation last updated before cutoff -- a crash, or a caller
// that never retried, converges here instead of staying stuck in
// Reconciling or a *_pending checkpoint forever.
func (s *OpenTaskService) ReconcileStaleOperations(ctx context.Context, cutoff time.Time, limit int) error {
	stale, err := s.store.StaleAcceptanceOperations(ctx, cutoff, limit)
	if err != nil {
		return err
	}
	var firstErr error
	for _, op := range stale {
		if _, _, driveErr := s.driveAcceptance(ctx, op); driveErr != nil && firstErr == nil {
			firstErr = driveErr
		}
	}
	return firstErr
}
