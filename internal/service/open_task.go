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

	// defaultOpenTaskListLimit/maxOpenTaskListLimit are applied here, in
	// ListPublic, rather than duplicated per-transport (REST/MCP): the two
	// store backends disagree on what an unclamped limit<=0 means (memory
	// treats it as "no limit"; Postgres's `LIMIT $1` with 0 returns zero
	// rows), so any caller that forwards a caller-omitted "0" straight to
	// the store gets a different answer depending on which store is
	// running underneath -- MCP's atos_search_open_tasks previously did
	// exactly that. Clamping once, centrally, makes every transport
	// behave identically regardless of backend.
	defaultOpenTaskListLimit = 50
	maxOpenTaskListLimit     = 100
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
//
// The expires_at-must-be-in-the-future check runs AFTER the Reserve/replay/
// crash-recovery gates, not before -- deliberately, not a style choice: it
// depends on time.Now(), which keeps moving forward, so checking it BEFORE
// those gates would make a legitimate retry (the client's original
// response was lost; it resends the exact same request, including the same
// original expires_at) spuriously fail once enough real time has passed,
// even though the original Publish already succeeded and a stored result
// exists to replay. Every check in this function that depends on anything
// other than the caller-supplied request content itself must be ordered
// the same way, for the same reason.
func (s *OpenTaskService) Publish(ctx context.Context, in PublishOpenTaskInput) (domain.OpenTask, error) {
	if err := in.validate(); err != nil {
		return domain.OpenTask{}, err
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
		// Re-derive the digest from the ALREADY-COMMITTED object's own
		// fields and compare against this call's freshly computed
		// requestHash before trusting it as a replay -- required, not
		// redundant with the Reserve-time hash check above: if the PREVIOUS
		// attempt committed this row via PutOpenTask but then failed before
		// (or during) its own Finish call, that attempt's deferred Release
		// hard-deletes the idempotency_records row entirely. A LATER call
		// reusing the same key -- even with genuinely different content --
		// then sees no existing reservation at all, reserves fresh, and
		// would otherwise land here and silently receive the OLD task
		// instead of being rejected as a conflicting reuse of the key.
		existingHash := hashRequest("atos-open-task-publish-v1", existing.Title, existing.Description, existing.Input,
			string(existing.RequestedTrustMode), existing.ProofRequirements, existing.MaxTotal, existing.ExpiresAt)
		if existingHash != requestHash {
			return domain.OpenTask{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false)
		}
		if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, existing.ID); err != nil {
			return domain.OpenTask{}, err
		}
		committed = true
		return existing, nil
	} else if lookupErr != store.ErrNotFound {
		return domain.OpenTask{}, lookupErr
	}

	// Reached only on a genuinely fresh (first-ever) attempt -- nothing has
	// been committed under this key yet, so it's safe to validate against
	// current time here.
	if !in.ExpiresAt.After(now) {
		return domain.OpenTask{}, domain.NewError(domain.ErrValidationFailed, "expires_at must be in the future", false)
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
// other owner-only field is never included here. limit<=0 (a caller that
// omitted it) defaults to defaultOpenTaskListLimit; anything above
// maxOpenTaskListLimit is clamped down to it.
func (s *OpenTaskService) ListPublic(ctx context.Context, limit int) ([]domain.OpenTask, error) {
	if limit <= 0 {
		limit = defaultOpenTaskListLimit
	} else if limit > maxOpenTaskListLimit {
		limit = maxOpenTaskListLimit
	}
	now := time.Now().UTC()
	tasks, err := s.store.ListPublicOpenTasks(ctx, limit, now)
	if err != nil {
		return nil, err
	}
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
//
// The idempotency digest is deliberately a function ONLY of caller-supplied
// content (task_id, capability_id, message, proposed_price) -- it must NOT
// include the live-resolved Capability version, or a legitimate retry whose
// capability was re-versioned between the original call and the retry would
// compute a different hash and be spuriously rejected as a conflicting
// reuse of the same key. Likewise, every live-state validation below (task
// still open, capability still active/owned) runs AFTER the Reserve/replay/
// crash-recovery gates, not before: those checks are only valid against a
// request that hasn't already succeeded once under this exact key, since
// state legitimately moves on after a successful Propose (the task can be
// accepted or expire, the capability can be paused) without invalidating
// the caller's right to receive their own already-created proposal back on
// retry.
func (s *OpenTaskService) Propose(ctx context.Context, in ProposeInput) (domain.OpenTaskProposal, error) {
	if err := in.validate(); err != nil {
		return domain.OpenTaskProposal{}, err
	}

	requestHash := hashRequest("atos-open-task-propose-v1", in.TaskID, in.CapabilityID, in.Message, in.ProposedPrice)
	now := time.Now().UTC()
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
		// See Publish's identical guard for why this comparison is
		// required: a prior attempt's row can be durably committed while
		// its idempotency_records reservation was subsequently deleted by
		// its own failed-Finish cleanup, so a later, genuinely different
		// request under the same key must not silently receive the old
		// proposal back.
		existingHash := hashRequest("atos-open-task-propose-v1", existing.TaskID, existing.CapabilityID, existing.Message, existing.ProposedPrice)
		if existingHash != requestHash {
			return domain.OpenTaskProposal{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false)
		}
		if err := s.store.Finish(ctx, in.ProviderID, in.IdempotencyKey, existing.ID); err != nil {
			return domain.OpenTaskProposal{}, err
		}
		committed = true
		return existing, nil
	} else if lookupErr != store.ErrNotFound {
		return domain.OpenTaskProposal{}, lookupErr
	}

	// Reached only on a genuinely fresh (first-ever) attempt -- nothing has
	// been committed under this key yet, so live-state validation is safe
	// here. This first read is a fast-path check only (not-found /
	// self-dealing / a plainly-closed task can be rejected without ever
	// taking the "open-task" lock) -- CreateOpenTaskProposal below
	// re-validates Status/Expired fresh under lock regardless, closing the
	// race where Accept/Cancel commits between this read and the actual
	// insert.
	task, err := s.store.GetOpenTask(ctx, in.TaskID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.OpenTaskProposal{}, domain.NewError(domain.ErrNotFound, "open task not found", false)
		}
		return domain.OpenTaskProposal{}, err
	}
	if task.Status != domain.OpenTaskOpen || task.Expired(now) {
		return domain.OpenTaskProposal{}, domain.NewError(domain.ErrOpenTaskNotOpen, "open task is not accepting proposals", false)
	}
	// Self-dealing guard: a task owner cannot apply to fulfill their own
	// task -- a self-referential operation this codebase's own
	// zero-trust/self-referential-operation-check discipline requires
	// explicitly rejecting, not merely leaving implicit. Accept re-checks
	// this independently as defense in depth (see its own doc comment).
	if task.PrincipalID == in.ProviderID {
		return domain.OpenTaskProposal{}, domain.NewError(domain.ErrPermissionDenied, "cannot propose on your own open task", false)
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

	// Capability is fetched and validated BEFORE calling
	// CreateOpenTaskProposal, never from inside its build() callback, for
	// the same reason Accept keeps Capability lookup out of
	// OpenAcceptanceOperation's build(): a nested store call from within
	// build would deadlock the in-memory store's single non-reentrant
	// mutex, and would read a separate, non-transactional snapshot
	// through the Postgres store's connection pool instead of
	// participating in the same transaction.
	proposal, err := s.store.CreateOpenTaskProposal(ctx, in.TaskID, now, func(task domain.OpenTask) (domain.OpenTaskProposal, error) {
		if task.PrincipalID == in.ProviderID {
			return domain.OpenTaskProposal{}, domain.NewError(domain.ErrPermissionDenied, "cannot propose on your own open task", false)
		}
		return domain.OpenTaskProposal{
			ID: "otprop_" + uuid.NewString(), TaskID: in.TaskID, ProviderID: in.ProviderID,
			CapabilityID: cap.ID, CapabilityVersion: cap.Version,
			Message: in.Message, ProposedPrice: in.ProposedPrice,
			ProposalIdempotencyKey: in.IdempotencyKey, CreatedAt: now, UpdatedAt: now,
		}, nil
	})
	if err != nil {
		return domain.OpenTaskProposal{}, err
	}
	if err := s.store.Finish(ctx, in.ProviderID, in.IdempotencyKey, proposal.ID); err != nil {
		return domain.OpenTaskProposal{}, err
	}
	committed = true
	return proposal, nil
}

// Withdraw marks a provider's own proposal withdrawn. Delegates entirely to
// store.OpenTasks.WithdrawOpenTaskProposal, which performs the ownership
// check and the "not already accepted" check under the same lock/row-lock
// discipline OpenAcceptanceOperation uses for this same proposal row -- see
// that method's own doc comment for why a plain pre-check-then-update
// sequence at this layer cannot close the race against a concurrent Accept.
type WithdrawProposalInput struct {
	ProviderID string
	ProposalID string
}

func (s *OpenTaskService) Withdraw(ctx context.Context, in WithdrawProposalInput) (domain.OpenTaskProposal, error) {
	if in.ProviderID == "" || in.ProposalID == "" {
		return domain.OpenTaskProposal{}, domain.NewError(domain.ErrValidationFailed, "provider_id and proposal_id are required", false)
	}
	return s.store.WithdrawOpenTaskProposal(ctx, in.ProposalID, in.ProviderID)
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

	// Capability is fetched and validated BEFORE calling
	// OpenAcceptanceOperation, never from inside its build() callback: a
	// nested store call from within build would deadlock the in-memory
	// store's single non-reentrant mutex, and would read a separate,
	// non-transactional snapshot through the Postgres store's connection
	// pool instead of participating in the same transaction. This narrows
	// the consistency window slightly for CAPABILITY staleness (a
	// concurrent Capability update landing between this check and the
	// actual Quote-creation step below is possible) -- but that window is
	// independently closed by QuoteService.Create's own
	// ExpectedCapabilityVersion pin (see driveAcceptance), which refuses
	// and reopens the task via FailAcceptance if the version has drifted
	// by the time Quote creation actually runs. proposalPreview is used
	// ONLY to pick which Capability to check and to reject self-dealing
	// early; its WithdrawnAt/TaskID fields are NOT trusted here -- build()
	// re-reads the proposal fresh, under the same lock/row-lock a
	// concurrent Withdraw uses, before ever claiming it as winner.
	proposalPreview, perr := s.store.GetOpenTaskProposal(ctx, in.ProposalID)
	if perr != nil {
		if perr == store.ErrNotFound {
			return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrNotFound, "proposal not found", false)
		}
		return domain.OpenTask{}, domain.AcceptanceOperation{}, perr
	}
	if proposalPreview.TaskID != in.TaskID {
		return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrValidationFailed, "proposal does not belong to this open task", false)
	}
	// Self-dealing guard: the task owner can never accept a proposal
	// submitted by themselves (Propose already refuses to create one in
	// the first place, but a proposal predating that check, or a future
	// bypass, must not be exploitable here either -- defense in depth for
	// a self-referential operation, per this codebase's own
	// zero-trust/self-referential-operation-check discipline).
	if proposalPreview.ProviderID == in.PrincipalID {
		return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrPermissionDenied, "cannot accept your own proposal", false)
	}
	cap, cerr := s.store.Get(ctx, proposalPreview.CapabilityID)
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
	if cap.ProviderID != proposalPreview.ProviderID || cap.Version != proposalPreview.CapabilityVersion || cap.Status != domain.CapabilityActive {
		return domain.OpenTask{}, domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskProposalStale,
			"proposal's capability version is stale or no longer active; the provider must submit a fresh proposal", false)
	}

	op, _, _, err := s.store.OpenAcceptanceOperation(ctx, in.TaskID, in.ProposalID, func(snapshot domain.OpenTask, proposal domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
		now := time.Now().UTC()
		if snapshot.Status != domain.OpenTaskOpen || snapshot.Expired(now) {
			return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskNotOpen, "open task is not open", false)
		}
		if proposal.TaskID != in.TaskID {
			return domain.AcceptanceOperation{}, domain.NewError(domain.ErrValidationFailed, "proposal does not belong to this open task", false)
		}
		if proposal.WithdrawnAt != nil {
			return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskProposalWithdrawn, "proposal has been withdrawn", false)
		}
		if proposal.ProviderID == in.PrincipalID {
			return domain.AcceptanceOperation{}, domain.NewError(domain.ErrPermissionDenied, "cannot accept your own proposal", false)
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
// signer-specific) rather than duplicated under a new name. The definitive
// (non-ambiguous) branch calls store.OpenTasks.FailAcceptance, which
// transitions the operation to Failed AND reopens the task (if it is still
// claimed by this exact operation) in ONE database transaction -- never a
// separate advance-then-reopen pair of calls, since Failed is terminal and
// therefore excluded from the reconciler's sweep: a crash between two
// separate commits here would leave the task permanently stuck Accepted
// with no future recovery path (see FailAcceptance's own doc comment).
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
	updated, task, err := s.store.FailAcceptance(ctx, op.ID, callErr.Error())
	if err != nil {
		return domain.OpenTask{}, updated, err
	}
	return task, updated, callErr
}

// driveAcceptance walks op from wherever its persisted Checkpoint is to
// Completed: winner_claimed -> quote_binding_pending -> quote_bound ->
// job_binding_pending -> job_bound -> completed. Reconciling is ambiguous
// between "the Quote call was in flight" and "the Job call was in flight" --
// op.QuoteID being durably set disambiguates, exactly like
// ExecutionSignerService.driveRotate's NewAuthorizationRef check.
func (s *OpenTaskService) driveAcceptance(ctx context.Context, op domain.AcceptanceOperation) (domain.OpenTask, domain.AcceptanceOperation, error) {
	if op.Checkpoint == domain.AcceptanceCompleted {
		// Re-run through CompleteAcceptance rather than a plain GetOpenTask:
		// it is a safe, idempotent no-op for the checkpoint itself, but
		// still re-applies the OpenTask projection (BoundQuoteID/BoundJobID/
		// Status=Fulfilled) if an EARLIER call reached Completed but never
		// got to project it -- see CompleteAcceptance's own doc comment for
		// why that projection is no longer at risk of being split across
		// two separate commits, but this call remains the self-healing path
		// for anything that predates this fix.
		completedOp, task, err := s.store.CompleteAcceptance(ctx, op.ID)
		return task, completedOp, err
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
			// Pin the Capability version this operation's winner claim
			// actually verified at Accept time. Without this, a Capability
			// version bump landing between winner-claim and this call (or a
			// resumed retry/reconciler pass running long after) would
			// silently price and bind a DIFFERENT version than the one the
			// proposal/operation both still claim -- QuoteService.Create
			// refuses instead, and that definitive rejection reopens the
			// task via handleAcceptanceFailure/FailAcceptance rather than
			// binding a version nobody re-verified.
			ExpectedCapabilityVersion: op.CapabilityVersion,
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
		// CompleteAcceptance -- not advanceAcceptance followed by a
		// separate UpdateOpenTask call -- transitions the operation to
		// Completed AND projects the OpenTask to Fulfilled in ONE database
		// transaction. See its doc comment for why splitting this into two
		// commits is unsafe: Completed is terminal and excluded from the
		// reconciler's stale sweep, so a crash between "operation marked
		// Completed" and "task projected Fulfilled" would otherwise strand
		// the task at Status=Accepted forever, with nothing left to trigger
		// a third attempt.
		completedOp, task, err := s.store.CompleteAcceptance(ctx, op.ID)
		return task, completedOp, err
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
