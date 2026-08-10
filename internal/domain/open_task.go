// Phase 3C: Open Task Marketplace (atos-spec docs/IMPLEMENTATION_ROADMAP.md
// §7.3). An OpenTask is a demand-side marketplace object -- NOT a
// replacement for Capability, Quote or Job, and NOT a parallel commercial
// contract. Its entire purpose is to select exactly one winning provider
// proposal and hand off to the existing Quote/Job/Receipt/settlement/dispute
// pipeline unchanged:
//
//	publish -> open -> proposals -> exactly one accepted proposal
//	  -> immutable Quote/Job binding -> normal Job/Receipt/settlement/dispute lifecycle
//
// Pricing, trust mode, proof requirements and Capability/version binding are
// NEVER decided here -- they are decided by the existing QuoteService, using
// the task owner as principal, the winning proposal's Capability/version, and
// the task's own requested trust mode / proof requirements / budget as
// constraints, exactly as if the task owner had called QuoteService.Create
// directly. See service.OpenTaskService's doc comment for the acceptance
// journal that makes the winner-selection -> Quote -> Job sequence durable
// and crash-recoverable.
package domain

import "time"

type OpenTaskStatus string

const (
	// OpenTaskOpen accepts new proposals. The only status Accept and
	// Propose may act on.
	OpenTaskOpen OpenTaskStatus = "open"
	// OpenTaskAccepted means a winning proposal has been durably claimed
	// (AcceptedProposalID is set) and an AcceptanceOperation is driving
	// the Quote/Job binding forward -- no new proposal may be accepted
	// while in this state, but see AcceptedProposalID's doc comment for
	// why this is not yet the terminal "done" state. A definitive
	// (non-ambiguous) failure of that binding reopens the task
	// (Open) rather than stranding it here forever -- atos-spec §7.3's
	// "expiry/cancel rules ... cannot strand an accepted Job" rule applies
	// symmetrically to a winner claim that never reaches a Job.
	OpenTaskAccepted OpenTaskStatus = "accepted"
	// OpenTaskFulfilled is terminal: the accepted proposal's Quote and Job
	// are both durably bound (BoundQuoteID/BoundJobID are set,
	// AcceptanceOperation reached Completed). From here on, the normal
	// Job/Receipt/settlement/dispute lifecycle is authoritative; this
	// OpenTask record is history, never mutated again.
	OpenTaskFulfilled OpenTaskStatus = "fulfilled"
	// OpenTaskCancelled is terminal: the owner cancelled before any
	// proposal was accepted. Cancel is refused once Status is Accepted or
	// Fulfilled -- see OpenTaskService.Cancel's doc comment; an accepted
	// winner is never silently discarded by a cancel race.
	OpenTaskCancelled OpenTaskStatus = "cancelled"
	// OpenTaskExpired is terminal: ExpiresAt passed while still Open, with
	// no accepted proposal. Checked lazily wherever it matters (Propose,
	// Accept, public reads) by comparing against the current time, the
	// same pattern domain.Quote.Expired already uses -- there is no
	// separate expiry sweep to keep in sync with that check.
	OpenTaskExpired OpenTaskStatus = "expired"
)

// Terminal reports whether status is a final state for this OpenTask's own
// lifecycle -- Fulfilled/Cancelled/Expired. Accepted is deliberately NOT
// terminal: an AcceptanceOperation is still in flight, or a definitive
// failure could still reopen the task to Open.
func (s OpenTaskStatus) Terminal() bool {
	switch s {
	case OpenTaskFulfilled, OpenTaskCancelled, OpenTaskExpired:
		return true
	default:
		return false
	}
}

// Expired reports whether t has passed OpenTask.ExpiresAt. Zero ExpiresAt
// means no expiry.
func (t OpenTask) Expired(now time.Time) bool {
	return !t.ExpiresAt.IsZero() && !now.Before(t.ExpiresAt)
}

// OpenTask is the durable marketplace demand object. Every field that
// downstream Quote/Job creation depends on is captured here once, at
// publish time, exactly the same "freeze at commitment time, never silently
// reinterpret from later live state" discipline domain.Quote and domain.Job
// already use for Capability version/binding/schema (see quote.go's
// CapabilityVersion/Binding doc comments) -- an OpenTask's own frozen input
// is that same kind of commitment, just one layer earlier in the pipeline.
type OpenTask struct {
	ID string `json:"id"`
	// PrincipalID is the task owner -- the only principal who may Accept
	// or Cancel this task, and the PrincipalID QuoteService.Create will be
	// called with at acceptance time (see
	// service.OpenTaskService.acceptanceQuoteInput). Sourced only from the
	// authenticated caller at Publish time, never trusted from request body.
	PrincipalID string `json:"principal_id"`
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
	// Input is the durable payload Job creation will use verbatim as
	// SubmitInput.Input once a proposal is accepted -- the same role
	// domain.Job.Input already plays, just captured earlier. Never
	// re-derived from a proposal or from the acceptance request; the
	// winning provider executes exactly what was published.
	//
	// Deliberately NOT `,omitempty`: this field round-trips through the
	// Postgres store's jsonb payload column (see postgres.scanOpenTask),
	// and Go's omitempty drops a zero-LENGTH map (an explicitly published
	// Input: {}) exactly like it would drop a nil one -- collapsing
	// "explicitly empty object" into "absent" and turning it back into a
	// nil map on read-back, which then fails Job creation's input-schema
	// validation for any capability whose schema requires an object with
	// no required properties (validateAgainstSchema correctly rejects
	// null against {"type":"object"}, but a caller who genuinely
	// published {} should not see their intent silently discarded).
	Input map[string]any `json:"input"`
	// RequestedTrustMode/ProofRequirements are the task owner's OWN
	// constraints, passed to QuoteService.Create at acceptance time
	// exactly as CreateQuoteInput.RequestedTrustMode/ProofRequirements --
	// never a provider-supplied override (see OpenTaskProposal doc
	// comment: a proposal's own trust-mode/price fields, if any, are
	// non-authoritative hints only).
	RequestedTrustMode RequestedTrustMode `json:"requested_trust_mode,omitempty"`
	ProofRequirements  ProofRequirements  `json:"proof_requirements,omitempty"`
	// MaxTotal is passed to QuoteService.Create as CreateQuoteInput.MaxTotal
	// -- the same "capability price exceeds requested max_total" rejection
	// path an ordinary direct Quote request already enforces. This is the
	// task's budget ceiling, not a price a provider can name; see
	// OpenTaskProposal.ProposedPrice's doc comment.
	MaxTotal *Money `json:"max_total,omitempty"`
	// ExpiresAt is when this task stops accepting proposals/acceptance if
	// still Open. Required (Publish rejects a zero value) -- an OpenTask
	// MUST NOT be able to sit open forever with no expiry, per atos-spec
	// §7.3's "expiry/cancel rules are explicit" rule.
	ExpiresAt time.Time `json:"expires_at"`

	Status OpenTaskStatus `json:"status"`
	// AcceptedProposalID is set durably (and Status moves to Accepted) the
	// instant a winner is claimed, in the SAME atomic step that rejects
	// any further Accept for this task -- see
	// store.OpenTasks.OpenAcceptanceOperation's doc comment. This is what
	// "exactly one accepted proposal" actually means operationally: it is
	// this field's write, guarded by store-level uniqueness, that is
	// authoritative, not any in-process check.
	AcceptedProposalID string `json:"accepted_proposal_id,omitempty"`
	// BoundQuoteID/BoundJobID are populated as the AcceptanceOperation
	// advances past quote_bound/job_bound -- both empty while Accepted
	// with an operation still in flight, both set once Status reaches
	// Fulfilled. A caller must not infer "fulfilled" from BoundJobID alone
	// being non-empty during a race; read Status.
	BoundQuoteID string `json:"bound_quote_id,omitempty"`
	BoundJobID   string `json:"bound_job_id,omitempty"`

	// PublicationIdempotencyKey is the caller-supplied key Publish opens
	// this record under (store.Idempotency-backed, the same generic
	// mechanism JobService.CreateJob already uses -- see
	// service.OpenTaskService.Publish's doc comment). Recorded here purely
	// for audit visibility; OpenTaskByIdempotencyKey is the actual lookup
	// path.
	PublicationIdempotencyKey string `json:"-"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Public strips owner-only fields (Input, the full Description if a
// deployment later wants to redact it) for marketplace listing/search
// responses -- callers other than the task owner or the winning provider
// must never receive Input verbatim. See
// service.OpenTaskService.redactForListing's doc comment for exactly which
// viewers get which shape; this method returns the narrowest (public,
// unauthenticated-safe) shape.
func (t OpenTask) Public() OpenTask {
	t.Input = nil
	t.PublicationIdempotencyKey = ""
	return t
}

type OpenTaskProposalStatus string

const (
	OpenTaskProposalSubmitted OpenTaskProposalStatus = "submitted"
	OpenTaskProposalWithdrawn OpenTaskProposalStatus = "withdrawn"
	// OpenTaskProposalAccepted/Rejected are derived (never stored) by
	// comparing a proposal against its OpenTask.AcceptedProposalID -- see
	// OpenTaskProposal's doc comment and
	// service.OpenTaskService.effectiveProposalStatus.
	OpenTaskProposalAccepted OpenTaskProposalStatus = "accepted"
	OpenTaskProposalRejected OpenTaskProposalStatus = "rejected"
)

// OpenTaskProposal is a provider's application to fulfill an OpenTask.
// Deliberately does NOT carry its own accepted/rejected status column --
// "accepted" and "rejected" are derived, never stored, by comparing a
// proposal's ID against its OpenTask's AcceptedProposalID/Status (see
// service.OpenTaskService.effectiveProposalStatus). This is a deliberate
// design choice, not an oversight: storing "accepted"/"rejected" directly
// on every losing proposal would require a fan-out write across every
// other proposal at accept time, an unbounded-size transaction for a
// popular task; deriving it from the single authoritative
// OpenTask.AcceptedProposalID instead keeps accept O(1) and there is
// exactly one source of truth for who won.
type OpenTaskProposal struct {
	ID     string `json:"id"`
	TaskID string `json:"task_id"`
	// ProviderID is sourced only from the authenticated caller at Propose
	// time, never trusted from the request body -- see
	// service.OpenTaskService.Propose's doc comment.
	ProviderID string `json:"provider_id"`
	// CapabilityID/CapabilityVersion is the EXACT version this proposal is
	// bound to. Accept re-verifies this exact version is still the
	// Capability's current version and still owned by ProviderID before
	// ever calling QuoteService -- a stale version is refused outright,
	// never silently swapped to whatever the Capability's current version
	// happens to be (atos-spec §7.3; see
	// service.OpenTaskService.Accept's doc comment).
	CapabilityID      string `json:"capability_id"`
	CapabilityVersion string `json:"capability_version"`
	Message           string `json:"message,omitempty"`
	// ProposedPrice is a NON-AUTHORITATIVE hint only, if a deployment
	// chooses to collect one at all -- Accept always re-derives the real
	// price via QuoteService.Create's own pricing rules from the
	// Capability's committed price_hint/MeteredRates, and MaxTotal from
	// the OpenTask itself bounds it exactly like any other direct Quote
	// request. A provider cannot name their own price and have it
	// honored; this field exists purely for a human/UI to see what the
	// provider expected before the real Quote is computed.
	ProposedPrice *Money `json:"proposed_price,omitempty"`

	// ProposalIdempotencyKey mirrors OpenTask.PublicationIdempotencyKey --
	// recorded for audit visibility; ProposalByIdempotencyKey is the real
	// lookup path.
	ProposalIdempotencyKey string `json:"-"`

	// WithdrawnAt is the one piece of proposal state that genuinely cannot
	// be derived (unlike accepted/rejected, above) -- a provider can
	// withdraw at any time before acceptance, independent of the task's own
	// status. nil means still submitted. Withdraw refuses to set this once
	// the proposal is the task's AcceptedProposalID (see
	// service.OpenTaskService.Withdraw's doc comment); once an
	// AcceptanceOperation has claimed a proposal as winner, a later
	// Withdraw call is rejected, not silently allowed to race the binding.
	WithdrawnAt *time.Time `json:"withdrawn_at,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ProposalView struct {
	OpenTaskProposal
	// Status is the DERIVED effective status (submitted/withdrawn/
	// accepted/rejected) -- see OpenTaskProposal's doc comment. Never
	// persisted; computed fresh on every read.
	Status OpenTaskProposalStatus `json:"status"`
}

// Public strips the message (a private negotiation channel between the
// provider and the task owner) for viewers who are neither the task owner
// nor this proposal's own provider -- see
// service.OpenTaskService.redactProposal.
func (p OpenTaskProposal) Public() OpenTaskProposal {
	p.Message = ""
	p.ProposedPrice = nil
	p.ProposalIdempotencyKey = ""
	return p
}

// AcceptanceCheckpoint is one step of the durable winner-selection ->
// Quote -> Job binding sequence, mirroring
// domain.ExecutionSignerOperation's checkpoint journal exactly (same
// crash-recovery discipline, same Reconciling ambiguous-outcome state, same
// "a crash at any boundary must converge on restart, never silently skip a
// step" requirement). See service.OpenTaskService's doc comment for the
// full sequence and store.OpenTasks.OpenAcceptanceOperation's doc comment
// for why serializing "read current winner, then open" atomically per
// TaskID is not by itself sufficient without an additional
// at-most-one-non-terminal-operation-per-task invariant.
type AcceptanceCheckpoint string

const (
	AcceptanceIntentPersisted     AcceptanceCheckpoint = "intent_persisted"
	AcceptanceWinnerClaimed       AcceptanceCheckpoint = "winner_claimed"
	AcceptanceQuoteBindingPending AcceptanceCheckpoint = "quote_binding_pending"
	AcceptanceQuoteBound          AcceptanceCheckpoint = "quote_bound"
	AcceptanceJobBindingPending   AcceptanceCheckpoint = "job_binding_pending"
	AcceptanceJobBound            AcceptanceCheckpoint = "job_bound"
	AcceptanceCompleted           AcceptanceCheckpoint = "completed"
	// AcceptanceFailed is terminal but NOT successful: a DEFINITIVE (not
	// ambiguous/retryable) rejection was observed -- e.g. the proposal's
	// Capability version is stale and QuoteService refuses to quote it.
	// Reaching Failed reopens the OpenTask (back to Open, AcceptedProposalID
	// cleared) rather than leaving it permanently Accepted with no path
	// forward -- see OpenTaskStatus.Accepted's doc comment.
	AcceptanceFailed AcceptanceCheckpoint = "failed"
	// AcceptanceReconciling marks that the last attempted step's remote
	// outcome is uncertain (lost response, process crash mid-step) --
	// exactly domain.ExecutionSignerOperation's CheckpointReconciling.
	// Never a terminal state; a subsequent attempt (retry or the
	// reconciler) must resolve it to a deterministic checkpoint.
	AcceptanceReconciling AcceptanceCheckpoint = "reconciling"
)

// Terminal reports whether checkpoint is a final state for this operation --
// Completed or Failed. Reconciling is deliberately NOT terminal.
func (c AcceptanceCheckpoint) Terminal() bool {
	return c == AcceptanceCompleted || c == AcceptanceFailed
}

// AcceptanceOperation is the durable journal record for one OpenTask's
// winner-selection -> Quote -> Job binding sequence. atos-protocol has no
// async/pending concept for QuoteService.Create/JobService.CreateJob any
// more than tos-protocol's signer RPCs did for Phase 3B (they are each
// synchronous, atomic-or-nothing via the shared store.Idempotency
// primitive) -- all crash recovery for the MULTI-STEP sequence across them
// is this operation's responsibility, exactly like
// domain.ExecutionSignerOperation's role for Phase 3B's
// authorize-then-revoke rotation sequence.
type AcceptanceOperation struct {
	ID         string `json:"id"`
	TaskID     string `json:"task_id"`
	ProposalID string `json:"proposal_id"`
	// PrincipalID/ProviderID/CapabilityID/CapabilityVersion are captured
	// once, at winner-claim time, from the OpenTask and winning
	// OpenTaskProposal -- never re-read from either at a later checkpoint,
	// so a concurrent Cancel or a later Capability update cannot silently
	// change what this operation is committed to binding.
	PrincipalID       string `json:"principal_id"`
	ProviderID        string `json:"provider_id"`
	CapabilityID      string `json:"capability_id"`
	CapabilityVersion string `json:"capability_version"`

	Checkpoint AcceptanceCheckpoint `json:"checkpoint"`
	// IdempotencyKey is the CALLER-supplied key from the Accept
	// request/tool call -- opens/resumes this operation record itself
	// (OpenAcceptanceOperation), exactly like every other
	// idempotency_key in this codebase. NewQuoteIdempotencyKey/
	// NewJobIdempotencyKey below are separate, internally-derived keys
	// for the two downstream calls this operation drives -- see their own
	// doc comments for why they must NOT simply reuse this field.
	IdempotencyKey string `json:"idempotency_key"`

	QuoteID string `json:"quote_id,omitempty"`
	JobID   string `json:"job_id,omitempty"`

	FailureReason string     `json:"failure_reason,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
}

// NewQuoteIdempotencyKey/NewJobIdempotencyKey are stable, deterministically
// derived from this operation's own ID (never from the caller's Accept
// IdempotencyKey, never regenerated per attempt) -- reusing op.ID itself
// guarantees they are identical across every retry/reconcile pass of this
// SAME operation, which is exactly what QuoteService.Create/
// JobService.CreateJob's own idempotency needs: a crash between "Quote
// created" and "operation checkpoint advanced to quote_bound" must retry
// with the IDENTICAL key so the retry resumes the same Quote instead of
// minting a second one. Suffixed (not reused verbatim as one key for both)
// so a Quote-scoped and a Job-scoped idempotency record can never collide
// even though both are opened under the same PrincipalID.
func (op AcceptanceOperation) NewQuoteIdempotencyKey() string { return op.ID + ":quote" }
func (op AcceptanceOperation) NewJobIdempotencyKey() string   { return op.ID + ":job" }
