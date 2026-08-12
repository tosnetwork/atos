package domain

import "time"

// DisputeReviewStatus is the human-review lifecycle of a Dispute, kept
// deliberately separate from DisputeEconomicState (see that type's doc
// comment): a reviewer's determination and the durable economic recovery
// required to carry it out are two different axes that do not always
// complete in lockstep -- e.g. a dispute can be conclusively decided for
// the principal while the underlying earning was already paid out, in
// which case the review reaches a terminal outcome immediately but the
// economic recovery remains ClawbackRequired indefinitely.
type DisputeReviewStatus string

const (
	DisputeOpened               DisputeReviewStatus = "opened"
	DisputeUnderReview          DisputeReviewStatus = "under_review"
	DisputeResolvedForPrincipal DisputeReviewStatus = "resolved_for_principal"
	DisputeResolvedForProvider  DisputeReviewStatus = "resolved_for_provider"
	DisputeRejected             DisputeReviewStatus = "rejected"
)

// Terminal reports whether no further review transition is possible. A
// terminal review status does NOT imply the economic recovery is done —
// see EconomicState.Terminal.
func (s DisputeReviewStatus) Terminal() bool {
	switch s {
	case DisputeResolvedForPrincipal, DisputeResolvedForProvider, DisputeRejected:
		return true
	default:
		return false
	}
}

// DisputeEconomicState is the durable economic-recovery checkpoint for the
// ProviderEarning a Dispute references, mirroring Job.EconomicState's role
// in Phase 1/2B: each value is a crash-safe checkpoint a reconciler can
// resume from, not merely a status label.
type DisputeEconomicState string

const (
	// DisputeEconomicPendingPayoutResolution means the earning was
	// EarningPayoutPending at open time (or was driven there by a
	// concurrent PayoutSweep winning the race against OpenDispute): its
	// external payout outcome was ambiguous, so it was deliberately left
	// untouched rather than guessed at. The reconciler polls the earning's
	// own status (which only the existing EarningsService payout state
	// machine may advance) until it resolves to EarningPaid (-> DisputeEconomicPaid)
	// or back to EarningAvailable (-> DisputeEconomicFrozen, frozen late).
	DisputeEconomicPendingPayoutResolution DisputeEconomicState = "pending_payout_resolution"
	// DisputeEconomicFrozen means the earning was successfully transitioned
	// to EarningFrozen: safely held, unpaid, and cannot enter payout. This
	// is the only state a principal-win or provider-win/rejected
	// resolution may proceed from.
	DisputeEconomicFrozen DisputeEconomicState = "frozen"
	// DisputeEconomicPaid means the earning was already EarningPaid at
	// open time, or reached EarningPaid while pending_payout_resolution:
	// the money has already left ATOS. It is never transitioned to
	// DisputeEconomicFrozen. A principal-win resolution against a Paid
	// earning moves to DisputeEconomicClawbackRequired rather than
	// attempting any reversal.
	DisputeEconomicPaid DisputeEconomicState = "paid"
	// DisputeEconomicRefunded is terminal for a principal-win resolution:
	// the earning reversal and the principal's account credit are
	// committed together in one transaction (store.Disputes.ResolveDispute)
	// -- there is no intermediate durable checkpoint between "reversed" and
	// "credited" to model, because that transition can never be observed
	// partially applied.
	DisputeEconomicRefunded DisputeEconomicState = "refunded"
	// DisputeEconomicReleased is terminal for a provider-win or rejected
	// resolution: the earning has been returned to EarningAvailable (or
	// its lifecycle-appropriate equivalent) and is payout-eligible again.
	DisputeEconomicReleased DisputeEconomicState = "released"
	// DisputeEconomicClawbackRequired is terminal: the dispute was decided
	// for the principal, but the disputed earning was already paid before
	// (or discovered to be paid during) resolution. No automated reversal
	// or refund occurs — ATOS has no external clawback/refund rail today —
	// so this durably records that manual/external economic recovery is
	// required rather than reporting funds recovered when they were not.
	DisputeEconomicClawbackRequired DisputeEconomicState = "clawback_required"
	// Verified checkpoints describe canonical TaskEscrow recovery. They are
	// deliberately distinct from Managed earning/account states.
	DisputeEconomicVerifiedOpenPending       DisputeEconomicState = "verified_open_pending"
	DisputeEconomicVerifiedDisputed          DisputeEconomicState = "verified_disputed"
	DisputeEconomicVerifiedResolutionPending DisputeEconomicState = "verified_resolution_pending"
	DisputeEconomicVerifiedResolved          DisputeEconomicState = "verified_resolved"
)

// Terminal reports whether no further automated economic recovery action
// will be taken by the reconciler. ClawbackRequired is terminal in this
// sense even though the underlying money has not actually been recovered —
// see that constant's doc comment.
func (s DisputeEconomicState) Terminal() bool {
	switch s {
	case DisputeEconomicRefunded, DisputeEconomicReleased, DisputeEconomicClawbackRequired, DisputeEconomicVerifiedResolved:
		return true
	default:
		return false
	}
}

// DisputeOutcome records which way a dispute was decided, set durably at
// the start of resolution (before DisputeEconomicState necessarily reaches
// its terminal value) so recovery always knows which path to continue down
// even if it crashes before ReviewStatus itself reaches a terminal value.
type DisputeOutcome string

const (
	DisputeOutcomeNone      DisputeOutcome = ""
	DisputeOutcomePrincipal DisputeOutcome = "principal"
	DisputeOutcomeProvider  DisputeOutcome = "provider"
	DisputeOutcomeRejected  DisputeOutcome = "rejected"
)

// DisputeEvidence is a durable reference to a previously uploaded
// StoredArtifact, never an inline payload — see docs on evidence handling.
type DisputeEvidence struct {
	ArtifactID  string `json:"artifact_id"`
	Description string `json:"description,omitempty"`
}

// Dispute is a Managed-mode economic/legal workflow layered on top of an
// already-completed, already-settled Job. Opening or resolving one MUST
// NEVER modify the Job's Quote, ExecutionReceipt, or BillingSnapshot, or
// the Quote's TrustMode/ProofStatus — those remain immutable historical
// evidence; a Dispute only ever creates new state describing what happened
// to them, never rewrites what they say.
//
// Every economic fact captured here (ProviderID, amounts, ...) is resolved
// once, internally, from durable Job/Quote/Receipt/BillingSnapshot/Earning
// state at open time — never accepted as caller-supplied input — and is
// then immutable for the life of the dispute; see store.Disputes.
type Dispute struct {
	ID           string `json:"dispute_id"`
	PrincipalID  string `json:"-"`
	ProviderID   string `json:"provider_id"`
	JobID        string `json:"job_id"`
	QuoteID      string `json:"quote_id"`
	CapabilityID string `json:"capability_id"`
	ReceiptID    string `json:"execution_receipt_id"`
	SettlementID string `json:"settlement_id"`
	EarningID    string `json:"earning_id"`
	// ChargedAmount and OriginalRefundAmount are copied from the Job's
	// BillingSnapshot at open time (GrossCharge / PrincipalRefund) purely
	// for a self-contained audit record; they are never themselves
	// re-read or reinterpreted after that.
	ChargedAmount        Money             `json:"charged_amount"`
	OriginalRefundAmount Money             `json:"original_refund_amount"`
	Reason               string            `json:"reason"`
	Description          string            `json:"description,omitempty"`
	Evidence             []DisputeEvidence `json:"evidence,omitempty"`
	EvidenceDigests      []string          `json:"evidence_digests,omitempty"`
	IdempotencyKey       string            `json:"-"`

	ReviewStatus   DisputeReviewStatus  `json:"status"`
	EconomicState  DisputeEconomicState `json:"economic_recovery_state,omitempty"`
	Outcome        DisputeOutcome       `json:"outcome,omitempty"`
	ReviewerID     string               `json:"-"`
	ReasonRejected string               `json:"reason_rejected,omitempty"`
	// PendingOutcome/Reviewer make the human decision a durable intent before
	// any Blnk side effect. They prevent a different resolution from racing a
	// retry after the financial operation committed but the local outcome did not.
	PendingOutcome        DisputeOutcome `json:"-"`
	PendingReviewerID     string         `json:"-"`
	ResolutionRequestedAt *time.Time     `json:"-"`

	// DisputePolicyHash is copied from the disputed Quote at open time, so
	// resolution always applies the policy (dispute window, etc.) the
	// Quote actually committed to, never whatever the current global
	// policy happens to be later.
	DisputePolicyHash    string    `json:"-"`
	TrustMode            TrustMode `json:"trust_mode"`
	EscrowID             string    `json:"escrow_id,omitempty"`
	ReceiptDigest        string    `json:"receipt_digest,omitempty"`
	DisputeDigest        string    `json:"dispute_digest,omitempty"`
	DisputeRef           string    `json:"dispute_ref,omitempty"`
	DisputeCheckpoint    uint64    `json:"dispute_checkpoint,omitempty"`
	ResolutionDigest     string    `json:"resolution_digest,omitempty"`
	ResolutionRef        string    `json:"resolution_ref,omitempty"`
	ResolutionCheckpoint uint64    `json:"resolution_checkpoint,omitempty"`

	OpenedAt      time.Time  `json:"opened_at"`
	UnderReviewAt *time.Time `json:"under_review_at,omitempty"`
	ResolvedAt    *time.Time `json:"resolved_at,omitempty"`
	UpdatedAt     time.Time  `json:"-"`
}

// CanAdvanceProjection rejects stale-replica regression while allowing the
// orthogonal reviewer status to advance during a canonical Verified checkpoint.
func (d Dispute) CanAdvanceProjection(next Dispute) bool {
	if d.ReviewStatus.Terminal() && (next.ReviewStatus != d.ReviewStatus || next.Outcome != d.Outcome || next.EconomicState != d.EconomicState) {
		return false
	}
	if d.TrustMode != TrustModeVerified {
		return true
	}
	if next.TrustMode != d.TrustMode || next.DisputeCheckpoint < d.DisputeCheckpoint || next.ResolutionCheckpoint < d.ResolutionCheckpoint {
		return false
	}
	if d.DisputeDigest != "" && (next.DisputeDigest != d.DisputeDigest || next.DisputeRef != d.DisputeRef) {
		return false
	}
	if d.ResolutionDigest != "" && (next.ResolutionDigest != d.ResolutionDigest || next.ResolutionRef != d.ResolutionRef) {
		return false
	}
	if d.EconomicState == next.EconomicState {
		return true
	}
	switch d.EconomicState {
	case DisputeEconomicVerifiedOpenPending:
		return next.EconomicState == DisputeEconomicVerifiedDisputed
	case DisputeEconomicVerifiedDisputed:
		return next.EconomicState == DisputeEconomicVerifiedResolutionPending
	case DisputeEconomicVerifiedResolutionPending:
		return next.EconomicState == DisputeEconomicVerifiedResolved
	default:
		return false
	}
}
