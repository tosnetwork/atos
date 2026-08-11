package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/financial"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
)

const (
	defaultDisputeReconcileInterval   = 20 * time.Second
	defaultDisputeReconcileStaleAfter = 30 * time.Second
	defaultDisputeReconcileBatch      = 100
)

// disputePolicyWindows maps a Quote's frozen DisputePolicyHash to the
// dispute window it committed to, so resolution always applies the policy
// the disputed Quote actually committed to -- never whatever ATOS's
// current global policy happens to be by the time a dispute is opened. An
// unrecognized hash (a Quote that committed to some other/future policy)
// fails closed rather than guessing a window.
var disputePolicyWindows = map[string]time.Duration{
	termsHash("atos-dispute-policy", "v0.2", "72h"): 72 * time.Hour,
}

// DisputeService owns the Managed-mode dispute workflow: opening a dispute
// against a completed, settled Job; routing it through human review; and
// durably, idempotently carrying out the economic consequence of its
// resolution. It never modifies the Job's Quote, ExecutionReceipt, or
// BillingSnapshot -- see domain.Dispute's doc comment.
type DisputeService struct {
	store     store.Store
	jobs      *JobService
	earnings  *EarningsService
	accounts  *AccountService
	artifacts *ArtifactService
	financial financial.ATOSFinancialAdapter
}

func (s *DisputeService) WithFinancialAuthority(adapter financial.ATOSFinancialAdapter) *DisputeService {
	s.financial = adapter
	return s
}

func NewDisputeService(st store.Store, jobs *JobService, earnings *EarningsService, accounts *AccountService, artifacts *ArtifactService) *DisputeService {
	return &DisputeService{store: st, jobs: jobs, earnings: earnings, accounts: accounts, artifacts: artifacts}
}

type OpenDisputeInput struct {
	PrincipalID    string
	JobID          string
	Reason         string
	Description    string
	Evidence       []domain.DisputeEvidence
	IdempotencyKey string
}

// Open validates and, if eligible, durably opens a dispute against JobID on
// behalf of PrincipalID. Every economic fact recorded on the resulting
// Dispute (ProviderID, amounts, IDs, ...) is resolved internally from the
// Job/Quote/ExecutionReceipt/BillingSnapshot/ProviderEarning -- never
// accepted as caller input -- and opening atomically freezes (or
// correctly defers/records the inability to freeze) the disputed
// ProviderEarning in the same transaction; see store.Disputes.OpenDispute.
//
// Idempotency is reserved BEFORE any dynamic eligibility check (dispute
// window, billing snapshot lookup, evidence accessibility) runs: a
// successful replay of the same (principal, idempotency_key) always
// returns the original result, even if evaluated "now" the dispute window
// would have expired or an evidence Artifact has since expired. Only a
// genuinely new request (unreserved key) performs those checks.
func (s *DisputeService) Open(ctx context.Context, in OpenDisputeInput) (domain.Dispute, error) {
	if in.PrincipalID == "" {
		return domain.Dispute{}, domain.NewError(domain.ErrAuthenticationRequired, "principal is required", false)
	}
	if in.JobID == "" || in.Reason == "" {
		return domain.Dispute{}, domain.NewError(domain.ErrValidationFailed, "job_id and reason are required", false)
	}
	if in.IdempotencyKey == "" {
		return domain.Dispute{}, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}

	now := time.Now().UTC()
	requestHash := hashRequest("open-dispute", in.JobID, in.Reason, in.Description, in.Evidence)
	rec, reserved, err := s.store.Reserve(ctx, in.PrincipalID, in.IdempotencyKey, requestHash, now.Add(idempotencyLease))
	if err != nil {
		return domain.Dispute{}, err
	}
	if !reserved {
		if rec.RequestHash != requestHash {
			return domain.Dispute{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different dispute request", false)
		}
		if rec.Status != store.IdempotencyCompleted {
			return domain.Dispute{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute open is still in progress; retry shortly", true)
		}
		return s.store.GetDispute(ctx, rec.ResponseKey)
	}
	committed := false
	defer func() {
		if !committed {
			_ = s.store.Release(context.Background(), in.PrincipalID, in.IdempotencyKey)
		}
	}()

	// Recover a process crash after OpenDispute committed but before Finish
	// was called, mirroring JobService.submit's JobByIdempotencyKey
	// recovery: the unique (principal_id, job_id) dispute identity makes
	// this unambiguous.
	if existing, lookupErr := s.store.DisputeByIdempotencyKey(ctx, in.PrincipalID, in.IdempotencyKey); lookupErr == nil {
		if err := s.ensureFinancialDisputeHold(ctx, existing); err != nil {
			return domain.Dispute{}, err
		}
		if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, existing.ID); err != nil {
			return domain.Dispute{}, err
		}
		committed = true
		return existing, nil
	} else if lookupErr != store.ErrNotFound {
		return domain.Dispute{}, lookupErr
	}

	// Everything from here on is a genuinely new request: dynamic
	// eligibility checks run now, never before the idempotency reservation
	// above, so a replay of an already-successful request can never be
	// re-evaluated against (and rejected by) state that has since changed.
	job, err := s.jobs.Get(ctx, in.JobID)
	if err != nil {
		return domain.Dispute{}, err
	}
	if job.PrincipalID != in.PrincipalID {
		return domain.Dispute{}, domain.NewError(domain.ErrPermissionDenied, "not the owning principal of this job", false)
	}
	if job.TrustMode != domain.TrustModeManaged {
		return domain.Dispute{}, domain.NewError(domain.ErrDisputeNotEligible, "only Managed-mode jobs may be disputed", false)
	}
	if job.State != domain.JobCompleted || job.EconomicState != domain.EconomicSettled {
		return domain.Dispute{}, domain.NewError(domain.ErrDisputeNotEligible, "job is not a completed, settled Managed job", false)
	}
	if job.CompletedAt == nil {
		return domain.Dispute{}, domain.NewError(domain.ErrDisputeNotEligible, "job has no completion timestamp", false)
	}
	if job.ExecutionReceipt == nil {
		return domain.Dispute{}, domain.NewError(domain.ErrDisputeNotEligible, "job has no execution receipt", false)
	}

	quote, err := s.store.GetQuote(ctx, job.QuoteID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Dispute{}, domain.NewError(domain.ErrDisputeNotEligible, "quote not found for this job", false)
		}
		return domain.Dispute{}, err
	}
	if quote.ID != job.QuoteID || quote.ProviderID != job.ProviderID || quote.CapabilityID != job.CapabilityID {
		return domain.Dispute{}, domain.NewError(domain.ErrDisputeNotEligible, "quote binding does not match job", false)
	}
	window, ok := disputePolicyWindows[quote.DisputePolicyHash]
	if !ok {
		return domain.Dispute{}, domain.NewError(domain.ErrDisputeNotEligible, "job's quote committed to an unrecognized dispute policy", false)
	}
	if !now.Before(job.CompletedAt.Add(window)) {
		return domain.Dispute{}, domain.NewError(domain.ErrDisputeWindowExpired, "the dispute window for this job has expired", false)
	}

	snap, err := s.earnings.BillingSnapshotForJob(ctx, job.ID)
	if err != nil {
		return domain.Dispute{}, err
	}
	if snap.JobID != job.ID || snap.ProviderID != job.ProviderID || snap.QuoteID != job.QuoteID {
		return domain.Dispute{}, domain.NewError(domain.ErrDisputeNotEligible, "billing snapshot binding does not match job", false)
	}
	if snap.ReceiptID != job.ExecutionReceipt.ID {
		return domain.Dispute{}, domain.NewError(domain.ErrDisputeNotEligible, "billing snapshot execution receipt binding does not match job", false)
	}

	// The durable settlement Receipt (distinct from the ExecutionReceipt
	// above) is what the disputed ProviderEarning's SettlementID must
	// equal -- see internal/service/economic_recovery.go's
	// RecordSettlement(ctx, billingSnapshot, settled.Receipt.ID) call.
	settlementReceipt, err := s.store.ReceiptByJob(ctx, job.ID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Dispute{}, domain.NewError(domain.ErrDisputeNotEligible, "settlement receipt not found for this job", false)
		}
		return domain.Dispute{}, err
	}

	evidence := make([]domain.DisputeEvidence, 0, len(in.Evidence))
	for _, e := range in.Evidence {
		if e.ArtifactID == "" {
			continue
		}
		if _, err := s.artifacts.Get(ctx, in.PrincipalID, e.ArtifactID); err != nil {
			return domain.Dispute{}, domain.NewError(domain.ErrValidationFailed, fmt.Sprintf("evidence artifact %s is not accessible: %s", e.ArtifactID, err.Error()), false)
		}
		evidence = append(evidence, domain.DisputeEvidence{ArtifactID: e.ArtifactID, Description: e.Description})
	}

	build := func(earning domain.ProviderEarning, earningExists bool) (domain.Dispute, domain.ProviderEarning, error) {
		// Full "all IDs bind consistently" validation against the
		// row-locked live earning: identity fields must trace back to
		// exactly this Job's Quote/Capability/ExecutionReceipt/settlement
		// Receipt, and the earning's amounts must still equal what the
		// BillingSnapshot actually computed -- never merely job_id.
		if !earningExists ||
			earning.JobID != job.ID ||
			earning.ProviderID != job.ProviderID ||
			earning.QuoteID != job.QuoteID ||
			earning.CapabilityID != job.CapabilityID ||
			earning.CapabilityVersion != job.CapabilityVersion ||
			earning.ReceiptID != snap.ReceiptID ||
			earning.SettlementID != settlementReceipt.ID ||
			earning.GrossAmount != snap.GrossCharge ||
			earning.GatewayFee != snap.GatewayFee ||
			earning.NetAmount != snap.ProviderGross {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrDisputeNotEligible, "earning binding does not match job/billing snapshot/settlement receipt", false)
		}
		dispute := domain.Dispute{
			ID: "dispute_" + uuid.NewString(), PrincipalID: job.PrincipalID, ProviderID: job.ProviderID,
			JobID: job.ID, QuoteID: job.QuoteID, CapabilityID: job.CapabilityID,
			ReceiptID: snap.ReceiptID, SettlementID: earning.SettlementID, EarningID: earning.ID,
			ChargedAmount: snap.GrossCharge, OriginalRefundAmount: snap.PrincipalRefund,
			Reason: in.Reason, Description: in.Description, Evidence: evidence, IdempotencyKey: in.IdempotencyKey,
			DisputePolicyHash: quote.DisputePolicyHash,
			ReviewStatus:      domain.DisputeOpened,
			OpenedAt:          now, UpdatedAt: now,
		}
		nextEarning := earning
		// Which of the three economic branches applies is decided here,
		// against the row-locked live status the store passes in -- never
		// a status read outside this transaction -- so a concurrent
		// PayoutSweep transition and this decision always serialize
		// through the same row lock (see domain.DisputeEconomicState's
		// doc comments for what each branch means).
		switch earning.Status {
		case domain.EarningMaturing, domain.EarningAvailable:
			nextEarning.Status = domain.EarningFrozen
			dispute.EconomicState = domain.DisputeEconomicFrozen
		case domain.EarningPayoutPending:
			// The in-flight payout attempt is allowed to resolve on its
			// own (Query/recover to Paid, or fail back to Available), but
			// the durable hold set here durably marks this earning as
			// disputed so that if it does fail back to Available,
			// beginPayoutUnderLock refuses to start a *new* payout intent
			// before the dispute reconciler gets a chance to freeze it
			// late -- see domain.ProviderEarning.DisputeHoldID's doc
			// comment.
			nextEarning.DisputeHoldID = dispute.ID
			dispute.EconomicState = domain.DisputeEconomicPendingPayoutResolution
		case domain.EarningPaid:
			dispute.EconomicState = domain.DisputeEconomicPaid
		default:
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrDisputeNotEligible, "earning is not in a disputable state: "+string(earning.Status), false)
		}
		return dispute, nextEarning, nil
	}
	dispute, _, _, err := s.store.OpenDispute(ctx, job.ID, settlementReceipt.ID, build)
	if err != nil {
		return domain.Dispute{}, err
	}
	if err := s.ensureFinancialDisputeHold(ctx, dispute); err != nil {
		return domain.Dispute{}, err
	}
	if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, dispute.ID); err != nil {
		return domain.Dispute{}, err
	}
	committed = true
	return dispute, nil
}

func (s *DisputeService) ensureFinancialDisputeHold(ctx context.Context, dispute domain.Dispute) error {
	if s.financial == nil || dispute.EconomicState != domain.DisputeEconomicFrozen {
		return nil
	}
	job, err := s.store.GetJob(ctx, dispute.JobID)
	if err != nil {
		return err
	}
	quote, err := s.store.GetQuote(ctx, dispute.QuoteID)
	if err != nil {
		return err
	}
	earning, err := s.store.EarningByJob(ctx, dispute.JobID)
	if err != nil {
		return err
	}
	amount, err := money.Parse(earning.NetAmount.Amount, earning.NetAmount.Currency, accountDecimals)
	if err != nil {
		return err
	}
	_, err = s.financial.HoldDispute(ctx, financial.TransferRequest{
		EventType: financial.EventDisputeHold, IdempotencyIdentity: "dispute:" + dispute.ID + ":hold:v1",
		Identities: financialIdentities(job, quote, "billing_"+job.ID, dispute.ReceiptID, dispute.SettlementID, dispute.EarningID, dispute.ID, ""),
		Asset:      amount.Currency, Decimals: accountDecimals, AtomicAmount: amount.Minor.String(),
		SourceCode: financial.ProviderPayable, SourceOwnerID: dispute.ProviderID,
		DestinationCode: financial.ProviderDisputed, DestinationOwnerID: dispute.ProviderID,
	})
	return err
}

// Review transitions a dispute from Opened to UnderReview, claiming it
// exclusively for reviewerID: once claimed, no other reviewer may claim or
// resolve it (see Resolve's ReviewerID check) -- only the original
// claimant may act on it, or claim again idempotently. reviewerID must be
// neither the dispute's principal nor its provider -- callers MUST pass an
// authenticated identity here, never a caller-supplied one.
func (s *DisputeService) Review(ctx context.Context, disputeID, reviewerID string) (domain.Dispute, error) {
	if reviewerID == "" {
		return domain.Dispute{}, domain.NewError(domain.ErrAuthenticationRequired, "reviewer is required", false)
	}
	return s.store.UpdateDispute(ctx, disputeID, func(d domain.Dispute, exists bool) (domain.Dispute, error) {
		if !exists {
			return domain.Dispute{}, domain.NewError(domain.ErrNotFound, "dispute not found", false)
		}
		if d.PrincipalID == reviewerID || d.ProviderID == reviewerID {
			return domain.Dispute{}, domain.NewError(domain.ErrPermissionDenied, "a party to the dispute cannot review it", false)
		}
		if d.ReviewStatus == domain.DisputeUnderReview {
			if d.ReviewerID == reviewerID {
				return d, nil
			}
			return domain.Dispute{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute is already claimed by another reviewer", false)
		}
		if d.ReviewStatus != domain.DisputeOpened {
			return domain.Dispute{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute is not in a reviewable state", false)
		}
		now := time.Now().UTC()
		d.ReviewStatus = domain.DisputeUnderReview
		d.ReviewerID = reviewerID
		d.UnderReviewAt = &now
		d.UpdatedAt = now
		return d, nil
	})
}

type ResolveDisputeInput struct {
	DisputeID      string
	ReviewerID     string
	Outcome        domain.DisputeOutcome
	ReasonRejected string
}

// Resolve durably decides a dispute and carries out its economic
// consequence, all within a single store.Disputes.ResolveDispute
// transaction (dispute + earning + account row-locked together) -- there
// is no intermediate durable state between "frozen" and "refunded" for a
// principal-win, because that transition can never be observed partially
// applied:
//   - DisputeOutcomePrincipal against a Frozen earning: earning reversed,
//     principal credited exactly once, dispute reaches its terminal
//     checkpoint -- all three in the same transaction.
//   - DisputeOutcomeProvider / DisputeOutcomeRejected against a Frozen
//     earning: released back to Maturing or Available (whichever the
//     earning's original, untouched MaturesAt now implies -- being
//     disputed must never let a provider skip ahead of its normal
//     maturation schedule) in the same transaction as the terminal
//     checkpoint, account left untouched, and the dispute hold cleared.
//   - Any outcome against an earning already known Paid: no earning or
//     account mutation is possible (the money already left ATOS). A
//     principal-win records ClawbackRequired; provider-win/rejected
//     require no further action.
//   - Against a still-PendingPayoutResolution earning: rejected as not yet
//     eligible -- the payout ambiguity must resolve first.
func (s *DisputeService) Resolve(ctx context.Context, in ResolveDisputeInput) (domain.Dispute, error) {
	if in.ReviewerID == "" {
		return domain.Dispute{}, domain.NewError(domain.ErrAuthenticationRequired, "reviewer is required", false)
	}
	switch in.Outcome {
	case domain.DisputeOutcomePrincipal, domain.DisputeOutcomeProvider, domain.DisputeOutcomeRejected:
	default:
		return domain.Dispute{}, domain.NewError(domain.ErrValidationFailed, "outcome must be principal, provider, or rejected", false)
	}

	// principalID is needed up front to key the account lock; a plain read
	// is safe here since Dispute.PrincipalID is immutable after creation
	// (see domain.Dispute's doc comment) -- the row-locked read inside
	// ResolveDispute is what makes every subsequent decision authoritative.
	seed, err := s.store.GetDispute(ctx, in.DisputeID)
	if err != nil {
		return domain.Dispute{}, err
	}
	seed, err = s.reserveResolutionIntent(ctx, seed.ID, in)
	if err != nil {
		return domain.Dispute{}, err
	}
	if err := s.applyFinancialDisputeResolution(ctx, seed, in.Outcome); err != nil {
		return domain.Dispute{}, err
	}

	dispute, _, _, err := s.store.ResolveDispute(ctx, in.DisputeID, seed.PrincipalID, s.accounts.defaultAccount(seed.PrincipalID),
		func(d domain.Dispute, e domain.ProviderEarning, earningExists bool, a domain.Account, accountExists bool) (domain.Dispute, domain.ProviderEarning, domain.Account, error) {
			if d.PrincipalID == in.ReviewerID || d.ProviderID == in.ReviewerID {
				return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrPermissionDenied, "a party to the dispute cannot resolve it", false)
			}
			// A dispute claimed via Review belongs exclusively to that
			// reviewer for the rest of its life -- an unclaimed dispute
			// (ReviewerID == "", only reachable by resolving Rejected
			// directly from Opened) may be resolved by anyone with
			// disputes:review, which implicitly claims it here.
			if d.ReviewerID != "" && d.ReviewerID != in.ReviewerID {
				return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute is claimed by a different reviewer", false)
			}
			if d.PendingOutcome != "" && (d.PendingOutcome != in.Outcome || d.PendingReviewerID != in.ReviewerID) {
				return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute has a different durable resolution intent", false)
			}
			if d.ReviewStatus.Terminal() {
				if d.Outcome == in.Outcome {
					return d, e, a, nil
				}
				return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute already resolved with a different outcome", false)
			}
			if d.ReviewStatus != domain.DisputeUnderReview && d.ReviewStatus != domain.DisputeOpened {
				return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute is not in a resolvable state", false)
			}
			if in.Outcome != domain.DisputeOutcomeRejected && d.ReviewStatus != domain.DisputeUnderReview {
				return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute must be under review before it can be decided for a party", false)
			}

			now := time.Now().UTC()
			d.Outcome = in.Outcome
			d.ReviewerID = in.ReviewerID
			if in.Outcome == domain.DisputeOutcomeRejected {
				d.ReasonRejected = in.ReasonRejected
			}

			switch d.EconomicState {
			case domain.DisputeEconomicPaid:
				// The money already left ATOS; no automated reversal or
				// credit is possible -- see
				// domain.DisputeEconomicClawbackRequired.
				switch in.Outcome {
				case domain.DisputeOutcomePrincipal:
					d.EconomicState = domain.DisputeEconomicClawbackRequired
					d.ReviewStatus = domain.DisputeResolvedForPrincipal
				case domain.DisputeOutcomeProvider:
					d.ReviewStatus = domain.DisputeResolvedForProvider
				case domain.DisputeOutcomeRejected:
					d.ReviewStatus = domain.DisputeRejected
				}
				d.ResolvedAt = &now
				d.UpdatedAt = now
				if earningExists {
					e.DisputeHoldID = ""
				}
				return d, e, a, nil
			case domain.DisputeEconomicFrozen:
				if !earningExists || e.Status != domain.EarningFrozen {
					return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrSettlementFailed, "disputed earning is not in the expected frozen state", false)
				}
				e.DisputeHoldID = ""
				switch in.Outcome {
				case domain.DisputeOutcomePrincipal:
					nextAccount := a
					if s.financial == nil {
						var err error
						nextAccount, err = s.accounts.creditAccountValue(a, d.ChargedAmount.Amount, d.ChargedAmount.Currency)
						if err != nil {
							return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
						}
					} else {
						var err error
						nextAccount, err = s.accounts.creditPolicyValue(a, d.ChargedAmount.Amount, d.ChargedAmount.Currency)
						if err != nil {
							return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, err
						}
					}
					e.Status = domain.EarningReversed
					d.EconomicState = domain.DisputeEconomicRefunded
					d.ReviewStatus = domain.DisputeResolvedForPrincipal
					d.ResolvedAt = &now
					d.UpdatedAt = now
					return d, e, nextAccount, nil
				case domain.DisputeOutcomeProvider, domain.DisputeOutcomeRejected:
					// Being disputed must not let a provider skip ahead of
					// the earning's original maturation schedule: release
					// back to Maturing (unchanged MaturesAt) if maturity
					// genuinely has not passed yet, and only to Available
					// once it has -- exactly the timing the earning would
					// have followed had it never been disputed at all.
					if now.Before(e.MaturesAt) {
						e.Status = domain.EarningMaturing
						e.AvailableAt = nil
					} else {
						e.Status = domain.EarningAvailable
						if e.AvailableAt == nil {
							e.AvailableAt = &now
						}
					}
					d.EconomicState = domain.DisputeEconomicReleased
					if in.Outcome == domain.DisputeOutcomeProvider {
						d.ReviewStatus = domain.DisputeResolvedForProvider
					} else {
						d.ReviewStatus = domain.DisputeRejected
					}
					d.ResolvedAt = &now
					d.UpdatedAt = now
					return d, e, a, nil
				}
			}
			return domain.Dispute{}, domain.ProviderEarning{}, domain.Account{}, domain.NewError(domain.ErrDisputeNotEligible, "dispute economic recovery is still pending payout resolution", true)
		})
	if err != nil {
		return domain.Dispute{}, err
	}
	return dispute, nil
}

func (s *DisputeService) reserveResolutionIntent(ctx context.Context, disputeID string, in ResolveDisputeInput) (domain.Dispute, error) {
	return s.store.UpdateDispute(ctx, disputeID, func(d domain.Dispute, exists bool) (domain.Dispute, error) {
		if !exists {
			return domain.Dispute{}, store.ErrNotFound
		}
		if d.ReviewStatus.Terminal() {
			if d.Outcome == in.Outcome {
				return d, nil
			}
			return domain.Dispute{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute already resolved with a different outcome", false)
		}
		if d.PrincipalID == in.ReviewerID || d.ProviderID == in.ReviewerID {
			return domain.Dispute{}, domain.NewError(domain.ErrPermissionDenied, "a party to the dispute cannot resolve it", false)
		}
		if d.ReviewerID != "" && d.ReviewerID != in.ReviewerID {
			return domain.Dispute{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute is claimed by a different reviewer", false)
		}
		if in.Outcome != domain.DisputeOutcomeRejected && d.ReviewStatus != domain.DisputeUnderReview {
			return domain.Dispute{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute must be under review before it can be decided for a party", false)
		}
		if d.PendingOutcome != "" && (d.PendingOutcome != in.Outcome || d.PendingReviewerID != in.ReviewerID) {
			return domain.Dispute{}, domain.NewError(domain.ErrDisputeInvalidTransition, "a different resolution intent is already durable", false)
		}
		d.PendingOutcome = in.Outcome
		d.PendingReviewerID = in.ReviewerID
		d.UpdatedAt = time.Now().UTC()
		return d, nil
	})
}

func (s *DisputeService) applyFinancialDisputeResolution(ctx context.Context, dispute domain.Dispute, outcome domain.DisputeOutcome) error {
	if s.financial == nil || dispute.ReviewStatus.Terminal() || dispute.EconomicState != domain.DisputeEconomicFrozen {
		return nil
	}
	job, err := s.store.GetJob(ctx, dispute.JobID)
	if err != nil {
		return err
	}
	quote, err := s.store.GetQuote(ctx, dispute.QuoteID)
	if err != nil {
		return err
	}
	earning, err := s.store.EarningByJob(ctx, dispute.JobID)
	if err != nil {
		return err
	}
	snapshot, err := s.store.BillingSnapshotByJob(ctx, dispute.JobID)
	if err != nil {
		return err
	}
	identities := financialIdentities(job, quote, "billing_"+job.ID, dispute.ReceiptID, dispute.SettlementID, dispute.EarningID, dispute.ID, "")
	providerAmount, err := money.Parse(earning.NetAmount.Amount, earning.NetAmount.Currency, accountDecimals)
	if err != nil {
		return err
	}
	if outcome == domain.DisputeOutcomeProvider || outcome == domain.DisputeOutcomeRejected {
		_, err = s.financial.ReleaseDispute(ctx, financial.TransferRequest{
			EventType: financial.EventDisputeRelease, IdempotencyIdentity: "dispute:" + dispute.ID + ":release:v1",
			Identities: identities, Asset: providerAmount.Currency, Decimals: accountDecimals, AtomicAmount: providerAmount.Minor.String(),
			SourceCode: financial.ProviderDisputed, SourceOwnerID: dispute.ProviderID,
			DestinationCode: financial.ProviderPayable, DestinationOwnerID: dispute.ProviderID,
		})
		return err
	}
	_, err = s.financial.FullRefund(ctx, financial.TransferRequest{
		EventType: financial.EventFullRefund, IdempotencyIdentity: "refund:" + dispute.ID + ":provider-full:v1",
		Identities: identities, Asset: providerAmount.Currency, Decimals: accountDecimals, AtomicAmount: providerAmount.Minor.String(),
		SourceCode: financial.ProviderDisputed, SourceOwnerID: dispute.ProviderID,
		DestinationCode: financial.PrincipalAvailable, DestinationOwnerID: dispute.PrincipalID,
	})
	if err != nil {
		return err
	}
	fee, err := money.Parse(snapshot.GatewayFee.Amount, snapshot.GatewayFee.Currency, accountDecimals)
	if err != nil || fee.IsZero() {
		return err
	}
	_, err = s.financial.FundGatewayRefund(ctx, financial.TransferRequest{
		EventType: financial.EventGatewayRefundFund, IdempotencyIdentity: "refund:" + dispute.ID + ":gateway-fee-fund:v1",
		Identities: identities, Asset: fee.Currency, Decimals: accountDecimals, AtomicAmount: fee.Minor.String(),
		SourceCode: financial.GatewayFeeRevenue, SourceOwnerID: "_",
		DestinationCode: financial.GatewayRefundLiability, DestinationOwnerID: "_",
	})
	if err != nil {
		return err
	}
	_, err = s.financial.PayGatewayRefund(ctx, financial.TransferRequest{
		EventType: financial.EventGatewayRefundPay, IdempotencyIdentity: "refund:" + dispute.ID + ":gateway-fee-pay:v1",
		Identities: identities, Asset: fee.Currency, Decimals: accountDecimals, AtomicAmount: fee.Minor.String(),
		SourceCode: financial.GatewayRefundLiability, SourceOwnerID: "_",
		DestinationCode: financial.PrincipalAvailable, DestinationOwnerID: dispute.PrincipalID,
	})
	return err
}

func (s *DisputeService) Get(ctx context.Context, id string) (domain.Dispute, error) {
	d, err := s.store.GetDispute(ctx, id)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Dispute{}, domain.NewError(domain.ErrNotFound, "dispute not found", false)
		}
		return domain.Dispute{}, err
	}
	return d, nil
}

// GetWithEarning reads a dispute together with its earning as a single
// atomic snapshot -- unlike calling Get and EarningsService.Get separately
// (two independent reads with no atomicity across them), a caller here can
// never observe a combination ResolveDispute's own atomic write never
// produced (e.g. the dispute already resolved_for_principal but the
// earning not yet Reversed). Use this whenever both are needed together;
// Get remains for callers that only need the dispute.
func (s *DisputeService) GetWithEarning(ctx context.Context, id string) (domain.Dispute, domain.ProviderEarning, bool, error) {
	d, e, earningExists, err := s.store.GetDisputeWithEarning(ctx, id)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Dispute{}, domain.ProviderEarning{}, false, domain.NewError(domain.ErrNotFound, "dispute not found", false)
		}
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}
	return d, e, earningExists, nil
}

func (s *DisputeService) ListByPrincipal(ctx context.Context, principalID string) ([]domain.Dispute, error) {
	return s.store.DisputesByPrincipal(ctx, principalID)
}

func (s *DisputeService) ListByProvider(ctx context.Context, providerID string) ([]domain.Dispute, error) {
	return s.store.DisputesByProvider(ctx, providerID)
}

func (s *DisputeService) ListUnderReview(ctx context.Context, limit int) ([]domain.Dispute, error) {
	return s.store.DisputesUnderReview(ctx, limit)
}

// ReconcileDispute drives one dispute's economic recovery forward from
// whatever checkpoint it is durably parked at. Safe to call repeatedly.
// PendingPayoutResolution is the only non-terminal EconomicState that can
// persist between calls -- Resolve's principal-win path has no
// intermediate checkpoint to recover (see ResolveDispute).
func (s *DisputeService) ReconcileDispute(ctx context.Context, disputeID string) (domain.Dispute, error) {
	dispute, err := s.store.GetDispute(ctx, disputeID)
	if err != nil {
		return domain.Dispute{}, err
	}
	if dispute.EconomicState == domain.DisputeEconomicPendingPayoutResolution {
		return s.reconcilePendingPayout(ctx, dispute)
	}
	return dispute, nil
}

// reconcilePendingPayout re-checks the disputed earning's own status,
// which only the existing EarningsService payout state machine may
// advance while a dispute sits in PendingPayoutResolution: EarningPaid
// means the external payout became real first (-> DisputeEconomicPaid,
// clawback path only if later upheld for the principal);
// EarningAvailable/EarningMaturing means the payout attempt did not move
// funds (e.g. StatusRejected) and it is now safe to freeze late; anything
// else (still EarningPayoutPending) means the outcome is still ambiguous
// and this dispute is left exactly where it is for the next sweep.
func (s *DisputeService) reconcilePendingPayout(ctx context.Context, dispute domain.Dispute) (domain.Dispute, error) {
	earning, err := s.store.EarningByJob(ctx, dispute.JobID)
	if err != nil {
		return domain.Dispute{}, err
	}
	switch earning.Status {
	case domain.EarningPaid:
		return s.store.UpdateDispute(ctx, dispute.ID, func(d domain.Dispute, exists bool) (domain.Dispute, error) {
			if !exists {
				return domain.Dispute{}, store.ErrNotFound
			}
			if d.EconomicState != domain.DisputeEconomicPendingPayoutResolution {
				return d, nil
			}
			d.EconomicState = domain.DisputeEconomicPaid
			d.UpdatedAt = time.Now().UTC()
			return d, nil
		})
	case domain.EarningAvailable, domain.EarningMaturing:
		updated, _, err := s.store.UpdateDisputeAndEarning(ctx, dispute.ID, func(d domain.Dispute, e domain.ProviderEarning, earningExists bool) (domain.Dispute, domain.ProviderEarning, error) {
			if d.EconomicState != domain.DisputeEconomicPendingPayoutResolution {
				return d, e, nil
			}
			if !earningExists || (e.Status != domain.EarningAvailable && e.Status != domain.EarningMaturing) {
				// Status moved again since the outer read (e.g. back into
				// payout_pending, or paid) -- leave this dispute exactly
				// where it is; the next sweep re-evaluates from scratch.
				return d, e, nil
			}
			e.Status = domain.EarningFrozen
			d.EconomicState = domain.DisputeEconomicFrozen
			d.UpdatedAt = time.Now().UTC()
			return d, e, nil
		})
		return updated, err
	default:
		return dispute, nil
	}
}

func (s *DisputeService) ReconcileDisputes(ctx context.Context, updatedBefore time.Time, limit int) (int, error) {
	disputes, err := s.store.DisputesForRecovery(ctx, updatedBefore, limit)
	if err != nil {
		return 0, err
	}
	var joined error
	for _, d := range disputes {
		if _, err := s.ReconcileDispute(ctx, d.ID); err != nil {
			joined = errors.Join(joined, fmt.Errorf("reconcile dispute %s: %w", d.ID, err))
		}
	}
	return len(disputes), joined
}

// RunReconciler periodically drives ReconcileDisputes, mirroring
// JobService/EarningsService's ticker-driven reconciler pattern.
func (s *DisputeService) RunReconciler(ctx context.Context, interval, staleAfter time.Duration, limit int, report func(error)) {
	if interval <= 0 {
		interval = defaultDisputeReconcileInterval
	}
	if staleAfter <= 0 {
		staleAfter = defaultDisputeReconcileStaleAfter
	}
	if limit <= 0 {
		limit = defaultDisputeReconcileBatch
	}
	sweep := func() {
		if _, err := s.ReconcileDisputes(ctx, time.Now().UTC().Add(-staleAfter), limit); err != nil && report != nil {
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
