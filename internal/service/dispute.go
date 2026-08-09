package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/domain"
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
// Job/Quote/BillingSnapshot/ProviderEarning -- never accepted as caller
// input -- and opening atomically freezes (or correctly defers/records the
// inability to freeze) the disputed ProviderEarning in the same
// transaction; see store.Disputes.OpenDispute.
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
	now := time.Now().UTC()
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

	requestHash := hashRequest("open-dispute", job.ID, in.Reason, in.Description, evidence)
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
		if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, existing.ID); err != nil {
			return domain.Dispute{}, err
		}
		committed = true
		return existing, nil
	} else if lookupErr != store.ErrNotFound {
		return domain.Dispute{}, lookupErr
	}

	build := func(earning domain.ProviderEarning, earningExists bool) (domain.Dispute, domain.ProviderEarning, error) {
		if !earningExists || earning.JobID != job.ID {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrDisputeNotEligible, "no matching provider earning found for this job's settlement", false)
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
			dispute.EconomicState = domain.DisputeEconomicPendingPayoutResolution
		case domain.EarningPaid:
			dispute.EconomicState = domain.DisputeEconomicPaid
		default:
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrDisputeNotEligible, "earning is not in a disputable state: "+string(earning.Status), false)
		}
		return dispute, nextEarning, nil
	}
	dispute, _, _, err := s.store.OpenDispute(ctx, job.ID, build)
	if err != nil {
		return domain.Dispute{}, err
	}
	if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, dispute.ID); err != nil {
		return domain.Dispute{}, err
	}
	committed = true
	return dispute, nil
}

// Review transitions a dispute from Opened to UnderReview, claiming it for
// reviewerID. reviewerID must be neither the dispute's principal nor its
// provider -- callers MUST pass an authenticated identity here, never a
// caller-supplied one.
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
			return d, nil
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

// Resolve durably decides a dispute and carries out (or begins carrying
// out) its economic consequence:
//   - DisputeOutcomeProvider / DisputeOutcomeRejected against a Frozen
//     earning: released back to Available in the same transaction as the
//     terminal checkpoint -- fully complete when this call returns.
//   - DisputeOutcomePrincipal against a Frozen earning: the earning is
//     reversed and the dispute checkpointed to RefundPending in this same
//     transaction, then the principal's account credit is completed
//     before returning (or left for the reconciler to complete if this
//     process dies in between -- see completePrincipalRefund).
//   - Any outcome against an earning already known Paid: no earning
//     mutation is possible (the money already left ATOS). A principal-win
//     records ClawbackRequired; provider-win/rejected require no further
//     action.
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

	dispute, _, err := s.store.UpdateDisputeAndEarning(ctx, in.DisputeID, func(d domain.Dispute, e domain.ProviderEarning, earningExists bool) (domain.Dispute, domain.ProviderEarning, error) {
		if d.PrincipalID == in.ReviewerID || d.ProviderID == in.ReviewerID {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrPermissionDenied, "a party to the dispute cannot resolve it", false)
		}
		if d.ReviewStatus.Terminal() {
			if d.Outcome == in.Outcome {
				return d, e, nil
			}
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute already resolved with a different outcome", false)
		}
		if d.EconomicState == domain.DisputeEconomicRefundPending {
			// Reversal already committed; the account credit is a
			// separate, safe-to-retry step -- see completePrincipalRefund.
			return d, e, nil
		}
		if d.ReviewStatus != domain.DisputeUnderReview && d.ReviewStatus != domain.DisputeOpened {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute is not in a resolvable state", false)
		}
		if in.Outcome != domain.DisputeOutcomeRejected && d.ReviewStatus != domain.DisputeUnderReview {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrDisputeInvalidTransition, "dispute must be under review before it can be decided for a party", false)
		}

		now := time.Now().UTC()
		d.Outcome = in.Outcome
		if in.Outcome == domain.DisputeOutcomeRejected {
			d.ReasonRejected = in.ReasonRejected
		}

		switch d.EconomicState {
		case domain.DisputeEconomicPaid:
			// The money already left ATOS; no automated reversal is
			// possible -- see domain.DisputeEconomicClawbackRequired.
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
			return d, e, nil
		case domain.DisputeEconomicFrozen:
			if !earningExists || e.Status != domain.EarningFrozen {
				return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrSettlementFailed, "disputed earning is not in the expected frozen state", false)
			}
			switch in.Outcome {
			case domain.DisputeOutcomePrincipal:
				e.Status = domain.EarningReversed
				d.EconomicState = domain.DisputeEconomicRefundPending
				d.UpdatedAt = now
				return d, e, nil
			case domain.DisputeOutcomeProvider, domain.DisputeOutcomeRejected:
				e.Status = domain.EarningAvailable
				d.EconomicState = domain.DisputeEconomicReleased
				if in.Outcome == domain.DisputeOutcomeProvider {
					d.ReviewStatus = domain.DisputeResolvedForProvider
				} else {
					d.ReviewStatus = domain.DisputeRejected
				}
				d.ResolvedAt = &now
				d.UpdatedAt = now
				return d, e, nil
			}
		}
		return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrDisputeNotEligible, "dispute economic recovery is still pending payout resolution", true)
	})
	if err != nil {
		return domain.Dispute{}, err
	}
	if dispute.EconomicState == domain.DisputeEconomicRefundPending {
		return s.completePrincipalRefund(ctx, dispute.ID)
	}
	return dispute, nil
}

// completePrincipalRefund credits the principal's account for a dispute
// whose earning has already been durably reversed (EconomicState ==
// RefundPending), completing the account credit and the dispute's
// terminal RefundStatus/ReviewStatus checkpoint in one transaction. Safe
// to call repeatedly: idempotent no-op once already Refunded.
func (s *DisputeService) completePrincipalRefund(ctx context.Context, disputeID string) (domain.Dispute, error) {
	dispute, err := s.store.GetDispute(ctx, disputeID)
	if err != nil {
		return domain.Dispute{}, err
	}
	updated, _, err := s.store.UpdateDisputeAndAccount(ctx, disputeID, dispute.PrincipalID, s.accounts.defaultAccount(dispute.PrincipalID),
		func(d domain.Dispute, exists bool, a domain.Account, _ bool) (domain.Dispute, domain.Account, error) {
			if !exists {
				return domain.Dispute{}, domain.Account{}, domain.NewError(domain.ErrNotFound, "dispute not found", false)
			}
			if d.EconomicState == domain.DisputeEconomicRefunded && d.ReviewStatus == domain.DisputeResolvedForPrincipal {
				return d, a, nil
			}
			if d.EconomicState != domain.DisputeEconomicRefundPending {
				return domain.Dispute{}, domain.Account{}, store.ErrConflict
			}
			nextAccount, err := s.accounts.creditAccountValue(a, d.ChargedAmount.Amount, d.ChargedAmount.Currency)
			if err != nil {
				return domain.Dispute{}, domain.Account{}, err
			}
			now := time.Now().UTC()
			d.EconomicState = domain.DisputeEconomicRefunded
			d.ReviewStatus = domain.DisputeResolvedForPrincipal
			d.ResolvedAt = &now
			d.UpdatedAt = now
			return d, nextAccount, nil
		})
	return updated, err
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
func (s *DisputeService) ReconcileDispute(ctx context.Context, disputeID string) (domain.Dispute, error) {
	dispute, err := s.store.GetDispute(ctx, disputeID)
	if err != nil {
		return domain.Dispute{}, err
	}
	switch dispute.EconomicState {
	case domain.DisputeEconomicPendingPayoutResolution:
		return s.reconcilePendingPayout(ctx, dispute)
	case domain.DisputeEconomicRefundPending:
		return s.completePrincipalRefund(ctx, dispute.ID)
	default:
		return dispute, nil
	}
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
