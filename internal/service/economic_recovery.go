package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/financial"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
)

const (
	defaultReconcileInterval   = 15 * time.Second
	defaultReconcileStaleAfter = 30 * time.Second
	defaultReconcileBatch      = 100
)

func domainErrorIs(err error, code domain.ErrorCode) bool {
	var typed *domain.Error
	return errors.As(err, &typed) && typed.Code == code
}

func (s *JobService) atomicDebitCheckpoint(ctx context.Context, job domain.Job, quote domain.Quote) (domain.Job, error) {
	if s.financial != nil {
		policyPending, _, err := s.store.UpdateJobAndAccount(ctx, job.ID, job.PrincipalID, s.accounts.defaultAccount(job.PrincipalID), func(current domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
			if !exists {
				return domain.Job{}, domain.Account{}, domain.NewError(domain.ErrNotFound, "job not found during policy reservation", false)
			}
			if current.EconomicState == domain.EconomicPolicyPending {
				return current, account, nil
			}
			if current.EconomicState != domain.EconomicNone || current.State != domain.JobSubmitted {
				return domain.Job{}, domain.Account{}, store.ErrConflict
			}
			nextAccount, policyErr := s.accounts.debitPolicyValue(account, quote.Price.TotalMax, quote.Price.Currency)
			if policyErr != nil {
				return domain.Job{}, domain.Account{}, policyErr
			}
			current.EconomicState = domain.EconomicPolicyPending
			current.UpdatedAt = time.Now().UTC()
			return current, nextAccount, nil
		})
		if err != nil {
			return job, err
		}
		amount, err := money.Parse(quote.Price.TotalMax, quote.Price.Currency, accountDecimals)
		if err != nil {
			return policyPending, err
		}
		_, err = s.financial.Reserve(ctx, financial.TransferRequest{
			EventType: financial.EventReserve, IdempotencyIdentity: "job:" + job.ID + ":reserve:v1",
			Identities: financialIdentities(job, quote, "", "", "", "", "", ""),
			Asset:      amount.Currency, Decimals: accountDecimals, AtomicAmount: amount.Minor.String(),
			SourceCode: financial.PrincipalAvailable, SourceOwnerID: job.PrincipalID,
			DestinationCode: financial.PrincipalReserved, DestinationOwnerID: job.PrincipalID,
		})
		if err != nil {
			return policyPending, err
		}
		updated, err := s.store.UpdateJob(ctx, job.ID, func(current domain.Job, exists bool) (domain.Job, error) {
			if !exists {
				return domain.Job{}, domain.NewError(domain.ErrNotFound, "job not found during economic reserve", false)
			}
			if current.EconomicState == domain.EconomicDebited {
				return current, nil
			}
			if current.EconomicState != domain.EconomicPolicyPending {
				return domain.Job{}, store.ErrConflict
			}
			current.EconomicState = domain.EconomicDebited
			current.ProofStatus.Quote = proofCheckpointForCommittedQuote(current.TrustMode)
			current.UpdatedAt = time.Now().UTC()
			return current, nil
		})
		return updated, err
	}
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

func financialIdentities(job domain.Job, quote domain.Quote, billingID, executionReceiptID, settlementID, earningID, disputeID, payoutID string) financial.Identities {
	return financial.Identities{
		PrincipalID: job.PrincipalID, ProviderID: job.ProviderID, JobID: job.ID,
		QuoteID: quote.ID, CapabilityID: job.CapabilityID, CapabilityVersion: job.CapabilityVersion,
		BillingSnapshotID: billingID, ExecutionReceiptID: executionReceiptID,
		SettlementID: settlementID, ProviderEarningID: earningID, DisputeID: disputeID, PayoutID: payoutID,
	}
}

func (s *JobService) settleFinancialLegs(ctx context.Context, job domain.Job, quote domain.Quote, snapshot domain.BillingSnapshot, settlementID string) error {
	if s.financial == nil {
		return nil
	}
	identities := financialIdentities(job, quote, "billing_"+snapshot.JobID, snapshot.ReceiptID, settlementID, "earn_"+settlementID, "", "")
	legs := []struct {
		event       financial.EventType
		suffix      string
		amount      domain.Money
		destination financial.AccountCode
		owner       string
	}{
		{financial.EventSettlementProvider, "provider", snapshot.ProviderGross, financial.ProviderPayable, snapshot.ProviderID},
		{financial.EventSettlementFee, "fee", snapshot.GatewayFee, financial.GatewayFeeRevenue, "_"},
		{financial.EventSettlementRefund, "refund", snapshot.PrincipalRefund, financial.PrincipalAvailable, job.PrincipalID},
	}
	for _, leg := range legs {
		amount, err := money.Parse(leg.amount.Amount, leg.amount.Currency, accountDecimals)
		if err != nil {
			return err
		}
		if amount.IsZero() {
			continue
		}
		_, err = s.financial.Settle(ctx, financial.TransferRequest{
			EventType: leg.event, IdempotencyIdentity: "settlement:" + settlementID + ":" + leg.suffix + ":v1",
			Identities: identities, Asset: amount.Currency, Decimals: accountDecimals, AtomicAmount: amount.Minor.String(),
			SourceCode: financial.ManagedEscrow, SourceOwnerID: job.ID,
			DestinationCode: leg.destination, DestinationOwnerID: leg.owner,
		})
		if err != nil {
			return err
		}
	}
	return nil
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

// validateRecoveredEscrow compares the recovered escrow against the
// Quote's own frozen identity (CapabilityVersion included), never the
// live Capability's current version -- an escrow created for a Job
// executing against a frozen, possibly-now-superseded Quote version must
// validate against that same frozen version on recovery, exactly like
// recoverOrCreateEscrowUnderLock's CreateEscrow call below.
func validateRecoveredEscrow(job domain.Job, quote domain.Quote, capability domain.Capability, escrow domain.Escrow) error {
	expectedReserve := domain.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency}
	if escrow.JobID != job.ID || escrow.QuoteID != quote.ID || escrow.PrincipalID != job.PrincipalID ||
		escrow.ProviderID != capability.ProviderID || escrow.CapabilityID != capability.ID ||
		escrow.CapabilityVersion != quote.CapabilityVersion || escrow.TrustMode != quote.TrustMode ||
		escrow.ProofProfile != quote.ProofProfile || escrow.Reserved != expectedReserve {
		return domain.NewError(domain.ErrSettlementFailed, "recovered escrow does not match the committed Job/Quote", false)
	}
	return nil
}

func (s *JobService) recoverOrCreateEscrowUnderLock(ctx context.Context, job domain.Job, quote domain.Quote, capability domain.Capability) (domain.Escrow, error) {
	if s.financial != nil {
		amount, err := money.Parse(quote.Price.TotalMax, quote.Price.Currency, accountDecimals)
		if err != nil {
			return domain.Escrow{}, err
		}
		_, err = s.financial.FundEscrow(ctx, financial.TransferRequest{
			EventType: financial.EventEscrowFund, IdempotencyIdentity: "job:" + job.ID + ":escrow-fund:v1",
			Identities: financialIdentities(job, quote, "", "", "", "", "", ""),
			Asset:      amount.Currency, Decimals: accountDecimals, AtomicAmount: amount.Minor.String(),
			SourceCode: financial.PrincipalReserved, SourceOwnerID: job.PrincipalID,
			DestinationCode: financial.ManagedEscrow, DestinationOwnerID: job.ID,
		})
		if err != nil {
			return domain.Escrow{}, err
		}
	}
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
		// CapabilityVersion is the Quote's own frozen version, not the
		// (possibly since-updated) live Capability's current one -- the
		// escrow/settlement/dispute audit trail must reference exactly
		// what the Job actually executed against, matching
		// domain.Job.CapabilityVersion and every other frozen-Quote
		// consumer (internal/service/billing.go, dispute.go).
		CapabilityID: capability.ID, CapabilityVersion: quote.CapabilityVersion,
		PrincipalID: job.PrincipalID, ProviderID: capability.ProviderID,
		TrustMode: quote.TrustMode, ProofProfile: quote.ProofProfile,
		Settlement: quote.Settlement,
		Reserved:   domain.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency},
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
	// capability.Version is deliberately NOT compared against
	// quote.CapabilityVersion here, for the same reason
	// internal/service/job.go's submit no longer does: an already-issued
	// Quote/Job MUST continue to use its frozen version/binding semantics
	// after a later Capability update (atos-spec
	// docs/IMPLEMENTATION_ROADMAP.md §7.1.0). The remaining three checks
	// are unrelated data-integrity sanity checks (provider_id is immutable
	// via Update; job.TrustMode/ProofProfile were themselves frozen from
	// this same quote at Job creation, so a mismatch here would indicate
	// storage corruption, not a legitimate live-configuration change) and
	// stay live-checked.
	if capability.ProviderID != quote.ProviderID || job.TrustMode != quote.TrustMode || job.ProofProfile != quote.ProofProfile {
		if job.EconomicState == domain.EconomicNone {
			failed := s.finalizeNoEconomyUnderLock(ctx, job, domain.JobFailed, domain.ErrQuoteMismatch, "execution contract no longer matches quote")
			return failed, capability, nil
		}
		return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, domain.JobFailed, domain.ErrQuoteMismatch, "execution contract drifted after funds were reserved"), capability, domain.NewError(domain.ErrQuoteMismatch, "execution contract drifted after funds were reserved", false)
	}
	if job.EconomicState == domain.EconomicNone || job.EconomicState == domain.EconomicPolicyPending {
		if quote.PrincipalID == "" {
			quote.PrincipalID = job.PrincipalID
		}
		if _, err := s.core.CommitQuote(ctx, quote); err != nil {
			if job.EconomicState == domain.EconomicPolicyPending {
				pending := s.markEconomicReconciliationUnderLock(ctx, job.ID, domain.EconomicPolicyPending, domain.JobWorking, errCode(err), "quote commitment recovery failed after policy reservation: "+err.Error())
				return pending, capability, err
			}
			failed := s.finalizeNoEconomyUnderLock(ctx, job, domain.JobFailed, errCode(err), "quote commitment failed: "+err.Error())
			return failed, capability, nil
		}
		job, err = s.atomicDebitCheckpoint(ctx, job, quote)
		if err != nil {
			if job.EconomicState == domain.EconomicPolicyPending {
				pending := s.markEconomicReconciliationUnderLock(ctx, job.ID, domain.EconomicPolicyPending, domain.JobWorking, errCode(err), "financial reserve requires idempotent recovery: "+err.Error())
				return pending, capability, err
			}
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
			if !exists {
				return domain.Job{}, store.ErrNotFound
			}
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
		if !exists {
			return domain.Job{}, store.ErrNotFound
		}
		if current.EconomicState != domain.EconomicNone {
			return domain.Job{}, store.ErrConflict
		}
		return finalizeTerminalJob(current, target, code, reason, domain.EconomicNone), nil
	})
	if err != nil {
		return job
	}
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
	if err != nil {
		return job, err
	}
	if s.financial != nil {
		amount, err := money.Parse(quote.Price.TotalMax, quote.Price.Currency, accountDecimals)
		if err != nil {
			return job, err
		}
		_, err = s.financial.ReleaseReservation(ctx, financial.TransferRequest{
			EventType: financial.EventReservationRelease, IdempotencyIdentity: "job:" + job.ID + ":reservation-release:v1",
			Identities: financialIdentities(job, quote, "", "", "", "", "", ""),
			Asset:      amount.Currency, Decimals: accountDecimals, AtomicAmount: amount.Minor.String(),
			SourceCode: financial.PrincipalReserved, SourceOwnerID: job.PrincipalID,
			DestinationCode: financial.PrincipalAvailable, DestinationOwnerID: job.PrincipalID,
		})
		if err != nil {
			return s.markEconomicReconciliationUnderLock(ctx, job.ID, domain.EconomicDebited, target, code, reason+"; financial reservation release requires recovery"), err
		}
		updated, _, updateErr := s.store.UpdateJobAndAccount(ctx, job.ID, job.PrincipalID, s.accounts.defaultAccount(job.PrincipalID), func(current domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
			if !exists {
				return domain.Job{}, domain.Account{}, store.ErrNotFound
			}
			if current.EconomicState == domain.EconomicReleased && current.State.Terminal() {
				return current, account, nil
			}
			if current.EconomicState != domain.EconomicDebited {
				return domain.Job{}, domain.Account{}, store.ErrConflict
			}
			nextAccount, policyErr := s.accounts.creditPolicyValue(account, quote.Price.TotalMax, quote.Price.Currency)
			if policyErr != nil {
				return domain.Job{}, domain.Account{}, policyErr
			}
			return finalizeTerminalJob(current, target, code, reason, domain.EconomicReleased), nextAccount, nil
		})
		return updated, updateErr
	}
	updated, _, err := s.store.UpdateJobAndAccount(ctx, job.ID, job.PrincipalID, s.accounts.defaultAccount(job.PrincipalID), func(current domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
		if !exists {
			return domain.Job{}, domain.Account{}, store.ErrNotFound
		}
		if current.EconomicState == domain.EconomicReleased && current.State.Terminal() {
			return current, account, nil
		}
		if current.EconomicState != domain.EconomicDebited {
			return domain.Job{}, domain.Account{}, store.ErrConflict
		}
		nextAccount, err := s.accounts.creditAccountValue(account, quote.Price.TotalMax, quote.Price.Currency)
		if err != nil {
			return domain.Job{}, domain.Account{}, err
		}
		return finalizeTerminalJob(current, target, code, reason, domain.EconomicReleased), nextAccount, nil
	})
	return updated, err
}

func (s *JobService) releaseForTerminalUnderLock(ctx context.Context, job domain.Job, target domain.JobState, code domain.ErrorCode, reason string) (domain.Job, error) {
	if job.EconomicState == domain.EconomicNone {
		return s.finalizeNoEconomyUnderLock(ctx, job, target, code, reason), nil
	}
	if job.EconomicState == domain.EconomicPolicyPending {
		quote, quoteErr := s.getQuote(ctx, job.QuoteID)
		if quoteErr != nil {
			return s.markEconomicReconciliationUnderLock(ctx, job.ID, domain.EconomicPolicyPending, target, code, reason+"; policy reservation quote recovery failed"), quoteErr
		}
		reserved, reserveErr := s.atomicDebitCheckpoint(ctx, job, quote)
		if reserveErr != nil {
			return s.markEconomicReconciliationUnderLock(ctx, job.ID, domain.EconomicPolicyPending, target, code, reason+"; financial reserve outcome requires recovery"), reserveErr
		}
		job = reserved
	}
	if job.EconomicState == domain.EconomicDebited {
		return s.refundDebitedWithoutEscrowUnderLock(ctx, job, target, code, reason)
	}
	quote, err := s.getQuote(ctx, job.QuoteID)
	if err != nil {
		return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, target, code, reason+"; quote recovery failed"), err
	}
	capability, err := s.store.Get(ctx, job.CapabilityID)
	if err != nil {
		return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, target, code, reason+"; capability recovery failed"), err
	}
	capability = normalizeCapability(capability)
	if job.EconomicState == domain.EconomicEscrowPending {
		escrow, recoverErr := s.recoverOrCreateEscrowUnderLock(ctx, job, quote, capability)
		if recoverErr != nil {
			return s.markEconomicReconciliationUnderLock(ctx, job.ID, domain.EconomicEscrowPending, target, code, reason+"; escrow outcome remains ambiguous: "+recoverErr.Error()), recoverErr
		}
		job, err = s.store.UpdateJob(ctx, job.ID, func(current domain.Job, exists bool) (domain.Job, error) {
			if !exists {
				return domain.Job{}, store.ErrNotFound
			}
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
		if err != nil {
			return job, err
		}
	}
	if job.EconomicState == domain.EconomicSettlementPending || job.EconomicState == domain.EconomicSettled {
		return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, domain.JobCompleted, domain.ErrSettlementFailed, "settlement outcome must be recovered before release"), domain.NewError(domain.ErrSettlementFailed, "settlement outcome must be recovered before release", true)
	}
	if job.EscrowID == "" {
		return s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, target, code, reason+"; escrow reference missing"), domain.NewError(domain.ErrSettlementFailed, "escrow reference missing", true)
	}
	if job.EconomicState != domain.EconomicReleasePending {
		job, err = s.store.UpdateJob(ctx, job.ID, func(current domain.Job, exists bool) (domain.Job, error) {
			if !exists {
				return domain.Job{}, store.ErrNotFound
			}
			current.State = domain.JobReconciling
			current.EconomicState = domain.EconomicReleasePending
			current.ReconciliationRequired = true
			current.ReconciliationTarget = target
			current.ErrorCode = code
			current.FailureReason = reason
			current.UpdatedAt = time.Now().UTC()
			return current, nil
		})
		if err != nil {
			return job, err
		}
	}
	receipt, releaseErr := s.core.ReleaseEscrow(ctx, job.EscrowID)
	if releaseErr != nil {
		return s.markEconomicReconciliationUnderLock(ctx, job.ID, domain.EconomicReleasePending, target, code, reason+"; escrow release requires replay: "+releaseErr.Error()), releaseErr
	}
	if s.financial != nil && nonZeroMoney(receipt.Refunded) {
		amount, parseErr := money.Parse(receipt.Refunded.Amount, receipt.Refunded.Currency, accountDecimals)
		if parseErr != nil {
			return job, parseErr
		}
		_, financialErr := s.financial.ReleaseEscrow(ctx, financial.TransferRequest{
			EventType: financial.EventEscrowRelease, IdempotencyIdentity: "job:" + job.ID + ":escrow-release:v1",
			Identities: financialIdentities(job, quote, "", "", receipt.ID, "", "", ""),
			Asset:      amount.Currency, Decimals: accountDecimals, AtomicAmount: amount.Minor.String(),
			SourceCode: financial.ManagedEscrow, SourceOwnerID: job.ID,
			DestinationCode: financial.PrincipalAvailable, DestinationOwnerID: job.PrincipalID,
		})
		if financialErr != nil {
			return s.markEconomicReconciliationUnderLock(ctx, job.ID, domain.EconomicReleasePending, target, code, reason+"; Blnk escrow release requires recovery"), financialErr
		}
	}
	if s.financial != nil {
		updated, _, updateErr := s.store.UpdateJobAndAccount(ctx, job.ID, job.PrincipalID, s.accounts.defaultAccount(job.PrincipalID), func(current domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
			if !exists {
				return domain.Job{}, domain.Account{}, store.ErrNotFound
			}
			if current.EconomicState == domain.EconomicReleased && current.State.Terminal() {
				return current, account, nil
			}
			if current.EconomicState != domain.EconomicReleasePending {
				return domain.Job{}, domain.Account{}, store.ErrConflict
			}
			nextAccount := account
			if nonZeroMoney(receipt.Refunded) {
				var policyErr error
				nextAccount, policyErr = s.accounts.creditPolicyValue(account, receipt.Refunded.Amount, receipt.Refunded.Currency)
				if policyErr != nil {
					return domain.Job{}, domain.Account{}, policyErr
				}
			}
			current.ProofStatus.Escrow = domain.ProofReleased
			current.ProofStatus.Settlement = domain.ProofReleased
			return finalizeTerminalJob(current, target, code, reason, domain.EconomicReleased), nextAccount, nil
		})
		return updated, updateErr
	}
	updated, _, err := s.store.UpdateJobAndAccount(ctx, job.ID, job.PrincipalID, s.accounts.defaultAccount(job.PrincipalID), func(current domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
		if !exists {
			return domain.Job{}, domain.Account{}, store.ErrNotFound
		}
		if current.EconomicState == domain.EconomicReleased && current.State.Terminal() {
			return current, account, nil
		}
		if current.EconomicState != domain.EconomicReleasePending {
			return domain.Job{}, domain.Account{}, store.ErrConflict
		}
		nextAccount := account
		var creditErr error
		if nonZeroMoney(receipt.Refunded) {
			nextAccount, creditErr = s.accounts.creditAccountValue(account, receipt.Refunded.Amount, receipt.Refunded.Currency)
			if creditErr != nil {
				return domain.Job{}, domain.Account{}, creditErr
			}
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
		if result.Output == nil {
			result.Output = cloneMap(current.Output)
		}
		if len(result.Artifacts) == 0 {
			result.Artifacts = append([]domain.Artifact(nil), current.Artifacts...)
		}
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
	// Metered billing is a deterministic function of the frozen Quote terms
	// and this receipt's verified usage -- never the capability's current
	// live pricing. It never charges more than quote.Price.TotalMax (see
	// computeBillingSnapshot). The result is computed once here, before the
	// receipt is committed/verified below, so the same value that gets
	// verified is the value that gets charged and later recorded as the
	// provider's earning.
	billingSnapshot, billErr := computeBillingSnapshot(quote, receipt)
	if billErr != nil {
		// computeBillingSnapshot is a pure function of the already-durable,
		// frozen Quote and verified Receipt: any error it returns (e.g. an
		// invalid/legacy frozen MeteredRate) is deterministic and fails
		// identically on every future reconciliation retry, so it can never
		// be treated as a transient outcome to reconcile away. Before the
		// receipt has been durably committed and verified (EconomicState is
		// still EconomicEscrowReserved), it is safe -- and required -- to
		// fail the Job now and release the escrow back to the principal
		// rather than leaving both the Job and its reserved funds stuck in
		// JobReconciling/EconomicEscrowReserved forever. Once a receipt has
		// already been verified (EconomicSettlementPending or later), the
		// provider's delivered work has already been proven and settlement
		// must still be recovered rather than unwound, so that case is left
		// to the existing reconciliation handling below.
		if current.EconomicState != domain.EconomicSettlementPending {
			failed, _ := s.releaseForTerminalUnderLock(ctx, current, domain.JobFailed, domain.ErrSettlementFailed, "billing calculation failed: "+billErr.Error())
			return failed
		}
		return s.markEconomicReconciliationUnderLock(ctx, current.ID, current.EconomicState, domain.JobCompleted, domain.ErrSettlementFailed, "billing calculation failed: "+billErr.Error())
	}
	receipt.Cost = billingSnapshot.GrossCharge
	if current.EconomicState != domain.EconomicSettlementPending {
		current.ProofStatus.Receipt = domain.ProofSigned
		proofRef, commitErr := s.core.CommitExecutionReceipt(ctx, receipt)
		if commitErr != nil {
			return s.markEconomicReconciliationUnderLock(ctx, current.ID, current.EconomicState, domain.JobWorking, errCode(commitErr), "execution receipt commitment requires replay: "+commitErr.Error())
		}
		if proofRef != "" {
			receipt.NetworkProofRef = proofRef
		}
		verify, verifyErr := s.core.VerifyExecutionReceipt(ctx, current.EscrowID, receipt)
		if verifyErr != nil {
			return s.markEconomicReconciliationUnderLock(ctx, current.ID, current.EconomicState, domain.JobWorking, domain.ErrSettlementFailed, "execution receipt verification unavailable: "+verifyErr.Error())
		}
		if !verify.Valid {
			failed, _ := s.releaseForTerminalUnderLock(ctx, current, domain.JobFailed, domain.ErrSettlementFailed, "execution receipt failed verification: "+verify.Reason)
			return failed
		}
		if verify.ProofRef != "" {
			receipt.NetworkProofRef = verify.ProofRef
		}
		current, err = s.store.UpdateJob(ctx, current.ID, func(job domain.Job, exists bool) (domain.Job, error) {
			if !exists {
				return domain.Job{}, store.ErrNotFound
			}
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
		if err != nil {
			return current
		}
	} else if current.ExecutionReceipt != nil {
		receipt = *current.ExecutionReceipt
	}
	settled, settleErr := s.core.SettleJob(ctx, toscore.SettleJobRequest{
		EscrowID: current.EscrowID, JobID: current.ID, ReceiptID: receipt.ID, ActualCost: receipt.Cost,
	})
	if settleErr != nil {
		return s.markEconomicReconciliationUnderLock(ctx, current.ID, domain.EconomicSettlementPending, domain.JobCompleted, domain.ErrSettlementFailed, "settlement outcome requires idempotent replay: "+settleErr.Error())
	}
	if financialErr := s.settleFinancialLegs(ctx, current, quote, billingSnapshot, settled.Receipt.ID); financialErr != nil {
		return s.markEconomicReconciliationUnderLock(ctx, current.ID, domain.EconomicSettlementPending, domain.JobCompleted, domain.ErrSettlementFailed, "Blnk settlement requires idempotent recovery: "+financialErr.Error())
	}
	if s.financial != nil {
		final, _, finalErr := s.store.UpdateJobAndAccount(ctx, current.ID, current.PrincipalID, s.accounts.defaultAccount(current.PrincipalID), func(job domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
			if !exists {
				return domain.Job{}, domain.Account{}, store.ErrNotFound
			}
			if job.EconomicState == domain.EconomicSettled && job.State == domain.JobCompleted {
				return job, account, nil
			}
			if job.EconomicState != domain.EconomicSettlementPending {
				return domain.Job{}, domain.Account{}, store.ErrConflict
			}
			nextAccount := account
			if nonZeroMoney(settled.Receipt.Refunded) {
				var policyErr error
				nextAccount, policyErr = s.accounts.creditPolicyValue(account, settled.Receipt.Refunded.Amount, settled.Receipt.Refunded.Currency)
				if policyErr != nil {
					return domain.Job{}, domain.Account{}, policyErr
				}
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
			return s.markEconomicReconciliationUnderLock(ctx, current.ID, domain.EconomicSettlementPending, domain.JobCompleted, domain.ErrSettlementFailed, "Blnk settlement finalized but local projection must be retried: "+finalErr.Error())
		}
		if s.earnings != nil {
			_, _ = s.earnings.RecordSettlement(ctx, billingSnapshot, settled.Receipt.ID)
		}
		_, _ = s.core.CommitProofOfServiceEvidence(ctx, receipt)
		return final
	}
	final, _, finalErr := s.store.UpdateJobAndAccount(ctx, current.ID, current.PrincipalID, s.accounts.defaultAccount(current.PrincipalID), func(job domain.Job, exists bool, account domain.Account, _ bool) (domain.Job, domain.Account, error) {
		if !exists {
			return domain.Job{}, domain.Account{}, store.ErrNotFound
		}
		if job.EconomicState == domain.EconomicSettled && job.State == domain.JobCompleted {
			return job, account, nil
		}
		if job.EconomicState != domain.EconomicSettlementPending {
			return domain.Job{}, domain.Account{}, store.ErrConflict
		}
		nextAccount := account
		var creditErr error
		if nonZeroMoney(settled.Receipt.Refunded) {
			nextAccount, creditErr = s.accounts.creditAccountValue(account, settled.Receipt.Refunded.Amount, settled.Receipt.Refunded.Currency)
			if creditErr != nil {
				return domain.Job{}, domain.Account{}, creditErr
			}
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
	// The customer-facing settlement (charge/refund, job completion) is now
	// durably finalized regardless of what happens next. Recording the
	// provider's earning is a best-effort attempt here: RecordSettlement is
	// idempotent (PutBillingSnapshot upserts by JobID, CreateEarning is
	// unique-by-settlement_id), so if this call fails or the process dies
	// before it runs, EarningsService.BackfillSweep finds this Job via
	// EconomicSettled and replays it later -- the Job's own State never
	// reverts away from Completed to wait for it.
	if s.earnings != nil {
		_, _ = s.earnings.RecordSettlement(ctx, billingSnapshot, settled.Receipt.ID)
	}
	_, _ = s.core.CommitProofOfServiceEvidence(ctx, receipt)
	return final
}

func (s *JobService) recoverProviderExecution(ctx context.Context, jobID string, allowSubmit bool) (domain.Job, error) {
	lock := s.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return domain.Job{}, err
	}
	if job.State.Terminal() {
		return job, nil
	}
	if job.EconomicState == domain.EconomicSettlementPending && job.ExecutionReceipt != nil {
		result := tosai.SubmitJobResult{State: domain.JobCompleted, Output: cloneMap(job.Output), Artifacts: append([]domain.Artifact(nil), job.Artifacts...), Receipt: job.ExecutionReceipt}
		return s.settleProviderResultUnderLock(ctx, job, result), nil
	}
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
		// job.Binding was frozen once, at creation time (domain.SelectBinding),
		// and is passed through unchanged here -- execution never re-fetches
		// or re-resolves the Capability's current bindings, so a live
		// Capability update (a semantically different provider binding,
		// possibly a version bump) can never silently redirect an
		// already-committed Job. See domain.Job.Binding's doc comment.
		result, getErr = s.provider.SubmitJob(ctx, tosai.SubmitJobRequest{
			JobID: job.ID, InvocationID: job.InvocationID,
			QuoteID: job.QuoteID, ServiceQuoteID: job.ServiceQuoteID,
			EscrowID: job.EscrowID, PrincipalID: job.PrincipalID,
			CapabilityID: job.CapabilityID, CapabilityVersion: job.CapabilityVersion,
			ProviderID: job.ProviderID, TrustMode: job.TrustMode, ProofProfile: job.ProofProfile,
			Input: job.Input, InputCommitment: hashCommitment(job.Input),
			ExecutionDeadline: job.ExecutionDeadline, RetainUntil: time.Now().UTC().Add(executionRetention),
			Binding: job.Binding,
		})
		if getErr != nil {
			pending := s.markEconomicReconciliationUnderLock(ctx, job.ID, job.EconomicState, domain.JobWorking, domain.ErrProviderFailed, "provider submission outcome requires recovery: "+getErr.Error())
			return pending, getErr
		}
	}
	if result.State == domain.JobCompleted {
		// Before accepting provider output into a successful settlement,
		// it must satisfy the frozen output schema -- checked against the
		// schema captured on the Job at creation time (job.OutputSchema),
		// never the Capability's possibly-since-updated current one.
		// Schema failure is a provider failure, never a successful
		// settlement.
		if err := validateAgainstSchema("output", job.OutputSchema, result.Output); err != nil {
			released, releaseErr := s.releaseForTerminalUnderLock(ctx, job, domain.JobFailed, domain.ErrProviderFailed, "provider output failed schema validation: "+err.Error())
			return released, releaseErr
		}
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

func (s *JobService) reconcilePrepareAndRun(ctx context.Context, jobID string) (domain.Job, error) {
	lock := s.jobLock(jobID)
	lock.Lock()
	job, _, err := s.prepareExecutionUnderLock(ctx, jobID)
	lock.Unlock()
	if err != nil {
		if current, getErr := s.store.GetJob(ctx, jobID); getErr == nil {
			return current, err
		}
		return job, err
	}
	if job.State == domain.JobWorking && job.EconomicState == domain.EconomicEscrowReserved {
		return s.recoverProviderExecution(ctx, jobID, true)
	}
	return job, nil
}

func (s *JobService) ReconcileJob(ctx context.Context, jobID string) (domain.Job, error) {
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return domain.Job{}, err
	}
	if job.State.Terminal() || job.State == domain.JobInputRequired {
		return job, nil
	}
	if job.PendingCredit != nil {
		return s.reconcileCredit(ctx, jobID)
	}
	switch job.EconomicState {
	case domain.EconomicNone:
		return s.reconcilePrepareAndRun(ctx, jobID)
	case domain.EconomicPolicyPending:
		return s.reconcilePrepareAndRun(ctx, jobID)
	case domain.EconomicDebited:
		if quote, quoteErr := s.getQuote(ctx, job.QuoteID); quoteErr == nil && (quote.Expired(time.Now().UTC()) || (!quote.ExecutionDeadline.IsZero() && !time.Now().UTC().Before(quote.ExecutionDeadline))) {
			lock := s.jobLock(jobID)
			lock.Lock()
			defer lock.Unlock()
			return s.refundDebitedWithoutEscrowUnderLock(ctx, job, domain.JobFailed, domain.ErrQuoteExpired, "quote expired after debit but before escrow creation")
		}
		return s.reconcilePrepareAndRun(ctx, jobID)
	case domain.EconomicEscrowPending:
		if job.ReconciliationTarget == domain.JobFailed || job.ReconciliationTarget == domain.JobCanceled {
			lock := s.jobLock(jobID)
			lock.Lock()
			defer lock.Unlock()
			return s.releaseForTerminalUnderLock(ctx, job, job.ReconciliationTarget, job.ErrorCode, job.FailureReason)
		}
		return s.reconcilePrepareAndRun(ctx, jobID)
	case domain.EconomicEscrowReserved:
		if job.ReconciliationTarget == domain.JobFailed || job.ReconciliationTarget == domain.JobCanceled {
			lock := s.jobLock(jobID)
			lock.Lock()
			defer lock.Unlock()
			return s.releaseForTerminalUnderLock(ctx, job, job.ReconciliationTarget, job.ErrorCode, job.FailureReason)
		}
		return s.recoverProviderExecution(ctx, jobID, true)
	case domain.EconomicSettlementPending:
		return s.recoverProviderExecution(ctx, jobID, false)
	case domain.EconomicReleasePending:
		lock := s.jobLock(jobID)
		lock.Lock()
		defer lock.Unlock()
		target := job.ReconciliationTarget
		if target == "" {
			target = domain.JobFailed
		}
		return s.releaseForTerminalUnderLock(ctx, job, target, job.ErrorCode, job.FailureReason)
	case domain.EconomicSettled, domain.EconomicReleased:
		return job, nil
	default:
		return job, domain.NewError(domain.ErrSettlementFailed, "unknown economic recovery checkpoint", false)
	}
}

func (s *JobService) ReconcileStaleJobs(ctx context.Context, updatedBefore time.Time, limit int) (int, error) {
	jobs, err := s.store.JobsForRecovery(ctx, updatedBefore, limit)
	if err != nil {
		return 0, err
	}
	var joined error
	for _, job := range jobs {
		if _, err := s.ReconcileJob(ctx, job.ID); err != nil {
			joined = errors.Join(joined, fmt.Errorf("reconcile %s: %w", job.ID, err))
		}
	}
	return len(jobs), joined
}

func (s *JobService) RunReconciler(ctx context.Context, interval, staleAfter time.Duration, limit int, report func(error)) {
	if interval <= 0 {
		interval = defaultReconcileInterval
	}
	if staleAfter <= 0 {
		staleAfter = defaultReconcileStaleAfter
	}
	if limit <= 0 {
		limit = defaultReconcileBatch
	}
	sweep := func() {
		_, err := s.ReconcileStaleJobs(ctx, time.Now().UTC().Add(-staleAfter), limit)
		if err != nil && report != nil {
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
