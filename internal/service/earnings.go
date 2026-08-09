package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/payout"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

const (
	// defaultMaturationPeriod is how long a freshly settled earning waits
	// in EarningMaturing before it becomes EarningAvailable for payout.
	// Reserved as a hook for a future dispute window (Phase 2C); Phase 2B
	// never disputes a settlement, so this is currently just a pacing knob.
	defaultMaturationPeriod = 24 * time.Hour
	defaultEarningsInterval = 30 * time.Second
	defaultEarningsBatch    = 100
	// defaultPayoutRetryBackoff bounds how soon a payout_pending earning is
	// retried by the reconciler, so a crash loop cannot hammer the external
	// payout rail once per sweep tick.
	defaultPayoutRetryBackoff = 5 * time.Minute
)

// EarningsService owns the provider earnings ledger and the idempotent
// external-payout state machine. A finalized settlement produces exactly
// one ProviderEarning -- enforced by store.Earnings.CreateEarning's
// settlement_id uniqueness, not application logic -- which matures,
// becomes available, and is then paid out through a payout.Adapter using a
// stable per-earning idempotency identity that never changes across
// retries or crashes.
type EarningsService struct {
	store            store.Store
	payoutAdapter    payout.Adapter
	maturationPeriod time.Duration
}

func NewEarningsService(s store.Store, adapter payout.Adapter) *EarningsService {
	return &EarningsService{store: s, payoutAdapter: adapter, maturationPeriod: defaultMaturationPeriod}
}

// WithMaturationPeriod overrides the default maturation window. Mainly for
// tests that need earnings to become Available without waiting 24h.
func (s *EarningsService) WithMaturationPeriod(d time.Duration) *EarningsService {
	if d > 0 {
		s.maturationPeriod = d
	}
	return s
}

// RecordSettlement persists the durable billing snapshot for a completed
// Job and creates its ProviderEarning if one does not already exist for
// settlementID. Both operations are safe to call more than once for the
// same inputs -- PutBillingSnapshot is a pure-function idempotent upsert
// keyed by JobID, and CreateEarning is a database-uniqueness-enforced
// exactly-once create keyed by settlementID -- which is what makes it safe
// to call RecordSettlement again from crash recovery.
//
// If a billing snapshot or earning already exists for this Job/settlement
// with DIFFERENT economic content than what's being recorded now (e.g. a
// crash-recovery replay that recomputed a different amount), the store
// layer returns domain.ErrIdempotencyConflict rather than silently keeping
// the old value or accepting the new one -- that conflict is returned to
// the caller unchanged rather than papered over here. When no conflict
// occurs, the earning is built from the store's canonical stored snapshot
// (not the possibly-different snap argument), so the earning's amounts can
// never drift from the durable billing record.
func (s *EarningsService) RecordSettlement(ctx context.Context, snap domain.BillingSnapshot, settlementID string) (domain.ProviderEarning, error) {
	if settlementID == "" {
		return domain.ProviderEarning{}, domain.NewError(domain.ErrSettlementFailed, "settlement id is required to record an earning", false)
	}
	stored, _, err := s.store.PutBillingSnapshot(ctx, snap)
	if err != nil {
		return domain.ProviderEarning{}, err
	}
	now := time.Now().UTC()
	earning := domain.ProviderEarning{
		ID:                "earn_" + settlementID,
		ProviderID:        stored.ProviderID,
		JobID:             stored.JobID,
		QuoteID:           stored.QuoteID,
		ReceiptID:         stored.ReceiptID,
		SettlementID:      settlementID,
		CapabilityID:      stored.CapabilityID,
		CapabilityVersion: stored.CapabilityVersion,
		// GrossAmount is the full amount charged to the principal for this
		// job; GatewayFee is ATOS's cut of it; NetAmount (= GrossAmount -
		// GatewayFee = stored.ProviderGross) is what the provider is
		// actually owed and what gets paid out.
		GrossAmount: stored.GrossCharge,
		GatewayFee:  stored.GatewayFee,
		NetAmount:   stored.ProviderGross,
		Status:      domain.EarningMaturing,
		CreatedAt:   now,
		MaturesAt:   now.Add(s.maturationPeriod),
	}
	out, _, err := s.store.CreateEarning(ctx, earning)
	return out, err
}

// BackfillSweep finds Jobs whose settlement is already durably finalized
// (EconomicState == EconomicSettled) but which have no ProviderEarning yet
// -- the crash window between settleProviderResultUnderLock committing the
// final settlement and its best-effort inline RecordSettlement call either
// never running or failing -- and replays RecordSettlement for each. This
// is the earnings-side counterpart to JobService's job-state reconciler: it
// operates entirely off durable economic state rather than the Job's public
// State field, since a settled Job is already terminal and JobService's own
// reconciler intentionally never revisits terminal Jobs.
func (s *EarningsService) BackfillSweep(ctx context.Context, limit int) (int, error) {
	jobs, err := s.store.SettledJobsMissingEarning(ctx, limit)
	if err != nil {
		return 0, err
	}
	var joined error
	for _, job := range jobs {
		if err := s.backfillJob(ctx, job); err != nil {
			joined = errors.Join(joined, fmt.Errorf("backfill earning for job %s: %w", job.ID, err))
		}
	}
	return len(jobs), joined
}

func (s *EarningsService) backfillJob(ctx context.Context, job domain.Job) error {
	if job.ExecutionReceipt == nil {
		return fmt.Errorf("job %s is settled but has no durable execution receipt", job.ID)
	}
	quote, err := s.store.GetQuote(ctx, job.QuoteID)
	if err != nil {
		return err
	}
	settlementReceipt, err := s.store.ReceiptByJob(ctx, job.ID)
	if err != nil {
		return err
	}
	snap, err := computeBillingSnapshot(quote, *job.ExecutionReceipt)
	if err != nil {
		return err
	}
	_, err = s.RecordSettlement(ctx, snap, settlementReceipt.ID)
	return err
}

// MaturationSweep advances EarningMaturing earnings whose MaturesAt has
// passed to EarningAvailable.
func (s *EarningsService) MaturationSweep(ctx context.Context, limit int) (int, error) {
	earnings, err := s.store.EarningsMaturing(ctx, time.Now().UTC(), limit)
	if err != nil {
		return 0, err
	}
	var joined error
	for _, e := range earnings {
		if _, err := s.store.UpdateEarning(ctx, e.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
			if !exists {
				return domain.ProviderEarning{}, store.ErrNotFound
			}
			if current.Status != domain.EarningMaturing {
				return current, nil
			}
			now := time.Now().UTC()
			current.Status = domain.EarningAvailable
			current.AvailableAt = &now
			return current, nil
		}); err != nil {
			joined = errors.Join(joined, fmt.Errorf("mature %s: %w", e.ID, err))
		}
	}
	return len(earnings), joined
}

// payoutIdempotencyKey is deterministic from the earning's own durable
// identity, never randomly generated, so a retry -- whether the very first
// attempt after committing payout_pending, or a reconciliation replay after
// a crash -- always presents the exact same external identity, satisfying
// "never generate a new payout identity during retry."
func payoutIdempotencyKey(earningID string) string {
	return "payout:" + earningID + ":v1"
}

// beginPayoutUnderLock is the durable-intent-commit half of the payout state
// machine: a compare-and-swap transition from EarningAvailable to
// EarningPayoutPending, assigning the stable idempotency key, committed to
// PostgreSQL BEFORE any external call is made. Concurrent callers (two
// reconciler workers, two ATOS replicas) racing this for the same earning
// serialize on the store's row lock; only one observes status==Available
// and performs the transition, and every other caller's CAS either no-ops
// against the now-payout_pending row or errors, never double-transitions.
func (s *EarningsService) beginPayoutUnderLock(ctx context.Context, earningID string) (domain.ProviderEarning, error) {
	return s.store.UpdateEarning(ctx, earningID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		if !exists {
			return domain.ProviderEarning{}, store.ErrNotFound
		}
		if current.Status == domain.EarningPayoutPending || current.Status.Terminal() {
			return current, nil
		}
		// A Dispute holds this earning out of the payout pipeline even
		// while it is transiently Available again (e.g. a prior in-flight
		// payout attempt was rejected, moving Status back to Available,
		// but the dispute reconciler has not yet had a chance to freeze it
		// late) -- see domain.ProviderEarning.DisputeHoldID's doc comment.
		// No new payout intent may begin for as long as the hold is set.
		if current.DisputeHoldID != "" {
			return current, nil
		}
		if current.Status != domain.EarningAvailable {
			return domain.ProviderEarning{}, store.ErrConflict
		}
		now := time.Now().UTC()
		current.Status = domain.EarningPayoutPending
		current.PayoutRequestedAt = &now
		if current.PayoutIdempotencyKey == "" {
			current.PayoutIdempotencyKey = payoutIdempotencyKey(current.ID)
		}
		return current, nil
	})
}

// attemptPayout performs (or replays) the external side effect for an
// earning already durably parked in payout_pending, then commits the
// durable completion checkpoint. It never guesses an outcome: an adapter
// error or an explicit StatusPending result leaves the earning in
// payout_pending for a later retry using the identical idempotency key --
// the only way it can ever reach that call is if a prior attempt's result
// is genuinely unknown.
func (s *EarningsService) attemptPayout(ctx context.Context, e domain.ProviderEarning) (domain.ProviderEarning, error) {
	if e.Status != domain.EarningPayoutPending {
		return e, nil
	}
	// Query first: if a prior attempt already completed on the rail but
	// this process crashed before recording it locally -- or a concurrent
	// worker/replica already completed it -- this recovers the result
	// without risking a second external call.
	result, found, err := s.payoutAdapter.Query(ctx, e.PayoutIdempotencyKey)
	if err != nil {
		return s.recordPayoutAttempt(ctx, e, err)
	}
	if !found {
		result, err = s.payoutAdapter.Payout(ctx, payout.Request{
			IdempotencyKey: e.PayoutIdempotencyKey, EarningID: e.ID, ProviderID: e.ProviderID, Amount: e.NetAmount,
		})
		if err != nil {
			return s.recordPayoutAttempt(ctx, e, err)
		}
	}
	switch result.Status {
	case payout.StatusPaid:
		return s.store.UpdateEarning(ctx, e.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
			if !exists {
				return domain.ProviderEarning{}, store.ErrNotFound
			}
			if current.Status == domain.EarningPaid {
				return current, nil
			}
			if current.Status != domain.EarningPayoutPending {
				return domain.ProviderEarning{}, store.ErrConflict
			}
			now := time.Now().UTC()
			current.Status = domain.EarningPaid
			current.PayoutReference = result.Reference
			current.PaidAt = &now
			current.PayoutFailureReason = ""
			return current, nil
		})
	case payout.StatusRejected:
		// The rail guarantees no funds moved for this idempotency key, so
		// it is safe to fall back to Available for a corrected future
		// retry (e.g. after an operator fixes payout details) rather than
		// leaving the earning stuck in payout_pending forever.
		return s.store.UpdateEarning(ctx, e.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
			if !exists {
				return domain.ProviderEarning{}, store.ErrNotFound
			}
			if current.Status != domain.EarningPayoutPending {
				return current, nil
			}
			current.Status = domain.EarningAvailable
			current.PayoutRequestedAt = nil
			current.PayoutFailureReason = result.Reason
			current.PayoutAttempts++
			current.PayoutLastAttemptAt = time.Now().UTC()
			return current, nil
		})
	default: // payout.StatusPending: outcome unknown, stay payout_pending.
		return s.recordPayoutAttempt(ctx, e, fmt.Errorf("payout pending: %s", result.Reason))
	}
}

func (s *EarningsService) recordPayoutAttempt(ctx context.Context, e domain.ProviderEarning, attemptErr error) (domain.ProviderEarning, error) {
	updated, err := s.store.UpdateEarning(ctx, e.ID, func(current domain.ProviderEarning, exists bool) (domain.ProviderEarning, error) {
		if !exists {
			return domain.ProviderEarning{}, store.ErrNotFound
		}
		if current.Status != domain.EarningPayoutPending {
			return current, nil
		}
		current.PayoutAttempts++
		current.PayoutLastAttemptAt = time.Now().UTC()
		current.PayoutFailureReason = attemptErr.Error()
		return current, nil
	})
	if err != nil {
		return updated, err
	}
	return updated, attemptErr
}

// PayoutSweep drives EarningAvailable earnings into payout_pending and
// attempts them, and replays payout_pending earnings whose outcome was
// never durably recorded (a crash between committing the intent and
// observing the external result, or an ambiguous/pending adapter response
// on a prior sweep). Both paths funnel through attemptPayout so the primary
// path and crash recovery share identical logic and can never diverge.
//
// With no payoutAdapter configured (the default -- see
// config.PayoutBackendDisabled), this is a deliberate no-op: earnings
// mature to Available and stop there rather than being driven to Paid by
// an adapter that cannot actually move funds. This is what prevents a real
// provider liability from ever being marked paid when nothing was paid.
func (s *EarningsService) PayoutSweep(ctx context.Context, limit int) (int, error) {
	if s.payoutAdapter == nil {
		return 0, nil
	}
	var joined error
	count := 0
	available, err := s.store.EarningsAvailableForPayout(ctx, limit)
	if err != nil {
		return 0, err
	}
	for _, e := range available {
		started, err := s.beginPayoutUnderLock(ctx, e.ID)
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("begin payout %s: %w", e.ID, err))
			continue
		}
		count++
		if _, err := s.attemptPayout(ctx, started); err != nil {
			joined = errors.Join(joined, fmt.Errorf("attempt payout %s: %w", e.ID, err))
		}
	}
	pending, err := s.store.EarningsPayoutPending(ctx, time.Now().UTC().Add(-defaultPayoutRetryBackoff), limit)
	if err != nil {
		return count, errors.Join(joined, err)
	}
	for _, e := range pending {
		count++
		if _, err := s.attemptPayout(ctx, e); err != nil {
			joined = errors.Join(joined, fmt.Errorf("replay payout %s: %w", e.ID, err))
		}
	}
	return count, joined
}

// RunReconciler periodically runs the maturation and payout sweeps, mirroring
// JobService.RunReconciler's ticker-driven pattern.
func (s *EarningsService) RunReconciler(ctx context.Context, interval time.Duration, limit int, report func(error)) {
	if interval <= 0 {
		interval = defaultEarningsInterval
	}
	if limit <= 0 {
		limit = defaultEarningsBatch
	}
	sweep := func() {
		if _, err := s.BackfillSweep(ctx, limit); err != nil && report != nil {
			report(err)
		}
		if _, err := s.MaturationSweep(ctx, limit); err != nil && report != nil {
			report(err)
		}
		if _, err := s.PayoutSweep(ctx, limit); err != nil && report != nil {
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

// Get returns the earning if it exists and belongs to providerID.
func (s *EarningsService) Get(ctx context.Context, id, providerID string) (domain.ProviderEarning, error) {
	e, err := s.store.GetEarning(ctx, id)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.ProviderEarning{}, domain.NewError(domain.ErrNotFound, "earning not found", false)
		}
		return domain.ProviderEarning{}, err
	}
	if e.ProviderID != providerID {
		return domain.ProviderEarning{}, domain.NewError(domain.ErrPermissionDenied, "not the earning's owning provider", false)
	}
	return e, nil
}

// ListByProvider returns every earning owned by providerID.
func (s *EarningsService) ListByProvider(ctx context.Context, providerID string) ([]domain.ProviderEarning, error) {
	return s.store.EarningsByProvider(ctx, providerID)
}

// BillingSnapshotForJob returns the durable billing snapshot for jobID.
// Callers are responsible for verifying the caller is authorized to see
// this Job (e.g. via JobService.Get, which already checks ownership)
// before calling this.
func (s *EarningsService) BillingSnapshotForJob(ctx context.Context, jobID string) (domain.BillingSnapshot, error) {
	snap, err := s.store.BillingSnapshotByJob(ctx, jobID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.BillingSnapshot{}, domain.NewError(domain.ErrNotFound, "billing snapshot not found", false)
		}
		return domain.BillingSnapshot{}, err
	}
	return snap, nil
}
