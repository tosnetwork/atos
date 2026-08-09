package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// billingSnapshotContentHash summarizes the semantically meaningful
// economic fields of a BillingSnapshot -- everything except CalculatedAt,
// which legitimately differs between the original computation and a later
// idempotent recomputation -- so a replayed PutBillingSnapshot for the same
// JobID can be recognized as identical (safe no-op) versus a computation
// that produced different economic content for the same Job (rejected).
func billingSnapshotContentHash(snap domain.BillingSnapshot) string {
	encoded, _ := json.Marshal(struct {
		JobID, QuoteID, ReceiptID, ProviderID, CapabilityID, CapabilityVersion string
		TrustMode                                                              domain.TrustMode
		Usage                                                                  domain.Usage
		UsageCommitment                                                        string
		PricingModel                                                           domain.PricingModel
		PricingTermsHash                                                       string
		GrossCharge, ProviderGross, GatewayFee, PrincipalRefund                domain.Money
	}{
		snap.JobID, snap.QuoteID, snap.ReceiptID, snap.ProviderID, snap.CapabilityID, snap.CapabilityVersion,
		snap.TrustMode, snap.Usage, snap.UsageCommitment, snap.PricingModel, snap.PricingTermsHash,
		snap.GrossCharge, snap.ProviderGross, snap.GatewayFee, snap.PrincipalRefund,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// earningContentHash summarizes the identity+economic fields of a
// ProviderEarning that must never differ for a given SettlementID --
// deliberately excluding lifecycle fields (Status, CreatedAt, MaturesAt,
// payout checkpoints, ...) which legitimately change over the earning's
// life and differ between the original create and a later retry.
func earningContentHash(e domain.ProviderEarning) string {
	encoded, _ := json.Marshal(struct {
		ProviderID, JobID, QuoteID, ReceiptID, SettlementID, CapabilityID, CapabilityVersion string
		GrossAmount, GatewayFee, NetAmount                                                   domain.Money
	}{
		e.ProviderID, e.JobID, e.QuoteID, e.ReceiptID, e.SettlementID, e.CapabilityID, e.CapabilityVersion,
		e.GrossAmount, e.GatewayFee, e.NetAmount,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func (s *Store) PutBillingSnapshot(ctx context.Context, snap domain.BillingSnapshot) (domain.BillingSnapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.billingSnapshots[snap.JobID]; ok {
		if billingSnapshotContentHash(existing) != billingSnapshotContentHash(snap) {
			return domain.BillingSnapshot{}, false, domain.NewError(domain.ErrIdempotencyConflict, "billing snapshot already exists for this job with different economic content", false)
		}
		return existing, false, nil
	}
	s.billingSnapshots[snap.JobID] = snap
	return snap, true, nil
}

func (s *Store) BillingSnapshotByJob(ctx context.Context, jobID string) (domain.BillingSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, ok := s.billingSnapshots[jobID]
	if !ok {
		return domain.BillingSnapshot{}, store.ErrNotFound
	}
	return snap, nil
}

// CreateEarning enforces settlement_id uniqueness under the same store
// mutex every other collection uses, mirroring the atomicity a Postgres
// UNIQUE constraint + ON CONFLICT DO NOTHING gives the real implementation.
func (s *Store) CreateEarning(ctx context.Context, e domain.ProviderEarning) (domain.ProviderEarning, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existingID, ok := s.earningsBySettlement[e.SettlementID]; ok {
		existing := s.earnings[existingID]
		if earningContentHash(existing) != earningContentHash(e) {
			return domain.ProviderEarning{}, false, domain.NewError(domain.ErrIdempotencyConflict, "an earning already exists for this settlement with different identity/economic fields", false)
		}
		return existing, false, nil
	}
	s.earnings[e.ID] = e
	s.earningsBySettlement[e.SettlementID] = e.ID
	s.earningsByJob[e.JobID] = e.ID
	return e, true, nil
}

func (s *Store) GetEarning(ctx context.Context, id string) (domain.ProviderEarning, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.earnings[id]
	if !ok {
		return domain.ProviderEarning{}, store.ErrNotFound
	}
	return e, nil
}

func (s *Store) EarningBySettlement(ctx context.Context, settlementID string) (domain.ProviderEarning, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.earningsBySettlement[settlementID]
	if !ok {
		return domain.ProviderEarning{}, store.ErrNotFound
	}
	return s.earnings[id], nil
}

func (s *Store) EarningByJob(ctx context.Context, jobID string) (domain.ProviderEarning, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.earningsByJob[jobID]
	if !ok {
		return domain.ProviderEarning{}, store.ErrNotFound
	}
	return s.earnings[id], nil
}

func (s *Store) EarningsByProvider(ctx context.Context, providerID string) ([]domain.ProviderEarning, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.ProviderEarning, 0)
	for _, e := range s.earnings {
		if e.ProviderID == providerID {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out, nil
}

func (s *Store) EarningsMaturing(ctx context.Context, before time.Time, limit int) ([]domain.ProviderEarning, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return filterEarningsLocked(s.earnings, limit, func(e domain.ProviderEarning) bool {
		return e.Status == domain.EarningMaturing && !e.MaturesAt.After(before)
	}), nil
}

func (s *Store) EarningsAvailableForPayout(ctx context.Context, limit int) ([]domain.ProviderEarning, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return filterEarningsLocked(s.earnings, limit, func(e domain.ProviderEarning) bool {
		return e.Status == domain.EarningAvailable
	}), nil
}

func (s *Store) EarningsPayoutPending(ctx context.Context, before time.Time, limit int) ([]domain.ProviderEarning, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return filterEarningsLocked(s.earnings, limit, func(e domain.ProviderEarning) bool {
		return e.Status == domain.EarningPayoutPending &&
			e.PayoutRequestedAt != nil && !e.PayoutRequestedAt.After(before)
	}), nil
}

func filterEarningsLocked(all map[string]domain.ProviderEarning, limit int, match func(domain.ProviderEarning) bool) []domain.ProviderEarning {
	if limit <= 0 {
		limit = 100
	}
	out := make([]domain.ProviderEarning, 0, limit)
	for _, e := range all {
		if !match(e) {
			continue
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *Store) SettledJobsMissingEarning(ctx context.Context, limit int) ([]domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	hasEarning := make(map[string]bool, len(s.earnings))
	for _, e := range s.earnings {
		hasEarning[e.JobID] = true
	}
	out := make([]domain.Job, 0, limit)
	for _, j := range s.jobs {
		if j.EconomicState != domain.EconomicSettled || hasEarning[j.ID] {
			continue
		}
		out = append(out, j)
		if len(out) >= limit {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *Store) UpdateEarning(ctx context.Context, id string, fn func(domain.ProviderEarning, bool) (domain.ProviderEarning, error)) (domain.ProviderEarning, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.earnings[id]
	next, err := fn(current, exists)
	if err != nil {
		return domain.ProviderEarning{}, err
	}
	// Identity/economic fields (ProviderID, SettlementID, GrossAmount,
	// GatewayFee, NetAmount, ...) are immutable for the lifetime of an
	// earning once created -- only lifecycle fields (Status, timestamps,
	// payout checkpoints) may legitimately change through UpdateEarning. A
	// callback that changes economic content is always a bug, not a valid
	// state transition, so it is rejected here rather than silently
	// persisted.
	if exists && earningContentHash(current) != earningContentHash(next) {
		return domain.ProviderEarning{}, domain.NewError(domain.ErrIdempotencyConflict, "earning update must not change identity/economic fields", false)
	}
	// ID is deliberately excluded from earningContentHash (CreateEarning
	// must recognize a replay with a different candidate ID as the same
	// settlement), so it needs its own explicit immutability check here:
	// changing it would move the earning to a different map key while
	// leaving the old key's entry stale and s.earningsBySettlement pointing
	// at the new key, corrupting this store's internal indexes.
	if exists && next.ID != current.ID {
		return domain.ProviderEarning{}, domain.NewError(domain.ErrIdempotencyConflict, "earning update must not change the earning id", false)
	}
	s.earnings[id] = next
	if next.SettlementID != "" {
		s.earningsBySettlement[next.SettlementID] = next.ID
	}
	return next, nil
}
