package memory

import (
	"context"
	"sort"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func (s *Store) PutBillingSnapshot(ctx context.Context, snap domain.BillingSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.billingSnapshots[snap.JobID] = snap
	return nil
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
		return s.earnings[existingID], false, nil
	}
	s.earnings[e.ID] = e
	s.earningsBySettlement[e.SettlementID] = e.ID
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
	s.earnings[id] = next
	if next.SettlementID != "" {
		s.earningsBySettlement[next.SettlementID] = next.ID
	}
	return next, nil
}
