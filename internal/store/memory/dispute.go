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

// disputeContentHash summarizes the identity+economic fields of a Dispute
// that must never differ for a given JobID -- deliberately excluding
// lifecycle fields (ReviewStatus, EconomicState, Outcome, ReviewerID,
// ReasonRejected, timestamps) which legitimately change over the dispute's
// life. Mirrors earningContentHash's role.
func disputeContentHash(d domain.Dispute) string {
	encoded, _ := json.Marshal(struct {
		PrincipalID, ProviderID, JobID, QuoteID, CapabilityID, ReceiptID, SettlementID, EarningID string
		ChargedAmount, OriginalRefundAmount                                                       domain.Money
		Reason, Description                                                                       string
		Evidence                                                                                  []domain.DisputeEvidence
		DisputePolicyHash                                                                         string
	}{
		d.PrincipalID, d.ProviderID, d.JobID, d.QuoteID, d.CapabilityID, d.ReceiptID, d.SettlementID, d.EarningID,
		d.ChargedAmount, d.OriginalRefundAmount, d.Reason, d.Description, d.Evidence, d.DisputePolicyHash,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func (s *Store) OpenDispute(ctx context.Context, jobID string, build func(domain.ProviderEarning, bool) (domain.Dispute, domain.ProviderEarning, error)) (domain.Dispute, domain.ProviderEarning, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existingID, ok := s.disputesByJob[jobID]; ok {
		return s.disputes[existingID], domain.ProviderEarning{}, false, nil
	}
	earningID, earningExists := s.earningsByJob[jobID]
	earning := s.earnings[earningID]
	dispute, nextEarning, err := build(earning, earningExists)
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, false, err
	}
	if earningExists {
		if nextEarning.ID != earning.ID {
			return domain.Dispute{}, domain.ProviderEarning{}, false, domain.NewError(domain.ErrIdempotencyConflict, "dispute open must not change the earning id", false)
		}
		if earningContentHash(earning) != earningContentHash(nextEarning) {
			return domain.Dispute{}, domain.ProviderEarning{}, false, domain.NewError(domain.ErrIdempotencyConflict, "dispute open must not change identity/economic earning fields", false)
		}
		s.earnings[nextEarning.ID] = nextEarning
	}
	s.disputes[dispute.ID] = dispute
	s.disputesByJob[jobID] = dispute.ID
	return dispute, nextEarning, true, nil
}

func (s *Store) GetDispute(ctx context.Context, id string) (domain.Dispute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.disputes[id]
	if !ok {
		return domain.Dispute{}, store.ErrNotFound
	}
	return d, nil
}

func (s *Store) DisputeByJob(ctx context.Context, jobID string) (domain.Dispute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.disputesByJob[jobID]
	if !ok {
		return domain.Dispute{}, store.ErrNotFound
	}
	return s.disputes[id], nil
}

func (s *Store) DisputeByIdempotencyKey(ctx context.Context, principalID, key string) (domain.Dispute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range s.disputes {
		if d.PrincipalID == principalID && d.IdempotencyKey == key {
			return d, nil
		}
	}
	return domain.Dispute{}, store.ErrNotFound
}

func (s *Store) DisputesByPrincipal(ctx context.Context, principalID string) ([]domain.Dispute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Dispute, 0)
	for _, d := range s.disputes {
		if d.PrincipalID == principalID {
			out = append(out, d)
		}
	}
	sortDisputesNewestFirst(out)
	return out, nil
}

func (s *Store) DisputesByProvider(ctx context.Context, providerID string) ([]domain.Dispute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Dispute, 0)
	for _, d := range s.disputes {
		if d.ProviderID == providerID {
			out = append(out, d)
		}
	}
	sortDisputesNewestFirst(out)
	return out, nil
}

func (s *Store) DisputesUnderReview(ctx context.Context, limit int) ([]domain.Dispute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	out := make([]domain.Dispute, 0, limit)
	for _, d := range s.disputes {
		if d.ReviewStatus != domain.DisputeOpened && d.ReviewStatus != domain.DisputeUnderReview {
			continue
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OpenedAt.Equal(out[j].OpenedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].OpenedAt.Before(out[j].OpenedAt)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) DisputesForRecovery(ctx context.Context, updatedBefore time.Time, limit int) ([]domain.Dispute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	out := make([]domain.Dispute, 0, limit)
	for _, d := range s.disputes {
		if d.EconomicState.Terminal() {
			continue
		}
		if d.EconomicState != domain.DisputeEconomicPendingPayoutResolution && d.EconomicState != domain.DisputeEconomicRefundPending {
			continue
		}
		if d.UpdatedAt.After(updatedBefore) {
			continue
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].UpdatedAt.Before(out[j].UpdatedAt)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) UpdateDispute(ctx context.Context, id string, fn func(domain.Dispute, bool) (domain.Dispute, error)) (domain.Dispute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.disputes[id]
	next, err := fn(current, exists)
	if err != nil {
		return domain.Dispute{}, err
	}
	if exists {
		if next.ID != current.ID {
			return domain.Dispute{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change the dispute id", false)
		}
		if disputeContentHash(current) != disputeContentHash(next) {
			return domain.Dispute{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change identity/economic fields", false)
		}
	}
	s.disputes[id] = next
	if next.JobID != "" {
		s.disputesByJob[next.JobID] = next.ID
	}
	return next, nil
}

func (s *Store) UpdateDisputeAndEarning(ctx context.Context, disputeID string, fn func(domain.Dispute, domain.ProviderEarning, bool) (domain.Dispute, domain.ProviderEarning, error)) (domain.Dispute, domain.ProviderEarning, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	currentDispute, exists := s.disputes[disputeID]
	if !exists {
		return domain.Dispute{}, domain.ProviderEarning{}, store.ErrNotFound
	}
	earningID, earningExists := s.earningsByJob[currentDispute.JobID]
	currentEarning := s.earnings[earningID]
	nextDispute, nextEarning, err := fn(currentDispute, currentEarning, earningExists)
	if err != nil {
		return domain.Dispute{}, domain.ProviderEarning{}, err
	}
	if nextDispute.ID != currentDispute.ID {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change the dispute id", false)
	}
	if disputeContentHash(currentDispute) != disputeContentHash(nextDispute) {
		return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change identity/economic fields", false)
	}
	if earningExists {
		if nextEarning.ID != currentEarning.ID {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change the earning id", false)
		}
		if earningContentHash(currentEarning) != earningContentHash(nextEarning) {
			return domain.Dispute{}, domain.ProviderEarning{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change identity/economic earning fields", false)
		}
		s.earnings[nextEarning.ID] = nextEarning
	}
	s.disputes[disputeID] = nextDispute
	return nextDispute, nextEarning, nil
}

func (s *Store) UpdateDisputeAndAccount(ctx context.Context, disputeID, principalID string, seed domain.Account, fn func(domain.Dispute, bool, domain.Account, bool) (domain.Dispute, domain.Account, error)) (domain.Dispute, domain.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	currentDispute, disputeExists := s.disputes[disputeID]
	if !disputeExists {
		return domain.Dispute{}, domain.Account{}, store.ErrNotFound
	}
	account, accountExists := s.accounts[principalID]
	if !accountExists {
		account = seed
	}
	nextDispute, nextAccount, err := fn(currentDispute, disputeExists, account, accountExists)
	if err != nil {
		return domain.Dispute{}, domain.Account{}, err
	}
	if nextDispute.ID != currentDispute.ID {
		return domain.Dispute{}, domain.Account{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change the dispute id", false)
	}
	if disputeContentHash(currentDispute) != disputeContentHash(nextDispute) {
		return domain.Dispute{}, domain.Account{}, domain.NewError(domain.ErrIdempotencyConflict, "dispute update must not change identity/economic fields", false)
	}
	if nextAccount.PrincipalID != principalID {
		return domain.Dispute{}, domain.Account{}, store.ErrConflict
	}
	s.disputes[disputeID] = nextDispute
	s.accounts[principalID] = nextAccount
	return nextDispute, nextAccount, nil
}

func sortDisputesNewestFirst(out []domain.Dispute) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].OpenedAt.Equal(out[j].OpenedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].OpenedAt.After(out[j].OpenedAt)
	})
}
