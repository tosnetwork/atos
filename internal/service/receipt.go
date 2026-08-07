package service

import (
	"context"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
)

// ReceiptService implements the receipts/usage half of docs/API.md's
// Account Endpoints and Public/Metadata Endpoints — everything that reads
// already-settled financial history rather than creating new obligations.
type ReceiptService struct {
	store store.Store
	core  toscore.Core
}

func NewReceiptService(s store.Store, core toscore.Core) *ReceiptService {
	return &ReceiptService{store: s, core: core}
}

// Get implements GET /receipts/{id}, scoped to the requesting principal —
// a receipt is private financial history, not a public object.
func (s *ReceiptService) Get(ctx context.Context, id, principalID string) (domain.Receipt, error) {
	r, err := s.store.GetReceipt(ctx, id)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Receipt{}, domain.NewError(domain.ErrNotFound, "receipt not found", false)
		}
		return domain.Receipt{}, err
	}
	if r.PrincipalID != principalID {
		return domain.Receipt{}, domain.NewError(domain.ErrPermissionDenied, "not the receipt's owning principal", false)
	}
	return r, nil
}

// ListByPrincipal implements GET /account/receipts.
func (s *ReceiptService) ListByPrincipal(ctx context.Context, principalID string) ([]domain.Receipt, error) {
	return s.store.ReceiptsByPrincipal(ctx, principalID)
}

// SettlementProof implements GET /receipts/{id}/settlement-proof — the
// explicit opt-in path from docs/SETTLEMENT.md's "TOS Abstraction
// Boundary": ordinary receipts stay chain-detail-free, this endpoint is
// for callers who explicitly ask for verifiability.
func (s *ReceiptService) SettlementProof(ctx context.Context, id, principalID string) (map[string]any, error) {
	if _, err := s.Get(ctx, id, principalID); err != nil {
		return nil, err
	}
	return s.core.ReadProof(ctx, id)
}

// UsageSummary implements GET /account/usage: a Phase 0 aggregate over the
// principal's own job/receipt history. Real usage analytics (time-boxed
// windows, per-capability breakdowns) are a Phase 1+ concern — this is
// enough to show a caller "what happened to my money and jobs" today.
type UsageSummary struct {
	TotalJobs     int          `json:"total_jobs"`
	Completed     int          `json:"completed"`
	Failed        int          `json:"failed"`
	Canceled      int          `json:"canceled"`
	InProgress    int          `json:"in_progress"`
	TotalCharged  domain.Money `json:"total_charged"`
	TotalRefunded domain.Money `json:"total_refunded"`
}

const usageDecimals = 2

func (s *ReceiptService) UsageSummary(ctx context.Context, principalID string) (UsageSummary, error) {
	jobs, err := s.store.JobsByPrincipal(ctx, principalID)
	if err != nil {
		return UsageSummary{}, err
	}
	receipts, err := s.store.ReceiptsByPrincipal(ctx, principalID)
	if err != nil {
		return UsageSummary{}, err
	}

	summary := UsageSummary{
		TotalJobs:     len(jobs),
		TotalCharged:  domain.Money{Amount: "0.00", Currency: "USD"},
		TotalRefunded: domain.Money{Amount: "0.00", Currency: "USD"},
	}
	for _, j := range jobs {
		switch j.State {
		case domain.JobCompleted:
			summary.Completed++
		case domain.JobFailed, domain.JobRejected:
			summary.Failed++
		case domain.JobCanceled:
			summary.Canceled++
		default:
			summary.InProgress++
		}
	}

	charged := money.Zero("USD", usageDecimals)
	refunded := money.Zero("USD", usageDecimals)
	for _, r := range receipts {
		if r.Charged.Currency == charged.Currency {
			if amt, err := money.Parse(r.Charged.Amount, r.Charged.Currency, usageDecimals); err == nil {
				if sum, err := charged.Add(amt); err == nil {
					charged = sum
				}
			}
		}
		if r.Refunded.Currency == refunded.Currency {
			if amt, err := money.Parse(r.Refunded.Amount, r.Refunded.Currency, usageDecimals); err == nil {
				if sum, err := refunded.Add(amt); err == nil {
					refunded = sum
				}
			}
		}
	}
	summary.TotalCharged = domain.Money{Amount: charged.String(), Currency: charged.Currency}
	summary.TotalRefunded = domain.Money{Amount: refunded.String(), Currency: refunded.Currency}
	return summary, nil
}
