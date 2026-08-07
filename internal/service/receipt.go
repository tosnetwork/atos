package service

import (
	"context"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
)

type ReceiptService struct {
	store store.Store
	core  toscore.Core
}

func NewReceiptService(s store.Store, core toscore.Core) *ReceiptService {
	return &ReceiptService{store: s, core: core}
}

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

func (s *ReceiptService) ByJob(ctx context.Context, jobID, principalID string) (domain.Receipt, error) {
	r, err := s.store.ReceiptByJob(ctx, jobID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Receipt{}, domain.NewError(domain.ErrNotFound, "receipt not found for job", false)
		}
		return domain.Receipt{}, err
	}
	if r.PrincipalID != principalID {
		return domain.Receipt{}, domain.NewError(domain.ErrPermissionDenied, "not the receipt's owning principal", false)
	}
	return r, nil
}

func (s *ReceiptService) ListByPrincipal(ctx context.Context, principalID string) ([]domain.Receipt, error) {
	return s.store.ReceiptsByPrincipal(ctx, principalID)
}

func (s *ReceiptService) SettlementProof(ctx context.Context, id, principalID string) (map[string]any, error) {
	if _, err := s.Get(ctx, id, principalID); err != nil {
		return nil, err
	}
	return s.core.ReadProof(ctx, id)
}

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
		TotalJobs: len(jobs),
		TotalCharged: domain.Money{Amount: "0.00", Currency: "USD"},
		TotalRefunded: domain.Money{Amount: "0.00", Currency: "USD"},
	}
	for _, job := range jobs {
		switch job.State {
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
	for _, receipt := range receipts {
		if receipt.Charged.Currency == charged.Currency {
			if amount, err := money.Parse(receipt.Charged.Amount, receipt.Charged.Currency, usageDecimals); err == nil {
				if sum, err := charged.Add(amount); err == nil {
					charged = sum
				}
			}
		}
		if receipt.Refunded.Currency == refunded.Currency {
			if amount, err := money.Parse(receipt.Refunded.Amount, receipt.Refunded.Currency, usageDecimals); err == nil {
				if sum, err := refunded.Add(amount); err == nil {
					refunded = sum
				}
			}
		}
	}
	summary.TotalCharged = domain.Money{Amount: charged.String(), Currency: charged.Currency}
	summary.TotalRefunded = domain.Money{Amount: refunded.String(), Currency: refunded.Currency}
	return summary, nil
}
