package service

import (
	"context"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
)

const accountDecimals = 2

type AccountService struct {
	store store.Store
}

func NewAccountService(s store.Store) *AccountService {
	return &AccountService{store: s}
}

func defaultAccount(principalID string) domain.Account {
	return domain.Account{
		PrincipalID: principalID,
		Balance:     domain.Money{Amount: "25.00", Currency: "USD"},
		SpendPolicy: domain.SpendPolicy{
			PerCallAutonomousLimit: domain.Money{Amount: "2.00", Currency: "USD"},
			DailyLimit:             domain.Money{Amount: "20.00", Currency: "USD"},
			RemainingToday:         domain.Money{Amount: "20.00", Currency: "USD"},
		},
		TrustPolicy: domain.TrustPolicy{
			DefaultRequestedTrustMode: domain.RequestedTrustAuto,
			MinimumForHighValue:       domain.TrustModeVerified,
		},
	}
}

func normalizeAccount(a domain.Account) domain.Account {
	if a.TrustPolicy.DefaultRequestedTrustMode == "" {
		a.TrustPolicy = defaultAccount(a.PrincipalID).TrustPolicy
	}
	return a
}

func (s *AccountService) Get(ctx context.Context, principalID string) (domain.Account, error) {
	a, err := s.store.GetAccount(ctx, principalID)
	if err == nil {
		normalized := normalizeAccount(a)
		if normalized.TrustPolicy.DefaultRequestedTrustMode != a.TrustPolicy.DefaultRequestedTrustMode {
			_ = s.store.PutAccount(ctx, normalized)
		}
		return normalized, nil
	}
	if err != store.ErrNotFound {
		return domain.Account{}, err
	}
	return s.store.UpdateAccount(ctx, principalID, defaultAccount(principalID), func(a domain.Account, exists bool) (domain.Account, error) {
		return normalizeAccount(a), nil
	})
}

func (s *AccountService) RequiresConfirmation(ctx context.Context, principalID, totalMax, currency string) (bool, error) {
	a, err := s.Get(ctx, principalID)
	if err != nil {
		return false, err
	}
	total, err := money.Parse(totalMax, currency, accountDecimals)
	if err != nil {
		return false, domain.NewError(domain.ErrValidationFailed, "invalid amount", false)
	}
	limit, err := money.Parse(a.SpendPolicy.PerCallAutonomousLimit.Amount, a.SpendPolicy.PerCallAutonomousLimit.Currency, accountDecimals)
	if err != nil {
		return false, err
	}
	if total.Currency != limit.Currency {
		return false, domain.NewError(domain.ErrValidationFailed, "currency mismatch with spend policy", false)
	}
	return total.Cmp(limit) > 0, nil
}

func (s *AccountService) Debit(ctx context.Context, principalID, amountStr, currency string) error {
	amount, err := money.Parse(amountStr, currency, accountDecimals)
	if err != nil {
		return domain.NewError(domain.ErrValidationFailed, "invalid amount", false)
	}

	_, err = s.store.UpdateAccount(ctx, principalID, defaultAccount(principalID), func(a domain.Account, exists bool) (domain.Account, error) {
		a = normalizeAccount(a)
		balance, err := money.Parse(a.Balance.Amount, a.Balance.Currency, accountDecimals)
		if err != nil {
			return domain.Account{}, err
		}
		remaining, err := money.Parse(a.SpendPolicy.RemainingToday.Amount, a.SpendPolicy.RemainingToday.Currency, accountDecimals)
		if err != nil {
			return domain.Account{}, err
		}
		if amount.Currency != balance.Currency {
			return domain.Account{}, domain.NewError(domain.ErrValidationFailed, "currency mismatch with account balance", false)
		}
		if amount.Cmp(remaining) > 0 {
			return domain.Account{}, domain.NewError(domain.ErrSpendLimitExceeded, "daily autonomous spend limit exceeded", false)
		}
		newBalance, err := balance.Sub(amount)
		if err != nil {
			return domain.Account{}, domain.NewError(domain.ErrInsufficientBalance, "insufficient balance", false)
		}
		newRemaining, err := remaining.Sub(amount)
		if err != nil {
			return domain.Account{}, domain.NewError(domain.ErrSpendLimitExceeded, "daily autonomous spend limit exceeded", false)
		}

		a.Balance = domain.Money{Amount: newBalance.String(), Currency: newBalance.Currency}
		a.SpendPolicy.RemainingToday = domain.Money{Amount: newRemaining.String(), Currency: newRemaining.Currency}
		return a, nil
	})
	return err
}

func (s *AccountService) Credit(ctx context.Context, principalID, amountStr, currency string) error {
	amount, err := money.Parse(amountStr, currency, accountDecimals)
	if err != nil {
		return domain.NewError(domain.ErrValidationFailed, "invalid amount", false)
	}
	if amount.IsZero() {
		return nil
	}

	_, err = s.store.UpdateAccount(ctx, principalID, defaultAccount(principalID), func(a domain.Account, exists bool) (domain.Account, error) {
		a = normalizeAccount(a)
		balance, err := money.Parse(a.Balance.Amount, a.Balance.Currency, accountDecimals)
		if err != nil {
			return domain.Account{}, err
		}
		remaining, err := money.Parse(a.SpendPolicy.RemainingToday.Amount, a.SpendPolicy.RemainingToday.Currency, accountDecimals)
		if err != nil {
			return domain.Account{}, err
		}
		dailyLimit, err := money.Parse(a.SpendPolicy.DailyLimit.Amount, a.SpendPolicy.DailyLimit.Currency, accountDecimals)
		if err != nil {
			return domain.Account{}, err
		}
		if amount.Currency != balance.Currency {
			return domain.Account{}, domain.NewError(domain.ErrValidationFailed, "currency mismatch with account balance", false)
		}

		newBalance, err := balance.Add(amount)
		if err != nil {
			return domain.Account{}, err
		}
		newRemaining, err := remaining.Add(amount)
		if err != nil {
			return domain.Account{}, err
		}
		if newRemaining.Cmp(dailyLimit) > 0 {
			newRemaining = dailyLimit
		}

		a.Balance = domain.Money{Amount: newBalance.String(), Currency: newBalance.Currency}
		a.SpendPolicy.RemainingToday = domain.Money{Amount: newRemaining.String(), Currency: newRemaining.Currency}
		return a, nil
	})
	return err
}
