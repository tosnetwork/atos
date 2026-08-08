package service

import (
	"context"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
)

const accountDecimals = 2

type AccountDefaults struct {
	InitialBalance domain.Money
	PerCallLimit   domain.Money
	DailyLimit     domain.Money
}

func DefaultAccountDefaults() AccountDefaults {
	return AccountDefaults{
		InitialBalance: domain.Money{Amount: "25.00", Currency: "USD"},
		PerCallLimit:   domain.Money{Amount: "2.00", Currency: "USD"},
		DailyLimit:     domain.Money{Amount: "20.00", Currency: "USD"},
	}
}

func ValidateAccountDefaults(defaults AccountDefaults) error {
	balance, err := money.Parse(defaults.InitialBalance.Amount, defaults.InitialBalance.Currency, accountDecimals)
	if err != nil || balance.IsZero() {
		return domain.NewError(domain.ErrValidationFailed, "invalid managed initial balance", false)
	}
	perCall, err := money.Parse(defaults.PerCallLimit.Amount, defaults.PerCallLimit.Currency, accountDecimals)
	if err != nil {
		return domain.NewError(domain.ErrValidationFailed, "invalid managed per-call limit", false)
	}
	daily, err := money.Parse(defaults.DailyLimit.Amount, defaults.DailyLimit.Currency, accountDecimals)
	if err != nil || daily.IsZero() {
		return domain.NewError(domain.ErrValidationFailed, "invalid managed daily limit", false)
	}
	if balance.Currency != perCall.Currency || balance.Currency != daily.Currency {
		return domain.NewError(domain.ErrValidationFailed, "managed account defaults must use one currency", false)
	}
	if perCall.Cmp(daily) > 0 {
		return domain.NewError(domain.ErrValidationFailed, "managed per-call limit exceeds daily limit", false)
	}
	return nil
}

type AccountService struct {
	store    store.Store
	defaults AccountDefaults
}

func NewAccountService(s store.Store, configured ...AccountDefaults) *AccountService {
	defaults := DefaultAccountDefaults()
	if len(configured) > 0 {
		defaults = configured[0]
	}
	if err := ValidateAccountDefaults(defaults); err != nil {
		panic(err)
	}
	return &AccountService{store: s, defaults: defaults}
}

func (s *AccountService) defaultAccount(principalID string) domain.Account {
	return domain.Account{
		PrincipalID: principalID,
		Balance:     s.defaults.InitialBalance,
		SpendPolicy: domain.SpendPolicy{
			PerCallAutonomousLimit: s.defaults.PerCallLimit,
			DailyLimit:             s.defaults.DailyLimit,
			RemainingToday:         s.defaults.DailyLimit,
		},
		TrustPolicy: domain.TrustPolicy{
			DefaultRequestedTrustMode: domain.RequestedTrustAuto,
			MinimumForHighValue:       domain.TrustModeVerified,
		},
	}
}

func (s *AccountService) normalizeAccount(a domain.Account) domain.Account {
	if a.TrustPolicy.DefaultRequestedTrustMode == "" {
		a.TrustPolicy = s.defaultAccount(a.PrincipalID).TrustPolicy
	}
	return a
}

func (s *AccountService) Get(ctx context.Context, principalID string) (domain.Account, error) {
	a, err := s.store.GetAccount(ctx, principalID)
	if err == nil {
		normalized := s.normalizeAccount(a)
		if normalized.TrustPolicy.DefaultRequestedTrustMode != a.TrustPolicy.DefaultRequestedTrustMode {
			_ = s.store.PutAccount(ctx, normalized)
		}
		return normalized, nil
	}
	if err != store.ErrNotFound {
		return domain.Account{}, err
	}
	return s.store.UpdateAccount(ctx, principalID, s.defaultAccount(principalID), func(a domain.Account, exists bool) (domain.Account, error) {
		return s.normalizeAccount(a), nil
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

	_, err = s.store.UpdateAccount(ctx, principalID, s.defaultAccount(principalID), func(a domain.Account, exists bool) (domain.Account, error) {
		a = s.normalizeAccount(a)
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

	_, err = s.store.UpdateAccount(ctx, principalID, s.defaultAccount(principalID), func(a domain.Account, exists bool) (domain.Account, error) {
		a = s.normalizeAccount(a)
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
