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
	}
}

// Get implements atos_account / GET /account, seeding a default account on
// first access so a brand-new principal_id from Device Auth can transact
// immediately in this Phase 0 skeleton. A real accounts service would
// reject unknown principals instead.
func (s *AccountService) Get(ctx context.Context, principalID string) (domain.Account, error) {
	a, err := s.store.GetAccount(ctx, principalID)
	if err == nil {
		return a, nil
	}
	if err != store.ErrNotFound {
		return domain.Account{}, err
	}
	// UpdateAccount (not GetAccount+PutAccount) so two concurrent first
	// requests for the same brand-new principal can't both "win" a
	// double-seed race — the store lock serializes them.
	return s.store.UpdateAccount(ctx, principalID, defaultAccount(principalID), func(a domain.Account, exists bool) (domain.Account, error) {
		return a, nil
	})
}

// RequiresConfirmation reports whether totalMax exceeds the account's
// per-call autonomous limit, mirroring the Spending Policy flow in
// README.md: under the limit invokes silently, over it must come back as
// MCP input_required / a client-side confirmation prompt.
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

// Debit atomically checks balance + daily limit and reserves amount, all
// under the store's single UpdateAccount lock — no separate read then
// write, so two concurrent debits for the same principal can never both
// observe the pre-debit balance and overspend.
func (s *AccountService) Debit(ctx context.Context, principalID, amountStr, currency string) error {
	amount, err := money.Parse(amountStr, currency, accountDecimals)
	if err != nil {
		return domain.NewError(domain.ErrValidationFailed, "invalid amount", false)
	}

	_, err = s.store.UpdateAccount(ctx, principalID, defaultAccount(principalID), func(a domain.Account, exists bool) (domain.Account, error) {
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

// Credit reverses a Debit — used when an escrow releases or refunds unused
// funds back to the client. It restores both Balance and RemainingToday:
// a released/refunded amount was never actually spent, so it must not go
// on consuming the caller's daily autonomous-spend allowance (a fully
// refunded canceled job must not permanently eat into today's limit).
// RemainingToday is capped at DailyLimit in case of any rounding drift.
func (s *AccountService) Credit(ctx context.Context, principalID, amountStr, currency string) error {
	amount, err := money.Parse(amountStr, currency, accountDecimals)
	if err != nil {
		return domain.NewError(domain.ErrValidationFailed, "invalid amount", false)
	}
	if amount.IsZero() {
		return nil
	}

	_, err = s.store.UpdateAccount(ctx, principalID, defaultAccount(principalID), func(a domain.Account, exists bool) (domain.Account, error) {
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
