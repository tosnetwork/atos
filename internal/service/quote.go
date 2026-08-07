package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
)

const (
	quoteTTL       = 10 * time.Minute
	quoteDecimals  = 2
	defaultFeeRate = 0.05
)

type QuoteService struct {
	store store.Store
}

func NewQuoteService(s store.Store) *QuoteService {
	return &QuoteService{store: s}
}

type CreateQuoteInput struct {
	CapabilityID string
	// MaxTotal, if set, caps the quote per the caller's constraints
	// (docs/MCP.md atos_quote "constraints.max_total"). A quote priced
	// above it, or quoted in a different currency than requested, is
	// rejected rather than silently capped or reinterpreted.
	MaxTotal *domain.Money
}

// Create implements atos_quote / POST /quotes. Pricing is derived from the
// capability's price_hint — real usage-based pricing (metered/per_unit)
// needs an input_summary to size the job, which is out of scope for this
// Phase 0 skeleton and left as a TODO for whoever wires in real providers.
func (s *QuoteService) Create(ctx context.Context, in CreateQuoteInput) (domain.Quote, error) {
	cap, err := s.store.Get(ctx, in.CapabilityID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Quote{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability not found", false)
		}
		return domain.Quote{}, err
	}
	if cap.Status != domain.CapabilityActive {
		return domain.Quote{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability is not active", false)
	}

	subtotal, err := money.Parse(cap.Pricing.PriceHint.Amount, cap.Pricing.PriceHint.Currency, quoteDecimals)
	if err != nil {
		return domain.Quote{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability has an invalid price_hint", false)
	}
	fees, err := applyFeeRate(subtotal, defaultFeeRate)
	if err != nil {
		return domain.Quote{}, err
	}
	totalMax, err := subtotal.Add(fees)
	if err != nil {
		return domain.Quote{}, err
	}

	if in.MaxTotal != nil {
		if in.MaxTotal.Currency == "" {
			return domain.Quote{}, domain.NewError(domain.ErrValidationFailed, "constraints.max_total.currency is required", false)
		}
		if in.MaxTotal.Currency != subtotal.Currency {
			return domain.Quote{}, domain.NewError(domain.ErrValidationFailed,
				fmt.Sprintf("constraints.max_total is in %s but this capability prices in %s", in.MaxTotal.Currency, subtotal.Currency), false)
		}
		bound, err := money.Parse(in.MaxTotal.Amount, subtotal.Currency, quoteDecimals)
		if err != nil {
			return domain.Quote{}, domain.NewError(domain.ErrValidationFailed, "invalid constraints.max_total.amount", false)
		}
		if totalMax.Cmp(bound) > 0 {
			return domain.Quote{}, domain.NewError(domain.ErrValidationFailed, "capability price exceeds requested max_total", false)
		}
	}

	now := time.Now().UTC()
	q := domain.Quote{
		ID:                "q_" + uuid.NewString(),
		CapabilityID:      cap.ID,
		CapabilityVersion: cap.Version,
		Price: domain.Price{
			Subtotal: subtotal.String(),
			Fees:     fees.String(),
			TotalMax: totalMax.String(),
			Currency: subtotal.Currency,
		},
		ExpiresAt: now.Add(quoteTTL),
		TermsHash: termsHash(cap.ID, cap.Version, totalMax.String()),
		CreatedAt: now,
	}
	if err := s.store.PutQuote(ctx, q); err != nil {
		return domain.Quote{}, err
	}
	return q, nil
}

func (s *QuoteService) Get(ctx context.Context, id string) (domain.Quote, error) {
	q, err := s.store.GetQuote(ctx, id)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Quote{}, domain.NewError(domain.ErrNotFound, "quote not found", false)
		}
		return domain.Quote{}, err
	}
	return q, nil
}

// applyFeeRate computes amount * ratePerMille / 1000 entirely in integer
// big.Int arithmetic — the fee rate constant is the only float in this
// path, and it never touches a money.Amount value directly.
func applyFeeRate(amount money.Amount, rate float64) (money.Amount, error) {
	ratePerMille := big.NewInt(int64(rate * 1000))
	scaled := new(big.Int).Mul(amount.Minor, ratePerMille)
	scaled.Div(scaled, big.NewInt(1000))
	return money.Amount{Minor: scaled, Currency: amount.Currency, Decimals: amount.Decimals}, nil
}

func termsHash(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return "sha256:" + hex.EncodeToString(h[:])
}
