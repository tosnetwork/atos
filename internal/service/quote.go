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
	CapabilityID       string
	InputSummary       map[string]any
	RequestedTrustMode domain.RequestedTrustMode
	ProofRequirements  domain.ProofRequirements
	MaxTotal           *domain.Money
}

func (s *QuoteService) Create(ctx context.Context, in CreateQuoteInput) (domain.Quote, error) {
	cap, err := s.store.Get(ctx, in.CapabilityID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Quote{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability not found", false)
		}
		return domain.Quote{}, err
	}
	cap = normalizeCapability(cap)
	if cap.Status != domain.CapabilityActive {
		return domain.Quote{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability is not active", false)
	}

	requested := in.RequestedTrustMode
	if requested == "" {
		requested = domain.RequestedTrustAuto
	}
	mode, profile, resolveErr := domain.ResolveTrustMode(requested, in.ProofRequirements, cap.ModeSupport)
	if resolveErr != nil {
		if concrete, ok := requested.Concrete(); ok && !cap.Supports(concrete) {
			return domain.Quote{}, domain.NewError(domain.ErrTrustModeUnavailable, resolveErr.Error(), true)
		}
		return domain.Quote{}, domain.NewError(domain.ErrProofRequirementsUnsatisfied, resolveErr.Error(), true)
	}
	if mode != domain.TrustModeManaged && profile == domain.ProofProfileNone {
		return domain.Quote{}, domain.NewError(domain.ErrProofProfileUnavailable, "selected mode has no active proof profile", true)
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

	settlement, proof := quoteGuarantees(mode, subtotal.Currency)
	now := time.Now().UTC()
	disputePolicyHash := termsHash("atos-dispute-policy", "v0.2", "72h")
	q := domain.Quote{
		ID:                     "q_" + uuid.NewString(),
		CapabilityID:           cap.ID,
		CapabilityVersion:      cap.Version,
		ProviderID:             cap.ProviderID,
		RequestedTrustMode:     requested,
		TrustMode:              mode,
		ProofProfile:           profile,
		Price: domain.Price{
			Subtotal: subtotal.String(),
			Fees:     fees.String(),
			TotalMax: totalMax.String(),
			Currency: subtotal.Currency,
		},
		Settlement:        settlement,
		Proof:             proof,
		ExpiresAt:         now.Add(quoteTTL),
		DisputePolicyHash: disputePolicyHash,
		CreatedAt:         now,
	}
	q.TermsHash = termsHash(
		q.CapabilityID, q.CapabilityVersion, q.ProviderID,
		string(q.TrustMode), string(q.ProofProfile),
		q.Price.TotalMax, q.Price.Currency,
		string(q.Settlement.Backend), string(q.Settlement.FundingModel),
		q.ExpiresAt.Format(time.RFC3339Nano), q.DisputePolicyHash,
	)
	if err := s.store.PutQuote(ctx, q); err != nil {
		return domain.Quote{}, err
	}
	return q, nil
}

func quoteGuarantees(mode domain.TrustMode, clientAsset string) (domain.SettlementDescriptor, domain.ProofDescriptor) {
	if mode == domain.TrustModeManaged {
		return domain.SettlementDescriptor{
			Backend: domain.SettlementATOSManaged, Escrow: true,
			FundingModel: domain.FundingManagedBalance, ClientAsset: clientAsset,
		}, domain.ProofDescriptor{ExecutionReceipt: true}
	}
	return domain.SettlementDescriptor{
		Backend: domain.SettlementTOS, Escrow: true,
		FundingModel: domain.FundingGatewaySponsored,
		ClientAsset: clientAsset, ProviderAsset: "TOS",
	}, domain.ProofDescriptor{
		QuoteCommitment: true, ExecutionReceipt: true,
		SettlementProof: true, ProofOfService: true,
	}
}

func (s *QuoteService) Get(ctx context.Context, id string) (domain.Quote, error) {
	q, err := s.store.GetQuote(ctx, id)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Quote{}, domain.NewError(domain.ErrNotFound, "quote not found", false)
		}
		return domain.Quote{}, err
	}
	if q.TrustMode == "" {
		q.RequestedTrustMode = domain.RequestedTrustManaged
		q.TrustMode = domain.TrustModeManaged
		q.Settlement, q.Proof = quoteGuarantees(q.TrustMode, q.Price.Currency)
	}
	return q, nil
}

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
