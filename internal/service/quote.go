package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
)

const (
	quoteTTL            = 10 * time.Minute
	quoteDecimals       = 2
	defaultFeeRate      = 0.05
	defaultExecutionTTL = 15 * time.Minute
	defaultMaxOutput    = 1 << 20
)

type QuoteService struct {
	store    store.Store
	quoter   tosai.Quoter
	accounts *AccountService
}

// NewQuoteService accepts an optional provider/Edge quoter. Omitting it keeps
// the explicit Phase 0 static-price path; RPC deployments pass the real
// tos-protocol client and therefore obtain a two-layer Quote.
func NewQuoteService(s store.Store, quoters ...tosai.Quoter) *QuoteService {
	service := &QuoteService{store: s}
	if len(quoters) > 0 {
		service.quoter = quoters[0]
	}
	return service
}

// WithAccountService adds gateway spending-policy evaluation to Quote output.
// The execution path still rechecks policy before reserving funds, so this
// presentation hint can never bypass authorization or accounting.
func (s *QuoteService) WithAccountService(accounts *AccountService) *QuoteService {
	s.accounts = accounts
	return s
}

type CreateQuoteInput struct {
	PrincipalID        string
	CapabilityID       string
	InputSummary       map[string]any
	RequestedTrustMode domain.RequestedTrustMode
	ProofRequirements  domain.ProofRequirements
	MaxTotal           *domain.Money
}

func (s *QuoteService) Create(ctx context.Context, in CreateQuoteInput) (domain.Quote, error) {
	if s.quoter != nil && in.PrincipalID == "" {
		return domain.Quote{}, domain.NewError(domain.ErrAuthenticationRequired, "principal is required for an RPC-backed Quote", false)
	}
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
	if err := domain.ValidateCommittedTrust(mode, profile); err != nil {
		return domain.Quote{}, domain.NewError(domain.ErrProofProfileUnavailable, err.Error(), false)
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

	requiresConfirmation := false
	if s.accounts != nil && strings.TrimSpace(in.PrincipalID) != "" {
		requiresConfirmation, err = s.accounts.RequiresConfirmation(ctx, in.PrincipalID, totalMax.String(), totalMax.Currency)
		if err != nil {
			return domain.Quote{}, err
		}
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
	executionDeadline := now.Add(defaultExecutionTTL)
	if cap.SLA.TimeoutMS > 0 {
		executionDeadline = now.Add(time.Duration(cap.SLA.TimeoutMS) * time.Millisecond)
	}
	inputSummaryBytes, err := json.Marshal(in.InputSummary)
	if err != nil {
		return domain.Quote{}, domain.NewError(domain.ErrValidationFailed, "input_summary is not serializable", false)
	}
	inputSummaryCommitment := hashCommitment(in.InputSummary)
	var serviceQuote tosai.ServiceExecutionQuote
	if s.quoter != nil {
		serviceQuote, err = s.quoter.QuoteExecution(ctx, tosai.QuoteExecutionRequest{
			Capability: cap, InputSummary: in.InputSummary,
			InputCommitment: inputSummaryCommitment,
			InputBytes:      uint64(len(inputSummaryBytes)), MaxOutputBytes: defaultMaxOutput,
			ExecutionDeadline: executionDeadline,
			TrustMode:         mode, ProofProfile: profile,
		})
		if err != nil {
			return domain.Quote{}, err
		}
		if serviceQuote.ID == "" || serviceQuote.ExpiresAt.IsZero() || serviceQuote.ExecutionDeadline.IsZero() {
			return domain.Quote{}, domain.NewError(domain.ErrProviderFailed, "tos-protocol returned an incomplete service execution quote", true)
		}
		executionDeadline = serviceQuote.ExecutionDeadline
	}
	disputePolicyHash := termsHash("atos-dispute-policy", "v0.2", "72h")
	q := domain.Quote{
		ID:                 "q_" + uuid.NewString(),
		CapabilityID:       cap.ID,
		CapabilityVersion:  cap.Version,
		ProviderID:         cap.ProviderID,
		PrincipalID:        in.PrincipalID,
		RequestedTrustMode: requested,
		TrustMode:          mode,
		ProofProfile:       profile,
		Price: domain.Price{
			Subtotal: subtotal.String(),
			Fees:     fees.String(),
			TotalMax: totalMax.String(),
			Currency: subtotal.Currency,
		},
		Settlement:             settlement,
		Proof:                  proof,
		ExpiresAt:              now.Add(quoteTTL),
		DisputePolicyHash:      disputePolicyHash,
		InputSummaryCommitment: inputSummaryCommitment,
		ExecutionDeadline:      executionDeadline,
		CreatedAt:              now,
		RequiresConfirmation:   requiresConfirmation,
	}
	if serviceQuote.ID != "" {
		q.ServiceQuoteID = serviceQuote.ID
		q.UnderlyingServiceQuoteRef = serviceQuote.Reference
		if serviceQuote.ExpiresAt.Before(q.ExpiresAt) {
			q.ExpiresAt = serviceQuote.ExpiresAt
		}
	}
	q.TermsHash = termsHash(
		q.CapabilityID, q.CapabilityVersion, q.ProviderID, q.PrincipalID,
		string(q.TrustMode), string(q.ProofProfile),
		q.Price.TotalMax, q.Price.Currency,
		string(q.Settlement.Backend), string(q.Settlement.FundingModel),
		q.ExpiresAt.Format(time.RFC3339Nano), q.ExecutionDeadline.Format(time.RFC3339Nano),
		q.DisputePolicyHash, q.ServiceQuoteID, q.UnderlyingServiceQuoteRef,
		q.InputSummaryCommitment,
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
			ClientAsset:  clientAsset, ProviderAsset: "TOS",
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
