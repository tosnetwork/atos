package service

import (
	"context"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
)

type SearchFilters struct {
	MaxPrice           *domain.Money
	DeliveryModes      []domain.DeliveryMode
	MinTrustScore      *float64
	MaxLatencyMS       *int64
	RequestedTrustMode domain.RequestedTrustMode
	ProofRequirements  domain.ProofRequirements
}

type SearchInput struct {
	Query   string
	Filters SearchFilters
	Limit   int
}

const candidatePoolSize = 200
const searchDecimals = 2

func (s *CapabilityService) Search(ctx context.Context, in SearchInput) ([]domain.Capability, error) {
	limit := in.Limit
	if limit <= 0 || limit > 20 {
		limit = 5
	}
	candidates, err := s.store.Search(ctx, in.Query, candidatePoolSize)
	if err != nil {
		return nil, err
	}
	type scored struct {
		capability domain.Capability
		score      float64
	}
	var ranked []scored
	for _, candidate := range candidates {
		c := normalizeCapability(candidate)
		if !passesFilters(c, in.Filters) {
			continue
		}
		ranked = append(ranked, scored{capability: c, score: rankScore(c, in.Query)})
	}
	sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].score > ranked[j].score })
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}
	out := make([]domain.Capability, len(ranked))
	for i, item := range ranked {
		out[i] = item.capability
	}
	return out, nil
}

func passesFilters(c domain.Capability, f SearchFilters) bool {
	if f.MaxPrice != nil {
		price, err := money.Parse(c.Pricing.PriceHint.Amount, c.Pricing.PriceHint.Currency, searchDecimals)
		maxPrice, err2 := money.Parse(f.MaxPrice.Amount, f.MaxPrice.Currency, searchDecimals)
		if err != nil || err2 != nil || price.Currency != maxPrice.Currency || price.Cmp(maxPrice) > 0 {
			return false
		}
	}
	if len(f.DeliveryModes) > 0 && !slices.Contains(f.DeliveryModes, c.DeliveryMode) {
		return false
	}
	if f.MinTrustScore != nil && c.Trust.Score < *f.MinTrustScore {
		return false
	}
	if f.MaxLatencyMS != nil && c.SLA.TargetLatencyMS > 0 && c.SLA.TargetLatencyMS > *f.MaxLatencyMS {
		return false
	}
	requested := f.RequestedTrustMode
	if requested == "" {
		requested = domain.RequestedTrustAuto
	}
	if _, _, err := domain.ResolveTrustMode(requested, f.ProofRequirements, c.ModeSupport); err != nil {
		return false
	}
	return true
}

func rankScore(c domain.Capability, query string) float64 {
	const (
		semanticWeight  = 3.0
		trustWeight     = 1.5
		freshnessWeight = 0.5
		priceWeight     = 0.5
		latencyWeight   = 0.5
		proofWeight     = 0.25
	)
	proofReadiness := 0.0
	if c.Supports(domain.TrustModeVerified) {
		proofReadiness = 0.6
	}
	if c.Supports(domain.TrustModeNative) {
		proofReadiness = 1.0
	}
	return semanticWeight*semanticFitScore(c, query) +
		trustWeight*c.Trust.Score +
		freshnessWeight*freshnessScore(c.UpdatedAt) +
		priceWeight*priceFitScore(c.Pricing.PriceHint.Amount) +
		latencyWeight*latencyFitScore(c.SLA.TargetLatencyMS) +
		proofWeight*proofReadiness
}

func semanticFitScore(c domain.Capability, query string) float64 {
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" {
		return 0.5
	}
	name := strings.ToLower(c.Name)
	desc := strings.ToLower(c.Description)
	score := 0.0
	switch {
	case name == q:
		score += 1.0
	case strings.Contains(name, q):
		score += 0.7
	}
	if strings.Contains(desc, q) {
		score += 0.3
	}
	for _, tag := range c.Tags {
		lower := strings.ToLower(tag)
		if lower == q {
			score += 0.4
			break
		}
		if strings.Contains(lower, q) {
			score += 0.2
			break
		}
	}
	return score
}

func freshnessScore(updatedAt time.Time) float64 {
	if updatedAt.IsZero() {
		return 0
	}
	const halfLife = 30 * 24 * time.Hour
	age := max(time.Since(updatedAt), 0)
	return math.Exp(-float64(age) / float64(halfLife))
}

func priceFitScore(amount string) float64 {
	value, err := strconv.ParseFloat(amount, 64)
	if err != nil || value < 0 {
		return 0
	}
	return 1.0 / (1.0 + math.Log1p(value))
}

func latencyFitScore(targetLatencyMS int64) float64 {
	if targetLatencyMS <= 0 {
		return 0.5
	}
	return 1.0 / (1.0 + float64(targetLatencyMS)/1000.0)
}
