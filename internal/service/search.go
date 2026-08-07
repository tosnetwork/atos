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

// SearchFilters implements the hard-filter half of docs/CAPABILITIES.md's
// Search Contract and docs/MCP.md's atos_search "filters" object. A
// capability failing any set filter is excluded outright, before ranking
// ever runs — filters are eligibility, ranking is ordering.
type SearchFilters struct {
	MaxPrice      *domain.Money
	DeliveryModes []domain.DeliveryMode
	MinTrustScore *float64
	MaxLatencyMS  *int64
}

type SearchInput struct {
	Query   string
	Filters SearchFilters
	Limit   int
}

// candidatePoolSize is how many text-match hits the store returns before
// hard filtering and ranking narrow them down to Limit — wide enough that
// a popular query with many matches still lets the best-fitting ones (by
// price/trust/latency) surface instead of whichever happened to come back
// first from the store.
const candidatePoolSize = 200

const searchDecimals = 2

// Search implements atos_search / GET /capabilities: store-level text
// retrieval, then service-level hard filtering and ranking, per
// docs/ARCHITECTURE.md's Capability Resolution steps 3-5 (that ordering is
// deliberate — filtering/ranking is gateway-control-plane business logic,
// not something the storage layer should own).
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
	for _, c := range candidates {
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
	for i, r := range ranked {
		out[i] = r.capability
	}
	return out, nil
}

func passesFilters(c domain.Capability, f SearchFilters) bool {
	if f.MaxPrice != nil {
		price, err := money.Parse(c.Pricing.PriceHint.Amount, c.Pricing.PriceHint.Currency, searchDecimals)
		maxPrice, err2 := money.Parse(f.MaxPrice.Amount, f.MaxPrice.Currency, searchDecimals)
		if err == nil && err2 == nil && price.Currency == maxPrice.Currency && price.Cmp(maxPrice) > 0 {
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
	return true
}

// rankScore combines the factors docs/CAPABILITIES.md names — semantic
// fit, provider trust, availability/freshness, latency fit and price fit —
// into one comparable number. Per that doc's "Do not expose the exact
// anti-gaming weights" rule, these weights are deliberately unexported and
// never serialized into any API response.
func rankScore(c domain.Capability, query string) float64 {
	const (
		semanticWeight  = 3.0
		trustWeight     = 1.5
		freshnessWeight = 0.5
		priceWeight     = 0.5
		latencyWeight   = 0.5
	)
	return semanticWeight*semanticFitScore(c, query) +
		trustWeight*c.Trust.Score +
		freshnessWeight*freshnessScore(c.UpdatedAt) +
		priceWeight*priceFitScore(c.Pricing.PriceHint.Amount) +
		latencyWeight*latencyFitScore(c.SLA.TargetLatencyMS)
}

// semanticFitScore is a Phase 0/1 stand-in for real semantic search
// (embeddings/vector similarity) — see docs/CAPABILITIES.md. An empty
// query means "browse", scored neutrally rather than zero so trust/price/
// latency still meaningfully order results with no text signal to combine
// with.
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
	for _, t := range c.Tags {
		lt := strings.ToLower(t)
		if lt == q {
			score += 0.4
			break
		}
		if strings.Contains(lt, q) {
			score += 0.2
			break
		}
	}
	return score
}

// freshnessScore decays exponentially with age so recently updated
// capabilities are preferred among otherwise-similar matches, without a
// stale capability from a year ago ever fully dropping out of ranking.
func freshnessScore(updatedAt time.Time) float64 {
	if updatedAt.IsZero() {
		return 0
	}
	const halfLife = 30 * 24 * time.Hour
	age := max(time.Since(updatedAt), 0)
	return math.Exp(-float64(age) / float64(halfLife))
}

// priceFitScore prefers cheaper capabilities on a soft log curve — there
// is no absolute budget to compare against unless the caller set
// MaxPrice (a hard filter, handled separately), so this only breaks ties
// among filter-eligible candidates.
func priceFitScore(amount string) float64 {
	amt, err := strconv.ParseFloat(amount, 64)
	if err != nil || amt < 0 {
		return 0
	}
	return 1.0 / (1.0 + math.Log1p(amt))
}

// latencyFitScore prefers lower target latency; an unset SLA (0) is
// treated as neutral rather than penalized, since most Phase 0
// capabilities never set one.
func latencyFitScore(targetLatencyMS int64) float64 {
	if targetLatencyMS <= 0 {
		return 0.5
	}
	return 1.0 / (1.0 + float64(targetLatencyMS)/1000.0)
}
