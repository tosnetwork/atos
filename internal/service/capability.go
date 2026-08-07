package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

type CapabilityService struct {
	store store.Store
}

func NewCapabilityService(s store.Store) *CapabilityService {
	return &CapabilityService{store: s}
}

// Search implements atos_search / GET /capabilities. Ranking is naive
// substring matching in Phase 0 (see internal/store/memory) — the
// "semantic fit + trust + price + latency" scoring from
// docs/CAPABILITIES.md is a Phase 1+ concern.
func (s *CapabilityService) Search(ctx context.Context, query string, limit int) ([]domain.Capability, error) {
	if limit <= 0 || limit > 20 {
		limit = 5
	}
	return s.store.Search(ctx, query, limit)
}

func (s *CapabilityService) Get(ctx context.Context, id string) (domain.Capability, error) {
	c, err := s.store.Get(ctx, id)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Capability{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability not found", false)
		}
		return domain.Capability{}, err
	}
	return c, nil
}

type RegisterCapabilityInput struct {
	ProviderID   string
	Name         string
	Description  string
	DeliveryMode domain.DeliveryMode
	InputSchema  map[string]any
	OutputSchema map[string]any
	Pricing      domain.Pricing
	Tags         []string
}

// Register implements atos_register_capability / POST /capabilities. New
// capabilities start self_asserted trust and version 1.0.0 — see
// docs/CAPABILITIES.md "Ownership Anchoring": stronger trust levels are
// earned later through tos-core, not granted at registration.
func (s *CapabilityService) Register(ctx context.Context, in RegisterCapabilityInput) (domain.Capability, error) {
	if in.ProviderID == "" || in.Name == "" || in.Description == "" {
		return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "provider_id, name and description are required", false)
	}
	if in.DeliveryMode != domain.DeliveryInstant && in.DeliveryMode != domain.DeliveryAsync && in.DeliveryMode != domain.DeliveryInteractive {
		return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "delivery_mode must be instant, async or interactive", false)
	}
	if in.InputSchema == nil || in.OutputSchema == nil {
		return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "input_schema and output_schema are required", false)
	}

	c := domain.Capability{
		ID:           "cap_" + uuid.NewString(),
		ProviderID:   in.ProviderID,
		Name:         in.Name,
		Description:  in.Description,
		Version:      "1.0.0",
		Tags:         in.Tags,
		DeliveryMode: in.DeliveryMode,
		InputSchema:  in.InputSchema,
		OutputSchema: in.OutputSchema,
		Pricing:      in.Pricing,
		AdapterType:  domain.AdapterTOSNative,
		Trust:        domain.Trust{Score: 0, Level: domain.TrustSelfAsserted},
		Status:       domain.CapabilityActive,
		UpdatedAt:    time.Now().UTC(),
	}
	if err := s.store.Put(ctx, c); err != nil {
		return domain.Capability{}, err
	}
	return c, nil
}

// Update implements atos_update_capability / PATCH /capabilities/{id}.
// Ownership and identity fields are immutable here on purpose (see
// docs/CAPABILITIES.md "Ownership Anchoring": reassigning provider_id must
// go through a new registration, never a patch).
func (s *CapabilityService) Update(ctx context.Context, id, requestingProviderID string, patch map[string]any) (domain.Capability, error) {
	c, err := s.Get(ctx, id)
	if err != nil {
		return domain.Capability{}, err
	}
	if c.ProviderID != requestingProviderID {
		return domain.Capability{}, domain.NewError(domain.ErrPermissionDenied, "not the owning provider", false)
	}
	if _, attemptsOwnerChange := patch["provider_id"]; attemptsOwnerChange {
		return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "provider_id cannot change via update; register a new capability instead", false)
	}

	if name, ok := patch["name"].(string); ok && name != "" {
		c.Name = name
	}
	if desc, ok := patch["description"].(string); ok && desc != "" {
		c.Description = desc
	}
	if status, ok := patch["status"].(string); ok {
		switch domain.CapabilityStatus(status) {
		case domain.CapabilityActive, domain.CapabilityPaused:
			c.Status = domain.CapabilityStatus(status)
		default:
			return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, fmt.Sprintf("invalid status %q", status), false)
		}
	}

	// Pricing/schema changes are terms changes, not metadata changes — see
	// docs/CAPABILITIES.md "Versioning": a breaking contract change bumps
	// the version so quotes/jobs created against the old terms can be
	// rejected at execution time (see JobService.executeJob's version
	// check) instead of silently running against new terms.
	termsChanged := false
	if pricing, ok := patch["pricing"]; ok {
		var p domain.Pricing
		b, _ := json.Marshal(pricing)
		if err := json.Unmarshal(b, &p); err != nil {
			return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "invalid pricing", false)
		}
		if !samePricing(c.Pricing, p) {
			c.Pricing = p
			termsChanged = true
		}
	}
	if inputSchema, ok := patch["input_schema"].(map[string]any); ok {
		c.InputSchema = inputSchema
		termsChanged = true
	}
	if outputSchema, ok := patch["output_schema"].(map[string]any); ok {
		c.OutputSchema = outputSchema
		termsChanged = true
	}
	if termsChanged {
		c.Version = bumpMinorVersion(c.Version)
	}

	c.UpdatedAt = time.Now().UTC()

	if err := s.store.Put(ctx, c); err != nil {
		return domain.Capability{}, err
	}
	return c, nil
}

func samePricing(a, b domain.Pricing) bool {
	return a.Model == b.Model && a.Unit == b.Unit &&
		a.PriceHint.Amount == b.PriceHint.Amount && a.PriceHint.Currency == b.PriceHint.Currency
}

// bumpMinorVersion increments the middle component of a "major.minor.patch"
// version string, resetting patch to 0 — good enough to make quotes issued
// against the old terms detectably stale (JobService.executeJob rejects a
// version mismatch) without a full semver policy debate for Phase 0.
func bumpMinorVersion(version string) string {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) != 3 {
		return "1.1.0"
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return "1.1.0"
	}
	return parts[0] + "." + strconv.Itoa(minor+1) + ".0"
}
