package service

import (
	"context"

	"github.com/tosnetwork/atos/internal/domain"
)

// CapabilityReadiness is the atos-spec docs/IMPLEMENTATION_ROADMAP.md
// §7.2.3 public read-response shape: a Capability extended with a
// per-mode `readiness` projection, alongside (never replacing) the
// existing `mode_support` object. It is not a new endpoint -- both the
// REST GET /capabilities/{id} handler and the atos_get_capability MCP
// tool build their response through GetCapabilityWithReadiness below,
// mirroring domain.Quote.Public()'s precedent of a dedicated response
// type rather than tagging fields onto the domain struct itself.
type CapabilityReadiness struct {
	domain.Capability
	// Readiness is keyed by mode and never includes "managed" -- Managed
	// has no readiness concept, it is unconditionally active the moment
	// it's requested (see domain.ModeAvailability's doc comment).
	// Deliberately absent (not an empty object) when no HealthService is
	// configured, so environments that don't wire one (e.g. most existing
	// service-level tests) get an unchanged Capability response shape.
	Readiness map[domain.TrustMode]domain.ModeAvailability `json:"readiness,omitempty"`
}

// GetCapabilityWithReadiness is the sole builder of CapabilityReadiness.
// health may be nil (readiness omitted entirely) for callers that have
// not wired a HealthService -- see CapabilityReadiness.Readiness's doc
// comment.
func GetCapabilityWithReadiness(ctx context.Context, capabilities *CapabilityService, health *HealthService, id string) (CapabilityReadiness, error) {
	cap, err := capabilities.Get(ctx, id)
	if err != nil {
		return CapabilityReadiness{}, err
	}
	if health == nil {
		return CapabilityReadiness{Capability: cap}, nil
	}
	availability, err := health.Availability(ctx, id)
	if err != nil {
		return CapabilityReadiness{}, err
	}
	readiness := make(map[domain.TrustMode]domain.ModeAvailability, len(availability))
	for _, mode := range availability {
		if mode.Mode == domain.TrustModeManaged {
			continue
		}
		readiness[mode.Mode] = mode
	}
	return CapabilityReadiness{Capability: cap, Readiness: readiness}, nil
}
