package domain

import "time"

// AdapterHealthStatus is the observed reachability of a Capability's
// registered transport binding, checked independently of trust-mode
// activation. It answers "can this specific adapter endpoint currently
// serve this Capability" -- never "should this mode become cryptographically
// trusted".
type AdapterHealthStatus string

const (
	AdapterHealthUnknown   AdapterHealthStatus = "unknown"
	AdapterHealthHealthy   AdapterHealthStatus = "healthy"
	AdapterHealthUnhealthy AdapterHealthStatus = "unhealthy"
)

// AdapterHealthCheck is the most recently observed health result for one
// Capability's one transport binding. It is readiness evidence, never
// activation authority -- see SandboxCertification's doc comment for the
// same rule applied to certification.
type AdapterHealthCheck struct {
	CapabilityID      string              `json:"capability_id"`
	CapabilityVersion string              `json:"capability_version"`
	Transport         EndpointAdapterType `json:"transport"`
	EndpointRef       string              `json:"endpoint_ref"`
	Status            AdapterHealthStatus `json:"status"`
	LatencyMS         int64               `json:"latency_ms,omitempty"`
	FailureReason     string              `json:"failure_reason,omitempty"`
	CheckedAt         time.Time           `json:"checked_at"`
}

// Stale reports whether this health check is too old to be trusted as
// current readiness evidence -- an old success must not be treated as
// indefinitely fresh.
func (h AdapterHealthCheck) Stale(now time.Time, maxAge time.Duration) bool {
	if h.CheckedAt.IsZero() {
		return true
	}
	return now.Sub(h.CheckedAt) > maxAge
}

// CertificationStatus is the durable outcome of one sandbox certification
// attempt for one Capability transport binding.
type CertificationStatus string

const (
	CertificationPending CertificationStatus = "pending"
	CertificationPassed  CertificationStatus = "passed"
	CertificationFailed  CertificationStatus = "failed"
)

func (s CertificationStatus) Terminal() bool {
	return s == CertificationPassed || s == CertificationFailed
}

// SandboxCertification is a durable, idempotent readiness-evidence record
// for one Capability transport binding.
//
// Passing certification (Status == CertificationPassed) is readiness
// evidence ONLY. It is never, by itself, trust-mode activation authority:
// no code path may set ModeSupport[mode].Status = ModeSupportActive (or add
// a mode to SupportedTrustModes) as a direct consequence of a certification
// result, a health check result, or any combination of the two. That
// activation path is explicitly out of scope for Phase 3A -- see
// atos-spec Roadmap §6.2's fail-closed rule for Verified/Native. This
// invariant has explicit regression test coverage; do not weaken it.
type SandboxCertification struct {
	ID                string              `json:"id"`
	ProviderID        string              `json:"provider_id"`
	CapabilityID      string              `json:"capability_id"`
	CapabilityVersion string              `json:"capability_version"`
	Transport         EndpointAdapterType `json:"transport"`
	EndpointRef       string              `json:"endpoint_ref"`
	Status            CertificationStatus `json:"status"`
	// IdempotencyKey scopes a repeated certification request for the same
	// semantic inputs to the same durable result; changed semantics under
	// the same key must conflict rather than silently overwrite prior
	// evidence -- see store.Certifications.OpenCertification.
	IdempotencyKey string         `json:"idempotency_key"`
	FailureReason  string         `json:"failure_reason,omitempty"`
	Evidence       map[string]any `json:"evidence,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	CompletedAt    *time.Time     `json:"completed_at,omitempty"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// ModeAvailability is the Phase 3A internal readiness projection for one
// Capability+TrustMode pair. It is deliberately kept separate from
// domain.ModeSupport (the trust-activation record) so nothing here can be
// mistaken for, or accidentally wired into, activation authority: Ready is
// computed purely from provider intent and readiness evidence, and never
// feeds back into Active.
type ModeAvailability struct {
	Mode TrustMode `json:"mode"`
	// Requested mirrors whether the provider requested this mode at all.
	Requested bool `json:"requested"`
	// Active mirrors the capability's current ModeSupport activation
	// status, included for read-model convenience only -- Ready below is
	// NEVER derived from, and never feeds, this field.
	Active bool `json:"active"`
	// TransportHealthy reflects the freshest AdapterHealthCheck across the
	// capability's bindings eligible for this mode.
	TransportHealthy bool `json:"transport_healthy"`
	// Certified reflects whether a CertificationPassed record exists for
	// an eligible binding.
	Certified bool `json:"certified"`
	// Ready is pure readiness evidence (Requested && TransportHealthy &&
	// Certified) -- ready-for-certification-to-proceed-toward-Phase-3B,
	// not "trusted" and not "active".
	Ready bool `json:"ready"`
}
