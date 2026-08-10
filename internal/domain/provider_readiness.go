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
	// DeepProbe distinguishes bare reachability from a transport-specific
	// deeper check -- see SandboxCertification's doc comment for the same
	// distinction applied to certification evidence. Only a remote probe
	// through tos-protocol/tos-ai's ThirdPartyExecutionService (see
	// docs/THIRD_PARTY_EXECUTION_PLANE.md §3.1) populates this; a locally
	// dialed check (provideradapter.ProviderAdapter.Health) never sets it,
	// since that path's own deeper check is a separate
	// provideradapter.CertificationProbe step this struct does not carry.
	DeepProbe bool      `json:"deep_probe,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
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

// ModeAvailability is the Phase 3A/3B readiness projection for one
// Capability+TrustMode pair. It is deliberately kept separate from
// domain.ModeSupport (the trust-activation record) so nothing here can be
// mistaken for, or accidentally wired into, activation authority: Ready is
// computed purely from provider intent and readiness evidence, and never
// feeds back into Active.
//
// Since atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.2.3, this is also the
// storage shape for the public `readiness` projection on the Capability
// read response (docs/API.md's exact field set) -- see
// service.GetCapabilityWithReadiness, the sole builder of that response.
// Mode is deliberately not part of that public shape (the response keys
// the projection by mode name instead), so it is excluded from JSON here.
type ModeAvailability struct {
	Mode TrustMode `json:"-"`
	// Requested mirrors whether the provider requested this mode at all.
	Requested bool `json:"requested"`
	// Active mirrors the capability's current ModeSupport activation
	// status, included for internal read-model convenience only -- Ready
	// below is NEVER derived from, and never feeds, this field. Excluded
	// from the public projection, which already carries the richer Status
	// below (of which Active is one possible value).
	Active bool `json:"-"`
	// Status mirrors domain.ModeSupport.Entry(mode).Status exactly -- the
	// public projection's status field MUST always equal mode_support's,
	// never a second source of truth (docs/API.md).
	Status ModeSupportStatus `json:"status"`
	// TransportHealthy reflects the freshest AdapterHealthCheck across the
	// capability's bindings eligible for this mode, regardless of that
	// check's age -- see HealthFresh for the separate staleness dimension.
	TransportHealthy bool `json:"transport_healthy"`
	// HealthFresh reports whether the freshest AdapterHealthCheck across
	// eligible bindings is still within maxHealthAge, independent of
	// whether it was Healthy or Unhealthy -- a stale-but-formerly-healthy
	// check is TransportHealthy:true, HealthFresh:false.
	HealthFresh bool `json:"health_fresh"`
	// Certified (public field name: certification_current) reflects
	// whether a CertificationPassed record exists for an eligible binding
	// AND that record is for the Capability's CURRENT version -- a
	// certification tied to a superseded version does not count, per the
	// existing version-bump test coverage this field's computation
	// already had before the public projection existed.
	Certified bool `json:"certification_current"`
	// SignerAuthorized reflects whether a current execution-signer
	// authorization exists for this Capability+version+mode. Managed
	// trivially reports true (no signer concept applies). Until atos-spec
	// §7.2.2's durable execution-signer authorize/rotate/revoke journal
	// exists in this codebase (tracked separately), nothing here ever
	// calls toscore.Core.AuthorizeExecutionSigner, so this is correctly
	// false for every Verified/Native mode today -- not a stub, an
	// accurate reflection of current system state.
	SignerAuthorized bool `json:"signer_authorized"`
	// ActivationAuthoritySatisfied mirrors whether the activation
	// authority has actually granted this mode active (Status ==
	// ModeSupportActive) -- it does not re-evaluate the authority itself,
	// only reports the outcome of whatever the last real evaluation was.
	ActivationAuthoritySatisfied bool `json:"activation_authority_satisfied"`
	// ReasonCode explains the current blocker, following docs/API.md's
	// fixed vocabulary; empty once Status is active. See
	// ReadinessReasonCode, the sole function that computes it.
	ReasonCode string `json:"reason_code,omitempty"`
	// Ready is pure readiness evidence (Requested && TransportHealthy &&
	// Certified) -- ready-for-certification-to-proceed-toward-activation,
	// not "trusted" and not "active". Internal read-model convenience,
	// excluded from the public projection (superseded there by the more
	// granular fields above).
	Ready bool `json:"-"`
}

// ReadinessReasonCode derives the frozen docs/API.md §7.2.3 reason_code
// for one mode's readiness projection from its current evidence. An
// unevaluated mode's blocker is always "no evidence yet" -- status
// requested means, by §7.2.0's own definition, that no readiness cycle
// has run for the current version, so reporting a specific evidence
// dimension (e.g. TRANSPORT_UNHEALTHY) before it ever had a chance to run
// would be misleading. Once a readiness cycle HAS run (status pending or
// suspended), the first evidence dimension that is not yet satisfied,
// checked in this fixed priority order, is the reported blocker.
func ReadinessReasonCode(status ModeSupportStatus, transportHealthy, healthFresh, certificationCurrent, signerAuthorized, activationAuthoritySatisfied bool) string {
	switch {
	case status == ModeSupportActive:
		return ""
	case status == ModeSupportRequested:
		return "NO_READINESS_EVIDENCE_YET"
	case !transportHealthy:
		return "TRANSPORT_UNHEALTHY"
	case !healthFresh:
		return "HEALTH_STALE"
	case !certificationCurrent:
		return "CERTIFICATION_NOT_CURRENT"
	case !signerAuthorized:
		return "SIGNER_NOT_AUTHORIZED"
	case !activationAuthoritySatisfied:
		return "ACTIVATION_AUTHORITY_UNAVAILABLE"
	default:
		return ""
	}
}
