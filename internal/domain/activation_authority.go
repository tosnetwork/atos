package domain

import "context"

// ActivationAuthority is the sole gate for the `pending -> active` and
// `suspended -> active` mode-support transitions defined by atos-spec
// docs/IMPLEMENTATION_ROADMAP.md §7.2.0/§7.2.1. No code path may activate a
// stronger trust mode from inlined boolean logic over health,
// certification or signer-authorization state (e.g.
// `if healthy && certified && signerExists`) -- every such decision MUST
// go through this interface, so the activation decision has exactly one
// implementation to audit in any given deployment and exactly one seam a
// future Phase 4 TOS-backed authority replaces.
type ActivationAuthority interface {
	// Evaluate decides whether mode may transition to active for this
	// provider+capability+version right now. granted=false MUST always
	// carry a non-empty, stable reasonCode; granted=true's reasonCode is
	// implementation-defined and may be empty. Evaluate MUST be read-only:
	// it never itself writes ModeSupport -- the caller applies
	// ModeSupport.Activate only after a granted=true result.
	Evaluate(ctx context.Context, providerID, capabilityID, capabilityVersion string, mode TrustMode) (granted bool, reasonCode string, err error)
}

// ActivationAuthorityUnavailable is the stable reason code every
// production deployment's fail-closed ActivationAuthority implementation
// MUST return for verified/native: Phase 4 has not shipped a real
// TOS-backed activation authority yet. Managed never calls into this
// interface at all -- see ActivationAuthority's doc comment and
// docs/IMPLEMENTATION_ROADMAP.md §7.2.1.
const ActivationAuthorityUnavailable = "ACTIVATION_AUTHORITY_UNAVAILABLE"
