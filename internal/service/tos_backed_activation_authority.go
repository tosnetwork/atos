package service

import (
	"context"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// Reason codes TOSBackedActivationAuthority returns on granted=false. Each
// is stable API surface (docs/API.md §2.2's mode_support.reason) -- never
// rename one without treating it as a breaking contract change.
const (
	ReasonProviderIdentityNotBound   = "PROVIDER_IDENTITY_NOT_BOUND"
	ReasonProviderIdentityRevoked    = "PROVIDER_IDENTITY_REVOKED"
	ReasonNetworkMismatch            = "NETWORK_MISMATCH"
	ReasonCapabilityOwnershipInvalid = "CAPABILITY_OWNERSHIP_INVALID"
	ReasonManifestNotCommitted       = "MANIFEST_NOT_COMMITTED"
	ReasonSignerNotAuthorized        = "SIGNER_NOT_AUTHORIZED"
	ReasonSignerRevoked              = "SIGNER_REVOKED"
	// ReasonNativeNotSupported: Phase 4A implements Verified's identity/
	// ownership/manifest/network guarantees only. Native additionally
	// requires gateway-independent global resolution that no phase through
	// 4A implements -- granting native here would silently claim a
	// guarantee this authority cannot back. Phase 4A's own scope boundary
	// explicitly excludes Native activation.
	ReasonNativeNotSupported = "NATIVE_NOT_SUPPORTED"
)

// TOSBackedActivationAuthority is Phase 4A's production
// domain.ActivationAuthority: it grants `verified` only when every
// checkpoint the brief requires is CURRENT, re-verified live against
// toscore.Core on every call -- never from a locally cached fact, per the
// brief's persistence/caching/freshness rule ("cached TOS evidence is
// never authority"). It never grants `native` (see ReasonNativeNotSupported).
//
// providerID's identity binding is resolved through the SAME namespace
// principal-identity bindings use (toscore.Core.ResolvePrincipalBindingStatus)
// -- CapabilityService.Register already treats provider_id and principal_id
// as the same namespace, so a provider's TOS identity binding IS its
// principal's TOS identity binding.
type TOSBackedActivationAuthority struct {
	core            toscore.Core
	capabilities    store.Capabilities
	executionSigner *ExecutionSignerService
	now             func() time.Time
}

// NewTOSBackedActivationAuthority constructs the production authority.
// The trusted network identity is read from core.Network() at evaluation
// time (single source of truth with whatever the underlying remote
// connection is actually configured for) rather than a separately-supplied
// value that could silently drift from it.
func NewTOSBackedActivationAuthority(core toscore.Core, capabilities store.Capabilities, executionSigner *ExecutionSignerService) *TOSBackedActivationAuthority {
	return &TOSBackedActivationAuthority{
		core: core, capabilities: capabilities, executionSigner: executionSigner, now: time.Now,
	}
}

func (a *TOSBackedActivationAuthority) Evaluate(ctx context.Context, providerID, capabilityID, capabilityVersion string, mode domain.TrustMode) (granted bool, reasonCode string, err error) {
	if mode != domain.TrustModeVerified {
		return false, ReasonNativeNotSupported, nil
	}
	network := a.core.Network()
	if network == "" {
		// Unconfigured network is a hard fail-closed condition, never a
		// wildcard that matches any provider identity binding (including
		// one that also happens to carry an empty Network field).
		return false, ReasonNetworkMismatch, nil
	}

	// 1. Provider identity: MUST be currently bound to a TOS Agent Identity
	// on the exact configured network. A revoked binding is distinguished
	// from "never bound" only for operator visibility -- both deny
	// activation identically (docs/TOS_RPC.md §10's CreatePrincipalBinding
	// section: "MUST fail closed / suspend, never silently downgrade, when
	// a previously-bound identity is no longer active").
	binding, bound, revoked, _, err := a.core.ResolvePrincipalBindingStatus(ctx, providerID)
	if err != nil {
		return false, "", err
	}
	// revoked is checked unconditionally, not only inside `if !bound` --
	// Bound and revoked are independently-sourced signals with no
	// guarantee they can never both be true at once (a defensive read: if
	// the underlying implementation is ever wrong about that invariant,
	// this must still fail closed rather than silently grant).
	if revoked {
		return false, ReasonProviderIdentityRevoked, nil
	}
	if !bound {
		return false, ReasonProviderIdentityNotBound, nil
	}
	if binding.Network != network {
		return false, ReasonNetworkMismatch, nil
	}

	// 2. Capability ownership + exact manifest/version commitment. The
	// capability's OWN current ManifestCommitment (not a caller-supplied
	// value) is what must still match the remote service's anchored digest
	// -- this is the manifest/version TOCTOU check: a provider who mutated a
	// capability's schema/pricing/bindings after committing must not keep
	// an activation that was only ever valid for the OLD manifest.
	cap, err := a.capabilities.Get(ctx, capabilityID)
	if err != nil {
		return false, "", err
	}
	if cap.ProviderID != providerID {
		return false, ReasonCapabilityOwnershipInvalid, nil
	}
	if cap.Version != capabilityVersion {
		return false, ReasonManifestNotCommitted, nil
	}
	if cap.ManifestCommitment == "" {
		return false, ReasonManifestNotCommitted, nil
	}
	verified, ownershipReason, err := a.core.VerifyCapabilityOwnership(ctx, capabilityID, providerID, capabilityVersion, cap.ManifestCommitment)
	if err != nil {
		return false, "", err
	}
	if !verified {
		switch ownershipReason {
		case "MANIFEST_MISMATCH":
			return false, ReasonManifestNotCommitted, nil
		default:
			return false, ReasonCapabilityOwnershipInvalid, nil
		}
	}

	// 3. Execution-signer authorization: MUST be currently valid for this
	// exact capability_id+version, re-verified live against the remote
	// service (not merely "a signer operation completed at some point
	// locally") -- mirrors Phase 3B's own real-RPC verification discipline.
	if a.executionSigner == nil {
		return false, ReasonSignerNotAuthorized, nil
	}
	_, signerID, signerFound, err := a.executionSigner.SignerAt(ctx, capabilityID, capabilityVersion)
	if err != nil {
		return false, "", err
	}
	if !signerFound {
		return false, ReasonSignerNotAuthorized, nil
	}
	auth, authValid, err := a.core.ResolveExecutionSignerAuthorization(ctx, providerID, capabilityID, capabilityVersion, signerID, a.now())
	if err != nil {
		return false, "", err
	}
	if !authValid {
		return false, ReasonSignerNotAuthorized, nil
	}
	if auth.Revoked {
		return false, ReasonSignerRevoked, nil
	}
	now := a.now()
	if now.Before(auth.ValidFrom) || !now.Before(auth.ValidUntil) {
		return false, ReasonSignerNotAuthorized, nil
	}

	return true, "", nil
}
