package domain

import "time"

// PrincipalIdentityBinding is the durable, current-state fact that a
// gateway principal (or provider -- provider_id is presently the same
// namespace as principal_id, see CapabilityService.Register) is bound to a
// global TOS Agent Identity. This is never caller-asserted: it exists only
// as the result of a successful IdentityBindingService.Bind call, which
// itself only succeeds after tos-protocol independently confirms agent_id
// resolves to a real AgentIdentity before anchoring the binding (see
// atos-spec docs/TOS_RPC.md §10's CreatePrincipalBinding).
type PrincipalIdentityBinding struct {
	PrincipalID string    `json:"principal_id"`
	AgentID     string    `json:"agent_id"`
	Network     string    `json:"network"`
	BindingRef  string    `json:"binding_ref"`
	BoundAt     time.Time `json:"bound_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// IdentityBindingOperationType identifies which of the two
// IdentityBindingService mutations a durable IdentityBindingOperation
// journal record represents.
type IdentityBindingOperationType string

const (
	IdentityBindingOperationBind   IdentityBindingOperationType = "bind"
	IdentityBindingOperationRevoke IdentityBindingOperationType = "revoke"
)

// IdentityBindingCheckpoint is one step of the durable external-operation
// journal (docs/IMPLEMENTATION_ROADMAP.md's "durable stable intent ->
// external operation -> durable observed outcome" rule). Unlike Phase 3B's
// SignerOperationCheckpoint, Bind/Revoke are each ONE atomic, idempotent
// tos-protocol RPC call -- there is no multi-step cutover sequence to
// checkpoint through, so only two real steps exist.
type IdentityBindingCheckpoint string

const (
	// IdentityBindingCheckpointIntentPersisted means this process is about
	// to call, or has called and lost the response for, the tos-protocol
	// RPC. A crash here is safe to resume: retrying with the SAME
	// idempotency_key lets tos-protocol's own atomicMutation replay its
	// original response rather than double-anchoring or double-revoking.
	IdentityBindingCheckpointIntentPersisted IdentityBindingCheckpoint = "intent_persisted"
	IdentityBindingCheckpointCompleted       IdentityBindingCheckpoint = "completed"
	// IdentityBindingCheckpointReconciling marks that the last attempted
	// remote call's outcome is uncertain (RPC timeout, lost response,
	// process crash mid-call) -- not a step of the sequence itself. A
	// subsequent attempt (retry or the reconciler) must resolve it to a
	// deterministic checkpoint, never leave it here indefinitely.
	IdentityBindingCheckpointReconciling IdentityBindingCheckpoint = "reconciling"
)

// Terminal reports whether checkpoint is this operation's final state.
func (c IdentityBindingCheckpoint) Terminal() bool {
	return c == IdentityBindingCheckpointCompleted
}

// IdentityBindingOperation is the durable journal record for one
// bind/revoke mutation, giving atos-side crash recovery for a remote call
// whose local persistence may not have completed before a crash. Field
// usage by Type: "bind" uses AgentID (ReasonCode empty); "revoke" uses
// ReasonCode (AgentID empty, carried from the binding that existed when
// revoke was requested, for audit).
type IdentityBindingOperation struct {
	ID             string                       `json:"id"`
	PrincipalID    string                       `json:"principal_id"`
	Type           IdentityBindingOperationType `json:"type"`
	Checkpoint     IdentityBindingCheckpoint    `json:"checkpoint"`
	IdempotencyKey string                       `json:"idempotency_key"`
	AgentID        string                       `json:"agent_id,omitempty"`
	ReasonCode     string                       `json:"reason_code,omitempty"`
	// BindingRef/RefNetwork are the opaque TOS reference for this
	// operation's anchored fact: for type="bind", CreatePrincipalBinding's
	// binding_ref; for type="revoke", RevokePrincipalBinding's
	// revocation_ref. Mutually exclusive by Type, like AgentID/ReasonCode.
	BindingRef    string     `json:"binding_ref,omitempty"`
	RefNetwork    string     `json:"ref_network,omitempty"`
	ContentHash   string     `json:"content_hash"`
	FailureReason string     `json:"failure_reason,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// OwnershipStatus is atos-spec docs/CAPABILITIES.md §1's
// ownership.status: a Capability version starts unanchored (Managed-only,
// self-asserted provider_id) and becomes anchored only once
// CapabilityOwnershipService durably commits its manifest/ownership
// through tos-protocol's CommitCapabilityManifest.
type OwnershipStatus string

const (
	OwnershipUnanchored OwnershipStatus = "unanchored"
	OwnershipAnchored   OwnershipStatus = "anchored"
)

// CapabilityOwnership is the public ownership-anchoring projection
// embedded on domain.Capability, mirroring docs/CAPABILITIES.md §1's
// example ownership object exactly. Commitment is an opaque
// "network:reference" string, matching this codebase's existing
// convention for every other TOS reference field (e.g.
// ExecutionReceipt.SignerAuthorizationRef) -- never a structured type.
type CapabilityOwnership struct {
	Status     OwnershipStatus `json:"status"`
	Network    string          `json:"network,omitempty"`
	Commitment string          `json:"commitment,omitempty"`
}

// CapabilityOwnershipCommitment is the durable, immutable record of one
// capability_id+version's manifest/ownership anchoring -- a version's
// commitment never changes once committed (a breaking change requires a
// new version, per docs/CAPABILITIES.md §11), mirroring
// ManifestCommitment's own immutability rule.
type CapabilityOwnershipCommitment struct {
	CapabilityID        string    `json:"capability_id"`
	Version             string    `json:"version"`
	ProviderID          string    `json:"provider_id"`
	Network             string    `json:"network"`
	ManifestCommitment  string    `json:"manifest_commitment"`
	OwnershipCommitment string    `json:"ownership_commitment"`
	CommittedAt         time.Time `json:"committed_at"`
}
