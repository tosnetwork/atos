package domain

import "time"

// SignerOperationType identifies which of the three execution-signer
// mutations (atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.2.2) a durable
// ExecutionSignerOperation represents.
type SignerOperationType string

const (
	SignerOperationAuthorize SignerOperationType = "authorize"
	SignerOperationRevoke    SignerOperationType = "revoke"
	SignerOperationRotate    SignerOperationType = "rotate"
)

// SignerOperationCheckpoint is one step of §7.2.2's frozen durable
// checkpoint sequence. Rotation walks the full sequence; plain authorize
// and plain revoke each walk the relevant subset (see
// ExecutionSignerOperation's doc comment).
type SignerOperationCheckpoint string

const (
	CheckpointIntentPersisted         SignerOperationCheckpoint = "intent_persisted"
	CheckpointNewAuthorizationPending SignerOperationCheckpoint = "new_authorization_pending"
	CheckpointNewAuthorized           SignerOperationCheckpoint = "new_authorized"
	CheckpointCutoverPending          SignerOperationCheckpoint = "cutover_pending"
	CheckpointOldRevocationPending    SignerOperationCheckpoint = "old_revocation_pending"
	CheckpointOldRevoked              SignerOperationCheckpoint = "old_revoked"
	CheckpointCompleted               SignerOperationCheckpoint = "completed"
	// CheckpointReconciling is not a step of the sequence itself -- it
	// marks that the last attempted step's remote outcome is uncertain
	// (RPC timeout, lost response, process crash mid-step). A subsequent
	// attempt (retry or the reconciler) must resolve it to a deterministic
	// checkpoint, never leave it here indefinitely and never silently
	// treat it as either success or failure.
	CheckpointReconciling SignerOperationCheckpoint = "reconciling"
)

// Terminal reports whether checkpoint is this operation's final state --
// no further transition is expected without a new operation.
func (c SignerOperationCheckpoint) Terminal() bool {
	return c == CheckpointCompleted
}

// ExecutionSignerOperation is the durable journal record for one
// authorize/revoke/rotate execution-signer mutation, giving atos-side
// crash recovery for a sequence tos-protocol itself has no async/pending
// concept for (every signer RPC there is synchronous, atomic-or-nothing --
// see atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.2.2). A crash at any
// checkpoint boundary must converge to the correct next step on restart
// from this record, never silently skip a step.
//
// Field usage by Type:
//   - authorize: only New* fields are meaningful. Checkpoints used:
//     intent_persisted -> new_authorization_pending -> new_authorized ->
//     completed (no cutover -- there is no old signer to keep serving).
//   - revoke: only Old* fields are meaningful. Checkpoints used:
//     intent_persisted -> old_revocation_pending -> old_revoked ->
//     completed.
//   - rotate: both New* and Old* fields are meaningful. Walks the full
//     sequence: intent_persisted -> new_authorization_pending ->
//     new_authorized -> cutover_pending -> old_revocation_pending ->
//     old_revoked -> completed. The old signer remains authoritative and
//     MUST NOT be advertised as superseded until new_authorized is
//     durably reached; its local record MUST NOT be irrecoverably
//     discarded before that point either -- see OldExecutionSignerID's
//     doc comment.
//
// Any checkpoint may transiently be Reconciling; that is not a
// Type-specific state.
type ExecutionSignerOperation struct {
	ID                string                    `json:"id"`
	ProviderID        string                    `json:"provider_id"`
	CapabilityID      string                    `json:"capability_id"`
	CapabilityVersion string                    `json:"capability_version"`
	Type              SignerOperationType       `json:"type"`
	Checkpoint        SignerOperationCheckpoint `json:"checkpoint"`
	IdempotencyKey    string                    `json:"idempotency_key"`

	// NewAuthorizationID is the stable idempotency identity ATOS generates
	// once, before ever calling tos-protocol, and durably persists here --
	// see toscore.AuthorizeExecutionSignerRequest's doc comment. Populated
	// for authorize/rotate only.
	NewAuthorizationID    string    `json:"new_authorization_id,omitempty"`
	NewExecutionSignerID  string    `json:"new_execution_signer_id,omitempty"`
	NewSignerPublicKey    []byte    `json:"new_signer_public_key,omitempty"`
	NewSignatureAlgorithm string    `json:"new_signature_algorithm,omitempty"`
	NewValidFrom          time.Time `json:"new_valid_from,omitempty"`
	NewValidUntil         time.Time `json:"new_valid_until,omitempty"`
	// NewValidFromExplicit/NewValidUntilExplicit record whether the CALLER
	// supplied the corresponding field, as opposed to the transport layer
	// defaulting it because the caller omitted it. Part of this
	// operation's identity, not a lifecycle field: an idempotency-key
	// replay must match both whether the field was explicit AND, if so,
	// its value -- omitting a field the ORIGINAL call explicitly supplied
	// is a different request, not a legitimate transport-retry shape, and
	// must conflict like any other changed field. See
	// service.ExecutionSignerService.resumeOrConflict.
	NewValidFromExplicit  bool `json:"new_valid_from_explicit,omitempty"`
	NewValidUntilExplicit bool `json:"new_valid_until_explicit,omitempty"`
	// NewAuthorizationRef is populated only once tos-protocol's
	// AuthorizeExecutionSigner call actually succeeds (checkpoint reaches
	// new_authorized) -- its presence is itself evidence that step
	// completed, independent of Checkpoint, useful for reconciliation.
	NewAuthorizationRef string `json:"new_authorization_ref,omitempty"`

	// OldAuthorizationID identifies the signer authorization being revoked
	// (revoke) or superseded (rotate). Populated for revoke/rotate only.
	// This is intentionally never cleared once set -- even after
	// old_revoked, the record of which authorization was revoked and why
	// remains part of this operation's permanent history, mirroring
	// SandboxCertification's evidence being kept rather than mutated away.
	OldAuthorizationID   string `json:"old_authorization_id,omitempty"`
	OldExecutionSignerID string `json:"old_execution_signer_id,omitempty"`
	RevocationReasonCode string `json:"revocation_reason_code,omitempty"`

	// FailureReason records the most recent ambiguous or definitive
	// failure observed for this operation, for operator visibility. It is
	// overwritten on each new attempt, not accumulated.
	FailureReason string     `json:"failure_reason,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
}
