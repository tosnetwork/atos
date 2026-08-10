package service

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

const (
	defaultSignerReconcileInterval   = 15 * time.Second
	defaultSignerReconcileStaleAfter = 30 * time.Second
	defaultSignerReconcileBatch      = 100
)

// ExecutionSignerService implements atos-spec docs/IMPLEMENTATION_ROADMAP.md
// §7.2.2's authorize/rotate/revoke workflows as a durable, crash-recoverable
// checkpoint sequence -- see domain.ExecutionSignerOperation's doc comment
// for the full sequence this service walks, and RunReconciler for how a
// crash or an abandoned caller still converges without help.
type ExecutionSignerService struct {
	store        store.Store
	core         toscore.Core
	capabilities *CapabilityService
}

func NewExecutionSignerService(s store.Store, core toscore.Core, capabilities *CapabilityService) *ExecutionSignerService {
	return &ExecutionSignerService{store: s, core: core, capabilities: capabilities}
}

// CurrentSigner derives "the currently authorized execution signer" for
// capabilityID from the most recent COMPLETED operation FOR THE
// CAPABILITY'S CURRENT VERSION -- a completed authorize/rotate makes its
// New* identity current; a completed revoke leaves no current signer.
//
// This deliberately queries LatestCompletedSignerOperationByCapability,
// NOT LatestSignerOperationByCapability: a newer operation that is still
// in flight or stuck Reconciling (e.g. a rotation whose new-signer
// authorize failed ambiguously) must never make this report "no current
// signer" -- the old signer remains authoritative exactly until the new
// one is durably new_authorized (§7.2.2). Using the unfiltered "latest"
// query here was an earlier bug: it made an in-flight rotation
// momentarily erase the still-valid old signer from view.
//
// Signer authorization currency is itself version-scoped (§7.2.0: a
// Capability version bump resets per-version readiness evidence,
// including signer authorization currency): a completed operation whose
// CapabilityVersion no longer matches the capability's CURRENT version is
// not current, even though it remains part of that operation's own
// auditable history untouched. This was an earlier bug too -- the version
// check was previously missing entirely, so a signer authorized for a
// superseded version stayed "current" forever across version bumps.
func (s *ExecutionSignerService) CurrentSigner(ctx context.Context, capabilityID string) (authorizationID, signerID string, found bool, err error) {
	cap, err := s.capabilities.Get(ctx, capabilityID)
	if err != nil {
		return "", "", false, err
	}
	op, ok, err := s.store.LatestCompletedSignerOperationByCapability(ctx, capabilityID)
	if err != nil || !ok {
		return "", "", false, err
	}
	if op.CapabilityVersion != cap.Version {
		return "", "", false, nil
	}
	switch op.Type {
	case domain.SignerOperationAuthorize, domain.SignerOperationRotate:
		return op.NewAuthorizationID, op.NewExecutionSignerID, true, nil
	default: // revoke
		return "", "", false, nil
	}
}

type AuthorizeSignerInput struct {
	ProviderID         string
	CapabilityID       string
	ExecutionSignerID  string
	SignerPublicKey    []byte
	SignatureAlgorithm string
	ValidFrom          time.Time
	ValidUntil         time.Time
	IdempotencyKey     string
}

func (in AuthorizeSignerInput) validate() error {
	if in.ProviderID == "" || in.CapabilityID == "" || in.ExecutionSignerID == "" || in.IdempotencyKey == "" {
		return domain.NewError(domain.ErrValidationFailed, "provider_id, capability_id, execution_signer_id and idempotency_key are required", false)
	}
	if len(in.SignerPublicKey) == 0 || in.SignatureAlgorithm == "" {
		return domain.NewError(domain.ErrValidationFailed, "signer_public_key and signature_algorithm are required", false)
	}
	if in.ValidFrom.IsZero() || in.ValidUntil.IsZero() || !in.ValidUntil.After(in.ValidFrom) {
		return domain.NewError(domain.ErrValidationFailed, "valid_until must be after valid_from", false)
	}
	return nil
}

// Authorize authorizes ExecutionSignerID as the (first) execution signer
// for CapabilityID. Ownership is enforced identically to every other
// provider mutation in this codebase: provider identity comes only from
// in.ProviderID (the caller's authenticated principal in the REST/MCP
// layer), never trusted from anywhere else.
func (s *ExecutionSignerService) Authorize(ctx context.Context, in AuthorizeSignerInput) (domain.ExecutionSignerOperation, error) {
	if err := in.validate(); err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	cap, err := s.capabilities.Get(ctx, in.CapabilityID)
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	if cap.ProviderID != in.ProviderID {
		return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrPermissionDenied, "not the owning provider", false)
	}
	now := time.Now().UTC()
	op := domain.ExecutionSignerOperation{
		ID: "sigop_" + uuid.NewString(), ProviderID: in.ProviderID,
		CapabilityID: cap.ID, CapabilityVersion: cap.Version,
		Type: domain.SignerOperationAuthorize, Checkpoint: domain.CheckpointIntentPersisted,
		IdempotencyKey:        in.IdempotencyKey,
		NewAuthorizationID:    "authz_" + uuid.NewString(),
		NewExecutionSignerID:  in.ExecutionSignerID,
		NewSignerPublicKey:    in.SignerPublicKey,
		NewSignatureAlgorithm: in.SignatureAlgorithm,
		NewValidFrom:          in.ValidFrom,
		NewValidUntil:         in.ValidUntil,
		CreatedAt:             now, UpdatedAt: now,
	}
	stored, _, err := s.store.OpenSignerOperation(ctx, in.ProviderID, op)
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	return s.driveAuthorize(ctx, stored)
}

type RevokeSignerInput struct {
	ProviderID     string
	CapabilityID   string
	ReasonCode     string
	IdempotencyKey string
}

// Revoke revokes CapabilityID's currently authorized execution signer.
//
// Safety rule (a deliberate scope decision, not derived from an explicit
// atos-spec sentence): revoking the only currently-authorized signer
// while a stronger trust mode is Active would leave that mode unable to
// execute at all -- Verified/Native job verification needs a resolvable
// signer authorization (see toscore/mock.Core.VerifyExecutionReceipt's
// doc comment for why Managed alone is exempt). Plain Revoke is refused
// in that case; Rotate is the safe path, since it never durably reaches a
// state with zero authorized signers.
func (s *ExecutionSignerService) Revoke(ctx context.Context, in RevokeSignerInput) (domain.ExecutionSignerOperation, error) {
	if in.ProviderID == "" || in.CapabilityID == "" || in.IdempotencyKey == "" {
		return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrValidationFailed, "provider_id, capability_id and idempotency_key are required", false)
	}
	cap, err := s.capabilities.Get(ctx, in.CapabilityID)
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	if cap.ProviderID != in.ProviderID {
		return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrPermissionDenied, "not the owning provider", false)
	}
	for _, mode := range []domain.TrustMode{domain.TrustModeVerified, domain.TrustModeNative} {
		if cap.ModeSupport.Active(mode) {
			return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrValidationFailed, "cannot revoke the only authorized execution signer while a stronger trust mode is active -- rotate instead", false)
		}
	}
	authorizationID, signerID, found, err := s.CurrentSigner(ctx, cap.ID)
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	if !found {
		return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrNotFound, "capability has no currently authorized execution signer to revoke", false)
	}
	now := time.Now().UTC()
	op := domain.ExecutionSignerOperation{
		ID: "sigop_" + uuid.NewString(), ProviderID: in.ProviderID,
		CapabilityID: cap.ID, CapabilityVersion: cap.Version,
		Type: domain.SignerOperationRevoke, Checkpoint: domain.CheckpointIntentPersisted,
		IdempotencyKey:       in.IdempotencyKey,
		OldAuthorizationID:   authorizationID,
		OldExecutionSignerID: signerID,
		RevocationReasonCode: in.ReasonCode,
		CreatedAt:            now, UpdatedAt: now,
	}
	stored, _, err := s.store.OpenSignerOperation(ctx, in.ProviderID, op)
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	return s.driveRevoke(ctx, stored)
}

type RotateSignerInput struct {
	ProviderID            string
	CapabilityID          string
	NewExecutionSignerID  string
	NewSignerPublicKey    []byte
	NewSignatureAlgorithm string
	NewValidFrom          time.Time
	NewValidUntil         time.Time
	RevocationReasonCode  string
	IdempotencyKey        string
}

// Rotate replaces CapabilityID's currently authorized execution signer
// with a new one, walking the full §7.2.2 checkpoint sequence: the old
// signer remains authoritative through new_authorized, only becomes
// superseded at cutover_pending, and is revoked only after that --
// never revoke-then-authorize.
func (s *ExecutionSignerService) Rotate(ctx context.Context, in RotateSignerInput) (domain.ExecutionSignerOperation, error) {
	if in.ProviderID == "" || in.CapabilityID == "" || in.NewExecutionSignerID == "" || in.IdempotencyKey == "" {
		return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrValidationFailed, "provider_id, capability_id, new_execution_signer_id and idempotency_key are required", false)
	}
	if len(in.NewSignerPublicKey) == 0 || in.NewSignatureAlgorithm == "" {
		return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrValidationFailed, "new_signer_public_key and new_signature_algorithm are required", false)
	}
	if in.NewValidFrom.IsZero() || in.NewValidUntil.IsZero() || !in.NewValidUntil.After(in.NewValidFrom) {
		return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrValidationFailed, "new_valid_until must be after new_valid_from", false)
	}
	cap, err := s.capabilities.Get(ctx, in.CapabilityID)
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	if cap.ProviderID != in.ProviderID {
		return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrPermissionDenied, "not the owning provider", false)
	}
	oldAuthorizationID, oldSignerID, found, err := s.CurrentSigner(ctx, cap.ID)
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	if !found {
		return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrNotFound, "capability has no currently authorized execution signer to rotate -- use Authorize for the first signer", false)
	}
	now := time.Now().UTC()
	op := domain.ExecutionSignerOperation{
		ID: "sigop_" + uuid.NewString(), ProviderID: in.ProviderID,
		CapabilityID: cap.ID, CapabilityVersion: cap.Version,
		Type: domain.SignerOperationRotate, Checkpoint: domain.CheckpointIntentPersisted,
		IdempotencyKey:        in.IdempotencyKey,
		NewAuthorizationID:    "authz_" + uuid.NewString(),
		NewExecutionSignerID:  in.NewExecutionSignerID,
		NewSignerPublicKey:    in.NewSignerPublicKey,
		NewSignatureAlgorithm: in.NewSignatureAlgorithm,
		NewValidFrom:          in.NewValidFrom,
		NewValidUntil:         in.NewValidUntil,
		OldAuthorizationID:    oldAuthorizationID,
		OldExecutionSignerID:  oldSignerID,
		RevocationReasonCode:  in.RevocationReasonCode,
		CreatedAt:             now, UpdatedAt: now,
	}
	stored, _, err := s.store.OpenSignerOperation(ctx, in.ProviderID, op)
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	return s.driveRotate(ctx, stored)
}

// Status returns the most recently updated execution-signer operation for
// capabilityID, if any -- found=false means no authorize/revoke/rotate
// has ever been attempted for this capability.
func (s *ExecutionSignerService) Status(ctx context.Context, capabilityID string) (domain.ExecutionSignerOperation, bool, error) {
	return s.store.LatestSignerOperationByCapability(ctx, capabilityID)
}

// SignerOperationStatusView is the atos-spec docs/API.md §2.1 public
// response shape for GET /capabilities/{id}/execution-signer and
// atos_get_execution_signer_status -- the sole builder is PublicStatus
// below, mirroring service.CapabilityReadiness's precedent of one shared
// function feeding both REST and MCP rather than two independent
// implementations that could drift.
type SignerOperationStatusView struct {
	CapabilityID         string `json:"capability_id"`
	CapabilityVersion    string `json:"capability_version"`
	OperationID          string `json:"operation_id,omitempty"`
	OperationType        string `json:"operation_type,omitempty"`
	Checkpoint           string `json:"checkpoint,omitempty"`
	OldExecutionSignerID string `json:"old_execution_signer_id,omitempty"`
	NewExecutionSignerID string `json:"new_execution_signer_id,omitempty"`
	// CurrentExecutionSignerID MUST remain the old signer until Checkpoint
	// reaches new_authorized, per §7.2.2's rotation ordering -- it is
	// derived from CurrentSigner (LatestCompletedSignerOperationByCapability),
	// never from OperationID's own (possibly non-terminal) operation, so a
	// caller can never observe the new signer advertised as current before
	// its authorization is confirmed durable.
	CurrentExecutionSignerID string `json:"current_execution_signer_id,omitempty"`
}

// PublicStatus builds the public status view for capabilityID, enforcing
// the same provider-ownership rule every execution-signer operation in
// this file enforces (§2.1: "Provider/admin only ... the authenticated
// provider MUST own capability_id").
func (s *ExecutionSignerService) PublicStatus(ctx context.Context, requestingProviderID, capabilityID string) (SignerOperationStatusView, error) {
	cap, err := s.capabilities.Get(ctx, capabilityID)
	if err != nil {
		return SignerOperationStatusView{}, err
	}
	if cap.ProviderID != requestingProviderID {
		return SignerOperationStatusView{}, domain.NewError(domain.ErrPermissionDenied, "not the owning provider", false)
	}
	view := SignerOperationStatusView{CapabilityID: cap.ID, CapabilityVersion: cap.Version}
	if op, found, err := s.store.LatestSignerOperationByCapability(ctx, capabilityID); err != nil {
		return SignerOperationStatusView{}, err
	} else if found {
		view.OperationID = op.ID
		view.OperationType = string(op.Type)
		view.Checkpoint = string(op.Checkpoint)
		view.OldExecutionSignerID = op.OldExecutionSignerID
		view.NewExecutionSignerID = op.NewExecutionSignerID
	}
	if _, signerID, found, err := s.CurrentSigner(ctx, capabilityID); err != nil {
		return SignerOperationStatusView{}, err
	} else if found {
		view.CurrentExecutionSignerID = signerID
	}
	return view, nil
}

// advance atomically moves op (by id) to checkpoint, optionally recording
// a newly obtained NewAuthorizationRef and/or a failureReason (cleared to
// "" on a clean transition). A concurrent caller that already drove the
// operation to Completed wins -- this is a no-op in that case, never a
// backwards transition.
func (s *ExecutionSignerService) advance(ctx context.Context, id string, checkpoint domain.SignerOperationCheckpoint, newAuthorizationRef, failureReason string) (domain.ExecutionSignerOperation, error) {
	return s.store.UpdateSignerOperation(ctx, id, func(current domain.ExecutionSignerOperation, exists bool) (domain.ExecutionSignerOperation, error) {
		if !exists {
			return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrNotFound, "execution-signer operation not found", false)
		}
		if current.Checkpoint.Terminal() {
			return current, nil
		}
		current.Checkpoint = checkpoint
		if newAuthorizationRef != "" {
			current.NewAuthorizationRef = newAuthorizationRef
		}
		current.FailureReason = failureReason
		current.UpdatedAt = time.Now().UTC()
		if checkpoint == domain.CheckpointCompleted {
			completedAt := current.UpdatedAt
			current.CompletedAt = &completedAt
		}
		return current, nil
	})
}

// isAmbiguousSignerFailure reports whether err represents an uncertain
// remote outcome (RPC timeout, lost response, transport failure) rather
// than a definitive rejection. tos-protocol's own error mapping already
// classifies this distinction (domain.Error.Retryable -- see
// internal/adapters/tosprotocol/mapping.go: NETWORK_UNAVAILABLE/
// PROVIDER_UNAVAILABLE/EXECUTION_UNCERTAIN and raw transport failures are
// retryable=true; well-formed rejections like CAPABILITY_OWNERSHIP_FAILED
// or an ALREADY_EXISTS conflict are retryable=false) -- this reuses that
// signal rather than inventing a second one. An error that isn't even a
// *domain.Error (an unexpected shape) is treated conservatively as
// ambiguous.
func isAmbiguousSignerFailure(err error) bool {
	var de *domain.Error
	if errors.As(err, &de) {
		return de.Retryable
	}
	return true
}

// handleAmbiguousOrFail is the sole error path every RPC call site in
// drive* funnels through. An ambiguous failure marks the operation
// Reconciling and returns a retryable error; a definitive failure records
// FailureReason without moving the checkpoint and returns the original
// error, so a caller retrying with the same idempotency key resumes
// exactly the same failed step (a corrected request needs a new
// idempotency key, exactly like every other provider mutation).
func (s *ExecutionSignerService) handleAmbiguousOrFail(ctx context.Context, op domain.ExecutionSignerOperation, callErr error) (domain.ExecutionSignerOperation, error) {
	if isAmbiguousSignerFailure(callErr) {
		updated, err := s.advance(ctx, op.ID, domain.CheckpointReconciling, "", callErr.Error())
		if err != nil {
			return updated, err
		}
		return updated, domain.NewError(domain.ErrNetworkUnavailable, "execution-signer operation outcome is uncertain, retry", true)
	}
	updated, err := s.advance(ctx, op.ID, op.Checkpoint, "", callErr.Error())
	if err != nil {
		return updated, err
	}
	return updated, callErr
}

// driveAuthorize walks op from wherever its persisted Checkpoint is to
// Completed: intent_persisted -> new_authorization_pending ->
// new_authorized -> completed. No cutover step -- a plain authorize has
// no old signer to keep serving.
func (s *ExecutionSignerService) driveAuthorize(ctx context.Context, op domain.ExecutionSignerOperation) (domain.ExecutionSignerOperation, error) {
	if op.Checkpoint == domain.CheckpointCompleted {
		return op, nil
	}
	var err error
	if op.Checkpoint == domain.CheckpointIntentPersisted {
		op, err = s.advance(ctx, op.ID, domain.CheckpointNewAuthorizationPending, "", "")
		if err != nil {
			return op, err
		}
	}
	if op.Checkpoint == domain.CheckpointNewAuthorizationPending || op.Checkpoint == domain.CheckpointReconciling {
		authorization, _, callErr := s.core.AuthorizeExecutionSigner(ctx, toscore.AuthorizeExecutionSignerRequest{
			AuthorizationID: op.NewAuthorizationID, ProviderID: op.ProviderID,
			CapabilityID: op.CapabilityID, CapabilityVersion: op.CapabilityVersion,
			ExecutionSignerID: op.NewExecutionSignerID, SignerPublicKey: op.NewSignerPublicKey,
			SignatureAlgorithm: op.NewSignatureAlgorithm, ValidFrom: op.NewValidFrom, ValidUntil: op.NewValidUntil,
		})
		if callErr != nil {
			return s.handleAmbiguousOrFail(ctx, op, callErr)
		}
		op, err = s.advance(ctx, op.ID, domain.CheckpointNewAuthorized, authorization.AuthorizationRef, "")
		if err != nil {
			return op, err
		}
	}
	return s.advance(ctx, op.ID, domain.CheckpointCompleted, "", "")
}

// driveRevoke walks op from wherever its persisted Checkpoint is to
// Completed: intent_persisted -> old_revocation_pending -> old_revoked ->
// completed.
func (s *ExecutionSignerService) driveRevoke(ctx context.Context, op domain.ExecutionSignerOperation) (domain.ExecutionSignerOperation, error) {
	if op.Checkpoint == domain.CheckpointCompleted {
		return op, nil
	}
	var err error
	if op.Checkpoint == domain.CheckpointIntentPersisted {
		op, err = s.advance(ctx, op.ID, domain.CheckpointOldRevocationPending, "", "")
		if err != nil {
			return op, err
		}
	}
	if op.Checkpoint == domain.CheckpointOldRevocationPending || op.Checkpoint == domain.CheckpointReconciling {
		_, _, callErr := s.core.RevokeExecutionSigner(ctx, toscore.RevokeExecutionSignerRequest{
			AuthorizationID: op.OldAuthorizationID, ProviderID: op.ProviderID, ReasonCode: op.RevocationReasonCode,
		})
		if callErr != nil {
			return s.handleAmbiguousOrFail(ctx, op, callErr)
		}
		op, err = s.advance(ctx, op.ID, domain.CheckpointOldRevoked, "", "")
		if err != nil {
			return op, err
		}
	}
	return s.advance(ctx, op.ID, domain.CheckpointCompleted, "", "")
}

// driveRotate walks op through the full §7.2.2 sequence:
// intent_persisted -> new_authorization_pending -> new_authorized ->
// cutover_pending -> old_revocation_pending -> old_revoked -> completed.
//
// Reconciling is ambiguous between "the authorize call was in flight" and
// "the revoke call was in flight" -- NewAuthorizationRef being durably set
// disambiguates: it is only ever written in the same transaction that
// reaches new_authorized, so its presence means authorize already
// succeeded and any in-flight ambiguity must have been the revoke half.
func (s *ExecutionSignerService) driveRotate(ctx context.Context, op domain.ExecutionSignerOperation) (domain.ExecutionSignerOperation, error) {
	if op.Checkpoint == domain.CheckpointCompleted {
		return op, nil
	}
	var err error
	if op.Checkpoint == domain.CheckpointIntentPersisted {
		op, err = s.advance(ctx, op.ID, domain.CheckpointNewAuthorizationPending, "", "")
		if err != nil {
			return op, err
		}
	}
	authorizingNew := op.Checkpoint == domain.CheckpointNewAuthorizationPending ||
		(op.Checkpoint == domain.CheckpointReconciling && op.NewAuthorizationRef == "")
	if authorizingNew {
		authorization, _, callErr := s.core.AuthorizeExecutionSigner(ctx, toscore.AuthorizeExecutionSignerRequest{
			AuthorizationID: op.NewAuthorizationID, ProviderID: op.ProviderID,
			CapabilityID: op.CapabilityID, CapabilityVersion: op.CapabilityVersion,
			ExecutionSignerID: op.NewExecutionSignerID, SignerPublicKey: op.NewSignerPublicKey,
			SignatureAlgorithm: op.NewSignatureAlgorithm, ValidFrom: op.NewValidFrom, ValidUntil: op.NewValidUntil,
		})
		if callErr != nil {
			return s.handleAmbiguousOrFail(ctx, op, callErr)
		}
		op, err = s.advance(ctx, op.ID, domain.CheckpointNewAuthorized, authorization.AuthorizationRef, "")
		if err != nil {
			return op, err
		}
	}
	if op.Checkpoint == domain.CheckpointNewAuthorized {
		// The new signer is durably authorized but the old signer is
		// still authoritative until this write lands -- cutover_pending
		// is the moment ATOS may start advertising the new signer as
		// current (see currentSigner), strictly before the old signer's
		// revocation ever begins.
		op, err = s.advance(ctx, op.ID, domain.CheckpointCutoverPending, "", "")
		if err != nil {
			return op, err
		}
	}
	if op.Checkpoint == domain.CheckpointCutoverPending {
		op, err = s.advance(ctx, op.ID, domain.CheckpointOldRevocationPending, "", "")
		if err != nil {
			return op, err
		}
	}
	revokingOld := op.Checkpoint == domain.CheckpointOldRevocationPending ||
		(op.Checkpoint == domain.CheckpointReconciling && op.NewAuthorizationRef != "")
	if revokingOld {
		_, _, callErr := s.core.RevokeExecutionSigner(ctx, toscore.RevokeExecutionSignerRequest{
			AuthorizationID: op.OldAuthorizationID, ProviderID: op.ProviderID, ReasonCode: op.RevocationReasonCode,
		})
		if callErr != nil {
			return s.handleAmbiguousOrFail(ctx, op, callErr)
		}
		op, err = s.advance(ctx, op.ID, domain.CheckpointOldRevoked, "", "")
		if err != nil {
			return op, err
		}
	}
	return s.advance(ctx, op.ID, domain.CheckpointCompleted, "", "")
}

// RunReconciler periodically drives forward every non-terminal operation
// that has been stuck for longer than staleAfter, exactly mirroring
// JobService.RunReconciler's shape (internal/service/economic_recovery.go)
// -- the same pattern this codebase already uses for the Managed economic
// reconciler.
func (s *ExecutionSignerService) RunReconciler(ctx context.Context, interval, staleAfter time.Duration, limit int, report func(error)) {
	if interval <= 0 {
		interval = defaultSignerReconcileInterval
	}
	if staleAfter <= 0 {
		staleAfter = defaultSignerReconcileStaleAfter
	}
	if limit <= 0 {
		limit = defaultSignerReconcileBatch
	}
	sweep := func() {
		if err := s.ReconcileStaleOperations(ctx, time.Now().UTC().Add(-staleAfter), limit); err != nil && report != nil {
			report(err)
		}
	}
	sweep()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

// ReconcileStaleOperations drives forward every non-terminal operation
// last updated before cutoff (bounded to limit), resuming each through
// the same drive* function Authorize/Revoke/Rotate use -- a crash, or a
// caller that never retried, converges here instead of staying stuck in
// Reconciling or a *_pending checkpoint forever.
func (s *ExecutionSignerService) ReconcileStaleOperations(ctx context.Context, cutoff time.Time, limit int) error {
	stale, err := s.store.StaleSignerOperations(ctx, cutoff, limit)
	if err != nil {
		return err
	}
	var firstErr error
	for _, op := range stale {
		var driveErr error
		switch op.Type {
		case domain.SignerOperationAuthorize:
			_, driveErr = s.driveAuthorize(ctx, op)
		case domain.SignerOperationRevoke:
			_, driveErr = s.driveRevoke(ctx, op)
		case domain.SignerOperationRotate:
			_, driveErr = s.driveRotate(ctx, op)
		}
		if driveErr != nil && firstErr == nil {
			firstErr = driveErr
		}
	}
	return firstErr
}
