package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

const (
	defaultIdentityBindingReconcileInterval   = 15 * time.Second
	defaultIdentityBindingReconcileStaleAfter = 30 * time.Second
	defaultIdentityBindingReconcileBatch      = 100
	// identityBindingCallerID identifies this operating ATOS backend to
	// tos-protocol (RequestContext.caller_id), never the principal a
	// binding is FOR -- see atos-spec docs/TOS_RPC.md §10.
	identityBindingCallerID = "atos-gateway"
)

// IdentityBindingService implements Phase 4A's durable, idempotent,
// crash-recoverable principal-to-TOS-Agent-Identity binding
// (docs/IMPLEMENTATION_ROADMAP.md §8.1's "production Agent identity
// binding" deliverable). It mirrors ExecutionSignerService's
// open-then-drive-then-reconcile shape, simplified for a single-step (not
// multi-checkpoint) remote operation: CreatePrincipalBinding/
// RevokePrincipalBinding are each ONE atomic, idempotent tos-protocol RPC
// call, so there is no cutover sequence to walk -- only
// intent_persisted -> completed, with Reconciling as the transient
// uncertain-outcome marker every checkpoint can pass through.
type IdentityBindingService struct {
	store store.Store
	core  toscore.Core
}

func NewIdentityBindingService(s store.Store, core toscore.Core) *IdentityBindingService {
	return &IdentityBindingService{store: s, core: core}
}

func (s *IdentityBindingService) resumeOrConflict(
	ctx context.Context, principalID, idempotencyKey string,
	sameStableIdentity func(existing domain.IdentityBindingOperation) bool,
) (op domain.IdentityBindingOperation, resume bool, err error) {
	existing, err := s.store.IdentityBindingOperationByIdempotencyKey(ctx, principalID, idempotencyKey)
	if errors.Is(err, store.ErrNotFound) {
		return domain.IdentityBindingOperation{}, false, nil
	}
	if err != nil {
		return domain.IdentityBindingOperation{}, false, err
	}
	if !sameStableIdentity(existing) {
		return domain.IdentityBindingOperation{}, false, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with different identity-binding operation content", false)
	}
	return existing, true, nil
}

type BindIdentityInput struct {
	PrincipalID    string
	AgentID        string
	IdempotencyKey string
}

func (in BindIdentityInput) validate() error {
	if strings.TrimSpace(in.PrincipalID) == "" || strings.TrimSpace(in.AgentID) == "" {
		return domain.NewError(domain.ErrValidationFailed, "principal_id and agent_id are required", false)
	}
	if strings.TrimSpace(in.IdempotencyKey) == "" {
		return domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}
	return nil
}

// Bind establishes (or, on an idempotent replay, resumes/returns) the
// durable binding from PrincipalID to AgentID. It does NOT itself verify
// AgentID resolves -- tos-protocol's CreatePrincipalBinding is the sole
// authority for that (see atos-spec docs/TOS_RPC.md §10: never trust a
// caller-supplied Agent ID by itself).
func (s *IdentityBindingService) Bind(ctx context.Context, in BindIdentityInput) (domain.IdentityBindingOperation, error) {
	if err := in.validate(); err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	existing, resume, err := s.resumeOrConflict(ctx, in.PrincipalID, in.IdempotencyKey, func(existing domain.IdentityBindingOperation) bool {
		return existing.Type == domain.IdentityBindingOperationBind && existing.AgentID == in.AgentID
	})
	if err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	if resume {
		return s.drive(ctx, existing)
	}
	now := time.Now().UTC()
	op := domain.IdentityBindingOperation{
		ID: "idop_" + uuid.NewString(), PrincipalID: in.PrincipalID,
		Type: domain.IdentityBindingOperationBind, Checkpoint: domain.IdentityBindingCheckpointIntentPersisted,
		IdempotencyKey: in.IdempotencyKey, AgentID: in.AgentID,
		CreatedAt: now, UpdatedAt: now,
	}
	stored, _, err := s.store.OpenIdentityBindingOperation(ctx, in.PrincipalID, op)
	if err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	return s.drive(ctx, stored)
}

type RevokeIdentityBindingInput struct {
	PrincipalID    string
	ReasonCode     string
	IdempotencyKey string
}

func (in RevokeIdentityBindingInput) validate() error {
	if strings.TrimSpace(in.PrincipalID) == "" {
		return domain.NewError(domain.ErrValidationFailed, "principal_id is required", false)
	}
	if strings.TrimSpace(in.IdempotencyKey) == "" {
		return domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}
	return nil
}

// Revoke ends PrincipalID's current binding. A principal with no current
// binding is not an error -- the operation still completes, mirroring
// tos-protocol's own RevokePrincipalBinding "revoked=false, nil error"
// convention.
func (s *IdentityBindingService) Revoke(ctx context.Context, in RevokeIdentityBindingInput) (domain.IdentityBindingOperation, error) {
	if err := in.validate(); err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	existing, resume, err := s.resumeOrConflict(ctx, in.PrincipalID, in.IdempotencyKey, func(existing domain.IdentityBindingOperation) bool {
		return existing.Type == domain.IdentityBindingOperationRevoke && existing.ReasonCode == in.ReasonCode
	})
	if err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	if resume {
		return s.drive(ctx, existing)
	}
	now := time.Now().UTC()
	op := domain.IdentityBindingOperation{
		ID: "idop_" + uuid.NewString(), PrincipalID: in.PrincipalID,
		Type: domain.IdentityBindingOperationRevoke, Checkpoint: domain.IdentityBindingCheckpointIntentPersisted,
		IdempotencyKey: in.IdempotencyKey, ReasonCode: in.ReasonCode,
		CreatedAt: now, UpdatedAt: now,
	}
	stored, _, err := s.store.OpenIdentityBindingOperation(ctx, in.PrincipalID, op)
	if err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	return s.drive(ctx, stored)
}

// CurrentBinding returns the durable current-state binding for
// principalID, if any -- the local read path (does not itself call
// tos-protocol; TOSBackedActivationAuthority re-resolves freshness
// separately, since cached local state is never authority -- see the
// Phase 4A brief's persistence/caching/freshness requirement).
func (s *IdentityBindingService) CurrentBinding(ctx context.Context, principalID string) (domain.PrincipalIdentityBinding, bool, error) {
	return s.store.CurrentPrincipalBinding(ctx, principalID)
}

func (s *IdentityBindingService) drive(ctx context.Context, op domain.IdentityBindingOperation) (domain.IdentityBindingOperation, error) {
	if op.Checkpoint == domain.IdentityBindingCheckpointCompleted {
		return op, nil
	}
	if op.Type == domain.IdentityBindingOperationBind {
		return s.driveBind(ctx, op)
	}
	return s.driveRevoke(ctx, op)
}

func (s *IdentityBindingService) driveBind(ctx context.Context, op domain.IdentityBindingOperation) (domain.IdentityBindingOperation, error) {
	binding, _, callErr := s.core.CreatePrincipalBinding(ctx, identityBindingCallerID, op.IdempotencyKey, op.PrincipalID, op.AgentID)
	if callErr != nil {
		return s.handleAmbiguousOrFail(ctx, op, callErr)
	}
	now := time.Now().UTC()
	if err := s.store.PutPrincipalBinding(ctx, domain.PrincipalIdentityBinding{
		PrincipalID: op.PrincipalID, AgentID: binding.AgentID, Network: binding.Network, BindingRef: binding.BindingRef,
		BoundAt: now, UpdatedAt: now,
	}); err != nil {
		return op, err
	}
	return s.advance(ctx, op.ID, op.Checkpoint, domain.IdentityBindingCheckpointCompleted, binding.BindingRef, binding.Network, "")
}

func (s *IdentityBindingService) driveRevoke(ctx context.Context, op domain.IdentityBindingOperation) (domain.IdentityBindingOperation, error) {
	_, revocationNetwork, revocationRef, callErr := s.core.RevokePrincipalBinding(ctx, identityBindingCallerID, op.IdempotencyKey, op.PrincipalID, op.ReasonCode)
	if callErr != nil {
		return s.handleAmbiguousOrFail(ctx, op, callErr)
	}
	if err := s.store.DeletePrincipalBinding(ctx, op.PrincipalID); err != nil {
		return op, err
	}
	// A revoke with nothing to revoke (revocationRef=="") still completes
	// the operation but leaves RefNetwork/BindingRef empty -- advance's
	// bindingRef!="" guard already handles that as a no-op write.
	return s.advance(ctx, op.ID, op.Checkpoint, domain.IdentityBindingCheckpointCompleted, revocationRef, revocationNetwork, "")
}

func (s *IdentityBindingService) advance(ctx context.Context, id string, expectedFrom, checkpoint domain.IdentityBindingCheckpoint, bindingRef, refNetwork, failureReason string) (domain.IdentityBindingOperation, error) {
	return s.store.UpdateIdentityBindingOperation(ctx, id, func(current domain.IdentityBindingOperation, exists bool) (domain.IdentityBindingOperation, error) {
		if !exists {
			return domain.IdentityBindingOperation{}, domain.NewError(domain.ErrNotFound, "identity-binding operation not found", false)
		}
		if current.Checkpoint.Terminal() || current.Checkpoint != expectedFrom {
			return current, nil
		}
		current.Checkpoint = checkpoint
		if bindingRef != "" {
			current.BindingRef = bindingRef
			current.RefNetwork = refNetwork
		}
		current.FailureReason = failureReason
		current.UpdatedAt = time.Now().UTC()
		if checkpoint == domain.IdentityBindingCheckpointCompleted {
			completedAt := current.UpdatedAt
			current.CompletedAt = &completedAt
		}
		return current, nil
	})
}

// isAmbiguousIdentityBindingFailure mirrors isAmbiguousSignerFailure's
// role exactly: distinguishes an uncertain remote outcome (retryable)
// from a definitive rejection, reusing domain.Error.Retryable rather than
// inventing a second signal.
func isAmbiguousIdentityBindingFailure(err error) bool {
	var de *domain.Error
	if errors.As(err, &de) {
		return de.Retryable
	}
	return true
}

func (s *IdentityBindingService) handleAmbiguousOrFail(ctx context.Context, op domain.IdentityBindingOperation, callErr error) (domain.IdentityBindingOperation, error) {
	if isAmbiguousIdentityBindingFailure(callErr) {
		updated, err := s.advance(ctx, op.ID, op.Checkpoint, domain.IdentityBindingCheckpointReconciling, "", "", callErr.Error())
		if err != nil {
			return updated, err
		}
		return updated, domain.NewError(domain.ErrNetworkUnavailable, "identity-binding operation outcome is uncertain, retry", true)
	}
	updated, err := s.advance(ctx, op.ID, op.Checkpoint, op.Checkpoint, "", "", callErr.Error())
	if err != nil {
		return updated, err
	}
	return updated, callErr
}

// RunReconciler periodically drives forward every non-terminal operation
// that has been stuck for longer than staleAfter, exactly mirroring
// ExecutionSignerService.RunReconciler's shape.
func (s *IdentityBindingService) RunReconciler(ctx context.Context, interval, staleAfter time.Duration, limit int, report func(error)) {
	if interval <= 0 {
		interval = defaultIdentityBindingReconcileInterval
	}
	if staleAfter <= 0 {
		staleAfter = defaultIdentityBindingReconcileStaleAfter
	}
	if limit <= 0 {
		limit = defaultIdentityBindingReconcileBatch
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
// last updated before cutoff (bounded to limit) -- a crash, or a caller
// that never retried, converges here instead of staying stuck in
// Reconciling or intent_persisted forever.
func (s *IdentityBindingService) ReconcileStaleOperations(ctx context.Context, cutoff time.Time, limit int) error {
	stale, err := s.store.StaleIdentityBindingOperations(ctx, cutoff, limit)
	if err != nil {
		return err
	}
	var firstErr error
	for _, op := range stale {
		if _, driveErr := s.drive(ctx, op); driveErr != nil && firstErr == nil {
			firstErr = driveErr
		}
	}
	return firstErr
}
