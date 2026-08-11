package service

import (
	"context"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

const (
	defaultIdentityEvidenceReconcileInterval = 5 * time.Minute
	defaultIdentityEvidenceReconcileBatch    = 200
)

// IdentityEvidenceReconciler periodically re-verifies every Capability
// currently active for Verified against domain.ActivationAuthority,
// suspending any whose provider identity, network, capability ownership,
// manifest or execution-signer evidence has gone stale SINCE activation
// (docs/IMPLEMENTATION_ROADMAP.md §8.1: "loss of current evidence suspends
// future Verified availability through the existing state machine"). Unlike
// EvaluateActivation (an explicit admin-triggered pending/suspended ->
// active transition), this reconciler drives the active -> suspended
// direction automatically, mirroring HealthService/CertificationService's
// existing periodic-sweep role for health/certification evidence -- before
// this, only an admin re-running EvaluateActivation could ever notice a
// revoked identity.
type IdentityEvidenceReconciler struct {
	capabilities *CapabilityService
	authority    domain.ActivationAuthority
}

func NewIdentityEvidenceReconciler(capabilities *CapabilityService, authority domain.ActivationAuthority) *IdentityEvidenceReconciler {
	return &IdentityEvidenceReconciler{capabilities: capabilities, authority: authority}
}

// SweepVerified re-evaluates every active-Verified Capability (bounded to
// limit) and suspends any that no longer pass. A capability that keeps
// failing is picked up again on the next sweep -- ActiveByMode's
// oldest-updated-first ordering means a just-suspended capability (whose
// UpdatedAt just advanced) sorts to the back of the next sweep, so one
// stuck capability cannot starve the rest of the scan.
func (r *IdentityEvidenceReconciler) SweepVerified(ctx context.Context, limit int) error {
	if r == nil || r.capabilities == nil || r.authority == nil {
		return nil
	}
	caps, err := r.capabilities.ActiveByMode(ctx, domain.TrustModeVerified, limit)
	if err != nil {
		return err
	}
	var firstErr error
	for _, cap := range caps {
		granted, reasonCode, err := r.authority.Evaluate(ctx, cap.ProviderID, cap.ID, cap.Version, domain.TrustModeVerified)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if granted {
			continue
		}
		if err := r.capabilities.SuspendMode(ctx, cap.ID, domain.TrustModeVerified, reasonCode); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// RunReconciler mirrors every other periodic sweep in this package
// (HealthService.RunReconciler, ExecutionSignerService.RunReconciler): an
// immediate sweep, then one every interval, until ctx is cancelled.
func (r *IdentityEvidenceReconciler) RunReconciler(ctx context.Context, interval time.Duration, limit int, report func(error)) {
	if interval <= 0 {
		interval = defaultIdentityEvidenceReconcileInterval
	}
	if limit <= 0 {
		limit = defaultIdentityEvidenceReconcileBatch
	}
	sweep := func() {
		if err := r.SweepVerified(ctx, limit); err != nil && report != nil {
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
