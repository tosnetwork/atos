package service

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// CertificationService runs the Phase 3A sandbox certification workflow: a
// durable, idempotent readiness check for one Capability transport
// binding. Passing certification is readiness evidence ONLY -- this
// service never reads or writes domain.ModeSupport/SupportedTrustModes.
// See domain.SandboxCertification's doc comment for the full invariant.
type CertificationService struct {
	store        store.Store
	capabilities *CapabilityService
	resolver     *provideradapter.Resolver
}

func NewCertificationService(s store.Store, capabilities *CapabilityService, resolver *provideradapter.Resolver) *CertificationService {
	return &CertificationService{store: s, capabilities: capabilities, resolver: resolver}
}

type OpenCertificationInput struct {
	ProviderID     string
	CapabilityID   string
	Transport      domain.EndpointAdapterType
	IdempotencyKey string
}

// Open idempotently opens (or recovers) a certification for one
// Capability transport binding, and runs the certification probe itself
// -- currently scoped to the adapter's Health check (endpoint reachable,
// protocol handshake succeeds), the minimum the roadmap's certification
// checklist requires; deeper functional probing (dispatching an actual
// sample invocation against a real third-party endpoint) is deliberately
// out of scope for Phase 3A to avoid causing a real side effect against a
// provider merely to certify it.
//
// The probe is a read-only reachability check, so it is always safe to
// repeat: a replay of the same (providerID, IdempotencyKey) whose prior
// attempt is still Pending (e.g. a crash between OpenCertification's
// commit and the completing UpdateCertification) re-runs the probe rather
// than being stuck forever -- this is what makes "restart/retry
// converges" hold without needing a separate reconciler loop. A replay
// whose prior attempt already reached a terminal status (Passed/Failed)
// returns it unchanged, never re-probing.
func (s *CertificationService) Open(ctx context.Context, in OpenCertificationInput) (domain.SandboxCertification, error) {
	if in.ProviderID == "" || in.CapabilityID == "" || in.Transport == "" || in.IdempotencyKey == "" {
		return domain.SandboxCertification{}, domain.NewError(domain.ErrValidationFailed, "provider_id, capability_id, transport and idempotency_key are required", false)
	}
	cap, err := s.capabilities.Get(ctx, in.CapabilityID)
	if err != nil {
		return domain.SandboxCertification{}, err
	}
	if cap.ProviderID != in.ProviderID {
		return domain.SandboxCertification{}, domain.NewError(domain.ErrPermissionDenied, "not the owning provider", false)
	}
	var binding domain.CapabilityBinding
	found := false
	for _, b := range cap.Bindings {
		if b.Transport == in.Transport {
			binding, found = b, true
			break
		}
	}
	if !found {
		return domain.SandboxCertification{}, domain.NewError(domain.ErrValidationFailed, "capability has no binding for the requested transport", false)
	}

	now := time.Now().UTC()
	cert := domain.SandboxCertification{
		ID: "cert_" + uuid.NewString(), ProviderID: in.ProviderID, CapabilityID: cap.ID, CapabilityVersion: cap.Version,
		Transport: binding.Transport, EndpointRef: binding.EndpointRef,
		Status: domain.CertificationPending, IdempotencyKey: in.IdempotencyKey,
		CreatedAt: now, UpdatedAt: now,
	}
	stored, created, err := s.store.OpenCertification(ctx, in.ProviderID, cert)
	if err != nil {
		return domain.SandboxCertification{}, err
	}
	if !created && stored.Status != domain.CertificationPending {
		// Already reached a terminal outcome -- idempotent no-op replay.
		return stored, nil
	}

	adapter, ok := s.resolver.For(binding.Transport)
	if !ok {
		return s.completeCertification(ctx, stored.ID, domain.CertificationFailed, "no provider adapter registered for this transport", nil)
	}
	check := adapter.Health(ctx, binding.EndpointRef)
	evidence := map[string]any{"health_status": string(check.Status), "latency_ms": check.LatencyMS}
	if check.Status == domain.AdapterHealthHealthy {
		return s.completeCertification(ctx, stored.ID, domain.CertificationPassed, "", evidence)
	}
	return s.completeCertification(ctx, stored.ID, domain.CertificationFailed, check.FailureReason, evidence)
}

func (s *CertificationService) completeCertification(ctx context.Context, id string, status domain.CertificationStatus, failureReason string, evidence map[string]any) (domain.SandboxCertification, error) {
	return s.store.UpdateCertification(ctx, id, func(c domain.SandboxCertification, exists bool) (domain.SandboxCertification, error) {
		if !exists {
			return domain.SandboxCertification{}, store.ErrNotFound
		}
		if c.Status.Terminal() {
			// Another concurrent caller already completed it -- leave it
			// exactly as-is rather than overwriting a terminal result.
			return c, nil
		}
		now := time.Now().UTC()
		c.Status = status
		c.FailureReason = failureReason
		c.Evidence = evidence
		c.CompletedAt = &now
		c.UpdatedAt = now
		return c, nil
	})
}

func (s *CertificationService) Get(ctx context.Context, id string) (domain.SandboxCertification, error) {
	c, err := s.store.GetCertification(ctx, id)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.SandboxCertification{}, domain.NewError(domain.ErrNotFound, "certification not found", false)
		}
		return domain.SandboxCertification{}, err
	}
	return c, nil
}

func (s *CertificationService) ListByCapability(ctx context.Context, capabilityID string) ([]domain.SandboxCertification, error) {
	return s.store.CertificationsByCapability(ctx, capabilityID)
}
