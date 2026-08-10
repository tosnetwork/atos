package service

import (
	"context"
	"maps"
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
	remoteProber ThirdPartyHealthProber
}

func NewCertificationService(s store.Store, capabilities *CapabilityService, resolver *provideradapter.Resolver) *CertificationService {
	return &CertificationService{store: s, capabilities: capabilities, resolver: resolver}
}

// WithRemoteProber routes third-party transport certification probing
// through p (the execution/data-plane boundary) instead of dialing
// binding.EndpointRef from this process via resolver. See
// ThirdPartyHealthProber's doc comment (internal/service/health.go).
func (s *CertificationService) WithRemoteProber(p ThirdPartyHealthProber) *CertificationService {
	s.remoteProber = p
	return s
}

type OpenCertificationInput struct {
	ProviderID     string
	CapabilityID   string
	Transport      domain.EndpointAdapterType
	IdempotencyKey string
}

// Open idempotently opens (or recovers) a certification for one
// Capability transport binding, and runs the certification probe itself:
// first Health (bounded reachability, cheap fail-fast), then -- if the
// resolved adapter implements provideradapter.CertificationProbe -- the
// deeper, transport-specific check that validates more than reachability
// (see each adapter's own doc comment for exactly what it can and cannot
// check). Dispatching an actual sample invocation against a real
// third-party endpoint remains deliberately out of scope for Phase 3A, to
// avoid causing a real side effect against a provider merely to certify
// it; CertificationProbe implementations are constrained to read-only/
// introspection wire operations for exactly this reason.
//
// Evidence always records whether the deeper probe ran ("deep_probe":
// true/false) so a Passed certification is never mistaken for uniform
// depth across transports -- an HTTP or A2A Passed result today is
// weaker evidence than an MCP Passed result, and that difference is
// visible in the persisted record, not hidden.
//
// The probe is read-only, so it is always safe to repeat: a replay of the
// same (providerID, IdempotencyKey) whose prior attempt is still Pending
// (e.g. a crash between OpenCertification's commit and the completing
// UpdateCertification) re-runs the probe rather than being stuck forever
// -- this is what makes "restart/retry converges" hold without needing a
// separate reconciler loop. A replay whose prior attempt already reached
// a terminal status (Passed/Failed) returns it unchanged, never
// re-probing.
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

	if s.remoteProber != nil {
		check, err := s.remoteProber.ProbeThirdPartyHealth(ctx, in.ProviderID, cap.ID, cap.Version, binding)
		if err != nil {
			return s.completeCertification(ctx, stored.ID, domain.CertificationFailed, err.Error(), nil)
		}
		evidence := map[string]any{"health_status": string(check.Status), "latency_ms": check.LatencyMS, "deep_probe": check.DeepProbe}
		if check.Status != domain.AdapterHealthHealthy {
			return s.completeCertification(ctx, stored.ID, domain.CertificationFailed, check.FailureReason, evidence)
		}
		return s.completeCertification(ctx, stored.ID, domain.CertificationPassed, "", evidence)
	}

	adapter, ok := s.resolver.For(binding.Transport)
	if !ok {
		return s.completeCertification(ctx, stored.ID, domain.CertificationFailed, "no provider adapter registered for this transport", nil)
	}
	check := adapter.Health(ctx, binding.EndpointRef)
	evidence := map[string]any{"health_status": string(check.Status), "latency_ms": check.LatencyMS}
	if check.Status != domain.AdapterHealthHealthy {
		return s.completeCertification(ctx, stored.ID, domain.CertificationFailed, check.FailureReason, evidence)
	}

	prober, deepProbeSupported := adapter.(provideradapter.CertificationProbe)
	evidence["deep_probe"] = deepProbeSupported
	if !deepProbeSupported {
		return s.completeCertification(ctx, stored.ID, domain.CertificationPassed, "", evidence)
	}
	probeEvidence, err := prober.ProbeCertification(ctx, binding.EndpointRef, cap.InputSchema, cap.OutputSchema)
	maps.Copy(evidence, probeEvidence)
	if err != nil {
		return s.completeCertification(ctx, stored.ID, domain.CertificationFailed, err.Error(), evidence)
	}
	return s.completeCertification(ctx, stored.ID, domain.CertificationPassed, "", evidence)
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
