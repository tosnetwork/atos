package service

import (
	"context"
	"slices"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// maxHealthAge bounds how long a recorded health check is trusted as
// current -- an old success must not be treated as indefinitely fresh.
const maxHealthAge = 10 * time.Minute

// ThirdPartyHealthProber probes a third-party transport binding through
// the execution/data-plane boundary (tos-protocol -> tos-ai's operator-
// allowlisted ThirdPartyExecutionService) instead of this process dialing
// binding.EndpointRef itself. Implemented by
// tosprotocol.Client.ProbeThirdPartyHealth -- see atos-spec
// docs/THIRD_PARTY_EXECUTION_PLANE.md §3.1 and this repository's own
// §7.1.1 placement rule, which HealthService/CertificationService satisfy
// only when a prober is configured via WithRemoteProber; a nil prober (the
// default) keeps dialing binding.EndpointRef locally via resolver, exactly
// as before this option existed.
type ThirdPartyHealthProber interface {
	ProbeThirdPartyHealth(ctx context.Context, providerID, capabilityID, capabilityVersion string, binding domain.CapabilityBinding) (domain.AdapterHealthCheck, error)
}

// HealthService checks provider adapter reachability per Capability
// binding and projects per-mode readiness from it. Health is evidence
// only: nothing here reads or writes domain.ModeSupport/
// SupportedTrustModes -- see domain.AdapterHealthCheck's doc comment.
type HealthService struct {
	store        store.Store
	capabilities *CapabilityService
	resolver     *provideradapter.Resolver
	remoteProber ThirdPartyHealthProber
}

func NewHealthService(s store.Store, capabilities *CapabilityService, resolver *provideradapter.Resolver) *HealthService {
	return &HealthService{store: s, capabilities: capabilities, resolver: resolver}
}

// WithRemoteProber routes third-party transport health probing through p
// (the execution/data-plane boundary) instead of dialing
// binding.EndpointRef from this process via resolver. See
// ThirdPartyHealthProber's doc comment.
func (s *HealthService) WithRemoteProber(p ThirdPartyHealthProber) *HealthService {
	s.remoteProber = p
	return s
}

// isThirdPartyTransport reports whether transport routes through a
// provideradapter.ProviderAdapter -- mirrors
// internal/adapters/tosai/dispatch's identically-named unexported check.
func isThirdPartyTransport(transport domain.EndpointAdapterType) bool {
	switch transport {
	case domain.AdapterHTTP, domain.AdapterMCP, domain.AdapterA2A:
		return true
	default:
		return false
	}
}

// CheckCapability probes every third-party transport binding on
// capabilityID and durably records the result. tos-native/human bindings
// have no external endpoint to probe and are skipped. A binding whose
// transport has no registered adapter is skipped (best-effort, not an
// error) rather than failing the whole capability's health sweep.
func (s *HealthService) CheckCapability(ctx context.Context, capabilityID string) ([]domain.AdapterHealthCheck, error) {
	cap, err := s.capabilities.Get(ctx, capabilityID)
	if err != nil {
		return nil, err
	}
	var out []domain.AdapterHealthCheck
	seen := map[domain.EndpointAdapterType]bool{}
	for _, binding := range cap.Bindings {
		if !isThirdPartyTransport(binding.Transport) || seen[binding.Transport] {
			continue
		}
		seen[binding.Transport] = true
		var check domain.AdapterHealthCheck
		if s.remoteProber != nil {
			var err error
			check, err = s.remoteProber.ProbeThirdPartyHealth(ctx, cap.ProviderID, cap.ID, cap.Version, binding)
			if err != nil {
				return nil, err
			}
		} else {
			adapter, ok := s.resolver.For(binding.Transport)
			if !ok {
				continue
			}
			check = adapter.Health(ctx, binding.EndpointRef)
		}
		check.CapabilityID = cap.ID
		check.CapabilityVersion = cap.Version
		check.Transport = binding.Transport
		check.EndpointRef = binding.EndpointRef
		if err := s.store.PutHealthCheck(ctx, check); err != nil {
			return nil, err
		}
		out = append(out, check)
	}
	return out, nil
}

// Availability computes the Phase 3A internal readiness projection for
// every trust mode capabilityID's provider requested. This is pure
// readiness evidence -- Active mirrors the capability's CURRENT
// ModeSupport for read-model convenience only, and Ready is computed
// purely from Requested/TransportHealthy/Certified, never derived from or
// fed back into Active. See domain.ModeAvailability's doc comment.
func (s *HealthService) Availability(ctx context.Context, capabilityID string) ([]domain.ModeAvailability, error) {
	cap, err := s.capabilities.Get(ctx, capabilityID)
	if err != nil {
		return nil, err
	}
	certs, err := s.store.CertificationsByCapability(ctx, capabilityID)
	if err != nil {
		return nil, err
	}
	certifiedTransports := map[domain.EndpointAdapterType]bool{}
	for _, c := range certs {
		if c.Status == domain.CertificationPassed && c.CapabilityVersion == cap.Version {
			certifiedTransports[c.Transport] = true
		}
	}

	now := time.Now().UTC()
	out := make([]domain.ModeAvailability, 0, len(cap.RequestedTrustModes))
	for _, mode := range cap.RequestedTrustModes {
		healthy, certified := false, false
		for _, binding := range cap.Bindings {
			if !slices.Contains(binding.EligibleTrustModes, mode) {
				continue
			}
			if !isThirdPartyTransport(binding.Transport) {
				// tos-native/human bindings have no external endpoint to
				// probe -- treated as inherently reachable.
				healthy = true
				continue
			}
			check, found, err := s.store.HealthCheck(ctx, cap.ID, cap.Version, binding.Transport)
			if err != nil {
				return nil, err
			}
			if found && !check.Stale(now, maxHealthAge) && check.Status == domain.AdapterHealthHealthy {
				healthy = true
			}
			if certifiedTransports[binding.Transport] {
				certified = true
			}
		}
		out = append(out, domain.ModeAvailability{
			Mode: mode, Requested: true, Active: cap.ModeSupport.Active(mode),
			TransportHealthy: healthy, Certified: certified,
			Ready: healthy && certified,
		})
	}
	return out, nil
}
