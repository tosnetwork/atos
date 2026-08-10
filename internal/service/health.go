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

const (
	defaultHealthReconcileInterval = 5 * time.Minute
	defaultHealthReconcileBatch    = 200
)

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
		// A recorded health check -- healthy or not -- is itself the
		// §7.2.0 `requested -> pending` readiness-evidence trigger; an
		// unhealthy result additionally invalidates any currently active
		// mode this binding was activated for (`active -> suspended`).
		if err := s.capabilities.RecordReadinessEvidence(ctx, cap.ID, binding.Transport); err != nil {
			return nil, err
		}
		if check.Status != domain.AdapterHealthHealthy {
			if err := s.capabilities.SuspendModeIfActive(ctx, cap.ID, binding.Transport, "health check failed: "+check.FailureReason); err != nil {
				return nil, err
			}
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
		healthy, fresh, certified := false, false, false
		for _, binding := range cap.Bindings {
			if !slices.Contains(binding.EligibleTrustModes, mode) {
				continue
			}
			if !isThirdPartyTransport(binding.Transport) {
				// tos-native/human bindings have no external endpoint to
				// probe -- treated as inherently reachable and always fresh.
				healthy, fresh = true, true
				continue
			}
			check, found, err := s.store.HealthCheck(ctx, cap.ID, cap.Version, binding.Transport)
			if err != nil {
				return nil, err
			}
			if found {
				// TransportHealthy and HealthFresh are deliberately
				// independent dimensions -- a stale-but-formerly-healthy
				// check reports healthy:true, fresh:false, so a caller can
				// tell "last known good" apart from "checked recently".
				if check.Status == domain.AdapterHealthHealthy {
					healthy = true
				}
				if !check.Stale(now, maxHealthAge) {
					fresh = true
				}
			}
			if certifiedTransports[binding.Transport] {
				certified = true
			}
		}
		status := cap.ModeSupport.Entry(mode).Status
		active := status == domain.ModeSupportActive
		// Managed has no execution-signer concept at all (docs/API.md
		// §2/§7.2.3), so it trivially reports satisfied rather than
		// blocked on a dimension that will never apply to it.
		signerAuthorized := mode == domain.TrustModeManaged
		out = append(out, domain.ModeAvailability{
			Mode: mode, Requested: true, Active: active, Status: status,
			TransportHealthy: healthy, HealthFresh: fresh, Certified: certified,
			SignerAuthorized: signerAuthorized, ActivationAuthoritySatisfied: active,
			ReasonCode: domain.ReadinessReasonCode(status, healthy, fresh, certified, signerAuthorized, active),
			Ready:      healthy && certified,
		})
	}
	return out, nil
}

// SweepStaleCapabilities checks every capability (bounded by limit, active
// capabilities only, per store.Search's own semantics) that has at least
// one third-party binding whose health evidence is missing or older than
// maxHealthAge. This is the production entry point that was previously
// missing entirely: CheckCapability existed with full test coverage but
// atos-spec docs/IMPLEMENTATION_ROADMAP.md's §7.1.1/§7.1.3 "known gap"
// notes recorded that nothing in cmd/api/main.go ever called it. A
// capability with only tos-native/human bindings, or whose third-party
// bindings are all already fresh, is skipped without dialing anything.
func (s *HealthService) SweepStaleCapabilities(ctx context.Context, limit int) error {
	caps, err := s.store.Search(ctx, "", limit)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	var firstErr error
	for _, cap := range caps {
		stale := false
		for _, binding := range cap.Bindings {
			if !isThirdPartyTransport(binding.Transport) {
				continue
			}
			check, found, checkErr := s.store.HealthCheck(ctx, cap.ID, cap.Version, binding.Transport)
			if checkErr != nil {
				if firstErr == nil {
					firstErr = checkErr
				}
				continue
			}
			if !found || check.Stale(now, maxHealthAge) {
				stale = true
				break
			}
		}
		if !stale {
			continue
		}
		if _, checkErr := s.CheckCapability(ctx, cap.ID); checkErr != nil && firstErr == nil {
			firstErr = checkErr
		}
	}
	return firstErr
}

// RunReconciler periodically sweeps for capabilities with stale or
// missing health evidence, mirroring JobService.RunReconciler's shape
// (internal/service/economic_recovery.go) -- the same established pattern
// this codebase already uses for its other periodic reconcilers.
func (s *HealthService) RunReconciler(ctx context.Context, interval time.Duration, limit int, report func(error)) {
	if interval <= 0 {
		interval = defaultHealthReconcileInterval
	}
	if limit <= 0 {
		limit = defaultHealthReconcileBatch
	}
	sweep := func() {
		if err := s.SweepStaleCapabilities(ctx, limit); err != nil && report != nil {
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
