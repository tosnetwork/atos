package service_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// grantingActivationAuthority is a test-only domain.ActivationAuthority
// that always grants -- it proves the positive `pending -> active` /
// `suspended -> active` path exists and is reachable through the real
// interface, without being wired into any production configuration (see
// service.FailClosedActivationAuthority's doc comment for the production
// implementation this stands in contrast to).
type grantingActivationAuthority struct{ calls int }

func (g *grantingActivationAuthority) Evaluate(_ context.Context, providerID, capabilityID, capabilityVersion string, mode domain.TrustMode) (bool, string, error) {
	g.calls++
	return true, "", nil
}

// TestHealthCheck_TriggersRequestedToPending proves a recorded health
// check -- healthy or not -- is the §7.2.0 `requested -> pending`
// readiness-evidence trigger, and that it is a no-op once the mode has
// already moved past `requested`.
func TestHealthCheck_TriggersRequestedToPending(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_ma_1", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	if got := cap.ModeSupport.Entry(domain.TrustModeVerified).Status; got != domain.ModeSupportRequested {
		t.Fatalf("freshly registered status = %q, want requested", got)
	}

	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	after, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := after.ModeSupport.Entry(domain.TrustModeVerified).Status; got != domain.ModeSupportPending {
		t.Fatalf("status after health check = %q, want pending", got)
	}

	// Calling it again must be a no-op, not an error and not a status
	// bounce -- pending is not a legal source for AdvanceToPending.
	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	after2, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := after2.ModeSupport.Entry(domain.TrustModeVerified).Status; got != domain.ModeSupportPending {
		t.Fatalf("status after second health check = %q, want still pending", got)
	}
}

// TestEvaluateActivation_RejectsIllegalSourceStates proves EvaluateActivation
// refuses to call the authority at all -- and performs no state change --
// for any mode not currently pending or suspended, per §7.2.0's frozen
// transition table (only pending/suspended are legal sources for active).
func TestEvaluateActivation_RejectsIllegalSourceStates(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_ma_2", Name: "Reject Test", Description: "d",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified},
		IdempotencyKey:      "register-agt-ma-2",
	})
	if err != nil {
		t.Fatal(err)
	}

	authority := &grantingActivationAuthority{}
	// Managed is already `active` -- not a legal source.
	if _, _, err := capabilities.EvaluateActivation(ctx, authority, "prn_admin_ma_2", cap.ID, domain.TrustModeManaged, "eval-ma-2-managed"); err == nil {
		t.Fatal("expected error evaluating activation for an already-active mode")
	}
	// Verified is freshly `requested`, not yet `pending` -- also not a
	// legal source.
	if _, _, err := capabilities.EvaluateActivation(ctx, authority, "prn_admin_ma_2", cap.ID, domain.TrustModeVerified, "eval-ma-2-verified"); err == nil {
		t.Fatal("expected error evaluating activation for a merely-requested mode")
	}
	if authority.calls != 0 {
		t.Fatalf("authority.Evaluate must not be called for an illegal source state, got %d calls", authority.calls)
	}
}

// TestEvaluateActivation_FailClosedAuthorityLeavesPending proves the
// production FailClosedActivationAuthority never activates verified/native
// and records its stable reason code, per §7.2.1.
func TestEvaluateActivation_FailClosedAuthorityLeavesPending(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_ma_3", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}

	granted, reasonCode, err := capabilities.EvaluateActivation(ctx, service.FailClosedActivationAuthority{}, "prn_admin_ma_3", cap.ID, domain.TrustModeVerified, "eval-ma-3")
	if err != nil {
		t.Fatal(err)
	}
	if granted {
		t.Fatal("FailClosedActivationAuthority must never grant")
	}
	if reasonCode != domain.ActivationAuthorityUnavailable {
		t.Fatalf("reason code = %q, want %q", reasonCode, domain.ActivationAuthorityUnavailable)
	}
	after, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry := after.ModeSupport.Entry(domain.TrustModeVerified)
	if entry.Status != domain.ModeSupportPending {
		t.Fatalf("status = %q, want still pending", entry.Status)
	}
	if entry.Reason != domain.ActivationAuthorityUnavailable {
		t.Fatalf("entry.Reason = %q, want %q", entry.Reason, domain.ActivationAuthorityUnavailable)
	}
	for _, mode := range after.SupportedTrustModes {
		if mode == domain.TrustModeVerified {
			t.Fatal("verified must not appear in supported_trust_modes without an activation grant")
		}
	}
}

// TestEvaluateActivation_GrantedActivatesAndDerivesSupportedModes proves
// the positive path: pending + a granting ActivationAuthority = active,
// and supported_trust_modes is correctly derived from it -- the §7.2.4
// success criterion's activation half.
func TestEvaluateActivation_GrantedActivatesAndDerivesSupportedModes(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_ma_4", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}

	authority := &grantingActivationAuthority{}
	granted, _, err := capabilities.EvaluateActivation(ctx, authority, "prn_admin_ma_4", cap.ID, domain.TrustModeVerified, "eval-ma-4")
	if err != nil {
		t.Fatal(err)
	}
	if !granted {
		t.Fatal("expected activation to be granted")
	}
	if authority.calls != 1 {
		t.Fatalf("authority.Evaluate calls = %d, want 1", authority.calls)
	}
	after, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("expected verified to be active")
	}
	found := false
	for _, mode := range after.SupportedTrustModes {
		if mode == domain.TrustModeVerified {
			found = true
		}
	}
	if !found {
		t.Fatal("expected verified in supported_trust_modes after a granted activation")
	}
}

// TestHealthCheckFailure_SuspendsActiveMode_AndAuthorityReactivates proves
// the remaining two legs of the §7.2.0 matrix: `active -> suspended` when
// readiness evidence an activation depended on becomes invalid, and
// `suspended -> active` when the activation authority re-evaluates and
// grants once readiness is restored.
func TestHealthCheckFailure_SuspendsActiveMode_AndAuthorityReactivates(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_ma_5", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	authority := &grantingActivationAuthority{}
	if granted, _, err := capabilities.EvaluateActivation(ctx, authority, "prn_admin_ma_5", cap.ID, domain.TrustModeVerified, "eval-ma-5-first"); err != nil || !granted {
		t.Fatalf("granted=%v err=%v, want granted", granted, err)
	}
	activeBefore, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !activeBefore.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("precondition: verified should be active before the endpoint goes unreachable")
	}

	// Endpoint becomes unreachable -- the next recorded health check is
	// evidence this activation depended on becoming invalid.
	srv.Close()
	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	suspended, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry := suspended.ModeSupport.Entry(domain.TrustModeVerified)
	if entry.Status != domain.ModeSupportSuspended {
		t.Fatalf("status after unreachable endpoint = %q, want suspended", entry.Status)
	}
	if suspended.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("verified must not still be active")
	}
	found := false
	for _, mode := range suspended.SupportedTrustModes {
		if mode == domain.TrustModeVerified {
			found = true
		}
	}
	if found {
		t.Fatal("suspended verified must not appear in supported_trust_modes")
	}

	// Readiness restored (operator resolves the outage); the activation
	// authority re-evaluates and grants -- suspended -> active.
	granted, _, err := capabilities.EvaluateActivation(ctx, authority, "prn_admin_ma_5", cap.ID, domain.TrustModeVerified, "eval-ma-5-second")
	if err != nil {
		t.Fatal(err)
	}
	if !granted {
		t.Fatal("expected re-activation to be granted")
	}
	reactivated, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reactivated.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("expected verified active again after suspended -> active grant")
	}
}

// TestCertificationOpen_NewAttemptTriggersRequestedToPending proves the
// certification-attempt half of the `requested -> pending` trigger,
// independent of health checks.
func TestCertificationOpen_NewAttemptTriggersRequestedToPending(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_ma_6", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_ma_6", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-ma-6",
	}); err != nil {
		t.Fatal(err)
	}
	after, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := after.ModeSupport.Entry(domain.TrustModeVerified).Status; got != domain.ModeSupportPending {
		t.Fatalf("status after certification attempt = %q, want pending", got)
	}
}

// TestCertificationFailure_SuspendsActiveMode proves a failed
// certification attempt on a transport an active mode depends on also
// applies the `active -> suspended` transition, not just health checks.
func TestCertificationFailure_SuspendsActiveMode(t *testing.T) {
	ctx := context.Background()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failing.Close()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: failing.Client()}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_ma_7", failing.URL, []domain.TrustMode{domain.TrustModeVerified})

	// Force verified active without going through the real pipeline, to
	// isolate the suspension trigger under test from the activation path
	// already covered above.
	forced, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry := forced.ModeSupport.Entry(domain.TrustModeVerified)
	entry.Status = domain.ModeSupportActive
	forced.ModeSupport[domain.TrustModeVerified] = entry
	forced.SupportedTrustModes = forced.ModeSupport.ActiveModes()
	if err := st.Put(ctx, forced); err != nil {
		t.Fatal(err)
	}

	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_ma_7", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-ma-7",
	}); err != nil {
		t.Fatal(err)
	}
	after, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := after.ModeSupport.Entry(domain.TrustModeVerified).Status; got != domain.ModeSupportSuspended {
		t.Fatalf("status after failed certification = %q, want suspended", got)
	}
}

// TestEvaluateActivation_IdempotentReplayReturnsOriginalDecisionWithoutRecallingAuthority
// is the regression test for the finding that a lost successful response
// followed by a retry would either call the authority twice or -- once
// the mode had already moved to active -- be rejected as an illegal
// source state, instead of transparently returning the original decision
// (atos-spec docs/API.md §2.2, docs/IMPLEMENTATION_ROADMAP.md §3.3's
// universal idempotency rule).
func TestEvaluateActivation_IdempotentReplayReturnsOriginalDecisionWithoutRecallingAuthority(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_ma_idem_1", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}

	authority := &grantingActivationAuthority{}
	first, reasonFirst, err := capabilities.EvaluateActivation(ctx, authority, "prn_idem_admin", cap.ID, domain.TrustModeVerified, "idem-key-1")
	if err != nil {
		t.Fatal(err)
	}
	if !first {
		t.Fatal("expected first call to be granted")
	}
	if authority.calls != 1 {
		t.Fatalf("authority calls after first attempt = %d, want 1", authority.calls)
	}

	replay, reasonReplay, err := capabilities.EvaluateActivation(ctx, authority, "prn_idem_admin", cap.ID, domain.TrustModeVerified, "idem-key-1")
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if replay != first || reasonReplay != reasonFirst {
		t.Fatalf("replay = (%v,%q), want same as original (%v,%q)", replay, reasonReplay, first, reasonFirst)
	}
	if authority.calls != 1 {
		t.Fatalf("authority calls after replay = %d, want still 1 -- a replay must not call the authority again", authority.calls)
	}
}

// TestEvaluateActivation_ReusedIdempotencyKeyWithDifferentRequestConflicts
// proves the same idempotency key against a genuinely different request
// (a different target capability) is rejected as a conflict, not silently
// treated as a replay of the first request's decision.
func TestEvaluateActivation_ReusedIdempotencyKeyWithDifferentRequestConflicts(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)

	capA := registerHTTPBoundCapability(t, capabilities, "agt_ma_idem_2a", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	if _, err := health.CheckCapability(ctx, capA.ID); err != nil {
		t.Fatal(err)
	}
	capB := registerHTTPBoundCapability(t, capabilities, "agt_ma_idem_2b", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	if _, err := health.CheckCapability(ctx, capB.ID); err != nil {
		t.Fatal(err)
	}

	authority := &grantingActivationAuthority{}
	if _, _, err := capabilities.EvaluateActivation(ctx, authority, "prn_idem_admin", capA.ID, domain.TrustModeVerified, "idem-key-shared"); err != nil {
		t.Fatal(err)
	}
	_, _, err := capabilities.EvaluateActivation(ctx, authority, "prn_idem_admin", capB.ID, domain.TrustModeVerified, "idem-key-shared")
	if err == nil {
		t.Fatal("expected an idempotency conflict for a reused key against a different capability")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}

// TestEvaluateActivation_SameIdempotencyKeyDifferentPrincipalsDoNotConflict
// proves the idempotency namespace is scoped by the calling admin's own
// principal ID -- two different admins independently using the identical
// literal idempotency key string must not collide with each other.
func TestEvaluateActivation_SameIdempotencyKeyDifferentPrincipalsDoNotConflict(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)

	capA := registerHTTPBoundCapability(t, capabilities, "agt_ma_idem_3a", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	if _, err := health.CheckCapability(ctx, capA.ID); err != nil {
		t.Fatal(err)
	}
	capB := registerHTTPBoundCapability(t, capabilities, "agt_ma_idem_3b", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	if _, err := health.CheckCapability(ctx, capB.ID); err != nil {
		t.Fatal(err)
	}

	authority := &grantingActivationAuthority{}
	if _, _, err := capabilities.EvaluateActivation(ctx, authority, "prn_idem_admin_a", capA.ID, domain.TrustModeVerified, "idem-key-common"); err != nil {
		t.Fatalf("admin A: %v", err)
	}
	if _, _, err := capabilities.EvaluateActivation(ctx, authority, "prn_idem_admin_b", capB.ID, domain.TrustModeVerified, "idem-key-common"); err != nil {
		t.Fatalf("admin B: %v", err)
	}
	if authority.calls != 2 {
		t.Fatalf("authority calls = %d, want 2 -- two different admins' identical key strings must not collide", authority.calls)
	}
}
