package service_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestGetCapabilityWithReadiness_ExcludesManagedAndMatchesModeSupportStatus
// proves the §7.2.3 public-projection builder: Managed never appears in
// the readiness map (it has no readiness concept), Verified/Native do,
// and readiness[mode].Status always equals mode_support[mode].status --
// never a second source of truth, per docs/API.md's explicit rule.
func TestGetCapabilityWithReadiness_ExcludesManagedAndMatchesModeSupportStatus(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_readiness_1", srv.URL,
		[]domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified, domain.TrustModeNative})

	result, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := result.Readiness[domain.TrustModeManaged]; present {
		t.Fatal("managed must never appear in the readiness projection")
	}
	verified, ok := result.Readiness[domain.TrustModeVerified]
	if !ok {
		t.Fatal("expected a readiness entry for verified")
	}
	if verified.Status != result.ModeSupport.Entry(domain.TrustModeVerified).Status {
		t.Fatalf("readiness status %q does not match mode_support status %q", verified.Status, result.ModeSupport.Entry(domain.TrustModeVerified).Status)
	}
	native, ok := result.Readiness[domain.TrustModeNative]
	if !ok {
		t.Fatal("expected a readiness entry for native")
	}
	if native.Status != result.ModeSupport.Entry(domain.TrustModeNative).Status {
		t.Fatalf("readiness status %q does not match mode_support status %q", native.Status, result.ModeSupport.Entry(domain.TrustModeNative).Status)
	}
}

// TestGetCapabilityWithReadiness_NilHealthServiceOmitsReadiness proves the
// nil-safety contract: callers that have not wired a HealthService (most
// existing test harnesses) get back an unchanged Capability response with
// no readiness field at all, not a panic or an empty-but-present map.
func TestGetCapabilityWithReadiness_NilHealthServiceOmitsReadiness(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	cap := registerCapability(t, h, "agt_readiness_nil", "1.00")

	result, err := service.GetCapabilityWithReadiness(ctx, h.capabilities, nil, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Readiness != nil {
		t.Fatalf("expected a nil Readiness map with no HealthService configured, got %+v", result.Readiness)
	}
}

// TestGetCapabilityWithReadiness_ReasonCodeAdvancesAsEvidenceArrives proves
// the projection's reason_code field actually reflects real, changing
// system state end to end (registration -> health check -> certification)
// rather than a fixed placeholder -- this is the behavior that was
// impossible to observe before HealthService/CertificationService had any
// production caller.
func TestGetCapabilityWithReadiness_ReasonCodeAdvancesAsEvidenceArrives(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_readiness_2", srv.URL, []domain.TrustMode{domain.TrustModeVerified})

	before, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := before.Readiness[domain.TrustModeVerified].ReasonCode; got != "NO_READINESS_EVIDENCE_YET" {
		t.Fatalf("reason_code before any evidence = %q, want NO_READINESS_EVIDENCE_YET", got)
	}

	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	afterHealth, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := afterHealth.Readiness[domain.TrustModeVerified].ReasonCode; got != "CERTIFICATION_NOT_CURRENT" {
		t.Fatalf("reason_code after health only = %q, want CERTIFICATION_NOT_CURRENT", got)
	}

	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_readiness_2", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-readiness-2",
	}); err != nil {
		t.Fatal(err)
	}
	afterCert, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := afterCert.Readiness[domain.TrustModeVerified].ReasonCode; got != "SIGNER_NOT_AUTHORIZED" {
		t.Fatalf("reason_code after health+certification = %q, want SIGNER_NOT_AUTHORIZED (no signer-authorize path exists in this codebase yet)", got)
	}
}
