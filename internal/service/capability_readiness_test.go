package service_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
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

	result, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, nil, cap.ID)
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

	result, err := service.GetCapabilityWithReadiness(ctx, h.capabilities, nil, nil, cap.ID)
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

	before, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, nil, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := before.Readiness[domain.TrustModeVerified].ReasonCode; got != "NO_READINESS_EVIDENCE_YET" {
		t.Fatalf("reason_code before any evidence = %q, want NO_READINESS_EVIDENCE_YET", got)
	}

	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	afterHealth, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, nil, cap.ID)
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
	afterCert, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, nil, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := afterCert.Readiness[domain.TrustModeVerified].ReasonCode; got != "SIGNER_NOT_AUTHORIZED" {
		t.Fatalf("reason_code after health+certification = %q, want SIGNER_NOT_AUTHORIZED (this call passes a nil ExecutionSignerService, so SignerAuthorized keeps HealthService.Availability's own always-false default for non-Managed modes -- see TestGetCapabilityWithReadiness_SignerAuthorizedReflectsRealSignerState for the wired case)", got)
	}
}

// TestGetCapabilityWithReadiness_SignerAuthorizedReflectsRealSignerState
// proves GetCapabilityWithReadiness's ExecutionSignerService integration:
// passing a non-nil signers argument overrides HealthService.Availability's
// own always-false-for-non-Managed default with the REAL current signer
// state, and correctly recomputes reason_code to match (this closes a gap
// between the readiness-projection slice, which shipped before the
// execution-signer journal existed, and the execution-signer slice, which
// landed afterward -- SignerAuthorized was a documented placeholder until
// this wiring).
func TestGetCapabilityWithReadiness_SignerAuthorizedReflectsRealSignerState(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)

	cap := registerSignerTestCapability(t, capabilities, "agt_readiness_signer", domain.TrustModeVerified)

	before, err := service.GetCapabilityWithReadiness(ctx, capabilities, nil, signers, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	// health is nil here, so readiness itself is entirely absent --
	// signers alone cannot produce a projection without HealthService.
	if before.Readiness != nil {
		t.Fatalf("expected no readiness projection with a nil HealthService even though signers is set, got %+v", before.Readiness)
	}

	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{}))
	health := service.NewHealthService(st, capabilities, resolver)
	beforeAuthorize, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, signers, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if beforeAuthorize.Readiness[domain.TrustModeVerified].SignerAuthorized {
		t.Fatal("expected signer_authorized=false before any signer has been authorized")
	}

	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_readiness_signer", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-readiness", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-readiness-wiring",
	}); err != nil {
		t.Fatal(err)
	}

	afterAuthorize, err := service.GetCapabilityWithReadiness(ctx, capabilities, health, signers, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	verified := afterAuthorize.Readiness[domain.TrustModeVerified]
	if !verified.SignerAuthorized {
		t.Fatal("expected signer_authorized=true once a signer has actually been authorized")
	}
	if verified.ReasonCode == "SIGNER_NOT_AUTHORIZED" {
		t.Fatal("reason_code must no longer report SIGNER_NOT_AUTHORIZED once a signer is genuinely authorized")
	}
}
