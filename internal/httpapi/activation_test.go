package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// grantingActivationAuthority mirrors internal/service's identically-named
// test-only type -- a domain.ActivationAuthority that always grants,
// proving the positive path reaches the transport layer, never wired into
// production.
type grantingActivationAuthority struct{}

func (grantingActivationAuthority) Evaluate(context.Context, string, string, string, domain.TrustMode) (bool, string, error) {
	return true, "", nil
}

func activationAccessToken(t *testing.T, svc *auth.Service, scopes ...auth.Scope) (token, principalID string) {
	t.Helper()
	raw := make([]string, len(scopes))
	for i, scope := range scopes {
		raw[i] = string(scope)
	}
	grant, err := svc.StartDevice("test", "REST Activation Test", raw)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	return pair.AccessToken, pair.Principal.ID
}

// newActivationTestServer registers a Capability with Verified requested
// and forces it straight to `pending` (bypassing the real health-check
// pipeline, which is out of scope for this transport-layer test -- see
// internal/service/mode_activation_test.go's TestCertificationFailure_SuspendsActiveMode
// for the identical direct-store-manipulation convention).
func newActivationTestServer(t *testing.T, authority domain.ActivationAuthority) (*Server, domain.Capability, string) {
	t.Helper()
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)

	token, providerID := activationAccessToken(t, authorization, auth.ScopeActivationEvaluate)
	ctx := context.Background()
	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Activation REST Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified},
		IdempotencyKey:      "register-rest-activation",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	entry := cap.ModeSupport.Entry(domain.TrustModeVerified)
	entry.Status = domain.ModeSupportPending
	cap.ModeSupport[domain.TrustModeVerified] = entry
	if err := st.Put(ctx, cap); err != nil {
		t.Fatal(err)
	}

	server := &Server{Auth: authorization, Capabilities: capabilities, ActivationAuthority: authority}
	return server, cap, token
}

func callEvaluateActivation(t *testing.T, server *Server, token, capabilityID, mode string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"mode": mode})
	req := httptest.NewRequest(http.MethodPost, "/v1/capabilities/"+capabilityID+"/activation/evaluate", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)
	return recorder
}

func TestHandleEvaluateActivation_GrantedActivates(t *testing.T) {
	server, cap, token := newActivationTestServer(t, grantingActivationAuthority{})
	recorder := callEvaluateActivation(t, server, token, cap.ID, "verified")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var decoded evaluateActivationResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !decoded.Granted {
		t.Fatalf("granted = false, want true: %s", recorder.Body.String())
	}
	if decoded.ModeSupport.Status != domain.ModeSupportActive {
		t.Fatalf("mode_support.status = %q, want active: %s", decoded.ModeSupport.Status, recorder.Body.String())
	}
}

// TestHandleEvaluateActivation_FailClosedProductionAuthority proves the
// actual production wiring (service.FailClosedActivationAuthority) denies
// through this endpoint with a normal 200 response, not an error -- the
// exact behavior main.go's real wiring produces today.
func TestHandleEvaluateActivation_FailClosedProductionAuthority(t *testing.T) {
	server, cap, token := newActivationTestServer(t, service.FailClosedActivationAuthority{})
	recorder := callEvaluateActivation(t, server, token, cap.ID, "verified")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var decoded evaluateActivationResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if decoded.Granted {
		t.Fatalf("granted = true, want false against the fail-closed authority")
	}
	if decoded.ReasonCode != domain.ActivationAuthorityUnavailable {
		t.Fatalf("reason_code = %q, want %q", decoded.ReasonCode, domain.ActivationAuthorityUnavailable)
	}
	if decoded.ModeSupport.Status != domain.ModeSupportPending {
		t.Fatalf("mode_support.status = %q, want pending (unchanged)", decoded.ModeSupport.Status)
	}
}

func TestHandleEvaluateActivation_RejectsIllegalSourceState(t *testing.T) {
	server, cap, token := newActivationTestServer(t, grantingActivationAuthority{})
	// Managed is unconditionally active, never pending/suspended -- an
	// illegal source state for this endpoint.
	recorder := callEvaluateActivation(t, server, token, cap.ID, "managed")
	if recorder.Code == http.StatusOK {
		t.Fatalf("expected a validation error for mode=managed, got 200: %s", recorder.Body.String())
	}
}

func TestHandleEvaluateActivation_RequiresScope(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	// Token deliberately carries no activation:evaluate scope.
	token, providerID := activationAccessToken(t, authorization, auth.ScopeCapabilitiesRead)
	ctx := context.Background()
	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Activation Scope Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified},
		IdempotencyKey:      "register-rest-activation-scope",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	server := &Server{Auth: authorization, Capabilities: capabilities, ActivationAuthority: grantingActivationAuthority{}}
	recorder := callEvaluateActivation(t, server, token, cap.ID, "verified")
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %s", recorder.Code, recorder.Body.String())
	}
}
