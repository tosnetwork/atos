package mcp

import (
	"context"
	"testing"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

type grantingActivationAuthority struct{}

func (grantingActivationAuthority) Evaluate(context.Context, string, string, string, domain.TrustMode) (bool, string, error) {
	return true, "", nil
}

func activationToolAccessToken(t *testing.T, svc *auth.Service, scopes ...auth.Scope) (token, principalID string) {
	t.Helper()
	raw := make([]string, len(scopes))
	for i, scope := range scopes {
		raw[i] = string(scope)
	}
	grant, err := svc.StartDevice("test", "MCP Activation Test", raw)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	return pair.AccessToken, pair.Principal.ID
}

func newActivationToolTestServer(t *testing.T, authority domain.ActivationAuthority, scopes ...auth.Scope) (*Server, domain.Capability, string) {
	t.Helper()
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)

	token, providerID := activationToolAccessToken(t, authorization, scopes...)
	ctx := context.Background()
	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Activation MCP Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified},
		IdempotencyKey:      "register-mcp-activation",
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

func TestToolEvaluateActivation_GrantedActivates(t *testing.T) {
	server, cap, token := newActivationToolTestServer(t, grantingActivationAuthority{}, auth.ScopeActivationEvaluate)
	resp := callTool(t, server, token, "atos_evaluate_activation", map[string]any{
		"capability_id": cap.ID, "mode": "verified",
	})
	if toolCallFailed(t, resp) {
		t.Fatalf("tool call failed: %+v", resp)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result shape: %+v", resp.Result)
	}
	structured, ok := result["structuredContent"].(map[string]any)
	if !ok {
		t.Fatalf("missing structuredContent: %+v", result)
	}
	if structured["granted"] != true {
		t.Fatalf("granted = %v, want true: %+v", structured["granted"], structured)
	}
}

func TestToolEvaluateActivation_FailClosedProductionAuthority(t *testing.T) {
	server, cap, token := newActivationToolTestServer(t, service.FailClosedActivationAuthority{}, auth.ScopeActivationEvaluate)
	resp := callTool(t, server, token, "atos_evaluate_activation", map[string]any{
		"capability_id": cap.ID, "mode": "verified",
	})
	if toolCallFailed(t, resp) {
		t.Fatalf("expected a normal (isError:false) result for a fail-closed denial, got: %+v", resp)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result shape: %+v", resp.Result)
	}
	structured, ok := result["structuredContent"].(map[string]any)
	if !ok {
		t.Fatalf("missing structuredContent: %+v", result)
	}
	if structured["granted"] != false {
		t.Fatalf("granted = %v, want false", structured["granted"])
	}
	if structured["reason_code"] != domain.ActivationAuthorityUnavailable {
		t.Fatalf("reason_code = %v, want %q", structured["reason_code"], domain.ActivationAuthorityUnavailable)
	}
}

// TestToolEvaluateActivation_HiddenWithoutScope proves the tool is
// invisible in tools/list AND rejected as unknown on tools/call for a
// principal lacking activation:evaluate -- the same "hidden, not merely
// denied" convention every other scope-gated tool in this package uses.
func TestToolEvaluateActivation_HiddenWithoutScope(t *testing.T) {
	server, cap, token := newActivationToolTestServer(t, grantingActivationAuthority{}, auth.ScopeCapabilitiesRead)

	listResp := listTools(t, server, token)
	for _, name := range names(listResp.Result.Tools) {
		if name == "atos_evaluate_activation" {
			t.Fatalf("atos_evaluate_activation must not appear in tools/list without activation:evaluate")
		}
	}

	resp := callTool(t, server, token, "atos_evaluate_activation", map[string]any{
		"capability_id": cap.ID, "mode": "verified",
	})
	if resp.Error == nil {
		t.Fatalf("expected a JSON-RPC error for a scope-gated tool call, got: %+v", resp)
	}
}
