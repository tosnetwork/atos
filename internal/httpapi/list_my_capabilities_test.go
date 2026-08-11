package httpapi

import (
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

func TestHandleListMyCapabilities_ReturnsOnlyOwnCapabilities(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	server := &Server{Auth: authorization, Capabilities: capabilities}

	token, providerID := signerAccessToken(t, authorization, auth.ScopeCapabilitiesWrite, auth.ScopeCapabilitiesRead)
	mine, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Mine", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-mine",
	})
	if err != nil {
		t.Fatalf("Register (mine): %v", err)
	}
	if _, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: "agt_someone_else", Name: "Not mine", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-not-mine",
	}); err != nil {
		t.Fatalf("Register (not mine): %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/capabilities/mine", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var decoded struct {
		Capabilities []domain.Capability `json:"capabilities"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(decoded.Capabilities) != 1 || decoded.Capabilities[0].ID != mine.ID {
		t.Fatalf("capabilities = %+v, want exactly [%s]", decoded.Capabilities, mine.ID)
	}
}

func TestHandleListMyCapabilities_RequiresWriteScope(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	server := &Server{Auth: authorization, Capabilities: capabilities}

	readOnlyToken, _ := signerAccessToken(t, authorization, auth.ScopeCapabilitiesRead)
	req := httptest.NewRequest(http.MethodGet, "/v1/capabilities/mine", nil)
	req.Header.Set("Authorization", "Bearer "+readOnlyToken)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 with only capabilities:read: %s", recorder.Code, recorder.Body.String())
	}
}
