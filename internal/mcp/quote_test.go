package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestToolQuote_DoesNotExposeInternalExecutionSnapshot is the MCP analog
// of the REST tests of the same shape (internal/httpapi/quotes_test.go):
// atos_quote's structuredContent must never leak
// Quote.Binding/InputSchema/OutputSchema -- ATOS's own internal frozen
// execution snapshot (see domain.Quote.Binding's doc comment), not part
// of the public Quote contract atos-spec docs/API.md §3 defines.
func TestToolQuote_DoesNotExposeInternalExecutionSnapshot(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	cap, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: "agt_mcp_quote_public_view", Name: "Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema: map[string]any{
			"type": "object", "properties": map[string]any{"x": map[string]any{"type": "number"}},
		},
		OutputSchema: map[string]any{"type": "object"},
		Pricing:      domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: "https://provider.example.com/invoke", EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
		},
		IdempotencyKey: "register-mcp-quote-public-view",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	server := &Server{Auth: authorization, Capabilities: capabilities, Quotes: quotes}
	token := accessToken(t, authorization, auth.ScopeQuotesRead)
	resp := callTool(t, server, token, "atos_quote", map[string]any{
		"capability_id": cap.ID, "requested_trust_mode": "auto",
	})
	if toolCallFailed(t, resp) {
		t.Fatalf("atos_quote failed: %+v", resp)
	}

	encoded, err := json.Marshal(resp.Result.(map[string]any)["structuredContent"])
	if err != nil {
		t.Fatalf("re-encode structuredContent: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode structuredContent: %v", err)
	}
	for _, forbidden := range []string{"binding", "input_schema", "output_schema"} {
		if _, present := decoded[forbidden]; present {
			t.Fatalf("atos_quote leaked internal field %q: %s", forbidden, encoded)
		}
	}
	if _, ok := decoded["quote_id"]; !ok {
		t.Fatalf("test setup invalid: structuredContent has no quote_id at all: %s", encoded)
	}
}
