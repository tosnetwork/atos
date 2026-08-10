package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func quotesAccessToken(t *testing.T, svc *auth.Service) string {
	t.Helper()
	grant, err := svc.StartDevice("test", "REST Test", []string{string(auth.ScopeQuotesRead)})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	return pair.AccessToken
}

// registerHTTPBoundQuoteCapability registers a Capability with a real
// third-party HTTP binding, whose non-trivial InputSchema/OutputSchema are
// exactly the kind of payload that must never silently leak into a public
// Quote response -- see TestCreateQuote/TestGetQuote below.
func registerHTTPBoundQuoteCapability(t *testing.T, capabilities *service.CapabilityService) domain.Capability {
	t.Helper()
	cap, err := capabilities.Register(t.Context(), service.RegisterCapabilityInput{
		ProviderID: "agt_quote_public_view", Name: "Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema: map[string]any{
			"type": "object", "properties": map[string]any{"x": map[string]any{"type": "number"}},
		},
		OutputSchema: map[string]any{"type": "object"},
		Pricing:      domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: "https://provider.example.com/invoke", EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
		},
		IdempotencyKey: "register-quote-public-view",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return cap
}

// assertNoInternalQuoteFields fails t if raw (a Quote JSON response body)
// contains any of ATOS's own internal frozen-execution-snapshot keys --
// atos-spec docs/API.md §3 never defines binding/input_schema/
// output_schema as part of the public Quote contract.
func assertNoInternalQuoteFields(t *testing.T, raw []byte) {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	for _, forbidden := range []string{"binding", "input_schema", "output_schema"} {
		if _, present := decoded[forbidden]; present {
			t.Fatalf("public Quote response leaked internal field %q: %s", forbidden, raw)
		}
	}
	if _, ok := decoded["quote_id"]; !ok {
		t.Fatalf("test setup invalid: response has no quote_id at all: %s", raw)
	}
}

// TestCreateQuote_DoesNotExposeInternalExecutionSnapshot proves
// POST /v1/quotes never leaks Quote.Binding/InputSchema/OutputSchema --
// ATOS's own internal frozen execution snapshot (see domain.Quote.Binding's
// doc comment), not part of the public Quote contract atos-spec
// docs/API.md §3 defines.
func TestCreateQuote_DoesNotExposeInternalExecutionSnapshot(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	cap := registerHTTPBoundQuoteCapability(t, capabilities)
	token := quotesAccessToken(t, authorization)

	server := &Server{Auth: authorization, Capabilities: capabilities, Quotes: quotes}
	body, _ := json.Marshal(map[string]any{"capability_id": cap.ID, "requested_trust_mode": "auto"})
	req := httptest.NewRequest(http.MethodPost, "/v1/quotes", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	assertNoInternalQuoteFields(t, recorder.Body.Bytes())
}

// TestGetQuote_DoesNotExposeInternalExecutionSnapshot is
// TestCreateQuote_DoesNotExposeInternalExecutionSnapshot's GET analog.
func TestGetQuote_DoesNotExposeInternalExecutionSnapshot(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	cap := registerHTTPBoundQuoteCapability(t, capabilities)
	quote, err := quotes.Create(t.Context(), service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	// Sanity check the fixture actually froze a binding -- otherwise this
	// test would pass trivially without ever exercising the leak path.
	if quote.Binding == nil {
		t.Fatal("test setup invalid: quote has no frozen binding to potentially leak")
	}
	token := quotesAccessToken(t, authorization)

	server := &Server{Auth: authorization, Capabilities: capabilities, Quotes: quotes}
	req := httptest.NewRequest(http.MethodGet, "/v1/quotes/"+quote.ID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	assertNoInternalQuoteFields(t, recorder.Body.Bytes())
}
