package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

type phase01Clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *phase01Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *phase01Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type phase01HTTPResponse struct {
	Status int
	Header http.Header
	Body   []byte
}

func phase01Request(t *testing.T, client *http.Client, method, endpoint, token string, body any, headers map[string]string) phase01HTTPResponse {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return phase01HTTPResponse{Status: resp.StatusCode, Header: resp.Header.Clone(), Body: content}
}

func phase01Decode[T any](t *testing.T, response phase01HTTPResponse) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(response.Body, &value); err != nil {
		t.Fatalf("decode status %d body %q: %v", response.Status, response.Body, err)
	}
	return value
}

func TestPhase1CleanClientAuthorizationSearchQuotePayInvoke(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	clock := &phase01Clock{now: time.Now().UTC()}
	authorization, err := auth.Open(auth.Config{
		PollInterval: time.Second,
		Now:          clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}

	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	provider := tosaimock.New()
	core := toscoremock.New(st)
	jobs := service.NewJobService(st, provider, core, accounts)
	receipts := service.NewReceiptService(st, core)

	register := func(providerID, name, price, key string) domain.Capability {
		capability, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
			ProviderID: providerID, Name: name, Description: "Phase 1 acceptance fixture",
			DeliveryMode: domain.DeliveryInstant,
			InputSchema:  map[string]any{"type": "object"},
			OutputSchema: map[string]any{"type": "object"},
			Pricing: domain.Pricing{
				Model:     domain.PricingFixed,
				PriceHint: domain.PriceHint{Amount: price, Currency: "USD"},
			},
			RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged},
			IdempotencyKey:      key,
		})
		if err != nil {
			t.Fatal(err)
		}
		return capability
	}
	low := register("agt_phase1_low", "Echo Low", "1.00", "phase1-low")
	high := register("agt_phase1_high", "Echo High", "5.00", "phase1-high")

	server := &Server{
		Auth: authorization, Capabilities: capabilities, Quotes: quotes,
		Jobs: jobs, Accounts: accounts, Receipts: receipts,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ApprovalToken: strings.Repeat("a", 32),
	}
	httpServer := httptest.NewServer(server.Mux())
	defer httpServer.Close()
	server.PublicBaseURL = httpServer.URL
	client := httpServer.Client()

	// A clean client can fetch the installable Skill from the gateway itself.
	skill := phase01Request(t, client, http.MethodGet, httpServer.URL+"/skills/atos/SKILL.md", "", nil, nil)
	if skill.Status != http.StatusOK || !bytes.Contains(skill.Body, []byte("Device Authorization")) || !bytes.Contains(skill.Body, []byte("atos_quote")) {
		t.Fatalf("skill endpoint status/body = %d %q", skill.Status, skill.Body)
	}
	if skill.Header.Get("ETag") == "" {
		t.Fatal("skill endpoint did not publish an ETag")
	}

	start := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/auth/device", "", map[string]any{
		"client_type": "codex",
		"client_name": "Codex Phase 1 Acceptance",
		"requested_scopes": []string{
			"capabilities:read", "quotes:read", "invocations:create",
			"jobs:create", "jobs:read", "jobs:cancel", "account:read",
		},
	}, nil)
	if start.Status != http.StatusOK {
		t.Fatalf("device start = %d %s", start.Status, start.Body)
	}
	grant := phase01Decode[struct {
		DeviceCode              string `json:"device_code"`
		UserCode                string `json:"user_code"`
		VerificationURIComplete string `json:"verification_uri_complete"`
	}](t, start)
	if grant.DeviceCode == "" || grant.UserCode == "" || !strings.Contains(grant.VerificationURIComplete, grant.UserCode) {
		t.Fatalf("incomplete device grant: %+v", grant)
	}

	pending := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/auth/device/token", "", map[string]any{"device_code": grant.DeviceCode}, nil)
	pendingError := phase01Decode[map[string]any](t, pending)
	if pending.Status != http.StatusBadRequest || pendingError["error"] != "authorization_pending" {
		t.Fatalf("pending token response = %d %s", pending.Status, pending.Body)
	}

	consentHeaders := map[string]string{
		"X-ATOS-Approval-Token": strings.Repeat("a", 32),
		"X-ATOS-Principal-ID":   "prn_phase1_acceptance",
	}
	activation := phase01Request(t, client, http.MethodGet, grant.VerificationURIComplete, "", nil, consentHeaders)
	if activation.Status != http.StatusOK || !bytes.Contains(activation.Body, []byte("Approve")) || !bytes.Contains(activation.Body, []byte("Codex Phase 1 Acceptance")) {
		t.Fatalf("activation consent page = %d %s", activation.Status, activation.Body)
	}
	match := regexp.MustCompile(`name="csrf_token" value="([0-9a-f]+)"`).FindSubmatch(activation.Body)
	if len(match) != 2 {
		t.Fatalf("activation page did not contain a CSRF token: %s", activation.Body)
	}
	form := url.Values{"user_code": {grant.UserCode}, "decision": {"approve"}, "csrf_token": {string(match[1])}}
	formRequest, err := http.NewRequest(http.MethodPost, httpServer.URL+"/activate", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	formRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for name, value := range consentHeaders {
		formRequest.Header.Set(name, value)
	}
	noRedirect := *client
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	formResponse, err := noRedirect.Do(formRequest)
	if err != nil {
		t.Fatal(err)
	}
	_ = formResponse.Body.Close()
	if formResponse.StatusCode != http.StatusSeeOther {
		t.Fatalf("activation decision status = %d", formResponse.StatusCode)
	}
	clock.Advance(2 * time.Second)

	tokenResponse := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/auth/device/token", "", map[string]any{"device_code": grant.DeviceCode}, nil)
	if tokenResponse.Status != http.StatusOK {
		t.Fatalf("token exchange = %d %s", tokenResponse.Status, tokenResponse.Body)
	}
	tokens := phase01Decode[struct {
		AccessToken string `json:"access_token"`
		PrincipalID string `json:"principal_id"`
		DeviceID    string `json:"device_id"`
	}](t, tokenResponse)
	if tokens.AccessToken == "" || tokens.PrincipalID != "prn_phase1_acceptance" || tokens.DeviceID == "" {
		t.Fatalf("invalid token response: %+v", tokens)
	}

	search := phase01Request(t, client, http.MethodGet, httpServer.URL+"/v1/capabilities?q=Echo", tokens.AccessToken, nil, nil)
	if search.Status != http.StatusOK || !bytes.Contains(search.Body, []byte(low.ID)) || !bytes.Contains(search.Body, []byte(high.ID)) {
		t.Fatalf("capability search = %d %s", search.Status, search.Body)
	}

	quoteFor := func(capabilityID string) domain.Quote {
		response := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/quotes", tokens.AccessToken, map[string]any{
			"capability_id": capabilityID, "requested_trust_mode": "auto",
		}, nil)
		if response.Status != http.StatusCreated {
			t.Fatalf("quote = %d %s", response.Status, response.Body)
		}
		quote := phase01Decode[domain.Quote](t, response)
		if quote.TrustMode != domain.TrustModeManaged || quote.RequestedTrustMode != domain.RequestedTrustAuto {
			t.Fatalf("quote did not resolve auto to managed: %+v", quote)
		}
		return quote
	}

	invoke := func(capabilityID, quoteID, key string, input map[string]any) phase01HTTPResponse {
		return phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/invocations", tokens.AccessToken, map[string]any{
			"capability_id": capabilityID, "quote_id": quoteID,
			"input": input, "idempotency_key": key,
		}, nil)
	}

	lowQuote := quoteFor(low.ID)
	lowResult := invoke(low.ID, lowQuote.ID, "phase1-low-invoke", map[string]any{"hello": "managed"})
	if lowResult.Status != http.StatusOK {
		t.Fatalf("low invocation = %d %s", lowResult.Status, lowResult.Body)
	}
	lowEnvelope := phase01Decode[submitResponse](t, lowResult)
	if lowEnvelope.ResultType != string(service.ResultCompleted) || lowEnvelope.Job.State != domain.JobCompleted {
		t.Fatalf("low invocation result = %+v", lowEnvelope)
	}

	highQuote := quoteFor(high.ID)
	if !highQuote.RequiresConfirmation {
		t.Fatal("high-value Quote did not advertise requires_confirmation")
	}
	highPending := invoke(high.ID, highQuote.ID, "phase1-high-invoke", map[string]any{"hello": "confirmed"})
	if highPending.Status != http.StatusOK {
		t.Fatalf("high invocation pending = %d %s", highPending.Status, highPending.Body)
	}
	highEnvelope := phase01Decode[submitResponse](t, highPending)
	if highEnvelope.ResultType != string(service.ResultInputRequired) || highEnvelope.Job.Confirmation == nil || highEnvelope.ConfirmationURI == "" {
		t.Fatalf("high invocation did not require bound confirmation: %+v", highEnvelope)
	}

	confirm := phase01Request(t, client, http.MethodPost,
		httpServer.URL+"/v1/confirmations/"+highEnvelope.Job.Confirmation.UserCode+"/decision",
		tokens.AccessToken, map[string]any{"decision": "approve"}, nil)
	if confirm.Status != http.StatusOK {
		t.Fatalf("spend confirmation = %d %s", confirm.Status, confirm.Body)
	}

	highCompleted := invoke(high.ID, highQuote.ID, "phase1-high-invoke", map[string]any{"hello": "confirmed"})
	if highCompleted.Status != http.StatusOK {
		t.Fatalf("confirmed invocation = %d %s", highCompleted.Status, highCompleted.Body)
	}
	highDone := phase01Decode[submitResponse](t, highCompleted)
	if highDone.ResultType != string(service.ResultCompleted) || highDone.Job.ID != highEnvelope.Job.ID {
		t.Fatalf("confirmed invocation created the wrong result: %+v", highDone)
	}

	accountResponse := phase01Request(t, client, http.MethodGet, httpServer.URL+"/v1/account", tokens.AccessToken, nil, nil)
	account := phase01Decode[domain.Account](t, accountResponse)
	if account.Balance.Amount != "18.70" {
		t.Fatalf("managed balance = %s, want 18.70", account.Balance.Amount)
	}

	// The former caller-controlled confirmed boolean is not part of the Phase 1
	// contract and must be rejected rather than silently ignored.
	unknown := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/invocations", tokens.AccessToken, map[string]any{
		"capability_id": low.ID, "quote_id": lowQuote.ID,
		"input": map[string]any{}, "idempotency_key": "phase1-unknown", "confirmed": true,
	}, nil)
	if unknown.Status != http.StatusBadRequest {
		t.Fatalf("unknown confirmed field was accepted: %d %s", unknown.Status, unknown.Body)
	}
}
