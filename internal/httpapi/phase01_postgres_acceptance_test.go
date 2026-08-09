package httpapi

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

// TestPhase1PublicHTTPFlowAgainstPostgres is the persistent-store
// acceptance gate for the Phase 1 success criterion. It exercises the
// public HTTP boundary rather than calling the service layer directly.
func TestPhase1PublicHTTPFlowAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres HTTP acceptance test")
	}
	ctx := context.Background()
	st, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	clock := &phase01Clock{now: time.Now().UTC()}
	authorization, err := auth.Open(auth.Config{PollInterval: time.Second, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authorization.Close() })

	capabilities := service.NewCapabilityService(st)
	accounts := service.NewAccountService(st)
	quotes := service.NewQuoteService(st).WithAccountService(accounts)
	provider := tosaimock.New()
	core := toscoremock.New(st)
	jobs := service.NewJobService(st, provider, core, accounts)
	receipts := service.NewReceiptService(st, core)

	capability, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID:   "agt_phase1_pg_" + suffix,
		Name:         "Postgres Echo " + suffix,
		Description:  "persistent Phase 1 HTTP acceptance fixture",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{
			Model:     domain.PricingFixed,
			PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"},
		},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged},
		IdempotencyKey:      "register-phase1-pg-" + suffix,
	})
	if err != nil {
		t.Fatal(err)
	}

	approvalToken := strings.Repeat("p", 32)
	server := &Server{
		Auth: authorization, Capabilities: capabilities, Quotes: quotes,
		Jobs: jobs, Accounts: accounts, Receipts: receipts,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ApprovalToken: approvalToken,
	}
	httpServer := httptest.NewServer(server.Mux())
	defer httpServer.Close()
	server.PublicBaseURL = httpServer.URL
	client := httpServer.Client()

	skill := phase01Request(t, client, http.MethodGet, httpServer.URL+"/skills/atos/SKILL.md", "", nil, nil)
	if skill.Status != http.StatusOK || skill.Header.Get("ETag") == "" || !bytes.Contains(skill.Body, []byte("atos_quote")) {
		t.Fatalf("skill endpoint = %d %q", skill.Status, skill.Body)
	}

	start := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/auth/device", "", map[string]any{
		"client_type": "codex", "client_name": "Postgres Phase 1 Acceptance",
		"requested_scopes": []string{
			"capabilities:read", "quotes:read", "invocations:create",
			"jobs:create", "jobs:read", "jobs:cancel", "account:read",
		},
	}, nil)
	if start.Status != http.StatusOK {
		t.Fatalf("device start = %d %s", start.Status, start.Body)
	}
	grant := phase01Decode[struct {
		DeviceCode string `json:"device_code"`
		UserCode   string `json:"user_code"`
	}](t, start)

	pending := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/auth/device/token", "", map[string]any{"device_code": grant.DeviceCode}, nil)
	if pending.Status != http.StatusBadRequest || !bytes.Contains(pending.Body, []byte("authorization_pending")) {
		t.Fatalf("pending authorization = %d %s", pending.Status, pending.Body)
	}

	principalID := "prn_phase1_pg_" + suffix
	decision := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/auth/device/decision", "", map[string]any{
		"user_code": grant.UserCode, "decision": "approve",
	}, map[string]string{"X-ATOS-Approval-Token": approvalToken, "X-ATOS-Principal-ID": principalID})
	if decision.Status != http.StatusOK {
		t.Fatalf("device decision = %d %s", decision.Status, decision.Body)
	}
	clock.Advance(2 * time.Second)

	tokenResponse := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/auth/device/token", "", map[string]any{"device_code": grant.DeviceCode}, nil)
	if tokenResponse.Status != http.StatusOK {
		t.Fatalf("token exchange = %d %s", tokenResponse.Status, tokenResponse.Body)
	}
	tokens := phase01Decode[struct {
		AccessToken string `json:"access_token"`
		PrincipalID string `json:"principal_id"`
	}](t, tokenResponse)
	if tokens.AccessToken == "" || tokens.PrincipalID != principalID {
		t.Fatalf("invalid token response: %+v", tokens)
	}

	search := phase01Request(t, client, http.MethodGet, httpServer.URL+"/v1/capabilities?q=Postgres+Echo", tokens.AccessToken, nil, nil)
	if search.Status != http.StatusOK || !bytes.Contains(search.Body, []byte(capability.ID)) {
		t.Fatalf("search = %d %s", search.Status, search.Body)
	}

	for _, mode := range []string{"verified", "native"} {
		response := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/quotes", tokens.AccessToken, map[string]any{
			"capability_id": capability.ID, "requested_trust_mode": mode,
		}, nil)
		if response.Status == http.StatusCreated || !bytes.Contains(response.Body, []byte("trust_mode_unavailable")) {
			t.Fatalf("explicit %s did not fail closed: %d %s", mode, response.Status, response.Body)
		}
	}

	quoteResponse := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/quotes", tokens.AccessToken, map[string]any{
		"capability_id": capability.ID, "requested_trust_mode": "auto",
	}, nil)
	if quoteResponse.Status != http.StatusCreated {
		t.Fatalf("auto quote = %d %s", quoteResponse.Status, quoteResponse.Body)
	}
	quote := phase01Decode[domain.Quote](t, quoteResponse)
	if quote.TrustMode != domain.TrustModeManaged || quote.RequestedTrustMode != domain.RequestedTrustAuto {
		t.Fatalf("auto quote did not resolve to managed: %+v", quote)
	}

	invokeBody := map[string]any{
		"capability_id": capability.ID, "quote_id": quote.ID,
		"input":           map[string]any{"persistent": true},
		"idempotency_key": "phase1-pg-invoke-" + suffix,
	}
	first := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/invocations", tokens.AccessToken, invokeBody, nil)
	if first.Status != http.StatusOK {
		t.Fatalf("first invocation = %d %s", first.Status, first.Body)
	}
	firstResult := phase01Decode[submitResponse](t, first)
	if firstResult.ResultType != string(service.ResultCompleted) || firstResult.Job.State != domain.JobCompleted {
		t.Fatalf("first invocation result = %+v", firstResult)
	}

	replay := phase01Request(t, client, http.MethodPost, httpServer.URL+"/v1/invocations", tokens.AccessToken, invokeBody, nil)
	if replay.Status != http.StatusOK {
		t.Fatalf("replay invocation = %d %s", replay.Status, replay.Body)
	}
	replayResult := phase01Decode[submitResponse](t, replay)
	if replayResult.Job.ID != firstResult.Job.ID {
		t.Fatalf("idempotent replay created another job: %s vs %s", replayResult.Job.ID, firstResult.Job.ID)
	}

	accountResponse := phase01Request(t, client, http.MethodGet, httpServer.URL+"/v1/account", tokens.AccessToken, nil, nil)
	if accountResponse.Status != http.StatusOK {
		t.Fatalf("account = %d %s", accountResponse.Status, accountResponse.Body)
	}
	account := phase01Decode[domain.Account](t, accountResponse)
	if account.Balance.Amount != "23.95" {
		t.Fatalf("balance after replay = %s, want 23.95", account.Balance.Amount)
	}
}
