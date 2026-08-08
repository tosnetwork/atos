// Integration tests against a real Postgres — skipped unless
// ATOS_TEST_DATABASE_URL is set, since they need a live database rather
// than being pure unit tests. Run with:
//
//	ATOS_TEST_DATABASE_URL="postgres://user@localhost:5432/atos_test?sslmode=disable" go test ./internal/store/postgres/...
package postgres_test

import (
	"context"
	"os"
	"sync"
	"testing"

	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

func openTestStore(t *testing.T) *postgres.Store {
	t.Helper()
	url := os.Getenv("ATOS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	s, err := postgres.Open(context.Background(), url)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// TestCapabilityCRUD proves basic Put/Get/Search/ByProvider round-trip
// through real jsonb columns, not just compile against the interface.
func TestCapabilityCRUD(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	cap := domain.Capability{
		ID:           "cap_pg_" + randSuffix(),
		ProviderID:   "agt_pg_test",
		Name:         "Postgres Test Capability",
		Description:  "round-trip check",
		Version:      "1.0.0",
		Tags:         []string{"pgtest", "roundtrip"},
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{
			Model:     domain.PricingFixed,
			PriceHint: domain.PriceHint{Amount: "2.50", Currency: "USD"},
		},
		Trust:  domain.Trust{Score: 0.5, Level: domain.TrustSelfAsserted},
		Status: domain.CapabilityActive,
	}
	if err := s.Put(ctx, cap); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := s.Get(ctx, cap.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != cap.Name || got.Pricing.PriceHint.Amount != "2.50" || len(got.Tags) != 2 {
		t.Errorf("round-tripped capability mismatch: %+v", got)
	}

	found, err := s.Search(ctx, "pgtest", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if !containsID(found, cap.ID) {
		t.Errorf("Search(%q) did not find %s among %d results", "pgtest", cap.ID, len(found))
	}

	byProvider, err := s.ByProvider(ctx, "agt_pg_test")
	if err != nil {
		t.Fatalf("ByProvider: %v", err)
	}
	if !containsID(byProvider, cap.ID) {
		t.Errorf("ByProvider did not return %s", cap.ID)
	}
}

func containsID(caps []domain.Capability, id string) bool {
	for _, c := range caps {
		if c.ID == id {
			return true
		}
	}
	return false
}

// TestUpdateAccountConcurrentDebitsNeverOverspend is the Postgres
// equivalent of internal/store/memory's test with the same name — proving
// SELECT ... FOR UPDATE gives the same "never overspend" guarantee the
// in-memory mutex does, against a real database with real transactions.
func TestUpdateAccountConcurrentDebitsNeverOverspend(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	principalID := "prn_pg_concurrency_" + randSuffix()
	seed := domain.Account{
		PrincipalID: principalID,
		Balance:     domain.Money{Amount: "10", Currency: "USD"},
	}

	const attempts = 30
	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := 0

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.UpdateAccount(ctx, principalID, seed, func(a domain.Account, exists bool) (domain.Account, error) {
				balance := mustParseWholeDollars(t, a.Balance.Amount)
				if balance < 1 {
					return domain.Account{}, store.ErrConflict
				}
				a.Balance.Amount = formatWholeDollars(balance - 1)
				return a, nil
			})
			if err == nil {
				mu.Lock()
				succeeded++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if succeeded != 10 {
		t.Errorf("expected exactly 10 successful $1 debits from a $10 balance, got %d", succeeded)
	}
	final, err := s.GetAccount(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Balance.Amount != "0" {
		t.Errorf("final balance = %q, want exactly drained to 0", final.Balance.Amount)
	}
}

// TestFullJobLifecycleAgainstPostgres runs the same happy-path pipeline as
// internal/service's in-memory integration test, but with the postgres
// store as the backing store.Store — proving the swap in cmd/api/main.go
// (memory.New() vs postgres.Open()) is truly drop-in, not just
// interface-compatible on paper.
func TestFullJobLifecycleAgainstPostgres(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	provider := tosaimock.New()
	core := toscoremock.New(s)
	capabilities := service.NewCapabilityService(s)
	quotes := service.NewQuoteService(s)
	accounts := service.NewAccountService(s)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(s, provider, core, accounts)

	suffix := randSuffix()
	principalID := "prn_pg_lifecycle_" + suffix
	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID:   "agt_pg_lifecycle",
		Name:         "PG Lifecycle Cap",
		Description:  "postgres integration test",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{
			Model:     domain.PricingFixed,
			PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"},
		},
		IdempotencyKey: "register-pg-lifecycle-" + suffix,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	quote, err := quotes.Create(ctx, service.CreateQuoteInput{PrincipalID: principalID, CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}

	result, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID:    principalID,
		CapabilityID:   cap.ID,
		QuoteID:        quote.ID,
		Input:          map[string]any{"x": 1},
		IdempotencyKey: "pg-lifecycle-1-" + suffix,
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Type != service.ResultCompleted {
		t.Fatalf("result_type = %q, want completed", result.Type)
	}

	acct, err := accounts.Get(ctx, principalID)
	if err != nil {
		t.Fatalf("Get account: %v", err)
	}
	if acct.Balance.Amount != "23.95" {
		t.Errorf("balance = %q, want 23.95 (25.00 - 1.05)", acct.Balance.Amount)
	}

	// Idempotent replay must not double-charge, exercising Reserve/Finish
	// against the real idempotency_records table.
	replay, err := jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID:    principalID,
		CapabilityID:   cap.ID,
		QuoteID:        quote.ID,
		Input:          map[string]any{"x": 1},
		IdempotencyKey: "pg-lifecycle-1-" + suffix,
	})
	if err != nil {
		t.Fatalf("replay Invoke: %v", err)
	}
	if replay.Job.ID != result.Job.ID {
		t.Errorf("replay returned a different job: %s vs %s", replay.Job.ID, result.Job.ID)
	}
	acctAfterReplay, err := accounts.Get(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if acctAfterReplay.Balance.Amount != "23.95" {
		t.Errorf("balance after replay = %q, want unchanged 23.95", acctAfterReplay.Balance.Amount)
	}
}

func mustParseWholeDollars(t *testing.T, s string) int {
	t.Helper()
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Fatalf("test helper only supports whole-dollar amounts, got %q", s)
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func formatWholeDollars(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

var suffixCounter int
var suffixMu sync.Mutex

// randSuffix keeps test rows unique across repeated `go test` runs against
// the same persistent database (unlike the in-memory store, Postgres data
// outlives the test process).
func randSuffix() string {
	suffixMu.Lock()
	defer suffixMu.Unlock()
	suffixCounter++
	return formatWholeDollars(os.Getpid()) + "_" + formatWholeDollars(suffixCounter)
}
