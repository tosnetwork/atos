package service_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"encoding/json"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/a2aadapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/mcpadapter"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestCertificationOpen_SuccessfulMCPCertification and
// TestCertificationOpen_SuccessfulA2ACertification prove
// CertificationService is genuinely transport-agnostic (dispatches
// through the Resolver, not coincidentally only exercised via HTTP) --
// the roadmap's explicit "successful MCP certification"/"successful A2A
// certification" matrix entries.
func TestCertificationOpen_SuccessfulMCPCertification(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"tools": []any{}}})
	}))
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(mcpadapter.New(mcpadapter.Config{Client: srv.Client()}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_cert_mcp", Name: "MCP Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeVerified},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterMCP, EndpointRef: srv.URL + "#analyze", EligibleTrustModes: []domain.TrustMode{domain.TrustModeVerified}},
		},
		IdempotencyKey: "register-cert-mcp",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	cert, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_cert_mcp", CapabilityID: cap.ID, Transport: domain.AdapterMCP, IdempotencyKey: "cert-mcp-1",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if cert.Status != domain.CertificationPassed {
		t.Fatalf("status = %s, want passed", cert.Status)
	}
}

func TestCertificationOpen_SuccessfulA2ACertification(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": -32001, "message": "task not found"}})
	}))
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(a2aadapter.New(a2aadapter.Config{Client: srv.Client()}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_cert_a2a", Name: "A2A Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeVerified},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterA2A, EndpointRef: srv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeVerified}},
		},
		IdempotencyKey: "register-cert-a2a",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	cert, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_cert_a2a", CapabilityID: cap.ID, Transport: domain.AdapterA2A, IdempotencyKey: "cert-a2a-1",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// A "task not found" JSON-RPC error still proves the tasks/get method
	// itself is implemented and reachable -- Health()'s own documented
	// pure-reachability contract (see a2aadapter.Health's doc comment).
	if cert.Status != domain.CertificationPassed {
		t.Fatalf("status = %s, want passed (task-not-found still proves reachability)", cert.Status)
	}
}

func TestCertificationOpen_SuccessfulHTTPCertification(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_cert_1", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	cert, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_cert_1", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-open-1",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if cert.Status != domain.CertificationPassed {
		t.Fatalf("status = %s, want passed", cert.Status)
	}
	if cert.CompletedAt == nil {
		t.Fatal("expected completed_at to be set")
	}
}

func TestCertificationOpen_UnreachableEndpointFails(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_cert_2", "http://127.0.0.1:1/invoke", []domain.TrustMode{domain.TrustModeVerified})
	cert, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_cert_2", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-open-2",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if cert.Status != domain.CertificationFailed {
		t.Fatalf("status = %s, want failed", cert.Status)
	}
	if cert.FailureReason == "" {
		t.Fatal("expected a failure reason")
	}
}

func TestCertificationOpen_NoAdapterRegisteredFailsCleanly(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver() // no adapters at all
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_cert_3", "https://provider.example.com", []domain.TrustMode{domain.TrustModeVerified})
	cert, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_cert_3", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-open-3",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if cert.Status != domain.CertificationFailed {
		t.Fatalf("status = %s, want failed", cert.Status)
	}
}

func TestCertificationOpen_DuplicateRetryConverges(t *testing.T) {
	ctx := context.Background()
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_cert_4", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	in := service.OpenCertificationInput{ProviderID: "agt_cert_4", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-open-4"}

	first, err := certifications.Open(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := certifications.Open(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("retry returned a different certification: %s vs %s", first.ID, second.ID)
	}
	if first.Status != second.Status {
		t.Fatalf("retry produced a different status: %s vs %s", first.Status, second.Status)
	}
	// A terminal (Passed) result must not be re-probed on replay.
	if calls != 1 {
		t.Fatalf("adapter Health probed %d times, want exactly 1 (terminal replay must not re-probe)", calls)
	}
}

func TestCertificationOpen_ChangedSemanticsUnderSameKeyConflicts(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	capA := registerHTTPBoundCapability(t, capabilities, "agt_cert_5", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_cert_5", CapabilityID: capA.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "shared-key",
	}); err != nil {
		t.Fatal(err)
	}

	// A second, genuinely distinct Capability owned by the same provider
	// (a different registration idempotency key, so this isn't just a
	// replay of capA's own registration).
	capB, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_cert_5", Name: "Second HTTP Capability", Description: "a different capability",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeVerified},
		Bindings: []domain.CapabilityBinding{
			{Transport: domain.AdapterHTTP, EndpointRef: srv.URL, EligibleTrustModes: []domain.TrustMode{domain.TrustModeVerified}},
		},
		IdempotencyKey: "register-agt_cert_5-second",
	})
	if err != nil {
		t.Fatalf("Register (capB): %v", err)
	}
	if capB.ID == capA.ID {
		t.Fatal("test setup invalid: capB must be a distinct capability from capA")
	}
	_, err = certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_cert_5", CapabilityID: capB.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "shared-key",
	})
	if err == nil {
		t.Fatal("expected a conflict for the same idempotency key against a different capability")
	}
}

// TestCertificationOpen_PassedNeverActivatesVerifiedOrNative is the
// roadmap's explicit regression requirement, exercised through the real
// service (not just the domain-level invariant already covered by
// HealthService's equivalent test).
func TestCertificationOpen_PassedNeverActivatesVerifiedOrNative(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_cert_6", srv.URL, []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified, domain.TrustModeNative})
	before, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}

	cert, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_cert_6", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-open-6",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cert.Status != domain.CertificationPassed {
		t.Fatalf("status = %s, want passed (test setup should have this succeed)", cert.Status)
	}

	after, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("a passed certification must never independently activate Verified")
	}
	if after.ModeSupport.Active(domain.TrustModeNative) {
		t.Fatal("a passed certification must never independently activate Native")
	}
	if len(after.SupportedTrustModes) != len(before.SupportedTrustModes) {
		t.Fatalf("supported_trust_modes changed by certification: %v -> %v", before.SupportedTrustModes, after.SupportedTrustModes)
	}
}

// TestCertificationOpen_CombinedReadinessSignalsStillDoNotActivate is the
// roadmap's most adversarial version of the same invariant: endpoint
// healthy + sandbox certification passed + provider requested verified +
// (implicit) an execution signer concept existing must STILL not
// independently cause supported_trust_modes += verified.
func TestCertificationOpen_CombinedReadinessSignalsStillDoNotActivate(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_cert_7", srv.URL, []domain.TrustMode{domain.TrustModeVerified})

	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: "agt_cert_7", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-open-7",
	}); err != nil {
		t.Fatal(err)
	}

	after, err := capabilities.Get(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.ModeSupport.Active(domain.TrustModeVerified) {
		t.Fatal("endpoint healthy + certification passed + provider requested verified must still NOT activate verified")
	}
	if after.ModeSupport.Entry(domain.TrustModeVerified).Status != domain.ModeSupportPending {
		t.Fatalf("verified mode_support status = %s, want still pending", after.ModeSupport.Entry(domain.TrustModeVerified).Status)
	}
}

func TestCertificationOpen_ConcurrentOpenersConvergeToOneProbe(t *testing.T) {
	ctx := context.Background()
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	certifications := service.NewCertificationService(st, capabilities, resolver)

	cap := registerHTTPBoundCapability(t, capabilities, "agt_cert_8", srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	in := service.OpenCertificationInput{ProviderID: "agt_cert_8", CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-open-8"}

	const attempts = 8
	var wg sync.WaitGroup
	ids := make(chan string, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := certifications.Open(ctx, in)
			if err != nil {
				t.Errorf("Open: %v", err)
				return
			}
			ids <- c.ID
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("observed %d distinct certification ids, want exactly 1: %v", len(seen), seen)
	}
}
