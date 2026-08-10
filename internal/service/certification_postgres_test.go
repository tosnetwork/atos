// Integration test against a real Postgres — skipped unless
// ATOS_TEST_DATABASE_URL is set. Run with:
//
//	ATOS_TEST_DATABASE_URL="postgres://user@localhost:5432/atos_test?sslmode=disable" go test ./internal/service/... -run TestHealthService_Availability_CertificationDoesNotCarryAcrossCapabilityVersionBump_Postgres
package service_test

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

// TestHealthService_Availability_CertificationDoesNotCarryAcrossCapabilityVersionBump_Postgres
// is the real-Postgres analog of the memory-store test of the same name:
// atos-spec IMPLEMENTATION_ROADMAP.md §7.1.3 explicitly requires this
// scenario ("stale results do not certify a new Capability version") to
// be proven against real PostgreSQL, not only the in-memory store, since
// the version-scoping this depends on (store.HealthCheck/
// CertificationsByCapability keyed by capability_id+version+transport) is
// a real schema/query concern, not just an in-process map lookup.
func TestHealthService_Availability_CertificationDoesNotCarryAcrossCapabilityVersionBump_Postgres(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres certification-staleness test")
	}
	ctx := context.Background()

	st, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	defer st.Close()

	srv := httptest.NewServer(certifiableHTTPHandler())
	defer srv.Close()

	capabilities := service.NewCapabilityService(st)
	resolver := provideradapter.NewResolver(httpadapter.New(httpadapter.Config{Client: srv.Client()}))
	health := service.NewHealthService(st, capabilities, resolver)
	certifications := service.NewCertificationService(st, capabilities, resolver)

	providerID := "agt_avail_stale_pg_" + uuid.NewString()
	cap := registerHTTPBoundCapability(t, capabilities, providerID, srv.URL, []domain.TrustMode{domain.TrustModeVerified})
	findVerified := func(avail []domain.ModeAvailability) domain.ModeAvailability {
		for _, a := range avail {
			if a.Mode == domain.TrustModeVerified {
				return a
			}
		}
		t.Fatal("expected a ModeAvailability entry for verified")
		return domain.ModeAvailability{}
	}

	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: providerID, CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-stale-pg-v1",
	}); err != nil {
		t.Fatalf("certifications.Open (version 1): %v", err)
	}
	beforeBump, err := health.Availability(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v := findVerified(beforeBump); !v.Ready {
		t.Fatalf("test setup invalid: availability before the version bump = %+v, want ready", v)
	}

	updated, err := capabilities.Update(ctx, cap.ID, providerID, map[string]any{
		"pricing": map[string]any{
			"model":      "fixed",
			"price_hint": map[string]any{"amount": "2.00", "currency": "USD"},
		},
	}, "update-avail-stale-pg-price")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Version == cap.Version {
		t.Fatal("test setup invalid: capability version must change after the price update")
	}

	if _, err := health.CheckCapability(ctx, cap.ID); err != nil {
		t.Fatal(err)
	}
	afterBump, err := health.Availability(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	verifiedAfterBump := findVerified(afterBump)
	if !verifiedAfterBump.TransportHealthy {
		t.Fatalf("availability after version bump = %+v, want transport_healthy", verifiedAfterBump)
	}
	if verifiedAfterBump.Certified {
		t.Fatalf("availability after version bump = %+v, want NOT certified -- the version-1 certification must not carry over to version %s", verifiedAfterBump, updated.Version)
	}
	if verifiedAfterBump.Ready {
		t.Fatalf("availability after version bump = %+v, want NOT ready", verifiedAfterBump)
	}

	if _, err := certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: providerID, CapabilityID: cap.ID, Transport: domain.AdapterHTTP, IdempotencyKey: "cert-stale-pg-v2",
	}); err != nil {
		t.Fatalf("certifications.Open (version 2): %v", err)
	}
	afterRecert, err := health.Availability(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v := findVerified(afterRecert); !v.Ready {
		t.Fatalf("availability after re-certifying against the current version = %+v, want ready", v)
	}
}
