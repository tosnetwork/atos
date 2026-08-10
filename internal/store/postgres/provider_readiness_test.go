package postgres_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

func testCertification(providerID, capabilityID, idempotencyKey string) domain.SandboxCertification {
	now := time.Now().UTC()
	return domain.SandboxCertification{
		ID: "cert_" + idempotencyKey, ProviderID: providerID, CapabilityID: capabilityID, CapabilityVersion: "1.0.0",
		Transport: domain.AdapterHTTP, EndpointRef: "https://provider.example.com",
		Status: domain.CertificationPending, IdempotencyKey: idempotencyKey,
		CreatedAt: now, UpdatedAt: now,
	}
}

func TestHealthCheck_PutThenGet(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	check := domain.AdapterHealthCheck{
		CapabilityID: "cap_health_" + suffix, CapabilityVersion: "1.0.0", Transport: domain.AdapterHTTP,
		EndpointRef: "https://provider.example.com", Status: domain.AdapterHealthHealthy, CheckedAt: time.Now().UTC(),
	}
	if err := s.PutHealthCheck(ctx, check); err != nil {
		t.Fatalf("PutHealthCheck: %v", err)
	}
	got, found, err := s.HealthCheck(ctx, check.CapabilityID, "1.0.0", domain.AdapterHTTP)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if got.Status != domain.AdapterHealthHealthy || got.EndpointRef != check.EndpointRef {
		t.Fatalf("got = %+v", got)
	}
}

func TestHealthCheck_UpsertOverwritesInPlace(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	capID := "cap_health_upsert_" + suffix
	if err := s.PutHealthCheck(ctx, domain.AdapterHealthCheck{CapabilityID: capID, CapabilityVersion: "1.0.0", Transport: domain.AdapterHTTP, Status: domain.AdapterHealthUnhealthy, CheckedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutHealthCheck(ctx, domain.AdapterHealthCheck{CapabilityID: capID, CapabilityVersion: "1.0.0", Transport: domain.AdapterHTTP, Status: domain.AdapterHealthHealthy, CheckedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.HealthCheck(ctx, capID, "1.0.0", domain.AdapterHTTP)
	if err != nil || !found {
		t.Fatal(err)
	}
	if got.Status != domain.AdapterHealthHealthy {
		t.Fatalf("status = %s, want the latest overwrite", got.Status)
	}
}

func TestOpenCertification_FirstCallCreates(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	cert, created, err := s.OpenCertification(ctx, "prov_"+suffix, testCertification("prov_"+suffix, "cap_"+suffix, "key_"+suffix))
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if cert.Status != domain.CertificationPending {
		t.Fatalf("status = %s", cert.Status)
	}
}

func TestOpenCertification_ReplaySameContentReturnsExisting(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	cert := testCertification("prov_"+suffix, "cap_"+suffix, "key_"+suffix)
	first, _, err := s.OpenCertification(ctx, "prov_"+suffix, cert)
	if err != nil {
		t.Fatal(err)
	}
	second, created, err := s.OpenCertification(ctx, "prov_"+suffix, cert)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("replay should report created=false")
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned a different record: %s vs %s", second.ID, first.ID)
	}
}

func TestOpenCertification_ChangedContentConflicts(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	providerID := "prov_" + suffix
	cert := testCertification(providerID, "cap_"+suffix, "key_"+suffix)
	if _, _, err := s.OpenCertification(ctx, providerID, cert); err != nil {
		t.Fatal(err)
	}
	changed := cert
	changed.CapabilityID = "cap_DIFFERENT_" + suffix
	_, _, err := s.OpenCertification(ctx, providerID, changed)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}

// TestOpenCertification_ConcurrentOpenersConvergeToOne proves the
// UNIQUE(provider_id, idempotency_key) constraint plus the advisory
// transaction lock make "at most one certification per idempotency key" a
// real database guarantee under concurrent connections, mirroring
// OpenDispute's equivalent test.
func TestOpenCertification_ConcurrentOpenersConvergeToOne(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	providerID := "prov_concurrent_" + suffix
	cert := testCertification(providerID, "cap_concurrent_"+suffix, "key_"+suffix)

	const attempts = 12
	var wg sync.WaitGroup
	var creators int64
	ids := make(chan string, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, created, err := s.OpenCertification(ctx, providerID, cert)
			if err != nil {
				t.Errorf("OpenCertification: %v", err)
				return
			}
			if created {
				atomic.AddInt64(&creators, 1)
			}
			ids <- c.ID
		}()
	}
	wg.Wait()
	close(ids)
	if creators != 1 {
		t.Fatalf("creators = %d, want exactly 1", creators)
	}
	seen := make(map[string]bool)
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("observed %d distinct certification ids, want exactly 1: %v", len(seen), seen)
	}
}

// TestOpenCertification_TwoIndependentPostgresInstancesConvergeToOne
// simulates two ATOS replicas racing to open the same certification.
func TestOpenCertification_TwoIndependentPostgresInstancesConvergeToOne(t *testing.T) {
	url := os.Getenv("ATOS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx := context.Background()
	sA, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sA.Close()
	sB, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sB.Close()

	suffix := randSuffix()
	providerID := "prov_replica_" + suffix
	cert := testCertification(providerID, "cap_replica_"+suffix, "key_"+suffix)

	var wg sync.WaitGroup
	var creators int64
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, created, err := sA.OpenCertification(ctx, providerID, cert)
		if err != nil {
			t.Errorf("replica A OpenCertification: %v", err)
			return
		}
		if created {
			atomic.AddInt64(&creators, 1)
		}
	}()
	go func() {
		defer wg.Done()
		_, created, err := sB.OpenCertification(ctx, providerID, cert)
		if err != nil {
			t.Errorf("replica B OpenCertification: %v", err)
			return
		}
		if created {
			atomic.AddInt64(&creators, 1)
		}
	}()
	wg.Wait()
	if creators != 1 {
		t.Fatalf("creators = %d across two replicas, want exactly 1", creators)
	}
}

func TestUpdateCertification_AllowsLifecycleFieldChanges(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	providerID := "prov_" + suffix
	cert, _, err := s.OpenCertification(ctx, providerID, testCertification(providerID, "cap_"+suffix, "key_"+suffix))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := s.UpdateCertification(ctx, cert.ID, func(c domain.SandboxCertification, exists bool) (domain.SandboxCertification, error) {
		c.Status = domain.CertificationPassed
		now := time.Now().UTC()
		c.CompletedAt = &now
		c.Evidence = map[string]any{"handshake": "ok"}
		return c, nil
	})
	if err != nil {
		t.Fatalf("UpdateCertification: %v", err)
	}
	if updated.Status != domain.CertificationPassed {
		t.Fatalf("status = %s, want passed", updated.Status)
	}
	if updated.Evidence["handshake"] != "ok" {
		t.Fatalf("evidence = %+v", updated.Evidence)
	}
}

func TestUpdateCertification_RejectsIdentityFieldChange(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	providerID := "prov_" + suffix
	cert, _, err := s.OpenCertification(ctx, providerID, testCertification(providerID, "cap_"+suffix, "key_"+suffix))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UpdateCertification(ctx, cert.ID, func(c domain.SandboxCertification, exists bool) (domain.SandboxCertification, error) {
		c.EndpointRef = "https://different.example.com"
		return c, nil
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}

func TestCertificationsByCapability_ReturnsAllForCapability(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	providerID := "prov_" + suffix
	capID := "cap_list_" + suffix
	if _, _, err := s.OpenCertification(ctx, providerID, testCertification(providerID, capID, "key_a_"+suffix)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.OpenCertification(ctx, providerID, testCertification(providerID, capID, "key_b_"+suffix)); err != nil {
		t.Fatal(err)
	}
	all, err := s.CertificationsByCapability(ctx, capID)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("found %d certifications, want 2", len(all))
	}
}
