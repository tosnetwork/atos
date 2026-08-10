package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

func TestHealthCheck_PutThenGet(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now().UTC()
	check := domain.AdapterHealthCheck{
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0", Transport: domain.AdapterHTTP,
		EndpointRef: "https://provider.example.com", Status: domain.AdapterHealthHealthy, CheckedAt: now,
	}
	if err := s.PutHealthCheck(ctx, check); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.HealthCheck(ctx, "cap_1", "1.0.0", domain.AdapterHTTP)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if got.Status != domain.AdapterHealthHealthy {
		t.Fatalf("status = %s", got.Status)
	}
}

func TestHealthCheck_NotFoundIsNotAnError(t *testing.T) {
	ctx := context.Background()
	s := New()
	_, found, err := s.HealthCheck(ctx, "cap_never_checked", "1.0.0", domain.AdapterHTTP)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected found=false")
	}
}

func TestHealthCheck_PutOverwritesInPlace(t *testing.T) {
	ctx := context.Background()
	s := New()
	base := domain.AdapterHealthCheck{CapabilityID: "cap_1", CapabilityVersion: "1.0.0", Transport: domain.AdapterHTTP, CheckedAt: time.Now().UTC()}
	unhealthy := base
	unhealthy.Status = domain.AdapterHealthUnhealthy
	if err := s.PutHealthCheck(ctx, unhealthy); err != nil {
		t.Fatal(err)
	}
	healthy := base
	healthy.Status = domain.AdapterHealthHealthy
	if err := s.PutHealthCheck(ctx, healthy); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.HealthCheck(ctx, "cap_1", "1.0.0", domain.AdapterHTTP)
	if err != nil || !found {
		t.Fatal(err)
	}
	if got.Status != domain.AdapterHealthHealthy {
		t.Fatalf("status = %s, want the latest overwrite", got.Status)
	}
}

func TestHealthCheck_DifferentTransportsAreIndependent(t *testing.T) {
	ctx := context.Background()
	s := New()
	http := domain.AdapterHealthCheck{CapabilityID: "cap_1", CapabilityVersion: "1.0.0", Transport: domain.AdapterHTTP, Status: domain.AdapterHealthHealthy, CheckedAt: time.Now().UTC()}
	mcp := domain.AdapterHealthCheck{CapabilityID: "cap_1", CapabilityVersion: "1.0.0", Transport: domain.AdapterMCP, Status: domain.AdapterHealthUnhealthy, CheckedAt: time.Now().UTC()}
	if err := s.PutHealthCheck(ctx, http); err != nil {
		t.Fatal(err)
	}
	if err := s.PutHealthCheck(ctx, mcp); err != nil {
		t.Fatal(err)
	}
	gotHTTP, _, _ := s.HealthCheck(ctx, "cap_1", "1.0.0", domain.AdapterHTTP)
	gotMCP, _, _ := s.HealthCheck(ctx, "cap_1", "1.0.0", domain.AdapterMCP)
	if gotHTTP.Status != domain.AdapterHealthHealthy || gotMCP.Status != domain.AdapterHealthUnhealthy {
		t.Fatalf("http=%s mcp=%s", gotHTTP.Status, gotMCP.Status)
	}
}

func testCert(providerID, idempotencyKey string) domain.SandboxCertification {
	now := time.Now().UTC()
	return domain.SandboxCertification{
		ID: "cert_" + idempotencyKey, ProviderID: providerID, CapabilityID: "cap_1", CapabilityVersion: "1.0.0",
		Transport: domain.AdapterHTTP, EndpointRef: "https://provider.example.com",
		Status: domain.CertificationPending, IdempotencyKey: idempotencyKey,
		CreatedAt: now, UpdatedAt: now,
	}
}

func TestOpenCertification_FirstCallCreates(t *testing.T) {
	ctx := context.Background()
	s := New()
	cert, created, err := s.OpenCertification(ctx, "prov_1", testCert("prov_1", "cert-key-1"))
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if cert.Status != domain.CertificationPending {
		t.Fatalf("status = %s", cert.Status)
	}
}

func TestOpenCertification_ReplaySameContentReturnsExisting(t *testing.T) {
	ctx := context.Background()
	s := New()
	first, _, err := s.OpenCertification(ctx, "prov_1", testCert("prov_1", "cert-key-2"))
	if err != nil {
		t.Fatal(err)
	}
	second, created, err := s.OpenCertification(ctx, "prov_1", testCert("prov_1", "cert-key-2"))
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
	s := New()
	if _, _, err := s.OpenCertification(ctx, "prov_1", testCert("prov_1", "cert-key-3")); err != nil {
		t.Fatal(err)
	}
	changed := testCert("prov_1", "cert-key-3")
	changed.CapabilityID = "cap_DIFFERENT"
	_, _, err := s.OpenCertification(ctx, "prov_1", changed)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}

func TestOpenCertification_DifferentProvidersIndependent(t *testing.T) {
	ctx := context.Background()
	s := New()
	certA := testCert("prov_a", "same-key")
	certA.ID = "cert_a_same-key"
	certB := testCert("prov_b", "same-key")
	certB.ID = "cert_b_same-key"

	a, createdA, err := s.OpenCertification(ctx, "prov_a", certA)
	if err != nil || !createdA {
		t.Fatal(err)
	}
	b, createdB, err := s.OpenCertification(ctx, "prov_b", certB)
	if err != nil || !createdB {
		t.Fatal(err)
	}
	if a.ID == b.ID {
		t.Fatal("two different providers using the same idempotency key must not collide")
	}
	// Each provider must be able to look up its own record by the shared
	// key without seeing the other's.
	gotA, err := s.CertificationByIdempotencyKey(ctx, "prov_a", "same-key")
	if err != nil || gotA.ID != a.ID {
		t.Fatalf("CertificationByIdempotencyKey(prov_a) = %+v, err=%v", gotA, err)
	}
	gotB, err := s.CertificationByIdempotencyKey(ctx, "prov_b", "same-key")
	if err != nil || gotB.ID != b.ID {
		t.Fatalf("CertificationByIdempotencyKey(prov_b) = %+v, err=%v", gotB, err)
	}
}

func TestUpdateCertification_AllowsLifecycleFieldChanges(t *testing.T) {
	ctx := context.Background()
	s := New()
	cert, _, err := s.OpenCertification(ctx, "prov_1", testCert("prov_1", "cert-key-4"))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := s.UpdateCertification(ctx, cert.ID, func(c domain.SandboxCertification, exists bool) (domain.SandboxCertification, error) {
		c.Status = domain.CertificationPassed
		now := time.Now().UTC()
		c.CompletedAt = &now
		return c, nil
	})
	if err != nil {
		t.Fatalf("UpdateCertification: %v", err)
	}
	if updated.Status != domain.CertificationPassed {
		t.Fatalf("status = %s, want passed", updated.Status)
	}
}

func TestUpdateCertification_RejectsIdentityFieldChange(t *testing.T) {
	ctx := context.Background()
	s := New()
	cert, _, err := s.OpenCertification(ctx, "prov_1", testCert("prov_1", "cert-key-5"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UpdateCertification(ctx, cert.ID, func(c domain.SandboxCertification, exists bool) (domain.SandboxCertification, error) {
		c.EndpointRef = "https://different-endpoint.example.com"
		return c, nil
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}

func TestCertificationsByCapability_ReturnsAllForCapability(t *testing.T) {
	ctx := context.Background()
	s := New()
	if _, _, err := s.OpenCertification(ctx, "prov_1", testCert("prov_1", "cert-key-6")); err != nil {
		t.Fatal(err)
	}
	c2 := testCert("prov_1", "cert-key-7")
	c2.ID = "cert_key7_other"
	if _, _, err := s.OpenCertification(ctx, "prov_1", c2); err != nil {
		t.Fatal(err)
	}
	all, err := s.CertificationsByCapability(ctx, "cap_1")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("found %d certifications, want 2", len(all))
	}
}
