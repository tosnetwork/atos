package service_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tosnetwork/atos/internal/adapters/storage/local"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func newArtifactHarness(t *testing.T) (*service.ArtifactService, string) {
	t.Helper()
	st := memory.New()
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	p, err := local.New(t.TempDir(), srv.URL, st)
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	mux.HandleFunc("/v1/blob/", p.BlobHandler())
	return service.NewArtifactService(st, p), srv.URL
}

// TestArtifactOwnershipEnforced guards docs/ARTIFACTS.md's access rule:
// an artifact identifier is not a bearer credential. A different principal
// must be rejected even when it knows the exact upload/artifact ID.
func TestArtifactOwnershipEnforced(t *testing.T) {
	ctx := context.Background()
	svc, _ := newArtifactHarness(t)

	target, err := svc.CreateUpload(ctx, service.CreateUploadInput{
		PrincipalID: "prn_owner",
		ContentType: "text/plain",
		SizeBytes:   5,
		Purpose:     "job_input",
	})
	if err != nil {
		t.Fatalf("CreateUpload: %v", err)
	}

	assertAccessDenied := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		de, ok := err.(*domain.Error)
		if !ok || de.Code != domain.ErrArtifactAccessDenied {
			t.Fatalf("got error %v, want domain.ErrArtifactAccessDenied", err)
		}
	}

	_, err = svc.CompleteUpload(ctx, "prn_attacker", target.UploadID)
	assertAccessDenied(t, err)

	_, err = svc.Get(ctx, "prn_attacker", target.UploadID)
	assertAccessDenied(t, err)

	_, err = svc.GetDownloadURL(ctx, "prn_attacker", target.UploadID)
	assertAccessDenied(t, err)

	if _, err := svc.Get(ctx, "prn_owner", target.UploadID); err != nil {
		t.Errorf("owner's own Get failed: %v", err)
	}
}

func TestCreateUploadValidation(t *testing.T) {
	ctx := context.Background()
	svc, _ := newArtifactHarness(t)

	cases := []struct {
		name string
		in   service.CreateUploadInput
	}{
		{"missing content_type", service.CreateUploadInput{PrincipalID: "prn_x", SizeBytes: 10, Purpose: "job_input"}},
		{"zero size_bytes", service.CreateUploadInput{PrincipalID: "prn_x", ContentType: "text/plain", Purpose: "job_input"}},
		{"negative size_bytes", service.CreateUploadInput{PrincipalID: "prn_x", ContentType: "text/plain", SizeBytes: -1, Purpose: "job_input"}},
		{"missing purpose", service.CreateUploadInput{PrincipalID: "prn_x", ContentType: "text/plain", SizeBytes: 10}},
		{"invalid purpose", service.CreateUploadInput{PrincipalID: "prn_x", ContentType: "text/plain", SizeBytes: 10, Purpose: "other"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := svc.CreateUpload(ctx, tc.in); err == nil {
				t.Error("expected validation error, got nil")
			}
		})
	}
}
