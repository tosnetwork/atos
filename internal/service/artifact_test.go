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
// an artifact is visible only to its owner_principal_id. A different
// principal must be rejected from completing the upload, reading its
// metadata, or getting a download URL, even knowing the exact artifact_id.
func TestArtifactOwnershipEnforced(t *testing.T) {
	ctx := context.Background()
	svc, _ := newArtifactHarness(t)

	target, err := svc.CreateUpload(ctx, service.CreateUploadInput{
		PrincipalID: "prn_owner",
		ContentType: "text/plain",
		SizeBytes:   5,
	})
	if err != nil {
		t.Fatalf("CreateUpload: %v", err)
	}

	assertPermissionDenied := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		de, ok := err.(*domain.Error)
		if !ok || de.Code != domain.ErrPermissionDenied {
			t.Fatalf("got error %v, want domain.ErrPermissionDenied", err)
		}
	}

	_, err = svc.CompleteUpload(ctx, "prn_attacker", target.UploadID)
	assertPermissionDenied(t, err)

	_, err = svc.Get(ctx, "prn_attacker", target.UploadID)
	assertPermissionDenied(t, err)

	_, err = svc.GetDownloadURL(ctx, "prn_attacker", target.UploadID)
	assertPermissionDenied(t, err)

	// The real owner, meanwhile, is unaffected by those rejected attempts.
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
		{"missing content_type", service.CreateUploadInput{PrincipalID: "prn_x", SizeBytes: 10}},
		{"zero size_bytes", service.CreateUploadInput{PrincipalID: "prn_x", ContentType: "text/plain"}},
		{"negative size_bytes", service.CreateUploadInput{PrincipalID: "prn_x", ContentType: "text/plain", SizeBytes: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := svc.CreateUpload(ctx, tc.in); err == nil {
				t.Error("expected validation error, got nil")
			}
		})
	}
}
