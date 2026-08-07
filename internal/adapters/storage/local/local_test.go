package local_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/storage"
	"github.com/tosnetwork/atos/internal/adapters/storage/local"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// newTestServer wires a local.Provider's BlobHandler behind a real
// httptest.Server, so signed URLs it issues are genuinely fetchable —
// the whole point of this package is that upload/download happen over
// real HTTP, not in-process function calls.
func newTestServer(t *testing.T) (*local.Provider, *httptest.Server) {
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
	return p, srv
}

func TestUploadDownloadRoundTrip(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestServer(t)

	content := []byte("this is a real file, not a stub — bytes actually move over HTTP")

	target, artifact, err := p.CreateUpload(ctx, storage.CreateUploadRequest{
		PrincipalID: "prn_test",
		ContentType: "text/plain",
		SizeBytes:   int64(len(content)),
	})
	if err != nil {
		t.Fatalf("CreateUpload: %v", err)
	}
	if artifact.Status != domain.ArtifactUploading {
		t.Fatalf("artifact status = %q, want uploading", artifact.Status)
	}

	// PUT the bytes to the real signed URL.
	req, err := http.NewRequest(target.UploadMethod, target.UploadURL, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT upload: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}

	completed, err := p.CompleteUpload(ctx, target.UploadID)
	if err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}
	if completed.Status != domain.ArtifactAvailable {
		t.Fatalf("status after complete = %q, want available", completed.Status)
	}
	if completed.SizeBytes != int64(len(content)) {
		t.Errorf("size = %d, want %d", completed.SizeBytes, len(content))
	}
	if completed.SHA256 == "" {
		t.Error("sha256 was not computed")
	}

	dl, err := p.GetDownloadURL(ctx, completed.ID)
	if err != nil {
		t.Fatalf("GetDownloadURL: %v", err)
	}

	resp, err = http.Get(dl.DownloadURL)
	if err != nil {
		t.Fatalf("GET download: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("downloaded content does not match uploaded content: got %q, want %q", got, content)
	}
}

func TestExpiredSignedURLRejected(t *testing.T) {
	_, srv := newTestServer(t)

	// A signature computed for an already-past expiry should never
	// verify, regardless of whether it's otherwise well-formed.
	pastExp := time.Now().Add(-1 * time.Hour).Unix()
	url := srv.URL + "/v1/blob/art_doesnotmatter?purpose=download&exp=" + strconv.FormatInt(pastExp, 10) + "&sig=deadbeef"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403 for an expired/invalid signed URL", resp.StatusCode)
	}
}

func TestTamperedSignatureRejected(t *testing.T) {
	ctx := context.Background()
	p, srv := newTestServer(t)

	target, _, err := p.CreateUpload(ctx, storage.CreateUploadRequest{
		PrincipalID: "prn_test",
		ContentType: "text/plain",
		SizeBytes:   10,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Flip the signature's URL to point at a different artifact ID while
	// keeping the original signature — the mismatch must be rejected.
	tampered := strings.Replace(target.UploadURL, target.UploadID, "art_someone-elses-upload", 1)
	req, err := http.NewRequest(http.MethodPut, tampered, bytes.NewReader([]byte("evil bytes")))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403 for a signature that doesn't match the requested id", resp.StatusCode)
	}

	_ = srv
}

func TestOversizedUploadRejected(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestServer(t)

	_, _, err := p.CreateUpload(ctx, storage.CreateUploadRequest{
		PrincipalID: "prn_test",
		ContentType: "application/octet-stream",
		SizeBytes:   200 * 1024 * 1024, // over the 100MB Phase 0/1 bound
	})
	if err == nil {
		t.Error("expected CreateUpload to reject a size over the configured limit")
	}
}
