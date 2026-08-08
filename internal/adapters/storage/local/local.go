// Package local is a disk-backed storage.Provider using HMAC-signed URLs.
// A real PUT/GET round trip through BlobHandler moves bytes; MCP/A2A/REST
// carry only identifiers and signed transport URLs.
package local

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/storage"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

const (
	uploadTTL      = 15 * time.Minute
	downloadTTL    = 15 * time.Minute
	maxUploadBytes = 100 * 1024 * 1024
	blobPathPrefix = "/v1/blob/"
)

type Provider struct {
	dir     string
	baseURL string
	secret  []byte
	store   store.Store
}

func New(dir, baseURL string, st store.Store) (*Provider, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("storage/local: create blob dir: %w", err)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("storage/local: generate signing secret: %w", err)
	}
	return &Provider{dir: dir, baseURL: strings.TrimRight(baseURL, "/"), secret: secret, store: st}, nil
}

func (p *Provider) blobPath(id string) string { return filepath.Join(p.dir, id+".blob") }

func (p *Provider) sign(purpose, id string, exp int64) string {
	mac := hmac.New(sha256.New, p.secret)
	_, _ = mac.Write([]byte(purpose + ":" + id + ":" + strconv.FormatInt(exp, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}

func (p *Provider) verify(purpose, id string, exp int64, sig string) bool {
	if time.Now().Unix() > exp {
		return false
	}
	expected := p.sign(purpose, id, exp)
	return hmac.Equal([]byte(expected), []byte(sig))
}

func (p *Provider) signedURL(purpose, id string, ttl time.Duration) (string, time.Time) {
	expiresAt := time.Now().Add(ttl)
	exp := expiresAt.Unix()
	sig := p.sign(purpose, id, exp)
	url := fmt.Sprintf("%s%s%s?purpose=%s&exp=%d&sig=%s", p.baseURL, blobPathPrefix, id, purpose, exp, sig)
	return url, expiresAt
}

func (p *Provider) CreateUpload(ctx context.Context, req storage.CreateUploadRequest) (storage.UploadTarget, domain.StoredArtifact, error) {
	if req.SizeBytes <= 0 {
		return storage.UploadTarget{}, domain.StoredArtifact{}, errors.New("storage/local: size_bytes must be positive")
	}
	if req.SizeBytes > maxUploadBytes {
		return storage.UploadTarget{}, domain.StoredArtifact{}, fmt.Errorf("storage/local: size_bytes exceeds the %d byte limit", maxUploadBytes)
	}
	id := "art_" + uuid.NewString()
	now := time.Now().UTC()
	artifact := domain.StoredArtifact{
		ID: id, OwnerPrincipalID: req.PrincipalID, ContentType: req.ContentType,
		SizeBytes: req.SizeBytes, Status: domain.ArtifactUploading,
		CreatedAt: now, ExpiresAt: now.Add(uploadTTL),
	}
	if err := p.store.PutArtifact(ctx, artifact); err != nil {
		return storage.UploadTarget{}, domain.StoredArtifact{}, err
	}
	url, expiresAt := p.signedURL("upload", id, uploadTTL)
	return storage.UploadTarget{UploadID: id, UploadURL: url, UploadMethod: http.MethodPut, ExpiresAt: expiresAt}, artifact, nil
}

func (p *Provider) CompleteUpload(ctx context.Context, uploadID string) (domain.StoredArtifact, error) {
	artifact, err := p.store.GetArtifact(ctx, uploadID)
	if err != nil {
		return domain.StoredArtifact{}, err
	}
	if artifact.Status != domain.ArtifactUploading {
		return domain.StoredArtifact{}, errors.New("storage/local: upload is not in uploading state")
	}
	if !time.Now().UTC().Before(artifact.ExpiresAt) {
		return domain.StoredArtifact{}, errors.New("storage/local: upload target expired")
	}
	f, err := os.Open(p.blobPath(uploadID))
	if err != nil {
		return domain.StoredArtifact{}, fmt.Errorf("storage/local: upload was never PUT to its signed URL: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return domain.StoredArtifact{}, err
	}
	if n != artifact.SizeBytes {
		return domain.StoredArtifact{}, fmt.Errorf("storage/local: uploaded size %d does not match declared size %d", n, artifact.SizeBytes)
	}
	artifact.SHA256 = "sha256:" + hex.EncodeToString(h.Sum(nil))
	artifact.Status = domain.ArtifactAvailable
	artifact.ExpiresAt = time.Now().UTC().Add(7 * 24 * time.Hour)
	if err := p.store.PutArtifact(ctx, artifact); err != nil {
		return domain.StoredArtifact{}, err
	}
	return artifact, nil
}

func (p *Provider) GetDownloadURL(ctx context.Context, artifactID string) (storage.DownloadTarget, error) {
	artifact, err := p.store.GetArtifact(ctx, artifactID)
	if err != nil {
		return storage.DownloadTarget{}, err
	}
	if artifact.Status != domain.ArtifactAvailable {
		return storage.DownloadTarget{}, errors.New("storage/local: artifact not available")
	}
	if !time.Now().UTC().Before(artifact.ExpiresAt) {
		return storage.DownloadTarget{}, errors.New("storage/local: artifact expired")
	}
	url, expiresAt := p.signedURL("download", artifactID, downloadTTL)
	return storage.DownloadTarget{DownloadURL: url, ExpiresAt: expiresAt, ContentType: artifact.ContentType, SizeBytes: artifact.SizeBytes}, nil
}

// BlobHandler serves actual bytes. The signed URL is the temporary transport
// credential; upload_id and artifact_id alone never authorize access.
func (p *Provider) BlobHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, blobPathPrefix)
		if id == "" {
			http.NotFound(w, r)
			return
		}
		exp, err := strconv.ParseInt(r.URL.Query().Get("exp"), 10, 64)
		purpose := r.URL.Query().Get("purpose")
		if err != nil || !p.verify(purpose, id, exp, r.URL.Query().Get("sig")) {
			http.Error(w, "invalid or expired signed URL", http.StatusForbidden)
			return
		}
		switch {
		case r.Method == http.MethodPut && purpose == "upload":
			p.handleUpload(w, r, id)
		case r.Method == http.MethodGet && purpose == "download":
			p.handleDownload(w, r, id)
		default:
			http.Error(w, "method does not match purpose", http.StatusMethodNotAllowed)
		}
	}
}

func (p *Provider) handleUpload(w http.ResponseWriter, r *http.Request, id string) {
	artifact, err := p.store.GetArtifact(r.Context(), id)
	if err != nil || artifact.Status != domain.ArtifactUploading {
		http.Error(w, "unknown upload target", http.StatusNotFound)
		return
	}
	if !time.Now().UTC().Before(artifact.ExpiresAt) {
		http.Error(w, "upload target expired", http.StatusGone)
		return
	}
	if r.ContentLength >= 0 && r.ContentLength != artifact.SizeBytes {
		http.Error(w, "content length does not match declared size", http.StatusBadRequest)
		return
	}
	temporary := p.blobPath(id) + ".partial"
	out, err := os.OpenFile(temporary, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}
	n, copyErr := io.Copy(out, io.LimitReader(r.Body, artifact.SizeBytes+1))
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(temporary)
		http.Error(w, "write failed", http.StatusInternalServerError)
		return
	}
	if n != artifact.SizeBytes {
		_ = os.Remove(temporary)
		http.Error(w, "uploaded size does not match declared size", http.StatusBadRequest)
		return
	}
	if err := os.Rename(temporary, p.blobPath(id)); err != nil {
		_ = os.Remove(temporary)
		http.Error(w, "storage commit failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (p *Provider) handleDownload(w http.ResponseWriter, r *http.Request, id string) {
	artifact, err := p.store.GetArtifact(r.Context(), id)
	if err != nil || artifact.Status != domain.ArtifactAvailable || !time.Now().UTC().Before(artifact.ExpiresAt) {
		http.NotFound(w, r)
		return
	}
	f, err := os.Open(p.blobPath(id))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	if artifact.ContentType != "" {
		w.Header().Set("Content-Type", artifact.ContentType)
	}
	w.Header().Set("Content-Length", strconv.FormatInt(artifact.SizeBytes, 10))
	_, _ = io.CopyN(w, f, artifact.SizeBytes)
}
