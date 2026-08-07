// Package local is a disk-backed storage.Provider using genuinely
// HMAC-signed URLs — not a stub that fakes success. A real PUT/GET round
// trip through BlobHandler is required to move bytes; nothing here
// pretends an upload happened without one. This stands in for a real
// S3-compatible bucket the way internal/adapters/tosai/mock stands in for
// a real execution network: same external contract, no real cloud
// dependency, Phase 0/1 only.
//
// Metadata (domain.StoredArtifact) is persisted through store.Store, not
// held privately in this package — the same pattern
// internal/adapters/toscore/mock uses, so swapping the memory store for
// Postgres durability-hardens this adapter too without touching it.
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
	maxUploadBytes = 100 * 1024 * 1024 // 100MB — Phase 0/1 bound per docs/ARTIFACTS.md
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
	return &Provider{
		dir:     dir,
		baseURL: strings.TrimRight(baseURL, "/"),
		secret:  secret,
		store:   st,
	}, nil
}

func (p *Provider) blobPath(id string) string {
	return filepath.Join(p.dir, id+".blob")
}

func (p *Provider) sign(purpose, id string, exp int64) string {
	mac := hmac.New(sha256.New, p.secret)
	mac.Write([]byte(purpose + ":" + id + ":" + strconv.FormatInt(exp, 10)))
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
		ID:               id,
		OwnerPrincipalID: req.PrincipalID,
		ContentType:      req.ContentType,
		SizeBytes:        req.SizeBytes,
		Status:           domain.ArtifactUploading,
		CreatedAt:        now,
		ExpiresAt:        now.Add(uploadTTL),
	}
	if err := p.store.PutArtifact(ctx, artifact); err != nil {
		return storage.UploadTarget{}, domain.StoredArtifact{}, err
	}

	url, expiresAt := p.signedURL("upload", id, uploadTTL)
	return storage.UploadTarget{
		UploadID:     id,
		UploadURL:    url,
		UploadMethod: http.MethodPut,
		ExpiresAt:    expiresAt,
	}, artifact, nil
}

func (p *Provider) CompleteUpload(ctx context.Context, uploadID string) (domain.StoredArtifact, error) {
	artifact, err := p.store.GetArtifact(ctx, uploadID)
	if err != nil {
		return domain.StoredArtifact{}, err
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

	artifact.SizeBytes = n
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
		return storage.DownloadTarget{}, errors.New("storage/local: artifact not yet available")
	}

	url, expiresAt := p.signedURL("download", artifactID, downloadTTL)
	return storage.DownloadTarget{
		DownloadURL: url,
		ExpiresAt:   expiresAt,
		ContentType: artifact.ContentType,
		SizeBytes:   artifact.SizeBytes,
	}, nil
}

// BlobHandler serves the actual PUT/GET bytes, authenticated purely by
// the signed URL's exp+sig query parameters — not by the caller's bearer
// token, matching real presigned-URL semantics where the URL itself is
// the credential. It is mounted separately from the Bearer-authenticated
// REST API (see cmd/api/main.go).
func (p *Provider) BlobHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, blobPathPrefix)
		if id == "" {
			http.NotFound(w, r)
			return
		}
		expStr := r.URL.Query().Get("exp")
		sig := r.URL.Query().Get("sig")
		purpose := r.URL.Query().Get("purpose")
		exp, err := strconv.ParseInt(expStr, 10, 64)
		if err != nil || !p.verify(purpose, id, exp, sig) {
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
	ctx := r.Context()
	if _, err := p.store.GetArtifact(ctx, id); err != nil {
		http.Error(w, "unknown upload target", http.StatusNotFound)
		return
	}

	out, err := os.Create(p.blobPath(id))
	if err != nil {
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}
	defer out.Close()

	n, err := io.Copy(out, io.LimitReader(r.Body, maxUploadBytes+1))
	if err != nil {
		http.Error(w, "write failed", http.StatusInternalServerError)
		return
	}
	if n > maxUploadBytes {
		_ = os.Remove(p.blobPath(id))
		http.Error(w, "upload exceeds size limit", http.StatusRequestEntityTooLarge)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (p *Provider) handleDownload(w http.ResponseWriter, r *http.Request, id string) {
	ctx := r.Context()
	artifact, err := p.store.GetArtifact(ctx, id)
	if err != nil || artifact.Status != domain.ArtifactAvailable {
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
	_, _ = io.Copy(w, f)
}
