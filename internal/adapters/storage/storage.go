// Package storage defines the signed-URL file transfer contract from
// ~/atos-spec/docs/ARTIFACTS.md. Per that document's core principle,
// binary bytes never travel through an MCP/A2A/REST tool call — every
// implementation of Provider issues or verifies a direct HTTP URL between
// the client and wherever the bytes actually live.
package storage

import (
	"context"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

type CreateUploadRequest struct {
	PrincipalID string
	ContentType string
	SizeBytes   int64
	Purpose     string // "job_input" | "capability_asset"
}

type UploadTarget struct {
	UploadID     string
	UploadURL    string
	UploadMethod string
	ExpiresAt    time.Time
}

type DownloadTarget struct {
	DownloadURL string
	ExpiresAt   time.Time
	ContentType string
	SizeBytes   int64
}

// Provider is deliberately small: three operations, matching the three
// optional MCP tools (atos_create_upload, atos_complete_upload,
// atos_get_download_url) 1:1. Ownership/access-control decisions live in
// internal/service.ArtifactService, not here — a Provider only knows how
// to move bytes and issue/verify signed URLs.
type Provider interface {
	CreateUpload(ctx context.Context, req CreateUploadRequest) (UploadTarget, domain.StoredArtifact, error)
	CompleteUpload(ctx context.Context, uploadID string) (domain.StoredArtifact, error)
	GetDownloadURL(ctx context.Context, artifactID string) (DownloadTarget, error)
}
