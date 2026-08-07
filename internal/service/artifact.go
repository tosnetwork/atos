// ArtifactService implements the file-transfer flow from
// ~/atos-spec/docs/ARTIFACTS.md: request a signed upload target, finalize
// after the client PUTs bytes, and issue signed download URLs — all
// ownership-checked here, with storage.Provider only moving bytes and
// signing/verifying URLs.
package service

import (
	"context"

	"github.com/tosnetwork/atos/internal/adapters/storage"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

type ArtifactService struct {
	store   store.Store
	storage storage.Provider
}

func NewArtifactService(s store.Store, p storage.Provider) *ArtifactService {
	return &ArtifactService{store: s, storage: p}
}

type CreateUploadInput struct {
	PrincipalID string
	ContentType string
	SizeBytes   int64
	Purpose     string
}

// CreateUpload implements atos_create_upload / POST /uploads.
func (s *ArtifactService) CreateUpload(ctx context.Context, in CreateUploadInput) (storage.UploadTarget, error) {
	if in.ContentType == "" {
		return storage.UploadTarget{}, domain.NewError(domain.ErrValidationFailed, "content_type is required", false)
	}
	if in.SizeBytes <= 0 {
		return storage.UploadTarget{}, domain.NewError(domain.ErrValidationFailed, "size_bytes must be positive", false)
	}
	target, _, err := s.storage.CreateUpload(ctx, storage.CreateUploadRequest{
		PrincipalID: in.PrincipalID,
		ContentType: in.ContentType,
		SizeBytes:   in.SizeBytes,
		Purpose:     in.Purpose,
	})
	if err != nil {
		return storage.UploadTarget{}, domain.NewError(domain.ErrValidationFailed, err.Error(), false)
	}
	return target, nil
}

// CompleteUpload implements atos_complete_upload / POST /uploads/{id}/complete.
func (s *ArtifactService) CompleteUpload(ctx context.Context, principalID, uploadID string) (domain.StoredArtifact, error) {
	if _, err := s.ownedArtifact(ctx, principalID, uploadID); err != nil {
		return domain.StoredArtifact{}, err
	}
	artifact, err := s.storage.CompleteUpload(ctx, uploadID)
	if err != nil {
		return domain.StoredArtifact{}, domain.NewError(domain.ErrValidationFailed, err.Error(), false)
	}
	return artifact, nil
}

// Get implements GET /artifacts/{id}.
func (s *ArtifactService) Get(ctx context.Context, principalID, artifactID string) (domain.StoredArtifact, error) {
	return s.ownedArtifact(ctx, principalID, artifactID)
}

// GetDownloadURL implements atos_get_download_url / GET /artifacts/{id}/download-url.
//
// Ownership note (docs/ARTIFACTS.md): the full rule also grants access to
// a job's owning principal for that job's *output* artifacts, not just
// the uploader. Nothing in this Phase 0/1 codebase yet produces a
// StoredArtifact as job output (the mock tos-ai provider only returns
// inline JSON — see internal/adapters/tosai/mock), so that half of the
// rule has no real code path to attach to yet; only uploader ownership is
// enforced here until job-output artifacts are real.
func (s *ArtifactService) GetDownloadURL(ctx context.Context, principalID, artifactID string) (storage.DownloadTarget, error) {
	if _, err := s.ownedArtifact(ctx, principalID, artifactID); err != nil {
		return storage.DownloadTarget{}, err
	}
	target, err := s.storage.GetDownloadURL(ctx, artifactID)
	if err != nil {
		return storage.DownloadTarget{}, domain.NewError(domain.ErrValidationFailed, err.Error(), false)
	}
	return target, nil
}

func (s *ArtifactService) ownedArtifact(ctx context.Context, principalID, artifactID string) (domain.StoredArtifact, error) {
	artifact, err := s.store.GetArtifact(ctx, artifactID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.StoredArtifact{}, domain.NewError(domain.ErrNotFound, "artifact not found", false)
		}
		return domain.StoredArtifact{}, err
	}
	if artifact.OwnerPrincipalID != principalID {
		return domain.StoredArtifact{}, domain.NewError(domain.ErrPermissionDenied, "not the artifact's owning principal", false)
	}
	return artifact, nil
}
