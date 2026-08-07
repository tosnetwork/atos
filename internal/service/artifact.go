// ArtifactService implements signed-URL Artifact transport. IDs are lookup
// identifiers, never bearer credentials; every operation re-checks access.
package service

import (
	"context"
	"strings"
	"time"

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

func (s *ArtifactService) CreateUpload(ctx context.Context, in CreateUploadInput) (storage.UploadTarget, error) {
	if in.PrincipalID == "" {
		return storage.UploadTarget{}, domain.NewError(domain.ErrAuthenticationRequired, "principal is required", false)
	}
	if strings.TrimSpace(in.ContentType) == "" {
		return storage.UploadTarget{}, domain.NewError(domain.ErrValidationFailed, "content_type is required", false)
	}
	if in.SizeBytes <= 0 {
		return storage.UploadTarget{}, domain.NewError(domain.ErrValidationFailed, "size_bytes must be positive", false)
	}
	if in.Purpose != "job_input" && in.Purpose != "capability_asset" {
		return storage.UploadTarget{}, domain.NewError(domain.ErrValidationFailed, "purpose must be job_input or capability_asset", false)
	}
	target, _, err := s.storage.CreateUpload(ctx, storage.CreateUploadRequest{
		PrincipalID: in.PrincipalID, ContentType: in.ContentType,
		SizeBytes: in.SizeBytes, Purpose: in.Purpose,
	})
	if err != nil {
		return storage.UploadTarget{}, domain.NewError(domain.ErrUploadMismatch, err.Error(), false)
	}
	return target, nil
}

func (s *ArtifactService) CompleteUpload(ctx context.Context, principalID, uploadID string) (domain.StoredArtifact, error) {
	artifact, err := s.ownedArtifact(ctx, principalID, uploadID)
	if err != nil {
		return domain.StoredArtifact{}, err
	}
	if artifact.Status != domain.ArtifactUploading {
		return domain.StoredArtifact{}, domain.NewError(domain.ErrUploadMismatch, "upload is not in uploading state", false)
	}
	if !time.Now().UTC().Before(artifact.ExpiresAt) {
		return domain.StoredArtifact{}, domain.NewError(domain.ErrUploadExpired, "upload target has expired", false)
	}
	completed, err := s.storage.CompleteUpload(ctx, uploadID)
	if err != nil {
		return domain.StoredArtifact{}, domain.NewError(domain.ErrUploadMismatch, err.Error(), false)
	}
	return completed, nil
}

func (s *ArtifactService) Get(ctx context.Context, principalID, artifactID string) (domain.StoredArtifact, error) {
	artifact, err := s.accessibleArtifact(ctx, principalID, artifactID)
	if err != nil {
		return domain.StoredArtifact{}, err
	}
	if artifact.Status == domain.ArtifactExpired || !time.Now().UTC().Before(artifact.ExpiresAt) {
		return domain.StoredArtifact{}, domain.NewError(domain.ErrArtifactNotFound, "artifact has expired", false)
	}
	return artifact, nil
}

func (s *ArtifactService) GetDownloadURL(ctx context.Context, principalID, artifactID string) (storage.DownloadTarget, error) {
	artifact, err := s.Get(ctx, principalID, artifactID)
	if err != nil {
		return storage.DownloadTarget{}, err
	}
	if artifact.Status != domain.ArtifactAvailable {
		return storage.DownloadTarget{}, domain.NewError(domain.ErrUploadMismatch, "artifact is not available", false)
	}
	target, err := s.storage.GetDownloadURL(ctx, artifactID)
	if err != nil {
		return storage.DownloadTarget{}, domain.NewError(domain.ErrUploadMismatch, err.Error(), false)
	}
	return target, nil
}

func (s *ArtifactService) ownedArtifact(ctx context.Context, principalID, artifactID string) (domain.StoredArtifact, error) {
	artifact, err := s.store.GetArtifact(ctx, artifactID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.StoredArtifact{}, domain.NewError(domain.ErrArtifactNotFound, "artifact not found", false)
		}
		return domain.StoredArtifact{}, err
	}
	if artifact.OwnerPrincipalID != principalID {
		return domain.StoredArtifact{}, domain.NewError(domain.ErrArtifactAccessDenied, "not the artifact's owning principal", false)
	}
	return artifact, nil
}

func (s *ArtifactService) accessibleArtifact(ctx context.Context, principalID, artifactID string) (domain.StoredArtifact, error) {
	artifact, err := s.store.GetArtifact(ctx, artifactID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.StoredArtifact{}, domain.NewError(domain.ErrArtifactNotFound, "artifact not found", false)
		}
		return domain.StoredArtifact{}, err
	}
	if artifact.OwnerPrincipalID == principalID {
		return artifact, nil
	}
	jobs, err := s.store.JobsByPrincipal(ctx, principalID)
	if err != nil {
		return domain.StoredArtifact{}, err
	}
	for _, job := range jobs {
		for _, output := range job.Artifacts {
			if output.ID == artifactID {
				return artifact, nil
			}
		}
	}
	return domain.StoredArtifact{}, domain.NewError(domain.ErrArtifactAccessDenied, "principal is not authorized for this artifact", false)
}
