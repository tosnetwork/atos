package domain

import "time"

// ArtifactStatus implements the state machine from
// ~/atos-spec/docs/ARTIFACTS.md: uploading is the only entry state,
// available and expired are terminal.
type ArtifactStatus string

const (
	ArtifactUploading ArtifactStatus = "uploading"
	ArtifactAvailable ArtifactStatus = "available"
	ArtifactExpired   ArtifactStatus = "expired"
)

// StoredArtifact is the file-object-storage entity from docs/ARTIFACTS.md
// — distinct from the inline Artifact struct in job.go (which carries
// small result content directly embedded in a Job). A StoredArtifact is
// retrieved by ID through a signed URL, never embedded inline.
type StoredArtifact struct {
	ID               string         `json:"artifact_id"`
	OwnerPrincipalID string         `json:"-"`
	ContentType      string         `json:"content_type"`
	SizeBytes        int64          `json:"size_bytes"`
	SHA256           string         `json:"sha256,omitempty"`
	Status           ArtifactStatus `json:"status"`
	CreatedAt        time.Time      `json:"created_at"`
	ExpiresAt        time.Time      `json:"expires_at"`
}
