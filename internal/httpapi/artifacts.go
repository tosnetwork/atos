package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type createUploadRequest struct {
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
	Purpose     string `json:"purpose"`
}

func (s *Server) handleCreateUpload(w http.ResponseWriter, r *http.Request) {
	var req createUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed JSON body", false)
		return
	}
	target, err := s.Artifacts.CreateUpload(r.Context(), service.CreateUploadInput{
		PrincipalID: principalFrom(r),
		ContentType: req.ContentType,
		SizeBytes:   req.SizeBytes,
		Purpose:     req.Purpose,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"upload_id":     target.UploadID,
		"upload_url":    target.UploadURL,
		"upload_method": target.UploadMethod,
		"expires_at":    target.ExpiresAt,
	})
}

func (s *Server) handleCompleteUpload(w http.ResponseWriter, r *http.Request) {
	artifact, err := s.Artifacts.CompleteUpload(r.Context(), principalFrom(r), r.PathValue("id"))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, artifact)
}

func (s *Server) handleGetArtifact(w http.ResponseWriter, r *http.Request) {
	artifact, err := s.Artifacts.Get(r.Context(), principalFrom(r), r.PathValue("id"))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, artifact)
}

func (s *Server) handleGetDownloadURL(w http.ResponseWriter, r *http.Request) {
	target, err := s.Artifacts.GetDownloadURL(r.Context(), principalFrom(r), r.PathValue("id"))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"download_url": target.DownloadURL,
		"expires_at":   target.ExpiresAt,
		"content_type": target.ContentType,
		"size_bytes":   target.SizeBytes,
	})
}
