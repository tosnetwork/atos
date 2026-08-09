package httpapi

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type submitRequest struct {
	CapabilityID   string         `json:"capability_id"`
	QuoteID        string         `json:"quote_id"`
	Input          map[string]any `json:"input"`
	IdempotencyKey string         `json:"idempotency_key"`
	MaxWaitMS      int64          `json:"max_wait_ms"`
}

func (r submitRequest) validate() *domain.Error {
	if r.CapabilityID == "" || r.QuoteID == "" || r.IdempotencyKey == "" {
		return domain.NewError(domain.ErrValidationFailed, "capability_id, quote_id and idempotency_key are required", false)
	}
	return nil
}

type submitResponse struct {
	ResultType      string     `json:"result_type"`
	Job             domain.Job `json:"job"`
	ConfirmationURI string     `json:"confirmation_uri,omitempty"`
	StreamURL       string     `json:"stream_url,omitempty"`
}

func (s *Server) writeSubmitResult(w http.ResponseWriter, result service.SubmitResult) {
	status := http.StatusOK
	if result.Type == service.ResultAccepted {
		status = http.StatusAccepted
	}
	response := submitResponse{ResultType: string(result.Type), Job: result.Job}
	if result.Job.Confirmation != nil {
		response.ConfirmationURI = strings.TrimRight(s.PublicBaseURL, "/") + "/confirm?code=" + url.QueryEscape(result.Job.Confirmation.UserCode)
	}
	if result.Job.ID != "" {
		response.StreamURL = s.jobStreamURL(result.Job.ID)
	}
	writeJSON(w, status, response)
}

func (s *Server) jobStreamURL(jobID string) string {
	return strings.TrimRight(s.PublicBaseURL, "/") + "/v1/jobs/" + url.PathEscape(jobID) + "/stream"
}

func (s *Server) handleInvoke(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed JSON body", false)
		return
	}
	if verr := req.validate(); verr != nil {
		writeDomainErr(w, verr)
		return
	}
	result, err := s.Jobs.Invoke(r.Context(), service.SubmitInput{
		PrincipalID:    principalFrom(r),
		CapabilityID:   req.CapabilityID,
		QuoteID:        req.QuoteID,
		Input:          req.Input,
		IdempotencyKey: req.IdempotencyKey,
		MaxWaitMS:      req.MaxWaitMS,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	s.writeSubmitResult(w, result)
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed JSON body", false)
		return
	}
	if verr := req.validate(); verr != nil {
		writeDomainErr(w, verr)
		return
	}
	result, err := s.Jobs.CreateJob(r.Context(), service.SubmitInput{
		PrincipalID:    principalFrom(r),
		CapabilityID:   req.CapabilityID,
		QuoteID:        req.QuoteID,
		Input:          req.Input,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	s.writeSubmitResult(w, result)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.Jobs.Get(r.Context(), id)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	if job.PrincipalID != principalFrom(r) {
		writeError(w, http.StatusForbidden, domain.ErrPermissionDenied, "not the job's owning principal", false)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

type cancelRequest struct {
	Reason         string `json:"reason"`
	IdempotencyKey string `json:"idempotency_key"`
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req cancelRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed JSON body", false)
		return
	}
	job, err := s.Jobs.Cancel(r.Context(), id, principalFrom(r), req.Reason, req.IdempotencyKey)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}
