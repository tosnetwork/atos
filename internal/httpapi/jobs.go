package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type submitRequest struct {
	CapabilityID   string         `json:"capability_id"`
	QuoteID        string         `json:"quote_id"`
	Input          map[string]any `json:"input"`
	IdempotencyKey string         `json:"idempotency_key"`
	MaxWaitMS      int64          `json:"max_wait_ms"`
	// Confirmed lets a client reissue the same idempotency_key after
	// seeing result_type=input_required, per docs/MCP.md's MRTR flow.
	Confirmed bool `json:"confirmed"`
}

func (r submitRequest) validate() *domain.Error {
	if r.CapabilityID == "" || r.QuoteID == "" || r.IdempotencyKey == "" {
		return domain.NewError(domain.ErrValidationFailed, "capability_id, quote_id and idempotency_key are required", false)
	}
	return nil
}

type submitResponse struct {
	ResultType string     `json:"result_type"`
	Job        domain.Job `json:"job"`
}

func writeSubmitResult(w http.ResponseWriter, result service.SubmitResult) {
	status := http.StatusOK
	if result.Type == service.ResultAccepted {
		status = http.StatusAccepted
	}
	writeJSON(w, status, submitResponse{ResultType: string(result.Type), Job: result.Job})
}

func (s *Server) handleInvoke(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
		Confirmed:      req.Confirmed,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeSubmitResult(w, result)
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
		Confirmed:      req.Confirmed,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeSubmitResult(w, result)
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
