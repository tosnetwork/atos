package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

func (s *Server) handleSearchCapabilities(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	caps, err := s.Capabilities.Search(r.Context(), query, 20)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"capabilities": caps})
}

func (s *Server) handleGetCapability(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cap, err := s.Capabilities.Get(r.Context(), id)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cap)
}

type registerCapabilityRequest struct {
	Name         string              `json:"name"`
	Description  string              `json:"description"`
	DeliveryMode domain.DeliveryMode `json:"delivery_mode"`
	InputSchema  map[string]any      `json:"input_schema"`
	OutputSchema map[string]any      `json:"output_schema"`
	Pricing      domain.Pricing      `json:"pricing"`
	Tags         []string            `json:"tags"`
}

func (s *Server) handleRegisterCapability(w http.ResponseWriter, r *http.Request) {
	var req registerCapabilityRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed JSON body", false)
		return
	}
	cap, err := s.Capabilities.Register(r.Context(), service.RegisterCapabilityInput{
		ProviderID:   principalFrom(r),
		Name:         req.Name,
		Description:  req.Description,
		DeliveryMode: req.DeliveryMode,
		InputSchema:  req.InputSchema,
		OutputSchema: req.OutputSchema,
		Pricing:      req.Pricing,
		Tags:         req.Tags,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, cap)
}

func (s *Server) handleUpdateCapability(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed JSON body", false)
		return
	}
	cap, err := s.Capabilities.Update(r.Context(), id, principalFrom(r), patch)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cap)
}
