package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// handleSearchCapabilities implements GET /capabilities, including the
// docs/CAPABILITIES.md filters as query parameters: max_price (+
// max_price_currency, default USD), min_trust_score, max_latency_ms,
// delivery_mode (repeatable).
func (s *Server) handleSearchCapabilities(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	in := service.SearchInput{Query: q.Get("q")}

	if raw := q.Get("max_price"); raw != "" {
		currency := q.Get("max_price_currency")
		if currency == "" {
			currency = "USD"
		}
		in.Filters.MaxPrice = &domain.Money{Amount: raw, Currency: currency}
	}
	if raw := q.Get("min_trust_score"); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			in.Filters.MinTrustScore = &v
		}
	}
	if raw := q.Get("max_latency_ms"); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
			in.Filters.MaxLatencyMS = &v
		}
	}
	if modes := q["delivery_mode"]; len(modes) > 0 {
		for _, m := range modes {
			in.Filters.DeliveryModes = append(in.Filters.DeliveryModes, domain.DeliveryMode(strings.TrimSpace(m)))
		}
	}

	caps, err := s.Capabilities.Search(r.Context(), in)
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

func (s *Server) handlePauseCapability(w http.ResponseWriter, r *http.Request) {
	cap, err := s.Capabilities.Pause(r.Context(), r.PathValue("id"), principalFrom(r))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cap)
}

func (s *Server) handleResumeCapability(w http.ResponseWriter, r *http.Request) {
	cap, err := s.Capabilities.Resume(r.Context(), r.PathValue("id"), principalFrom(r))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cap)
}
