package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

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
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "invalid min_trust_score", false)
			return
		}
		in.Filters.MinTrustScore = &value
	}
	if raw := q.Get("max_latency_ms"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "invalid max_latency_ms", false)
			return
		}
		in.Filters.MaxLatencyMS = &value
	}
	for _, mode := range q["delivery_mode"] {
		in.Filters.DeliveryModes = append(in.Filters.DeliveryModes, domain.DeliveryMode(strings.TrimSpace(mode)))
	}
	in.Filters.RequestedTrustMode = domain.RequestedTrustMode(q.Get("requested_trust_mode"))
	var err error
	in.Filters.ProofRequirements.NetworkVerifiableReceipt, err = optionalBool(q.Get("network_verifiable_receipt"))
	if err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, err.Error(), false)
		return
	}
	in.Filters.ProofRequirements.TOSSettlement, err = optionalBool(q.Get("tos_settlement"))
	if err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, err.Error(), false)
		return
	}
	in.Filters.ProofRequirements.PortableProofOfService, err = optionalBool(q.Get("portable_proof_of_service"))
	if err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, err.Error(), false)
		return
	}

	caps, err := s.Capabilities.Search(r.Context(), in)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"capabilities": caps})
}

func optionalBool(raw string) (bool, error) {
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, domain.NewError(domain.ErrValidationFailed, "invalid boolean query value", false)
	}
	return value, nil
}

func (s *Server) handleGetCapability(w http.ResponseWriter, r *http.Request) {
	cap, err := s.Capabilities.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cap)
}

type registerCapabilityRequest struct {
	Name                string                     `json:"name"`
	Description         string                     `json:"description"`
	DeliveryMode        domain.DeliveryMode        `json:"delivery_mode"`
	InputSchema         map[string]any             `json:"input_schema"`
	OutputSchema        map[string]any             `json:"output_schema"`
	Pricing             domain.Pricing             `json:"pricing"`
	Tags                []string                   `json:"tags"`
	RequestedTrustModes []domain.TrustMode         `json:"requested_trust_modes"`
	Bindings            []domain.CapabilityBinding `json:"bindings"`
}

func (s *Server) handleRegisterCapability(w http.ResponseWriter, r *http.Request) {
	var req registerCapabilityRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed capability request: "+err.Error(), false)
		return
	}
	cap, err := s.Capabilities.Register(r.Context(), service.RegisterCapabilityInput{
		ProviderID: principalFrom(r), Name: req.Name, Description: req.Description,
		DeliveryMode: req.DeliveryMode, InputSchema: req.InputSchema,
		OutputSchema: req.OutputSchema, Pricing: req.Pricing, Tags: req.Tags,
		RequestedTrustModes: req.RequestedTrustModes, Bindings: req.Bindings,
		IdempotencyKey: idempotencyKeyFrom(r),
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, cap)
}

func (s *Server) handleUpdateCapability(w http.ResponseWriter, r *http.Request) {
	var patch map[string]any
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed JSON body", false)
		return
	}
	cap, err := s.Capabilities.Update(
		r.Context(), r.PathValue("id"), principalFrom(r), patch, idempotencyKeyFrom(r),
	)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cap)
}

func (s *Server) handlePauseCapability(w http.ResponseWriter, r *http.Request) {
	cap, err := s.Capabilities.Pause(
		r.Context(), r.PathValue("id"), principalFrom(r), idempotencyKeyFrom(r),
	)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cap)
}

func (s *Server) handleResumeCapability(w http.ResponseWriter, r *http.Request) {
	cap, err := s.Capabilities.Resume(
		r.Context(), r.PathValue("id"), principalFrom(r), idempotencyKeyFrom(r),
	)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cap)
}

func idempotencyKeyFrom(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get("Idempotency-Key"))
}
