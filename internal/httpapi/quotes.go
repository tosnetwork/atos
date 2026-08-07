package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type createQuoteRequest struct {
	CapabilityID string         `json:"capability_id"`
	Constraints  map[string]any `json:"constraints"`
}

func (s *Server) handleCreateQuote(w http.ResponseWriter, r *http.Request) {
	var req createQuoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed JSON body", false)
		return
	}
	if req.CapabilityID == "" {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "capability_id is required", false)
		return
	}

	in := service.CreateQuoteInput{CapabilityID: req.CapabilityID}
	if req.Constraints != nil {
		if maxTotal, ok := req.Constraints["max_total"].(map[string]any); ok {
			amount, _ := maxTotal["amount"].(string)
			currency, _ := maxTotal["currency"].(string)
			if amount != "" {
				in.MaxTotal = &domain.Money{Amount: amount, Currency: currency}
			}
		}
	}

	q, err := s.Quotes.Create(r.Context(), in)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, q)
}

func (s *Server) handleGetQuote(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q, err := s.Quotes.Get(r.Context(), id)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, q)
}
