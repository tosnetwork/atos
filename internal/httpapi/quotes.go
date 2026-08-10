package httpapi

import (
	"net/http"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type createQuoteRequest struct {
	CapabilityID       string                    `json:"capability_id"`
	InputSummary       map[string]any            `json:"input_summary"`
	RequestedTrustMode domain.RequestedTrustMode `json:"requested_trust_mode"`
	ProofRequirements  domain.ProofRequirements  `json:"proof_requirements"`
	Constraints        struct {
		MaxTotal *domain.Money `json:"max_total"`
		Deadline string        `json:"deadline"`
	} `json:"constraints"`
}

func (s *Server) handleCreateQuote(w http.ResponseWriter, r *http.Request) {
	var req createQuoteRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed quote request: "+err.Error(), false)
		return
	}
	if req.CapabilityID == "" {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "capability_id is required", false)
		return
	}
	q, err := s.Quotes.Create(r.Context(), service.CreateQuoteInput{
		PrincipalID:        principalFrom(r),
		CapabilityID:       req.CapabilityID,
		InputSummary:       req.InputSummary,
		RequestedTrustMode: req.RequestedTrustMode,
		ProofRequirements:  req.ProofRequirements,
		MaxTotal:           req.Constraints.MaxTotal,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, q.Public())
}

func (s *Server) handleGetQuote(w http.ResponseWriter, r *http.Request) {
	q, err := s.Quotes.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	if q.PrincipalID != "" && q.PrincipalID != principalFrom(r) {
		writeError(w, http.StatusForbidden, domain.ErrPermissionDenied, "not the Quote's owning principal", false)
		return
	}
	writeJSON(w, http.StatusOK, q.Public())
}
