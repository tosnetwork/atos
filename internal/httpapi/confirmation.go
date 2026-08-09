package httpapi

import (
	"html"
	"net/http"
	"net/url"
	"strings"

	"github.com/tosnetwork/atos/internal/domain"
)

func (s *Server) handleGetConfirmation(w http.ResponseWriter, r *http.Request) {
	job, err := s.Jobs.Confirmation(r.Context(), r.PathValue("code"), principalFrom(r))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"job_id":        job.ID,
		"quote_id":      job.QuoteID,
		"capability_id": job.CapabilityID,
		"trust_mode":    job.TrustMode,
		"proof_profile": job.ProofProfile,
		"confirmation":  job.Confirmation,
	})
}

func (s *Server) handleConfirmationDecision(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Decision string `json:"decision"`
	}
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed confirmation decision", false)
		return
	}
	approve := strings.EqualFold(req.Decision, "approve")
	if !approve && !strings.EqualFold(req.Decision, "deny") {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "decision must be approve or deny", false)
		return
	}
	job, err := s.Jobs.DecideConfirmation(r.Context(), r.PathValue("code"), principalFrom(r), approve)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job, "confirmation": job.Confirmation})
}

func (s *Server) handleConfirmationPage(w http.ResponseWriter, r *http.Request) {
	code := strings.TrimSpace(r.URL.Query().Get("code"))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	base := strings.TrimRight(s.PublicBaseURL, "/")
	api := base + "/v1/confirmations/" + url.PathEscape(code) + "/decision"
	_, _ = w.Write([]byte("<!doctype html><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">" +
		"<title>ATOS Spend Confirmation</title><style>body{font-family:system-ui;max-width:760px;margin:64px auto;padding:0 24px;line-height:1.5}" +
		"code{background:#f2f2f2;padding:.3rem .5rem;border-radius:.4rem}</style>" +
		"<h1>ATOS Spend Confirmation</h1><p>Confirmation code: <code>" + html.EscapeString(code) + "</code></p>" +
		"<p>Approve or deny this exact Quote from an authenticated ATOS client. The approval is bound to the Quote, maximum price, trust mode, input commitment, principal, and idempotency key.</p>" +
		"<p>Decision API: <code>POST " + html.EscapeString(api) + "</code></p>"))
}
