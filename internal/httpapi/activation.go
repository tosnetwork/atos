package httpapi

import (
	"net/http"

	"github.com/tosnetwork/atos/internal/domain"
)

type evaluateActivationRequest struct {
	Mode domain.TrustMode `json:"mode"`
}

// evaluateActivationResponse mirrors atos-spec docs/API.md §2.2: granted
// false is a normal 200 response, never a domain error.
type evaluateActivationResponse struct {
	Granted     bool                    `json:"granted"`
	ReasonCode  string                  `json:"reason_code,omitempty"`
	ModeSupport domain.ModeSupportEntry `json:"mode_support"`
}

// handleEvaluateActivation is the admin-triggered entry point for
// domain.ActivationAuthority.Evaluate (atos-spec
// docs/IMPLEMENTATION_ROADMAP.md §7.2.1, docs/API.md §2.2). Deliberately
// not capability-owner-scoped -- authorization is activation:evaluate
// alone, never checked against principalFrom(r) owning capabilityID, since
// this is an activation-authority-side operation, not a provider one.
func (s *Server) handleEvaluateActivation(w http.ResponseWriter, r *http.Request) {
	var req evaluateActivationRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed activation evaluate request: "+err.Error(), false)
		return
	}
	if req.Mode != domain.TrustModeVerified && req.Mode != domain.TrustModeNative {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "mode must be verified or native", false)
		return
	}
	capabilityID := r.PathValue("id")
	granted, reasonCode, err := s.Capabilities.EvaluateActivation(r.Context(), s.ActivationAuthority, capabilityID, req.Mode)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	cap, err := s.Capabilities.Get(r.Context(), capabilityID)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, evaluateActivationResponse{
		Granted: granted, ReasonCode: reasonCode, ModeSupport: cap.ModeSupport.Entry(req.Mode),
	})
}
