package httpapi

import (
	"net/http"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type openCertificationRequest struct {
	Transport domain.EndpointAdapterType `json:"transport"`
}

// handleOpenCertification is the entry point for the Phase 3A sandbox
// certification workflow (service.CertificationService.Open), previously
// implemented with full test coverage but unreachable from any REST/MCP
// surface -- see atos-spec docs/API.md §2.3. Provider-owned and
// idempotency-key-scoped, mirroring every other provider mutation
// endpoint in this package (execution_signer.go's Authorize/Rotate/Revoke).
func (s *Server) handleOpenCertification(w http.ResponseWriter, r *http.Request) {
	var req openCertificationRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed certification request: "+err.Error(), false)
		return
	}
	idempotencyKey := idempotencyKeyFrom(r)
	if idempotencyKey == "" {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "Idempotency-Key header is required", false)
		return
	}
	cert, err := s.Certifications.Open(r.Context(), service.OpenCertificationInput{
		ProviderID: principalFrom(r), CapabilityID: r.PathValue("id"),
		Transport: req.Transport, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cert)
}

// handleGetCertificationStatus returns the certification history for the
// requesting provider's own capability, newest first (atos-spec
// docs/API.md §2.3 GET).
func (s *Server) handleGetCertificationStatus(w http.ResponseWriter, r *http.Request) {
	certs, err := s.Certifications.PublicStatus(r.Context(), principalFrom(r), r.PathValue("id"))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, certs)
}
