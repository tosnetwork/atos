package httpapi

import (
	"encoding/base64"
	"net/http"
	"strings"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// defaultSignerValidity is used when a request omits valid_until -- the
// atos-spec docs/API.md §2.1 example request carries no validity window
// at all, so the server must supply a sensible default rather than reject
// an otherwise-conformant minimal request.
const defaultSignerValidity = 365 * 24 * time.Hour

// decodeSignerPublicKey accepts the "base64:..." prefixed form
// docs/API.md's example shows, or bare base64 -- never a private key,
// per this endpoint's own contract.
func decodeSignerPublicKey(raw string) ([]byte, error) {
	raw = strings.TrimPrefix(raw, "base64:")
	if raw == "" {
		return nil, domain.NewError(domain.ErrValidationFailed, "signer_public_key is required", false)
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, domain.NewError(domain.ErrValidationFailed, "signer_public_key must be base64-encoded", false)
	}
	return decoded, nil
}

func signerValidityWindow(validFrom, validUntil *time.Time) (time.Time, time.Time) {
	from := time.Now().UTC()
	if validFrom != nil {
		from = validFrom.UTC()
	}
	until := from.Add(defaultSignerValidity)
	if validUntil != nil {
		until = validUntil.UTC()
	}
	return from, until
}

// checkSignerCapabilityVersion enforces the optional capability_version
// field docs/API.md's example request carries: if the caller asserts a
// version, it must match the capability's CURRENT version, guarding
// against a caller unknowingly authorizing/rotating/revoking against a
// capability that moved on since they last read it. Omitted entirely
// (empty string), it is not checked.
func (s *Server) checkSignerCapabilityVersion(r *http.Request, capabilityID, assertedVersion string) error {
	if assertedVersion == "" {
		return nil
	}
	cap, err := s.Capabilities.Get(r.Context(), capabilityID)
	if err != nil {
		return err
	}
	if cap.Version != assertedVersion {
		return domain.NewError(domain.ErrValidationFailed, "capability_version does not match the capability's current version", false)
	}
	return nil
}

type authorizeExecutionSignerRequest struct {
	CapabilityVersion  string     `json:"capability_version"`
	ExecutionSignerID  string     `json:"execution_signer_id"`
	SignerPublicKey    string     `json:"signer_public_key"`
	SignatureAlgorithm string     `json:"signature_algorithm"`
	ValidFrom          *time.Time `json:"valid_from"`
	ValidUntil         *time.Time `json:"valid_until"`
}

func (s *Server) writeSignerStatus(w http.ResponseWriter, r *http.Request, status int, capabilityID string) {
	view, err := s.ExecutionSigners.PublicStatus(r.Context(), principalFrom(r), capabilityID)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, status, view)
}

func (s *Server) handleAuthorizeExecutionSigner(w http.ResponseWriter, r *http.Request) {
	var req authorizeExecutionSignerRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed execution signer authorize request: "+err.Error(), false)
		return
	}
	capabilityID := r.PathValue("id")
	if err := s.checkSignerCapabilityVersion(r, capabilityID, req.CapabilityVersion); err != nil {
		writeDomainErr(w, err)
		return
	}
	publicKey, err := decodeSignerPublicKey(req.SignerPublicKey)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	validFrom, validUntil := signerValidityWindow(req.ValidFrom, req.ValidUntil)
	if _, err := s.ExecutionSigners.Authorize(r.Context(), service.AuthorizeSignerInput{
		ProviderID: principalFrom(r), CapabilityID: capabilityID,
		ExecutionSignerID: req.ExecutionSignerID, SignerPublicKey: publicKey,
		SignatureAlgorithm: req.SignatureAlgorithm, ValidFrom: validFrom, ValidUntil: validUntil,
		ValidFromExplicit: req.ValidFrom != nil, ValidUntilExplicit: req.ValidUntil != nil,
		IdempotencyKey: idempotencyKeyFrom(r),
	}); err != nil {
		writeDomainErr(w, err)
		return
	}
	s.writeSignerStatus(w, r, http.StatusOK, capabilityID)
}

type rotateExecutionSignerRequest struct {
	CapabilityVersion    string     `json:"capability_version"`
	NewExecutionSignerID string     `json:"execution_signer_id"`
	SignerPublicKey      string     `json:"signer_public_key"`
	SignatureAlgorithm   string     `json:"signature_algorithm"`
	ValidFrom            *time.Time `json:"valid_from"`
	ValidUntil           *time.Time `json:"valid_until"`
	ReasonCode           string     `json:"reason_code"`
}

func (s *Server) handleRotateExecutionSigner(w http.ResponseWriter, r *http.Request) {
	var req rotateExecutionSignerRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed execution signer rotate request: "+err.Error(), false)
		return
	}
	capabilityID := r.PathValue("id")
	if err := s.checkSignerCapabilityVersion(r, capabilityID, req.CapabilityVersion); err != nil {
		writeDomainErr(w, err)
		return
	}
	publicKey, err := decodeSignerPublicKey(req.SignerPublicKey)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	validFrom, validUntil := signerValidityWindow(req.ValidFrom, req.ValidUntil)
	if _, err := s.ExecutionSigners.Rotate(r.Context(), service.RotateSignerInput{
		ProviderID: principalFrom(r), CapabilityID: capabilityID,
		NewExecutionSignerID: req.NewExecutionSignerID, NewSignerPublicKey: publicKey,
		NewSignatureAlgorithm: req.SignatureAlgorithm, NewValidFrom: validFrom, NewValidUntil: validUntil,
		NewValidFromExplicit: req.ValidFrom != nil, NewValidUntilExplicit: req.ValidUntil != nil,
		RevocationReasonCode: req.ReasonCode, IdempotencyKey: idempotencyKeyFrom(r),
	}); err != nil {
		writeDomainErr(w, err)
		return
	}
	s.writeSignerStatus(w, r, http.StatusOK, capabilityID)
}

type revokeExecutionSignerRequest struct {
	CapabilityVersion string `json:"capability_version"`
	ReasonCode        string `json:"reason_code"`
}

func (s *Server) handleRevokeExecutionSigner(w http.ResponseWriter, r *http.Request) {
	var req revokeExecutionSignerRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed execution signer revoke request: "+err.Error(), false)
		return
	}
	capabilityID := r.PathValue("id")
	if err := s.checkSignerCapabilityVersion(r, capabilityID, req.CapabilityVersion); err != nil {
		writeDomainErr(w, err)
		return
	}
	if _, err := s.ExecutionSigners.Revoke(r.Context(), service.RevokeSignerInput{
		ProviderID: principalFrom(r), CapabilityID: capabilityID,
		ReasonCode: req.ReasonCode, IdempotencyKey: idempotencyKeyFrom(r),
	}); err != nil {
		writeDomainErr(w, err)
		return
	}
	s.writeSignerStatus(w, r, http.StatusOK, capabilityID)
}

func (s *Server) handleGetExecutionSignerStatus(w http.ResponseWriter, r *http.Request) {
	s.writeSignerStatus(w, r, http.StatusOK, r.PathValue("id"))
}
