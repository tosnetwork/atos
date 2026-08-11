package httpapi

import (
	"errors"
	"net/http"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store"
)

type bindIdentityRequest struct {
	AgentID string `json:"agent_id"`
}

type bindIdentityResponse struct {
	PrincipalID string `json:"principal_id"`
	AgentID     string `json:"agent_id"`
	Network     string `json:"network"`
	BindingRef  string `json:"binding_ref"`
	Created     bool   `json:"created"`
}

// handleBindIdentity is Phase 4A's admin-triggered entry point for
// IdentityBindingService.Bind (atos-spec docs/API.md §9A). Deliberately not
// principal-owner-scoped -- like activation:evaluate, this is an
// operator/activation-authority-side action: a principal can never bind
// itself to an arbitrary claimed TOS identity merely by being
// authenticated.
func (s *Server) handleBindIdentity(w http.ResponseWriter, r *http.Request) {
	var req bindIdentityRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed bind request: "+err.Error(), false)
		return
	}
	idempotencyKey := idempotencyKeyFrom(r)
	if idempotencyKey == "" {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "Idempotency-Key header is required", false)
		return
	}
	principalID := r.PathValue("principal_id")
	op, err := s.IdentityBindings.Bind(r.Context(), service.BindIdentityInput{
		PrincipalID: principalID, AgentID: req.AgentID, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeIdentityBindingErr(w, err)
		return
	}
	// Built directly from op -- a successful Bind() call's returned
	// operation already carries AgentID/RefNetwork/BindingRef/Created,
	// populated together in the SAME advance() transaction that completed
	// it. A separate CurrentBinding re-read would introduce its own race
	// (a crash between driveBind's PutPrincipalBinding and advance() calls
	// could leave the local projection not yet consistent with the just-
	// completed operation) for no benefit, since op is already the
	// authoritative, self-consistent source for everything this response
	// needs.
	writeJSON(w, http.StatusOK, bindIdentityResponse{
		PrincipalID: principalID, AgentID: op.AgentID, Network: op.RefNetwork,
		BindingRef: op.BindingRef, Created: op.Created,
	})
}

type revokeIdentityRequest struct {
	ReasonCode string `json:"reason_code"`
}

type revokeIdentityResponse struct {
	Revoked       bool   `json:"revoked"`
	Network       string `json:"network,omitempty"`
	RevocationRef string `json:"revocation_ref,omitempty"`
}

// handleRevokeIdentity is Phase 4A's admin-triggered entry point for
// IdentityBindingService.Revoke. revoked=false (nothing was bound) is a
// normal 200 response, never an error -- mirrors RevokeExecutionSigner's
// convention.
func (s *Server) handleRevokeIdentity(w http.ResponseWriter, r *http.Request) {
	var req revokeIdentityRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed revoke request: "+err.Error(), false)
		return
	}
	idempotencyKey := idempotencyKeyFrom(r)
	if idempotencyKey == "" {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "Idempotency-Key header is required", false)
		return
	}
	principalID := r.PathValue("principal_id")
	op, err := s.IdentityBindings.Revoke(r.Context(), service.RevokeIdentityBindingInput{
		PrincipalID: principalID, ReasonCode: req.ReasonCode, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeIdentityBindingErr(w, err)
		return
	}
	// Revoked comes from op.Revoked -- the remote RPC's own authoritative
	// signal, NOT a local pre-call CurrentBinding check (a lost-response
	// retry under a fresh idempotency_key finds this principal's LOCAL
	// binding row already deleted by the original successful call, but the
	// remote service still honestly reports revoked=true for the retry) NOR
	// inferred from op.BindingRef being non-empty (revoked/revocation_ref
	// are independent RPC response fields with no wire-level guarantee they
	// always agree).
	writeJSON(w, http.StatusOK, revokeIdentityResponse{
		Revoked: op.Revoked, Network: op.RefNetwork, RevocationRef: op.BindingRef,
	})
}

type identityBindingStatusResponse struct {
	PrincipalID          string `json:"principal_id"`
	Bound                bool   `json:"bound"`
	AgentID              string `json:"agent_id,omitempty"`
	Network              string `json:"network,omitempty"`
	BindingRef           string `json:"binding_ref,omitempty"`
	Status               string `json:"status"`
	RevocationReasonCode string `json:"revocation_reason_code,omitempty"`
}

// handleIdentityBindingStatus reads the durable LOCAL current-state
// projection only -- it does not itself re-verify against tos-protocol
// (TOSBackedActivationAuthority does that at evaluation time; cached
// status here is for operator visibility/audit, never an activation
// decision).
func (s *Server) handleIdentityBindingStatus(w http.ResponseWriter, r *http.Request) {
	principalID := r.PathValue("principal_id")
	binding, found, err := s.IdentityBindings.CurrentBinding(r.Context(), principalID)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	if !found {
		reasonCode, wasRevoked, err := s.IdentityBindings.RevocationHistory(r.Context(), principalID)
		if err != nil {
			writeDomainErr(w, err)
			return
		}
		if wasRevoked {
			writeJSON(w, http.StatusOK, identityBindingStatusResponse{
				PrincipalID: principalID, Bound: false, Status: "revoked", RevocationReasonCode: reasonCode,
			})
			return
		}
		writeJSON(w, http.StatusOK, identityBindingStatusResponse{
			PrincipalID: principalID, Bound: false, Status: "unspecified",
		})
		return
	}
	writeJSON(w, http.StatusOK, identityBindingStatusResponse{
		PrincipalID: principalID, Bound: true, AgentID: binding.AgentID,
		Network: binding.Network, BindingRef: binding.BindingRef, Status: "active",
	})
}

func writeIdentityBindingErr(w http.ResponseWriter, err error) {
	var de *domain.Error
	if errors.As(err, &de) {
		switch de.Code {
		case domain.ErrNotFound:
			writeError(w, http.StatusNotFound, de.Code, de.Message, false)
			return
		case domain.ErrIdempotencyConflict:
			writeError(w, http.StatusConflict, de.Code, de.Message, de.Retryable)
			return
		case domain.ErrValidationFailed:
			writeError(w, http.StatusBadRequest, de.Code, de.Message, false)
			return
		}
	}
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, domain.ErrNotFound, "not found", false)
		return
	}
	writeDomainErr(w, err)
}
