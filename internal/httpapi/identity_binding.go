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
	binding, found, err := s.IdentityBindings.CurrentBinding(r.Context(), principalID)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	if !found {
		// The operation completed but the local projection isn't durably
		// consistent yet (a crash between advance() and this read) -- a
		// genuine, if rare, transient state; report it as such rather than
		// fabricating a response from the (possibly stale) operation record.
		writeError(w, http.StatusConflict, domain.ErrValidationFailed, "binding operation completed but is not yet locally consistent; retry", true)
		return
	}
	writeJSON(w, http.StatusOK, bindIdentityResponse{
		PrincipalID: principalID, AgentID: binding.AgentID, Network: binding.Network,
		// op.Created (not op.Checkpoint == Completed, which is true for BOTH
		// a genuinely new bind and an idempotent replay of an existing one)
		// -- see domain.IdentityBindingOperation.Created's doc comment.
		BindingRef: binding.BindingRef, Created: op.Created,
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
	// Revoked is derived from the operation's own outcome (op.BindingRef
	// populated means tos-protocol reported a real revocation_ref), NOT a
	// local pre-call CurrentBinding check: a lost-response retry under a
	// fresh idempotency_key finds this principal's LOCAL binding row
	// already deleted by the original successful call, but tos-protocol
	// still honestly reports revoked=true with the ORIGINAL ref for the
	// retry -- basing Revoked on the local pre-check would then report
	// revoked:false alongside a populated network/revocation_ref, an
	// internally inconsistent response that violates this endpoint's own
	// frozen contract (network/revocation_ref must be empty when
	// revoked:false).
	writeJSON(w, http.StatusOK, revokeIdentityResponse{
		Revoked: op.BindingRef != "", Network: op.RefNetwork, RevocationRef: op.BindingRef,
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
