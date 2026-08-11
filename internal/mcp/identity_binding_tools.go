package mcp

import (
	"context"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type bindIdentityResult struct {
	PrincipalID string `json:"principal_id"`
	AgentID     string `json:"agent_id"`
	Network     string `json:"network"`
	BindingRef  string `json:"binding_ref"`
	Created     bool   `json:"created"`
}

// toolBindIdentity mirrors httpapi's handleBindIdentity exactly (atos-spec
// docs/API.md §9A) -- same operator/activation-authority-side authorization
// model as atos_evaluate_activation: never principal-owner-scoped, since a
// principal cannot bind itself to an arbitrary claimed TOS identity merely
// by being authenticated.
func (s *Server) toolBindIdentity(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	idempotencyKey := argString(args, "idempotency_key")
	if idempotencyKey == "" {
		return nil, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}
	principalID := argString(args, "principal_id")
	agentID := argString(args, "agent_id")
	op, err := s.IdentityBindings.Bind(ctx, service.BindIdentityInput{
		PrincipalID: principalID, AgentID: agentID, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return nil, err
	}
	binding, found, err := s.IdentityBindings.CurrentBinding(ctx, principalID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, domain.NewError(domain.ErrValidationFailed, "binding operation completed but is not yet locally consistent; retry", true)
	}
	return bindIdentityResult{
		PrincipalID: principalID, AgentID: binding.AgentID, Network: binding.Network,
		// op.Created, not op.Checkpoint == Completed -- see httpapi's
		// identical fix and domain.IdentityBindingOperation.Created's doc
		// comment.
		BindingRef: binding.BindingRef, Created: op.Created,
	}, nil
}

type revokeIdentityResult struct {
	Revoked       bool   `json:"revoked"`
	Network       string `json:"network,omitempty"`
	RevocationRef string `json:"revocation_ref,omitempty"`
}

// toolRevokeIdentity mirrors httpapi's handleRevokeIdentity: revoked:false
// is a normal result (nothing was bound), not an error.
func (s *Server) toolRevokeIdentity(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	idempotencyKey := argString(args, "idempotency_key")
	if idempotencyKey == "" {
		return nil, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}
	principalID := argString(args, "principal_id")
	op, err := s.IdentityBindings.Revoke(ctx, service.RevokeIdentityBindingInput{
		PrincipalID: principalID, ReasonCode: argString(args, "reason_code"), IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return nil, err
	}
	// Revoked is derived from the operation's own outcome, not a local
	// pre-call CurrentBinding check -- see httpapi.handleRevokeIdentity's
	// identical comment: a fresh-idempotency-key retry after a lost
	// response would otherwise report revoked:false alongside a populated
	// network/revocation_ref, since tos-protocol honestly replays the
	// original revocation for the retry even though this principal's local
	// binding row was already deleted by the original successful call.
	return revokeIdentityResult{Revoked: op.BindingRef != "", Network: op.RefNetwork, RevocationRef: op.BindingRef}, nil
}

type identityBindingStatusResult struct {
	PrincipalID          string `json:"principal_id"`
	Bound                bool   `json:"bound"`
	AgentID              string `json:"agent_id,omitempty"`
	Network              string `json:"network,omitempty"`
	BindingRef           string `json:"binding_ref,omitempty"`
	Status               string `json:"status"`
	RevocationReasonCode string `json:"revocation_reason_code,omitempty"`
}

// toolIdentityBindingStatus reads the durable LOCAL current-state
// projection only -- see httpapi.handleIdentityBindingStatus's identical
// "never an activation decision" doc comment.
func (s *Server) toolIdentityBindingStatus(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	principalID := argString(args, "principal_id")
	binding, found, err := s.IdentityBindings.CurrentBinding(ctx, principalID)
	if err != nil {
		return nil, err
	}
	if !found {
		return identityBindingStatusResult{PrincipalID: principalID, Bound: false, Status: "unspecified"}, nil
	}
	return identityBindingStatusResult{
		PrincipalID: principalID, Bound: true, AgentID: binding.AgentID,
		Network: binding.Network, BindingRef: binding.BindingRef, Status: "active",
	}, nil
}
