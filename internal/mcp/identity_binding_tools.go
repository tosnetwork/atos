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
		BindingRef: binding.BindingRef, Created: op.Checkpoint == domain.IdentityBindingCheckpointCompleted,
	}, nil
}

type revokeIdentityResult struct {
	Revoked bool `json:"revoked"`
}

// toolRevokeIdentity mirrors httpapi's handleRevokeIdentity: revoked:false
// is a normal result (nothing was bound), not an error.
func (s *Server) toolRevokeIdentity(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	idempotencyKey := argString(args, "idempotency_key")
	if idempotencyKey == "" {
		return nil, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}
	principalID := argString(args, "principal_id")
	// Captured BEFORE calling Revoke -- see httpapi.handleRevokeIdentity's
	// identical comment: the operation's own checkpoint can't distinguish
	// "actually revoked something" from "nothing to revoke."
	_, wasBound, err := s.IdentityBindings.CurrentBinding(ctx, principalID)
	if err != nil {
		return nil, err
	}
	if _, err := s.IdentityBindings.Revoke(ctx, service.RevokeIdentityBindingInput{
		PrincipalID: principalID, ReasonCode: argString(args, "reason_code"), IdempotencyKey: idempotencyKey,
	}); err != nil {
		return nil, err
	}
	return revokeIdentityResult{Revoked: wasBound}, nil
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
