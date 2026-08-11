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
	// Built directly from op -- see httpapi.handleBindIdentity's identical
	// fix/comment: a successful Bind() call's returned operation is already
	// the authoritative, self-consistent source for everything this result
	// needs, without a separate CurrentBinding re-read's own race window.
	return bindIdentityResult{
		PrincipalID: principalID, AgentID: op.AgentID, Network: op.RefNetwork,
		BindingRef: op.BindingRef, Created: op.Created,
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
	// Revoked comes from op.Revoked, the remote RPC's own authoritative
	// signal -- see httpapi.handleRevokeIdentity's identical comment for
	// why neither a local pre-call check nor op.BindingRef non-emptiness is
	// the right source.
	return revokeIdentityResult{Revoked: op.Revoked, Network: op.RefNetwork, RevocationRef: op.BindingRef}, nil
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
// "never an activation decision" doc comment. The active/revoked/
// unspecified branching itself lives in
// service.IdentityBindingService.Status, shared with the REST transport, so
// this tool is just a field mapping.
func (s *Server) toolIdentityBindingStatus(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	principalID := argString(args, "principal_id")
	status, err := s.IdentityBindings.Status(ctx, principalID)
	if err != nil {
		return nil, err
	}
	return identityBindingStatusResult{
		PrincipalID: principalID, Bound: status.Bound, AgentID: status.AgentID,
		Network: status.Network, BindingRef: status.BindingRef,
		Status: status.Status, RevocationReasonCode: status.RevocationReasonCode,
	}, nil
}
