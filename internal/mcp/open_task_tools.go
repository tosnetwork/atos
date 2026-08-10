package mcp

import (
	"context"
	"time"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

func moneyArg(args map[string]any, key string) *domain.Money {
	raw, ok := args[key].(map[string]any)
	if !ok {
		return nil
	}
	amount, _ := raw["amount"].(string)
	currency, _ := raw["currency"].(string)
	if amount == "" {
		return nil
	}
	return &domain.Money{Amount: amount, Currency: currency}
}

func (s *Server) toolPublishOpenTask(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	var maxTotal *domain.Money
	if constraints := argObject(args, "constraints"); constraints != nil {
		maxTotal = moneyArg(constraints, "max_total")
	}
	var proofRequirements domain.ProofRequirements
	if raw := args["proof_requirements"]; raw != nil {
		if err := decodeValue(raw, &proofRequirements); err != nil {
			return nil, domain.NewError(domain.ErrValidationFailed, "invalid proof_requirements", false)
		}
	}
	expiresAt, err := time.Parse(time.RFC3339, argString(args, "expires_at"))
	if err != nil {
		return nil, domain.NewError(domain.ErrValidationFailed, "expires_at must be RFC3339", false)
	}
	task, err := s.OpenTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: principal.ID, Title: argString(args, "title"), Description: argString(args, "description"),
		Input: argObject(args, "input"), RequestedTrustMode: domain.RequestedTrustMode(argString(args, "requested_trust_mode")),
		ProofRequirements: proofRequirements, MaxTotal: maxTotal, ExpiresAt: expiresAt.UTC(),
		IdempotencyKey: argString(args, "idempotency_key"),
	})
	if err != nil {
		return nil, err
	}
	return task, nil
}

func (s *Server) toolSearchOpenTasks(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	limit := int(argInt64(args, "limit"))
	tasks, err := s.OpenTasks.ListPublic(ctx, limit)
	if err != nil {
		return nil, err
	}
	return map[string]any{"open_tasks": tasks}, nil
}

func (s *Server) toolGetOpenTask(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	return s.OpenTasks.Get(ctx, principal.ID, argString(args, "task_id"))
}

func (s *Server) toolApplyToOpenTask(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	proposal, err := s.OpenTasks.Propose(ctx, service.ProposeInput{
		ProviderID: principal.ID, TaskID: argString(args, "task_id"), CapabilityID: argString(args, "capability_id"),
		Message: argString(args, "message"), ProposedPrice: moneyArg(args, "proposed_price"),
		IdempotencyKey: argString(args, "idempotency_key"),
	})
	if err != nil {
		return nil, err
	}
	return proposal, nil
}

func (s *Server) toolListOpenTaskProposals(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	proposals, err := s.OpenTasks.ListProposals(ctx, principal.ID, argString(args, "task_id"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"proposals": proposals}, nil
}

func (s *Server) toolWithdrawOpenTaskProposal(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	return s.OpenTasks.Withdraw(ctx, service.WithdrawProposalInput{
		ProviderID: principal.ID, ProposalID: argString(args, "proposal_id"),
	})
}

func (s *Server) toolAcceptOpenTaskProposal(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	task, op, err := s.OpenTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: principal.ID, TaskID: argString(args, "task_id"), ProposalID: argString(args, "proposal_id"),
		IdempotencyKey: argString(args, "idempotency_key"),
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"open_task": task, "acceptance": op}, nil
}

func (s *Server) toolCancelOpenTask(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	return s.OpenTasks.Cancel(ctx, service.CancelOpenTaskInput{
		PrincipalID: principal.ID, TaskID: argString(args, "task_id"),
	})
}
