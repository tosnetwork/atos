package a2a

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// This file implements the openTasks/* JSON-RPC method namespace frozen in
// atos-spec docs/A2A.md's "Open Task Marketplace Extension" section --
// deliberately disjoint from tasks/* (Invariant 1: an A2A Task always maps
// to one ATOS Job; Invariant 9: an OpenTask is never wrapped in the
// message/send or Task/Message model). Every handler here is a thin
// translation of the same wire contract REST (internal/httpapi/open_tasks.go)
// and MCP (internal/mcp/open_task_tools.go) already expose -- object field
// names/shapes and idempotency-key-as-param convention are identical, per
// docs/A2A.md's own "never a second naming or shape for the same objects"
// rule.

type openTasksPublishParams struct {
	Title              string                    `json:"title"`
	Description        string                    `json:"description"`
	Input              map[string]any            `json:"input"`
	RequestedTrustMode domain.RequestedTrustMode `json:"requested_trust_mode"`
	ProofRequirements  domain.ProofRequirements  `json:"proof_requirements"`
	Constraints        struct {
		MaxTotal *domain.Money `json:"max_total"`
	} `json:"constraints"`
	ExpiresAt      time.Time `json:"expires_at"`
	IdempotencyKey string    `json:"idempotency_key"`
}

func (s *Server) handleOpenTasksPublish(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params openTasksPublishParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		writeRPCError(w, req.ID, codeInvalidParams, "malformed openTasks/publish params", nil)
		return
	}
	task, err := s.OpenTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: principalID, Title: params.Title, Description: params.Description,
		Input: params.Input, RequestedTrustMode: params.RequestedTrustMode, ProofRequirements: params.ProofRequirements,
		MaxTotal: params.Constraints.MaxTotal, ExpiresAt: params.ExpiresAt.UTC(), IdempotencyKey: params.IdempotencyKey,
	})
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, task)
}

func (s *Server) handleOpenTasksSearch(ctx context.Context, w http.ResponseWriter, req rpcRequest, _ string) {
	var params struct {
		Limit int `json:"limit"`
	}
	// limit is the only param and it's optional -- unlike every other
	// openTasks/* method, an entirely absent (zero-length) params value is
	// a normal call, not a malformed one.
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			writeRPCError(w, req.ID, codeInvalidParams, "malformed openTasks/search params", nil)
			return
		}
	}
	tasks, err := s.OpenTasks.ListPublic(ctx, params.Limit)
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, map[string]any{"open_tasks": tasks})
}

type openTaskIDParams struct {
	TaskID string `json:"task_id"`
}

func (s *Server) handleOpenTasksGet(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params openTaskIDParams
	if err := json.Unmarshal(req.Params, &params); err != nil || params.TaskID == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "openTasks/get requires params.task_id", nil)
		return
	}
	task, err := s.OpenTasks.Get(ctx, principalID, params.TaskID)
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, task)
}

func (s *Server) handleOpenTasksCancel(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params openTaskIDParams
	if err := json.Unmarshal(req.Params, &params); err != nil || params.TaskID == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "openTasks/cancel requires params.task_id", nil)
		return
	}
	task, err := s.OpenTasks.Cancel(ctx, service.CancelOpenTaskInput{PrincipalID: principalID, TaskID: params.TaskID})
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, task)
}

type openTasksProposalsSubmitParams struct {
	TaskID         string        `json:"task_id"`
	CapabilityID   string        `json:"capability_id"`
	Message        string        `json:"message"`
	ProposedPrice  *domain.Money `json:"proposed_price"`
	IdempotencyKey string        `json:"idempotency_key"`
}

func (s *Server) handleOpenTasksProposalsSubmit(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params openTasksProposalsSubmitParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		writeRPCError(w, req.ID, codeInvalidParams, "malformed openTasks/proposals/submit params", nil)
		return
	}
	proposal, err := s.OpenTasks.Propose(ctx, service.ProposeInput{
		ProviderID: principalID, TaskID: params.TaskID, CapabilityID: params.CapabilityID,
		Message: params.Message, ProposedPrice: params.ProposedPrice, IdempotencyKey: params.IdempotencyKey,
	})
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, proposal)
}

func (s *Server) handleOpenTasksProposalsList(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params openTaskIDParams
	if err := json.Unmarshal(req.Params, &params); err != nil || params.TaskID == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "openTasks/proposals/list requires params.task_id", nil)
		return
	}
	proposals, err := s.OpenTasks.ListProposals(ctx, principalID, params.TaskID)
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, map[string]any{"proposals": proposals})
}

type openTasksProposalIDParams struct {
	ProposalID string `json:"proposal_id"`
}

func (s *Server) handleOpenTasksProposalsWithdraw(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params openTasksProposalIDParams
	if err := json.Unmarshal(req.Params, &params); err != nil || params.ProposalID == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "openTasks/proposals/withdraw requires params.proposal_id", nil)
		return
	}
	proposal, err := s.OpenTasks.Withdraw(ctx, service.WithdrawProposalInput{ProviderID: principalID, ProposalID: params.ProposalID})
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, proposal)
}

type openTasksProposalsAcceptParams struct {
	TaskID         string `json:"task_id"`
	ProposalID     string `json:"proposal_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

func (s *Server) handleOpenTasksProposalsAccept(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params openTasksProposalsAcceptParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		writeRPCError(w, req.ID, codeInvalidParams, "malformed openTasks/proposals/accept params", nil)
		return
	}
	task, op, err := s.OpenTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: principalID, TaskID: params.TaskID, ProposalID: params.ProposalID, IdempotencyKey: params.IdempotencyKey,
	})
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, map[string]any{"open_task": task, "acceptance": op})
}
