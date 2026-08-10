package httpapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

const defaultOpenTaskListLimit = 50

type publishOpenTaskRequest struct {
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

func (s *Server) handlePublishOpenTask(w http.ResponseWriter, r *http.Request) {
	var req publishOpenTaskRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed open task request: "+err.Error(), false)
		return
	}
	task, err := s.OpenTasks.Publish(r.Context(), service.PublishOpenTaskInput{
		PrincipalID: principalFrom(r), Title: req.Title, Description: req.Description,
		Input: req.Input, RequestedTrustMode: req.RequestedTrustMode, ProofRequirements: req.ProofRequirements,
		MaxTotal: req.Constraints.MaxTotal, ExpiresAt: req.ExpiresAt, IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, task)
}

// handleListOpenTasks is GET /v1/open-tasks -- the marketplace
// browse/search surface, public open tasks only, unless the caller passes
// ?mine=true, which switches to their own tasks (any status, full detail)
// instead. Both branches go through OpenTaskService, never a second
// listing implementation -- see the service methods' own doc comments for
// the exact redaction rules.
func (s *Server) handleListOpenTasks(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("mine") == "true" {
		tasks, err := s.OpenTasks.ListMine(r.Context(), principalFrom(r))
		if err != nil {
			writeDomainErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"open_tasks": tasks})
		return
	}
	limit := defaultOpenTaskListLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	tasks, err := s.OpenTasks.ListPublic(r.Context(), limit)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"open_tasks": tasks})
}

func (s *Server) handleGetOpenTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.OpenTasks.Get(r.Context(), principalFrom(r), r.PathValue("task_id"))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) handleCancelOpenTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.OpenTasks.Cancel(r.Context(), service.CancelOpenTaskInput{
		PrincipalID: principalFrom(r), TaskID: r.PathValue("task_id"),
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

type proposeOpenTaskRequest struct {
	CapabilityID   string        `json:"capability_id"`
	Message        string        `json:"message"`
	ProposedPrice  *domain.Money `json:"proposed_price"`
	IdempotencyKey string        `json:"idempotency_key"`
}

func (s *Server) handleProposeOpenTask(w http.ResponseWriter, r *http.Request) {
	var req proposeOpenTaskRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed proposal request: "+err.Error(), false)
		return
	}
	proposal, err := s.OpenTasks.Propose(r.Context(), service.ProposeInput{
		ProviderID: principalFrom(r), TaskID: r.PathValue("task_id"), CapabilityID: req.CapabilityID,
		Message: req.Message, ProposedPrice: req.ProposedPrice, IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, proposal)
}

func (s *Server) handleListOpenTaskProposals(w http.ResponseWriter, r *http.Request) {
	proposals, err := s.OpenTasks.ListProposals(r.Context(), principalFrom(r), r.PathValue("task_id"))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"proposals": proposals})
}

func (s *Server) handleWithdrawOpenTaskProposal(w http.ResponseWriter, r *http.Request) {
	proposal, err := s.OpenTasks.Withdraw(r.Context(), service.WithdrawProposalInput{
		ProviderID: principalFrom(r), ProposalID: r.PathValue("proposal_id"),
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, proposal)
}

type acceptOpenTaskProposalRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
}

func (s *Server) handleAcceptOpenTaskProposal(w http.ResponseWriter, r *http.Request) {
	var req acceptOpenTaskProposalRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed accept request: "+err.Error(), false)
		return
	}
	task, op, err := s.OpenTasks.Accept(r.Context(), service.AcceptProposalInput{
		PrincipalID: principalFrom(r), TaskID: r.PathValue("task_id"), ProposalID: r.PathValue("proposal_id"),
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"open_task": task, "acceptance": op})
}
