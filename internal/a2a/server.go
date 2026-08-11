package a2a

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// Server exposes A2A Task operations over the same JobService used by REST and
// MCP. The caller never chooses a new trust mode here: the supplied Quote is
// authoritative and its concrete mode/profile flow into Task metadata.
type Server struct {
	Auth   *auth.Service
	Quotes *service.QuoteService
	Jobs   *service.JobService
	// OpenTasks backs the openTasks/* method namespace (docs/A2A.md's
	// "Open Task Marketplace Extension") -- deliberately not optional in
	// production wiring, mirroring Jobs/Quotes' own treatment.
	OpenTasks     *service.OpenTaskService
	Logger        *slog.Logger
	PublicBaseURL string
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
	codeUnauthorized   = -32001
	codeForbidden      = -32003
)

func (s *Server) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req rpcRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&req); err != nil {
			writeRPCError(w, nil, codeParseError, "invalid JSON", nil)
			return
		}
		if req.JSONRPC != "2.0" || req.Method == "" {
			writeRPCError(w, req.ID, codeInvalidRequest, "expected jsonrpc 2.0 request with a method", nil)
			return
		}
		if s.Auth == nil {
			writeRPCError(w, req.ID, codeUnauthorized, "authorization service unavailable", map[string]any{"code": domain.ErrAuthenticationRequired})
			return
		}
		authz := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(authz, "Bearer ")
		if !ok {
			writeRPCError(w, req.ID, codeUnauthorized, "missing bearer token", map[string]any{"code": domain.ErrAuthenticationRequired})
			return
		}
		principal, err := s.Auth.Authenticate(strings.TrimSpace(token))
		if err != nil {
			writeRPCError(w, req.ID, codeUnauthorized, err.Error(), map[string]any{"code": domain.ErrAuthenticationRequired})
			return
		}

		ctx := r.Context()
		switch req.Method {
		case "message/send":
			if !principal.Has(auth.ScopeInvocationsCreate) {
				writeRPCError(w, req.ID, codeForbidden, "message/send requires invocations:create", map[string]any{"code": domain.ErrPermissionDenied})
				return
			}
			s.handleMessageSend(ctx, w, req, principal.ID)
		case "tasks/get":
			if !principal.Has(auth.ScopeJobsRead) {
				writeRPCError(w, req.ID, codeForbidden, "tasks/get requires jobs:read", map[string]any{"code": domain.ErrPermissionDenied})
				return
			}
			s.handleTasksGet(ctx, w, req, principal.ID)
		case "tasks/cancel":
			if !principal.Has(auth.ScopeJobsCancel) {
				writeRPCError(w, req.ID, codeForbidden, "tasks/cancel requires jobs:cancel", map[string]any{"code": domain.ErrPermissionDenied})
				return
			}
			s.handleTasksCancel(ctx, w, req, principal.ID)
		case "openTasks/publish":
			if !principal.Has(auth.ScopeOpenTasksWrite) {
				writeRPCError(w, req.ID, codeForbidden, "openTasks/publish requires open_tasks:write", map[string]any{"code": domain.ErrPermissionDenied})
				return
			}
			s.handleOpenTasksPublish(ctx, w, req, principal.ID)
		case "openTasks/search":
			if !principal.Has(auth.ScopeOpenTasksRead) {
				writeRPCError(w, req.ID, codeForbidden, "openTasks/search requires open_tasks:read", map[string]any{"code": domain.ErrPermissionDenied})
				return
			}
			s.handleOpenTasksSearch(ctx, w, req, principal.ID)
		case "openTasks/get":
			if !principal.Has(auth.ScopeOpenTasksRead) {
				writeRPCError(w, req.ID, codeForbidden, "openTasks/get requires open_tasks:read", map[string]any{"code": domain.ErrPermissionDenied})
				return
			}
			s.handleOpenTasksGet(ctx, w, req, principal.ID)
		case "openTasks/cancel":
			if !principal.Has(auth.ScopeOpenTasksWrite) {
				writeRPCError(w, req.ID, codeForbidden, "openTasks/cancel requires open_tasks:write", map[string]any{"code": domain.ErrPermissionDenied})
				return
			}
			s.handleOpenTasksCancel(ctx, w, req, principal.ID)
		case "openTasks/proposals/submit":
			if !principal.Has(auth.ScopeOpenTaskProposalsWrite) {
				writeRPCError(w, req.ID, codeForbidden, "openTasks/proposals/submit requires open_task_proposals:write", map[string]any{"code": domain.ErrPermissionDenied})
				return
			}
			s.handleOpenTasksProposalsSubmit(ctx, w, req, principal.ID)
		case "openTasks/proposals/list":
			if !principal.Has(auth.ScopeOpenTasksRead) {
				writeRPCError(w, req.ID, codeForbidden, "openTasks/proposals/list requires open_tasks:read", map[string]any{"code": domain.ErrPermissionDenied})
				return
			}
			s.handleOpenTasksProposalsList(ctx, w, req, principal.ID)
		case "openTasks/proposals/withdraw":
			if !principal.Has(auth.ScopeOpenTaskProposalsWrite) {
				writeRPCError(w, req.ID, codeForbidden, "openTasks/proposals/withdraw requires open_task_proposals:write", map[string]any{"code": domain.ErrPermissionDenied})
				return
			}
			s.handleOpenTasksProposalsWithdraw(ctx, w, req, principal.ID)
		case "openTasks/proposals/accept":
			if !principal.Has(auth.ScopeOpenTasksWrite) {
				writeRPCError(w, req.ID, codeForbidden, "openTasks/proposals/accept requires open_tasks:write", map[string]any{"code": domain.ErrPermissionDenied})
				return
			}
			s.handleOpenTasksProposalsAccept(ctx, w, req, principal.ID)
		default:
			writeRPCError(w, req.ID, codeMethodNotFound, "unknown method "+req.Method, nil)
		}
	}
}

type messageSendParams struct {
	Message Message `json:"message"`
}

func (s *Server) handleMessageSend(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params messageSendParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		writeRPCError(w, req.ID, codeInvalidParams, "malformed message/send params", nil)
		return
	}
	ext := params.Message.Commerce()
	if ext.CapabilityID == "" || ext.QuoteID == "" || ext.IdempotencyKey == "" {
		writeRPCError(w, req.ID, codeInvalidParams,
			"message.metadata[\""+CommerceExtensionURI+"\"] must include capability_id, quote_id and idempotency_key", nil)
		return
	}

	input := map[string]any{}
	for _, part := range params.Message.Parts {
		switch part.Kind {
		case "text":
			if part.Text != "" {
				input["text"] = part.Text
			}
		case "data":
			for key, value := range part.Data {
				input[key] = value
			}
		}
	}
	result, err := s.Jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: principalID, CapabilityID: ext.CapabilityID,
		QuoteID: ext.QuoteID, Input: input, IdempotencyKey: ext.IdempotencyKey,
	})
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, TaskFromJob(result.Job, s.jobStreamURL(result.Job.ID)))
}

type taskIDParams struct {
	ID       string         `json:"id"`
	Metadata map[string]any `json:"metadata"`
}

func (s *Server) handleTasksGet(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params taskIDParams
	if err := json.Unmarshal(req.Params, &params); err != nil || params.ID == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "tasks/get requires params.id", nil)
		return
	}
	job, err := s.Jobs.Get(ctx, params.ID)
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	if job.PrincipalID != principalID {
		writeRPCError(w, req.ID, codeForbidden, "not the task's owning principal", map[string]any{"code": domain.ErrPermissionDenied})
		return
	}
	writeRPCResult(w, req.ID, TaskFromJob(job, s.jobStreamURL(job.ID)))
}

func (s *Server) handleTasksCancel(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params taskIDParams
	if err := json.Unmarshal(req.Params, &params); err != nil || params.ID == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "tasks/cancel requires params.id", nil)
		return
	}
	ext := (Message{Metadata: params.Metadata}).Commerce()
	idempotencyKey := ext.IdempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = "a2a-cancel-" + params.ID
	}
	job, err := s.Jobs.Cancel(ctx, params.ID, principalID, ext.Reason, idempotencyKey)
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, TaskFromJob(job, s.jobStreamURL(job.ID)))
}

func (s *Server) jobStreamURL(jobID string) string {
	if jobID == "" {
		return ""
	}
	base := strings.TrimRight(s.PublicBaseURL, "/")
	if base == "" {
		base = "http://localhost:8080"
	}
	return base + "/v1/jobs/" + url.PathEscape(jobID) + "/stream"
}

func writeRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string, data any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message, Data: data}})
}

func writeDomainErrorAsRPC(w http.ResponseWriter, id json.RawMessage, err error) {
	de, ok := err.(*domain.Error)
	if !ok {
		writeRPCError(w, id, codeInternalError, err.Error(), nil)
		return
	}
	code := codeInvalidParams
	if de.Code == domain.ErrPermissionDenied {
		code = codeForbidden
	}
	writeRPCError(w, id, code, de.Message, map[string]any{"code": de.Code, "retryable": de.Retryable})
}
