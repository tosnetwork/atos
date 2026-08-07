package a2a

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// Server implements POST /a2a: JSON-RPC 2.0 methods message/send,
// tasks/get and tasks/cancel, sharing the same JobService REST and MCP
// dispatch into — an A2A caller and an MCP caller reach the identical
// quote -> escrow -> execute -> verify -> settle pipeline.
type Server struct {
	Quotes *service.QuoteService
	Jobs   *service.JobService
	Logger *slog.Logger
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
}

const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
)

func (s *Server) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		token, _ := strings.CutPrefix(authz, "Bearer ")
		principalID := strings.TrimSpace(token)

		var req rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRPCError(w, nil, codeParseError, "invalid JSON")
			return
		}
		if req.JSONRPC != "2.0" || req.Method == "" {
			writeRPCError(w, req.ID, codeInvalidRequest, "expected jsonrpc 2.0 request with a method")
			return
		}
		if principalID == "" {
			writeRPCError(w, req.ID, codeInvalidParams, "missing bearer token")
			return
		}

		ctx := r.Context()
		switch req.Method {
		case "message/send":
			s.handleMessageSend(ctx, w, req, principalID)
		case "tasks/get":
			s.handleTasksGet(ctx, w, req, principalID)
		case "tasks/cancel":
			s.handleTasksCancel(ctx, w, req, principalID)
		default:
			writeRPCError(w, req.ID, codeMethodNotFound, "unknown method "+req.Method)
		}
	}
}

type messageSendParams struct {
	Message Message `json:"message"`
}

// handleMessageSend implements docs/A2A.md's Task/Message mapping: a new
// message (no taskId) creates a Job via the same quote/escrow/execute
// pipeline REST's atos_invoke uses; a message with taskId + the commerce
// extension's confirmed=true continues an existing input-required Task,
// mirroring MCP's confirmed-reissue flow (see internal/service/job.go).
func (s *Server) handleMessageSend(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params messageSendParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		writeRPCError(w, req.ID, codeInvalidParams, "malformed message/send params")
		return
	}
	ext := params.Message.Commerce()
	if ext.CapabilityID == "" || ext.QuoteID == "" || ext.IdempotencyKey == "" {
		writeRPCError(w, req.ID, codeInvalidParams,
			"message.metadata[\""+CommerceExtensionURI+"\"] must include capability_id, quote_id and idempotency_key")
		return
	}

	input := map[string]any{}
	for _, p := range params.Message.Parts {
		if p.Kind == "text" && p.Text != "" {
			input["text"] = p.Text
		}
		if p.Kind == "data" && p.Data != nil {
			for k, v := range p.Data {
				input[k] = v
			}
		}
	}

	result, err := s.Jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID:    principalID,
		CapabilityID:   ext.CapabilityID,
		QuoteID:        ext.QuoteID,
		Input:          input,
		IdempotencyKey: ext.IdempotencyKey,
		Confirmed:      ext.Confirmed,
	})
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, TaskFromJob(result.Job))
}

type taskIDParams struct {
	ID       string         `json:"id"`
	Metadata map[string]any `json:"metadata"`
}

func (s *Server) handleTasksGet(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params taskIDParams
	if err := json.Unmarshal(req.Params, &params); err != nil || params.ID == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "tasks/get requires params.id")
		return
	}
	job, err := s.Jobs.Get(ctx, params.ID)
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	if job.PrincipalID != principalID {
		writeRPCError(w, req.ID, codeInvalidParams, "not the task's owning principal")
		return
	}
	writeRPCResult(w, req.ID, TaskFromJob(job))
}

func (s *Server) handleTasksCancel(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params taskIDParams
	if err := json.Unmarshal(req.Params, &params); err != nil || params.ID == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "tasks/cancel requires params.id")
		return
	}
	msg := Message{Metadata: params.Metadata}
	ext := msg.Commerce()
	idempotencyKey := ext.IdempotencyKey
	if idempotencyKey == "" {
		// tasks/cancel has no natural idempotency key of its own the way
		// message/send does (the caller isn't retrying a business
		// operation, just asking to stop one) — mint one deterministically
		// from the task id so a client retrying the exact same cancel call
		// still gets idempotent behavior without being forced to invent a
		// key for a cancel.
		idempotencyKey = "a2a-cancel-" + params.ID
	}
	job, err := s.Jobs.Cancel(ctx, params.ID, principalID, ext.Reason, idempotencyKey)
	if err != nil {
		writeDomainErrorAsRPC(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, TaskFromJob(job))
}

func writeRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message}})
}

// writeDomainErrorAsRPC surfaces a domain.Error's machine code/message
// through the JSON-RPC error envelope instead of collapsing everything to
// a generic internal error.
func writeDomainErrorAsRPC(w http.ResponseWriter, id json.RawMessage, err error) {
	de, ok := err.(*domain.Error)
	if !ok {
		writeRPCError(w, id, codeInternalError, err.Error())
		return
	}
	writeRPCError(w, id, codeInvalidParams, string(de.Code)+": "+de.Message)
}
