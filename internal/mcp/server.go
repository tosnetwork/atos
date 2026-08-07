// Package mcp implements the MCP surface from ~/atos-spec/docs/MCP.md: the
// 10 default tools over JSON-RPC 2.0. This is a single-request request/
// response implementation of Streamable HTTP (tools/list, tools/call) —
// it does not yet implement session resumability or true MRTR
// (input_required) round trips; input_required is returned as a normal
// tool result the client is expected to re-issue after user confirmation,
// which is a reasonable Phase 0 stand-in but not the full MCP elicitation
// flow described in docs/MCP.md.
package mcp

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tosnetwork/atos/internal/service"
)

type Server struct {
	Capabilities *service.CapabilityService
	Quotes       *service.QuoteService
	Jobs         *service.JobService
	Accounts     *service.AccountService
	Receipts     *service.ReceiptService
	Logger       *slog.Logger
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
		token = strings.TrimSpace(token)

		var req rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRPCError(w, nil, codeParseError, "invalid JSON")
			return
		}
		if req.JSONRPC != "2.0" || req.Method == "" {
			writeRPCError(w, req.ID, codeInvalidRequest, "expected jsonrpc 2.0 request with a method")
			return
		}

		ctx := r.Context()

		switch req.Method {
		case "initialize":
			writeRPCResult(w, req.ID, map[string]any{
				"protocolVersion": "2026-07-28",
				"serverInfo":      map[string]any{"name": "atos-mcp", "version": "0.1.0"},
				"capabilities":    map[string]any{"tools": map[string]any{}},
			})
		case "tools/list":
			writeRPCResult(w, req.ID, map[string]any{"tools": toolDefinitions})
		case "tools/call":
			s.handleToolCall(ctx, w, req, token)
		case "resources/list":
			writeRPCResult(w, req.ID, map[string]any{"resources": resourceDefinitions})
		case "resources/read":
			s.handleResourceRead(ctx, w, req, token)
		default:
			writeRPCError(w, req.ID, codeMethodNotFound, "unknown method "+req.Method)
		}
	}
}

type toolCallParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

func (s *Server) handleToolCall(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params toolCallParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		writeRPCError(w, req.ID, codeInvalidParams, "malformed tools/call params")
		return
	}
	if principalID == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "missing bearer token")
		return
	}

	handler, ok := s.dispatch()[params.Name]
	if !ok {
		writeRPCError(w, req.ID, codeMethodNotFound, "unknown tool "+params.Name)
		return
	}

	result, toolErr := handler(ctx, principalID, params.Arguments)
	if toolErr != nil {
		// MCP convention: tool-level errors are structured content with
		// isError, not a JSON-RPC transport error — the call itself
		// succeeded, the underlying operation did not.
		writeRPCResult(w, req.ID, map[string]any{
			"isError":           true,
			"content":           []map[string]any{{"type": "text", "text": toolErr.Error()}},
			"structuredContent": map[string]any{"error": toolErr.Error()},
		})
		return
	}

	writeRPCResult(w, req.ID, map[string]any{
		"isError":           false,
		"content":           []map[string]any{{"type": "text", "text": "ok"}},
		"structuredContent": result,
	})
}

func writeRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message}})
}
