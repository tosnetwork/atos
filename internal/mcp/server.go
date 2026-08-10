// Package mcp implements the ATOS v0.2 MCP surface over stateless
// Streamable HTTP JSON-RPC requests.
package mcp

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type Server struct {
	Auth         *auth.Service
	Capabilities *service.CapabilityService
	// Health is optional: nil omits the readiness projection from
	// atos_get_capability entirely rather than erroring (see
	// service.CapabilityReadiness's doc comment).
	Health           *service.HealthService
	ExecutionSigners *service.ExecutionSignerService
	// ActivationAuthority backs atos_evaluate_activation -- unlike Health,
	// this is not optional: production wiring MUST always set it (see
	// httpapi.Server.ActivationAuthority's identical doc comment).
	ActivationAuthority domain.ActivationAuthority
	OpenTasks           *service.OpenTaskService
	Quotes              *service.QuoteService
	Jobs                *service.JobService
	Accounts            *service.AccountService
	Receipts            *service.ReceiptService
	Earnings            *service.EarningsService
	Disputes            *service.DisputeService
	Artifacts           *service.ArtifactService
	Logger              *slog.Logger
	PublicBaseURL       string
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

const defaultProtocolVersion = "2026-07-28"

func negotiateProtocolVersion(params json.RawMessage) string {
	var p struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := json.Unmarshal(params, &p); err == nil && p.ProtocolVersion != "" {
		return p.ProtocolVersion
	}
	return defaultProtocolVersion
}

func (s *Server) toolsForPrincipal(principal auth.Principal) []map[string]any {
	tools := make([]map[string]any, 0, len(orderedToolSpecs))
	for _, spec := range orderedToolSpecs {
		if principal.HasAll(spec.RequiredScopes...) {
			tools = append(tools, spec.Definition)
		}
	}
	return tools
}

func bearerToken(r *http.Request) string {
	authz := r.Header.Get("Authorization")
	token, ok := strings.CutPrefix(authz, "Bearer ")
	if !ok {
		return ""
	}
	return strings.TrimSpace(token)
}

func (s *Server) authenticate(r *http.Request) (auth.Principal, error) {
	if s.Auth == nil {
		return auth.Principal{}, domain.NewError(domain.ErrAuthenticationRequired, "authorization service unavailable", true)
	}
	principal, err := s.Auth.Authenticate(bearerToken(r))
	if err != nil {
		return auth.Principal{}, domain.NewError(domain.ErrAuthenticationRequired, err.Error(), false)
	}
	return principal, nil
}

func (s *Server) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req rpcRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
		if err := dec.Decode(&req); err != nil {
			writeRPCError(w, nil, codeParseError, "invalid JSON", nil)
			return
		}
		if req.JSONRPC != "2.0" || req.Method == "" {
			writeRPCError(w, req.ID, codeInvalidRequest, "expected jsonrpc 2.0 request with a method", nil)
			return
		}

		if req.Method == "notifications/initialized" {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if req.Method == "initialize" {
			writeRPCResult(w, req.ID, map[string]any{
				"protocolVersion": negotiateProtocolVersion(req.Params),
				"serverInfo":      map[string]any{"name": "atos-mcp", "version": "0.2.0"},
				"capabilities": map[string]any{
					"tools":     map[string]any{"listChanged": false},
					"resources": map[string]any{"subscribe": false, "listChanged": false},
				},
			})
			return
		}
		if req.Method == "ping" {
			writeRPCResult(w, req.ID, map[string]any{})
			return
		}

		principal, err := s.authenticate(r)
		if err != nil {
			writeRPCError(w, req.ID, codeUnauthorized, err.Error(), map[string]any{"code": domain.ErrAuthenticationRequired})
			return
		}
		ctx := r.Context()
		switch req.Method {
		case "tools/list":
			writeRPCResult(w, req.ID, map[string]any{
				"tools":      s.toolsForPrincipal(principal),
				"ttlMs":      toolsListTTLMS,
				"cacheScope": toolsCacheScope,
			})
		case "tools/call":
			s.handleToolCall(ctx, w, req, principal)
		case "resources/list":
			writeRPCResult(w, req.ID, map[string]any{
				"resources":  resourcesForPrincipal(principal),
				"ttlMs":      toolsListTTLMS,
				"cacheScope": toolsCacheScope,
			})
		case "resources/read":
			s.handleResourceRead(ctx, w, req, principal)
		default:
			writeRPCError(w, req.ID, codeMethodNotFound, "unknown method "+req.Method, nil)
		}
	}
}

type toolCallParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

func (s *Server) handleToolCall(ctx context.Context, w http.ResponseWriter, req rpcRequest, principal auth.Principal) {
	var params toolCallParams
	if err := json.Unmarshal(req.Params, &params); err != nil || params.Name == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "malformed tools/call params", nil)
		return
	}
	spec, known := toolSpecByName(params.Name)
	if !known {
		writeRPCError(w, req.ID, codeMethodNotFound, "unknown tool "+params.Name, nil)
		return
	}
	if !principal.HasAll(spec.RequiredScopes...) {
		// Do not reveal a scope-gated tool that was absent from this caller's list.
		writeRPCError(w, req.ID, codeMethodNotFound, "unknown tool "+params.Name, nil)
		return
	}
	handler, ok := s.dispatch()[params.Name]
	if !ok {
		writeRPCError(w, req.ID, codeMethodNotFound, "tool is not implemented", nil)
		return
	}
	result, toolErr := handler(ctx, principal, params.Arguments)
	if toolErr != nil {
		code := domain.ErrProviderFailed
		retryable := false
		if de, ok := toolErr.(*domain.Error); ok {
			code = de.Code
			retryable = de.Retryable
		}
		writeRPCResult(w, req.ID, map[string]any{
			"isError": true,
			"content": []map[string]any{{"type": "text", "text": toolErr.Error()}},
			"structuredContent": map[string]any{
				"error": map[string]any{"code": code, "message": toolErr.Error(), "retryable": retryable},
			},
		})
		return
	}
	writeRPCResult(w, req.ID, map[string]any{
		"isError":           false,
		"content":           []map[string]any{{"type": "text", "text": "ATOS operation completed"}},
		"structuredContent": result,
	})
}

func writeRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string, data any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message, Data: data}})
}

var _ = codeForbidden // reserved for future protocol-level authorization responses
