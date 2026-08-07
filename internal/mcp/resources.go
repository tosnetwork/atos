package mcp

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/tosnetwork/atos/internal/service"
)

// resourceDefinitions mirrors the "MCP Resources" list in
// ~/atos-spec/docs/MCP.md. All of them are read-only.
var resourceDefinitions = []map[string]any{
	{"uri": "atos://taxonomy", "name": "Capability taxonomy", "description": "Distinct tags currently in use across active capabilities.", "mimeType": "application/json"},
	{"uri": "atos://capabilities/trending", "name": "Trending capabilities", "description": "Phase 0 stand-in: recent search results, not a real trending signal yet.", "mimeType": "application/json"},
	{"uri": "atos://account/policy", "name": "Account spending policy", "description": "The authenticated principal's autonomous spending policy.", "mimeType": "application/json"},
	{"uri": "atos://network/status", "name": "TOS Network status", "description": "Whether this gateway has a live TOS Network connection.", "mimeType": "application/json"},
	{"uri": "atos://docs/protocol-version", "name": "Protocol version", "description": "MCP/ATOS protocol version this server implements.", "mimeType": "application/json"},
}

func (s *Server) handleResourceRead(ctx context.Context, w http.ResponseWriter, req rpcRequest, principalID string) {
	var params struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		writeRPCError(w, req.ID, codeInvalidParams, "malformed resources/read params")
		return
	}

	var content any
	var err error
	switch params.URI {
	case "atos://taxonomy":
		var tags []string
		tags, err = s.Capabilities.Taxonomy(ctx)
		content = map[string]any{"tags": tags}
	case "atos://capabilities/trending":
		var caps any
		caps, err = s.Capabilities.Search(ctx, service.SearchInput{Limit: 5})
		content = map[string]any{"capabilities": caps}
	case "atos://account/policy":
		if principalID == "" {
			writeRPCError(w, req.ID, codeInvalidParams, "missing bearer token")
			return
		}
		var account any
		account, err = s.Accounts.Get(ctx, principalID)
		content = account
	case "atos://network/status":
		content = map[string]any{
			"network": "TOS",
			"status":  "not_connected",
			"note":    "Phase 0/1: tos-core is an in-process mock. No live TOS Network connection exists yet.",
		}
	case "atos://docs/protocol-version":
		content = map[string]any{"mcp_protocol_version": "2026-07-28", "atos_version": "0.1.0"}
	default:
		writeRPCError(w, req.ID, codeInvalidParams, "unknown resource uri "+params.URI)
		return
	}
	if err != nil {
		writeRPCError(w, req.ID, codeInternalError, err.Error())
		return
	}

	writeRPCResult(w, req.ID, map[string]any{
		"contents": []map[string]any{
			{"uri": params.URI, "mimeType": "application/json", "text": mustJSON(content)},
		},
	})
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
