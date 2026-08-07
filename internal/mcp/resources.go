package mcp

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type resourceSpec struct {
	Definition     map[string]any
	RequiredScopes []auth.Scope
	AnyScope       []auth.Scope
}

var orderedResourceSpecs = []resourceSpec{
	{Definition: map[string]any{"uri": "atos://taxonomy", "name": "Capability taxonomy", "description": "Distinct tags used by active capabilities.", "mimeType": "application/json"}, RequiredScopes: []auth.Scope{auth.ScopeCapabilitiesRead}},
	{Definition: map[string]any{"uri": "atos://capabilities/trending", "name": "Trending capabilities", "description": "Current gateway projection of notable capabilities.", "mimeType": "application/json"}, RequiredScopes: []auth.Scope{auth.ScopeCapabilitiesRead}},
	{Definition: map[string]any{"uri": "atos://account/policy", "name": "Account policy", "description": "Authenticated principal spend and trust policy.", "mimeType": "application/json"}, RequiredScopes: []auth.Scope{auth.ScopeAccountRead}},
	{Definition: map[string]any{"uri": "atos://network/status", "name": "ATOS trust-mode status", "description": "High-level Managed, Verified and Native availability without chain plumbing.", "mimeType": "application/json"}, AnyScope: []auth.Scope{auth.ScopeCapabilitiesRead, auth.ScopeNetworkRead}},
	{Definition: map[string]any{"uri": "atos://docs/protocol-version", "name": "Protocol version", "description": "MCP and ATOS protocol versions implemented by this gateway.", "mimeType": "application/json"}},
}

func resourcesForPrincipal(principal auth.Principal) []map[string]any {
	out := make([]map[string]any, 0, len(orderedResourceSpecs))
	for _, spec := range orderedResourceSpecs {
		if !principal.HasAll(spec.RequiredScopes...) {
			continue
		}
		if len(spec.AnyScope) > 0 && !principal.HasAny(spec.AnyScope...) {
			continue
		}
		out = append(out, spec.Definition)
	}
	return out
}

func (s *Server) handleResourceRead(ctx context.Context, w http.ResponseWriter, req rpcRequest, principal auth.Principal) {
	var params struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil || params.URI == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "malformed resources/read params", nil)
		return
	}
	if !resourceVisible(params.URI, principal) {
		writeRPCError(w, req.ID, codeMethodNotFound, "unknown resource uri "+params.URI, nil)
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
		content, err = s.Accounts.Get(ctx, principal.ID)
	case "atos://network/status":
		content = map[string]any{
			"managed":  "available",
			"verified": "unavailable",
			"native":   "unavailable",
			"network":  "TOS",
			"note":     "Phase 0/1 uses mock tos-core; Verified/Native are never silently mapped to Managed.",
		}
	case "atos://docs/protocol-version":
		content = map[string]any{"mcp_protocol_version": defaultProtocolVersion, "atos_version": "0.2.0"}
	default:
		writeRPCError(w, req.ID, codeMethodNotFound, "unknown resource uri "+params.URI, nil)
		return
	}
	if err != nil {
		code := domain.ErrProviderFailed
		if de, ok := err.(*domain.Error); ok {
			code = de.Code
		}
		writeRPCError(w, req.ID, codeInternalError, err.Error(), map[string]any{"code": code})
		return
	}
	writeRPCResult(w, req.ID, map[string]any{
		"contents": []map[string]any{{"uri": params.URI, "mimeType": "application/json", "text": mustJSON(content)}},
	})
}

func resourceVisible(uri string, principal auth.Principal) bool {
	for _, spec := range orderedResourceSpecs {
		if spec.Definition["uri"] != uri {
			continue
		}
		return principal.HasAll(spec.RequiredScopes...) && (len(spec.AnyScope) == 0 || principal.HasAny(spec.AnyScope...))
	}
	return false
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
