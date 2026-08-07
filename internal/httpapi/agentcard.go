package httpapi

import "net/http"

// handleAgentCard serves the static platform Agent Card from
// docs/AGENT_CARD.md. Provider-specific cards (GET
// /providers/{id}/agent-card) are a Phase 1+ addition once capabilities
// carry enough metadata to build one per provider.
func (s *Server) handleAgentCard(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"name":        "ATOS",
		"description": "Gateway to the Agent Internet. Discover, invoke and pay for capabilities across TOS Network.",
		"url":         "https://a2a.atos.im",
		"version":     "0.1.0",
		"provider": map[string]any{
			"organization": "TOS Network",
			"url":          "https://tos.network",
		},
		"capabilities": map[string]any{
			"streaming":         false,
			"pushNotifications": false,
		},
		"defaultInputModes":  []string{"text", "application/json"},
		"defaultOutputModes": []string{"text", "application/json"},
		"skills": []map[string]any{
			{
				"id":          "capability-discovery",
				"name":        "Capability Discovery",
				"description": "Find and rank external agent capabilities.",
				"tags":        []string{"discovery", "agents", "capabilities"},
			},
			{
				"id":          "capability-invocation",
				"name":        "Capability Invocation",
				"description": "Invoke a selected capability with policy-aware commercial settlement.",
				"tags":        []string{"invoke", "commerce", "agents"},
			},
		},
		"extensions": map[string]any{
			"atos": map[string]any{
				"version": "1",
				"mcp": map[string]any{
					"url":          "https://mcp.atos.im/mcp",
					"legacySseUrl": "https://mcp.atos.im/sse",
				},
				"api":                  "https://api.atos.im/v1",
				"deviceAuth":           "https://api.atos.im/v1/auth/device",
				"network":              "TOS",
				"clientWalletRequired": false,
			},
		},
	})
}
