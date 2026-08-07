package httpapi

import "net/http"

func (s *Server) handleAgentCard(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"name":        "ATOS",
		"description": "Open gateway and commerce protocol for discovering, invoking, verifying and settling capabilities across the Agent Internet.",
		"url":         "https://a2a.atos.im",
		"version":     "0.2.0",
		"provider": map[string]any{
			"organization": "TOS Network",
			"url":          "https://tos.network",
		},
		"capabilities": map[string]any{
			"streaming":         false,
			"pushNotifications": false,
		},
		"defaultInputModes":  []string{"text", "application/json", "application/octet-stream-reference"},
		"defaultOutputModes": []string{"text", "application/json", "application/octet-stream-reference"},
		"skills": []map[string]any{
			{
				"id":          "capability-discovery",
				"name":        "Capability Discovery",
				"description": "Find and rank third-party capabilities with price, mode and proof constraints.",
				"tags":        []string{"discovery", "agents", "capabilities", "proof-of-service"},
			},
			{
				"id":          "capability-commerce",
				"name":        "Capability Commerce",
				"description": "Quote, invoke and settle work under Managed, Verified or Native guarantees.",
				"tags":        []string{"invoke", "jobs", "commerce", "settlement"},
			},
			{
				"id":          "artifact-transport",
				"name":        "Artifact Transport",
				"description": "Exchange large binary inputs and outputs through authorized signed URLs while keeping bulk bytes off-chain.",
				"tags":        []string{"files", "artifacts", "signed-url"},
			},
		},
		"extensions": map[string]any{
			"atos": map[string]any{
				"version": "0.2.0",
				"mcp": map[string]any{
					"url":               "https://mcp.atos.im/mcp",
					"legacySseUrl":      "https://mcp.atos.im/sse",
					"toolVisibility":    "authorization-derived",
					"consumerToolCount": 9,
					"cacheScope":        "private",
					"listTtlMs":         30000,
				},
				"api":                  "https://api.atos.im/v1",
				"deviceAuth":           "https://api.atos.im/v1/auth/device",
				"network":              "TOS",
				"clientWalletRequired": false,
				"trustModes":           []string{"managed", "verified", "native"},
				"requestedTrustModes":  []string{"managed", "verified", "native", "auto"},
				"proofProfiles":        []string{"tos_verified_v1", "tos_native_v1"},
				"modeAvailability": map[string]string{
					"managed":  "available",
					"verified": "unavailable",
					"native":   "unavailable",
				},
			},
		},
	})
}
