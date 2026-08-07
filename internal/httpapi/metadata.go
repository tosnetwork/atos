package httpapi

import "net/http"

func (s *Server) handleTaxonomy(w http.ResponseWriter, r *http.Request) {
	tags, err := s.Capabilities.Taxonomy(r.Context())
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	if tags == nil {
		tags = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"tags": tags})
}

func (s *Server) handleNetworkStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"network": "TOS",
		"status":  "not_connected",
		"modes": map[string]any{
			"managed":  map[string]any{"status": "available", "proof_profile": nil},
			"verified": map[string]any{"status": "unavailable", "proof_profile": "tos_verified_v1"},
			"native":   map[string]any{"status": "unavailable", "proof_profile": "tos_native_v1"},
		},
		"note": "Phase 0/1 uses the fail-closed in-process tos-core mock. Explicit Verified or Native requests return trust_mode_unavailable/network_unavailable and are never silently treated as Managed.",
	})
}

func (s *Server) handleProviderAgentCard(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("id")
	caps, err := s.Capabilities.ListByProvider(r.Context(), providerID)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	skills := make([]map[string]any, 0, len(caps))
	identityAssurance := "self_asserted"
	for _, capability := range caps {
		if string(capability.Trust.Level) > identityAssurance {
			identityAssurance = string(capability.Trust.Level)
		}
		skills = append(skills, map[string]any{
			"id":          capability.ID,
			"name":        capability.Name,
			"description": capability.Description,
			"tags":        capability.Tags,
			"inputModes":  capability.Modalities,
			"extensions": map[string]any{
				"atos": map[string]any{
					"canonicalUri":             capability.CanonicalURI,
					"version":                  capability.Version,
					"manifestCommitment":       capability.ManifestCommitment,
					"requestedTrustModes":      capability.RequestedTrustModes,
					"supportedTrustModes":      capability.SupportedTrustModes,
					"modeSupport":              capability.ModeSupport,
					"bindings":                 capability.Bindings,
					"requiresArtifactTransfer": capability.RequiresArtifactTransfer,
					"artifactInputFields":      capability.ArtifactInputFields,
					"artifactOutputFields":     capability.ArtifactOutputFields,
				},
			},
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name":               providerID,
		"description":        "ATOS provider capability card.",
		"version":            "0.2.0",
		"skills":             skills,
		"defaultInputModes":  []string{"text", "application/json", "application/octet-stream-reference"},
		"defaultOutputModes": []string{"text", "application/json", "application/octet-stream-reference"},
		"extensions": map[string]any{
			"atos": map[string]any{
				"version":           "0.2.0",
				"identityAssurance": identityAssurance,
				"network":           "TOS",
			},
		},
	})
}
