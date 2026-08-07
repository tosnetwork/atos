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

// handleNetworkStatus implements GET /network/status. Phase 0/1 honesty:
// there is no real TOS Network connection behind this gateway yet (see
// internal/adapters/toscore/mock), so this reports that plainly instead of
// fabricating chain metrics nobody can back up.
func (s *Server) handleNetworkStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"network": "TOS",
		"status":  "not_connected",
		"note":    "Phase 0/1: tos-core is an in-process mock (internal/adapters/toscore/mock). No live TOS Network connection exists yet.",
	})
}

// handleProviderAgentCard implements GET /providers/{id}/agent-card: a
// per-provider discovery card built from that provider's own active
// capabilities, per docs/AGENT_CARD.md "Individual Provider Cards".
func (s *Server) handleProviderAgentCard(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("id")
	caps, err := s.Capabilities.ListByProvider(r.Context(), providerID)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	skills := make([]map[string]any, 0, len(caps))
	for _, c := range caps {
		skills = append(skills, map[string]any{
			"id":          c.ID,
			"name":        c.Name,
			"description": c.Description,
			"tags":        c.Tags,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name":               providerID,
		"description":        "ATOS provider capability card.",
		"skills":             skills,
		"defaultInputModes":  []string{"text", "application/json"},
		"defaultOutputModes": []string{"text", "application/json"},
		"extensions": map[string]any{
			"atos": map[string]any{"trustLevel": "self_asserted"},
		},
	})
}
