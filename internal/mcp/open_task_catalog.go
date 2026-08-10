package mcp

// Phase 3C: Open Task Marketplace tool schemas (atos-spec
// docs/IMPLEMENTATION_ROADMAP.md §7.3). Every tool here dispatches to the
// exact same service.OpenTaskService methods the REST surface
// (internal/httpapi/open_tasks.go) uses -- REST and MCP never form two
// parallel semantics for OpenTask lifecycle rules.

func openTaskConstraintsSchema() map[string]any {
	return objectSchema(nil, map[string]any{"max_total": moneySchema()})
}

func publishOpenTaskTool() map[string]any {
	return map[string]any{
		"name":        "atos_publish_open_task",
		"description": "Publish a new open marketplace task -- a demand-side listing that providers can propose to fulfill. Not a Quote or Job; accepting a proposal creates those separately, using the existing Quote/Job pricing and trust-mode rules.",
		"inputSchema": objectSchema([]string{"title", "expires_at", "idempotency_key"}, map[string]any{
			"title":                map[string]any{"type": "string", "minLength": 1},
			"description":          map[string]any{"type": "string"},
			"input":                map[string]any{"type": "object"},
			"requested_trust_mode": requestedModeSchema(),
			"proof_requirements":   proofRequirementsSchema(),
			"constraints":          openTaskConstraintsSchema(),
			"expires_at":           map[string]any{"type": "string", "format": "date-time"},
			"idempotency_key":      map[string]any{"type": "string", "minLength": 1},
		}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func searchOpenTasksTool() map[string]any {
	return map[string]any{
		"name":        "atos_search_open_tasks",
		"description": "Browse currently open marketplace tasks. Only publicly safe fields are returned -- task input and other owner-only details are never included here.",
		"inputSchema": objectSchema(nil, map[string]any{
			"limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 100, "default": 50},
		}),
		"outputSchema": objectSchema([]string{"open_tasks"}, map[string]any{
			"open_tasks": map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		}),
	}
}

func getOpenTaskTool() map[string]any {
	return map[string]any{
		"name":         "atos_get_open_task",
		"description":  "Get an open task's current state. Full input is only visible to the task's own owner or, once accepted, the winning provider; every other caller receives the redacted public view.",
		"inputSchema":  objectSchema([]string{"task_id"}, map[string]any{"task_id": map[string]any{"type": "string", "minLength": 1}}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func applyToOpenTaskTool() map[string]any {
	return map[string]any{
		"name":        "atos_apply_to_open_task",
		"description": "Submit a proposal to fulfill an open task, as the calling provider. capability_version is never caller-supplied -- it is frozen from the capability's current version. Any proposed_price is a non-authoritative hint only; the real price is always computed by the existing Quote pricing rules at acceptance time.",
		"inputSchema": objectSchema([]string{"task_id", "capability_id", "idempotency_key"}, map[string]any{
			"task_id":         map[string]any{"type": "string", "minLength": 1},
			"capability_id":   map[string]any{"type": "string", "minLength": 1},
			"message":         map[string]any{"type": "string"},
			"proposed_price":  moneySchema(),
			"idempotency_key": map[string]any{"type": "string", "minLength": 1},
		}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func listOpenTaskProposalsTool() map[string]any {
	return map[string]any{
		"name":        "atos_list_open_task_proposals",
		"description": "List proposals for an open task. The task owner sees every proposal in full; a provider sees their own proposal in full and every other proposal redacted; anyone else sees only redacted proposals.",
		"inputSchema": objectSchema([]string{"task_id"}, map[string]any{"task_id": map[string]any{"type": "string", "minLength": 1}}),
		"outputSchema": objectSchema([]string{"proposals"}, map[string]any{
			"proposals": map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		}),
	}
}

func withdrawOpenTaskProposalTool() map[string]any {
	return map[string]any{
		"name":         "atos_withdraw_open_task_proposal",
		"description":  "Withdraw the calling provider's own proposal. Refused once that proposal has already been accepted.",
		"inputSchema":  objectSchema([]string{"proposal_id"}, map[string]any{"proposal_id": map[string]any{"type": "string", "minLength": 1}}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func acceptOpenTaskProposalTool() map[string]any {
	return map[string]any{
		"name":        "atos_accept_open_task_proposal",
		"description": "Accept a proposal as the task owner, durably claiming it as the winner and driving the existing Quote/Job creation pipeline forward using the task owner as principal and the winning proposal's capability/version. Idempotent: retrying with the same idempotency_key resumes the same acceptance instead of creating a second Quote/Job.",
		"inputSchema": objectSchema([]string{"task_id", "proposal_id", "idempotency_key"}, map[string]any{
			"task_id":         map[string]any{"type": "string", "minLength": 1},
			"proposal_id":     map[string]any{"type": "string", "minLength": 1},
			"idempotency_key": map[string]any{"type": "string", "minLength": 1},
		}),
		"outputSchema": objectSchema([]string{"open_task", "acceptance"}, map[string]any{
			"open_task": map[string]any{"type": "object"}, "acceptance": map[string]any{"type": "object"},
		}),
	}
}

func cancelOpenTaskTool() map[string]any {
	return map[string]any{
		"name":         "atos_cancel_open_task",
		"description":  "Cancel an open task as its owner. Refused once a proposal has been accepted -- an accepted winner is never silently discarded by a cancel.",
		"inputSchema":  objectSchema([]string{"task_id"}, map[string]any{"task_id": map[string]any{"type": "string", "minLength": 1}}),
		"outputSchema": map[string]any{"type": "object"},
	}
}
