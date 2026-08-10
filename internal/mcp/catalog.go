package mcp

import "github.com/tosnetwork/atos/internal/auth"

type toolSpec struct {
	Definition     map[string]any
	RequiredScopes []auth.Scope
}

const (
	toolsListTTLMS  = 30000
	toolsCacheScope = "private"
)

var orderedToolSpecs = []toolSpec{
	{Definition: searchTool(), RequiredScopes: []auth.Scope{auth.ScopeCapabilitiesRead}},
	{Definition: getCapabilityTool(), RequiredScopes: []auth.Scope{auth.ScopeCapabilitiesRead}},
	{Definition: quoteTool(), RequiredScopes: []auth.Scope{auth.ScopeQuotesRead}},
	{Definition: invokeTool(), RequiredScopes: []auth.Scope{auth.ScopeInvocationsCreate}},
	{Definition: createJobTool(), RequiredScopes: []auth.Scope{auth.ScopeJobsCreate}},
	{Definition: getJobTool(), RequiredScopes: []auth.Scope{auth.ScopeJobsRead}},
	{Definition: cancelJobTool(), RequiredScopes: []auth.Scope{auth.ScopeJobsCancel}},
	{Definition: accountTool(), RequiredScopes: []auth.Scope{auth.ScopeAccountRead}},
	{Definition: artifactTool()},
	{Definition: registerCapabilityTool(), RequiredScopes: []auth.Scope{auth.ScopeCapabilitiesWrite}},
	{Definition: updateCapabilityTool(), RequiredScopes: []auth.Scope{auth.ScopeCapabilitiesWrite}},
	{Definition: listMyCapabilitiesTool(), RequiredScopes: []auth.Scope{auth.ScopeCapabilitiesWrite}},
	{Definition: pauseCapabilityTool(), RequiredScopes: []auth.Scope{auth.ScopeCapabilitiesWrite}},
	{Definition: providerEarningsTool(), RequiredScopes: []auth.Scope{auth.ScopeEarningsRead}},
	{Definition: providerJobsTool(), RequiredScopes: []auth.Scope{auth.ScopeProviderJobsRead}},
	{Definition: deliverJobTool(), RequiredScopes: []auth.Scope{auth.ScopeProviderJobsDeliver}},
	{Definition: requestSettlementTool(), RequiredScopes: []auth.Scope{auth.ScopeSettlementWrite}},
	{Definition: disputeJobTool(), RequiredScopes: []auth.Scope{auth.ScopeDisputesReview}},
	{Definition: authorizeExecutionSignerTool(), RequiredScopes: []auth.Scope{auth.ScopeExecutionSignersWrite}},
	{Definition: rotateExecutionSignerTool(), RequiredScopes: []auth.Scope{auth.ScopeExecutionSignersWrite}},
	{Definition: revokeExecutionSignerTool(), RequiredScopes: []auth.Scope{auth.ScopeExecutionSignersWrite}},
	{Definition: getExecutionSignerStatusTool(), RequiredScopes: []auth.Scope{auth.ScopeExecutionSignersRead}},
	{Definition: publishOpenTaskTool(), RequiredScopes: []auth.Scope{auth.ScopeOpenTasksWrite}},
	{Definition: searchOpenTasksTool(), RequiredScopes: []auth.Scope{auth.ScopeOpenTasksRead}},
	{Definition: getOpenTaskTool(), RequiredScopes: []auth.Scope{auth.ScopeOpenTasksRead}},
	{Definition: applyToOpenTaskTool(), RequiredScopes: []auth.Scope{auth.ScopeOpenTaskProposalsWrite}},
	{Definition: listOpenTaskProposalsTool(), RequiredScopes: []auth.Scope{auth.ScopeOpenTasksRead}},
	{Definition: withdrawOpenTaskProposalTool(), RequiredScopes: []auth.Scope{auth.ScopeOpenTaskProposalsWrite}},
	{Definition: acceptOpenTaskProposalTool(), RequiredScopes: []auth.Scope{auth.ScopeOpenTasksWrite}},
	{Definition: cancelOpenTaskTool(), RequiredScopes: []auth.Scope{auth.ScopeOpenTasksWrite}},
}

func toolName(spec toolSpec) string {
	name, _ := spec.Definition["name"].(string)
	return name
}

func toolSpecByName(name string) (toolSpec, bool) {
	for _, spec := range orderedToolSpecs {
		if toolName(spec) == name {
			return spec, true
		}
	}
	return toolSpec{}, false
}

func objectSchema(required []string, properties map[string]any) map[string]any {
	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": properties,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func concreteModeSchema() map[string]any {
	return map[string]any{"type": "string", "enum": []string{"managed", "verified", "native"}}
}

func requestedModeSchema() map[string]any {
	return map[string]any{"type": "string", "enum": []string{"managed", "verified", "native", "auto"}, "default": "auto"}
}

func proofRequirementsSchema() map[string]any {
	return objectSchema(nil, map[string]any{
		"network_verifiable_receipt": map[string]any{"type": "boolean"},
		"tos_settlement":             map[string]any{"type": "boolean"},
		"portable_proof_of_service":  map[string]any{"type": "boolean"},
	})
}

func moneySchema() map[string]any {
	return objectSchema([]string{"amount", "currency"}, map[string]any{
		"amount":   map[string]any{"type": "string", "minLength": 1},
		"currency": map[string]any{"type": "string", "minLength": 3, "maxLength": 12},
	})
}

func searchTool() map[string]any {
	return map[string]any{
		"name":        "atos_search",
		"description": "Search and rank ATOS capabilities by intent, price, delivery, trust mode and proof requirements.",
		"inputSchema": objectSchema([]string{"query"}, map[string]any{
			"query": map[string]any{"type": "string", "minLength": 1},
			"filters": objectSchema(nil, map[string]any{
				"max_price":            moneySchema(),
				"delivery_modes":       map[string]any{"type": "array", "uniqueItems": true, "items": map[string]any{"type": "string", "enum": []string{"instant", "async", "interactive"}}},
				"requested_trust_mode": requestedModeSchema(),
				"proof_requirements":   proofRequirementsSchema(),
				"min_trust_score":      map[string]any{"type": "number", "minimum": 0, "maximum": 1},
				"max_latency_ms":       map[string]any{"type": "integer", "minimum": 0},
			}),
			"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": 20, "default": 5},
			"cursor": map[string]any{"type": []string{"string", "null"}},
		}),
		"outputSchema": objectSchema([]string{"matches"}, map[string]any{
			"matches":     map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
			"next_cursor": map[string]any{"type": []string{"string", "null"}},
		}),
	}
}

func getCapabilityTool() map[string]any {
	return map[string]any{
		"name":         "atos_get_capability",
		"description":  "Get capability schemas, pricing, active trust modes, proof profiles, bindings and artifact requirements.",
		"inputSchema":  objectSchema([]string{"capability_id"}, map[string]any{"capability_id": map[string]any{"type": "string", "minLength": 1}}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func quoteTool() map[string]any {
	return map[string]any{
		"name":        "atos_quote",
		"description": "Create a short-lived executable Quote and freeze one concrete trust mode and proof profile.",
		"inputSchema": objectSchema([]string{"capability_id"}, map[string]any{
			"capability_id":        map[string]any{"type": "string", "minLength": 1},
			"input_summary":        map[string]any{"type": "object"},
			"requested_trust_mode": requestedModeSchema(),
			"proof_requirements":   proofRequirementsSchema(),
			"constraints": objectSchema(nil, map[string]any{
				"max_total": moneySchema(),
				"deadline":  map[string]any{"type": "string", "format": "date-time"},
			}),
		}),
		"outputSchema": objectSchema(
			[]string{"quote_id", "capability_id", "capability_version", "provider_id", "requested_trust_mode", "trust_mode", "price", "settlement", "proof", "expires_at", "terms_hash"},
			map[string]any{
				"quote_id": map[string]any{"type": "string"}, "capability_id": map[string]any{"type": "string"},
				"capability_version": map[string]any{"type": "string"}, "provider_id": map[string]any{"type": "string"},
				"requested_trust_mode": requestedModeSchema(), "trust_mode": concreteModeSchema(),
				"proof_profile": map[string]any{"type": []string{"string", "null"}}, "price": map[string]any{"type": "object"},
				"settlement": map[string]any{"type": "object"}, "proof": map[string]any{"type": "object"},
				"expires_at": map[string]any{"type": "string"}, "requires_confirmation": map[string]any{"type": "boolean"},
				"terms_hash": map[string]any{"type": "string"}, "dispute_policy_hash": map[string]any{"type": []string{"string", "null"}},
			},
		),
	}
}

func invocationInputSchema() map[string]any {
	return objectSchema([]string{"capability_id", "quote_id", "input", "idempotency_key"}, map[string]any{
		"capability_id":   map[string]any{"type": "string", "minLength": 1},
		"quote_id":        map[string]any{"type": "string", "minLength": 1},
		"input":           map[string]any{},
		"idempotency_key": map[string]any{"type": "string", "minLength": 1},
		"max_wait_ms":     map[string]any{"type": "integer", "minimum": 1000, "maximum": 120000},
	})
}

func invokeTool() map[string]any {
	return map[string]any{
		"name": "atos_invoke", "description": "Invoke a bounded capability using an immutable Quote.",
		"inputSchema": invocationInputSchema(),
		"outputSchema": objectSchema([]string{"result_type", "quote_id", "trust_mode"}, map[string]any{
			"result_type":   map[string]any{"type": "string", "enum": []string{"completed", "accepted", "input_required", "failed"}},
			"invocation_id": map[string]any{"type": []string{"string", "null"}}, "job_id": map[string]any{"type": []string{"string", "null"}},
			"quote_id": map[string]any{"type": "string"}, "trust_mode": concreteModeSchema(),
			"proof_profile": map[string]any{"type": []string{"string", "null"}}, "output": map[string]any{},
			"artifacts": map[string]any{"type": "array"}, "receipt": map[string]any{"type": []string{"object", "null"}},
			"confirmation": map[string]any{"type": []string{"object", "null"}}, "confirmation_uri": map[string]any{"type": []string{"string", "null"}},
		}),
	}
}

func createJobTool() map[string]any {
	input := invocationInputSchema()
	delete(input["properties"].(map[string]any), "max_wait_ms")
	return map[string]any{
		"name": "atos_create_job", "description": "Create long-running or interactive work using an immutable Quote.",
		"inputSchema": input, "outputSchema": map[string]any{"type": "object"},
	}
}

func getJobTool() map[string]any {
	return map[string]any{
		"name": "atos_get_job", "description": "Get current Job state, trust mode, proof progress, artifacts and receipt.",
		"inputSchema":  objectSchema([]string{"job_id"}, map[string]any{"job_id": map[string]any{"type": "string", "minLength": 1}}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func cancelJobTool() map[string]any {
	return map[string]any{
		"name": "atos_cancel_job", "description": "Cancel a non-terminal Job without weakening its quoted settlement guarantees.",
		"inputSchema": objectSchema([]string{"job_id", "idempotency_key"}, map[string]any{
			"job_id": map[string]any{"type": "string", "minLength": 1}, "reason": map[string]any{"type": "string"},
			"idempotency_key": map[string]any{"type": "string", "minLength": 1},
		}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func accountTool() map[string]any {
	return map[string]any{
		"name": "atos_account", "description": "Get balance, spend policy and default trust policy.",
		"inputSchema": objectSchema(nil, map[string]any{}), "outputSchema": map[string]any{"type": "object"},
	}
}

func artifactTool() map[string]any {
	return map[string]any{
		"name":        "atos_artifact",
		"description": "Work with ATOS artifacts using signed URLs; binary bytes never pass through this tool call.",
		"inputSchema": map[string]any{"oneOf": []any{
			objectSchema([]string{"operation", "content_type", "size_bytes", "purpose"}, map[string]any{
				"operation": map[string]any{"const": "create_upload"}, "content_type": map[string]any{"type": "string", "minLength": 1},
				"size_bytes": map[string]any{"type": "integer", "minimum": 1}, "purpose": map[string]any{"type": "string", "enum": []string{"job_input", "capability_asset"}},
			}),
			objectSchema([]string{"operation", "upload_id"}, map[string]any{"operation": map[string]any{"const": "complete_upload"}, "upload_id": map[string]any{"type": "string", "minLength": 1}}),
			objectSchema([]string{"operation", "artifact_id"}, map[string]any{"operation": map[string]any{"const": "get_download_url"}, "artifact_id": map[string]any{"type": "string", "minLength": 1}}),
		}},
		"outputSchema": map[string]any{"oneOf": []any{
			objectSchema([]string{"operation", "upload_id", "upload_url", "upload_method", "expires_at"}, map[string]any{
				"operation": map[string]any{"const": "create_upload"}, "upload_id": map[string]any{"type": "string"}, "upload_url": map[string]any{"type": "string"},
				"upload_method": map[string]any{"const": "PUT"}, "expires_at": map[string]any{"type": "string"},
			}),
			objectSchema([]string{"operation", "artifact_id", "content_type", "size_bytes", "sha256"}, map[string]any{
				"operation": map[string]any{"const": "complete_upload"}, "artifact_id": map[string]any{"type": "string"}, "content_type": map[string]any{"type": "string"},
				"size_bytes": map[string]any{"type": "integer", "minimum": 0}, "sha256": map[string]any{"type": "string"},
			}),
			objectSchema([]string{"operation", "download_url", "expires_at", "content_type", "size_bytes"}, map[string]any{
				"operation": map[string]any{"const": "get_download_url"}, "download_url": map[string]any{"type": "string"}, "expires_at": map[string]any{"type": "string"},
				"content_type": map[string]any{"type": "string"}, "size_bytes": map[string]any{"type": "integer", "minimum": 0},
			}),
		}},
	}
}

func capabilityRegistrationProperties() map[string]any {
	return map[string]any{
		"name": map[string]any{"type": "string", "minLength": 1}, "description": map[string]any{"type": "string", "minLength": 1},
		"delivery_mode": map[string]any{"type": "string", "enum": []string{"instant", "async", "interactive"}},
		"input_schema":  map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"}, "pricing": map[string]any{"type": "object"},
		"tags":                  map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		"requested_trust_modes": map[string]any{"type": "array", "minItems": 1, "uniqueItems": true, "items": concreteModeSchema()},
		"bindings": map[string]any{"type": "array", "items": objectSchema([]string{"transport", "endpoint_ref", "eligible_trust_modes"}, map[string]any{
			"transport":            map[string]any{"type": "string", "enum": []string{"http", "mcp", "a2a", "human", "tos-native"}},
			"endpoint_ref":         map[string]any{"type": "string", "minLength": 1},
			"eligible_trust_modes": map[string]any{"type": "array", "minItems": 1, "uniqueItems": true, "items": concreteModeSchema()},
		})},
	}
}

func registerCapabilityTool() map[string]any {
	return map[string]any{
		"name": "atos_register_capability", "description": "Register a provider capability and request concrete trust modes.",
		"inputSchema":  objectSchema([]string{"name", "description", "delivery_mode", "input_schema", "output_schema", "pricing", "requested_trust_modes"}, capabilityRegistrationProperties()),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func updateCapabilityTool() map[string]any {
	return map[string]any{
		"name": "atos_update_capability", "description": "Update mutable capability metadata or requested trust modes; active support remains derived.",
		"inputSchema":  objectSchema([]string{"capability_id", "patch"}, map[string]any{"capability_id": map[string]any{"type": "string", "minLength": 1}, "patch": map[string]any{"type": "object"}}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func listMyCapabilitiesTool() map[string]any {
	return map[string]any{
		"name": "atos_list_my_capabilities", "description": "List capabilities owned by the authenticated provider.",
		"inputSchema": objectSchema(nil, map[string]any{}), "outputSchema": objectSchema([]string{"capabilities"}, map[string]any{"capabilities": map[string]any{"type": "array"}}),
	}
}

func pauseCapabilityTool() map[string]any {
	return map[string]any{
		"name": "atos_pause_capability", "description": "Pause one capability owned by the authenticated provider.",
		"inputSchema":  objectSchema([]string{"capability_id"}, map[string]any{"capability_id": map[string]any{"type": "string", "minLength": 1}}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func providerEarningsTool() map[string]any {
	return map[string]any{
		"name":        "atos_provider_earnings",
		"description": "List earnings owned by the authenticated provider, or get one by earning_id.",
		"inputSchema": objectSchema(nil, map[string]any{"earning_id": map[string]any{"type": "string", "minLength": 1}}),
		"outputSchema": map[string]any{"oneOf": []any{
			objectSchema([]string{"earnings"}, map[string]any{"earnings": map[string]any{"type": "array"}}),
			map[string]any{"type": "object"},
		}},
	}
}

func providerJobsTool() map[string]any {
	return map[string]any{
		"name":        "atos_provider_jobs",
		"description": "List Jobs owned by the authenticated provider, or get one by job_id.",
		"inputSchema": objectSchema(nil, map[string]any{"job_id": map[string]any{"type": "string", "minLength": 1}}),
		"outputSchema": map[string]any{"oneOf": []any{
			objectSchema([]string{"jobs"}, map[string]any{"jobs": map[string]any{"type": "array"}}),
			map[string]any{"type": "object"},
		}},
	}
}

func deliverJobTool() map[string]any {
	return map[string]any{
		"name":        "atos_deliver_job",
		"description": "Deliver a completed result for a Job owned by the authenticated provider. The Job's Quote remains authoritative for trust_mode/pricing; only output is accepted here.",
		"inputSchema": objectSchema([]string{"job_id", "output"}, map[string]any{
			"job_id": map[string]any{"type": "string", "minLength": 1},
			"output": map[string]any{"type": "object"},
		}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func requestSettlementTool() map[string]any {
	return map[string]any{
		"name":        "atos_request_settlement",
		"description": "Request settlement reconciliation for a Job owned by the authenticated provider. Never accepts a caller-supplied settlement amount -- the frozen Quote, verified Receipt and durable Job/settlement state remain the sole source of economic truth.",
		"inputSchema": objectSchema([]string{"job_id"}, map[string]any{
			"job_id": map[string]any{"type": "string", "minLength": 1},
		}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func disputeJobTool() map[string]any {
	return map[string]any{
		"name":        "atos_dispute_job",
		"description": "Provider/admin dispute operations (review, resolve) over the existing Phase 2C dispute workflow. Strictly operation-discriminated -- exactly one of review/resolve fields is honored per call, per the value of \"operation\".",
		"inputSchema": objectSchema([]string{"operation", "dispute_id"}, map[string]any{
			"operation":       map[string]any{"type": "string", "enum": []string{"review", "resolve"}},
			"dispute_id":      map[string]any{"type": "string", "minLength": 1},
			"outcome":         map[string]any{"type": "string", "enum": []string{"principal", "provider", "rejected"}, "description": "Required when operation=resolve."},
			"reason_rejected": map[string]any{"type": "string", "description": "Optional detail when outcome=rejected."},
		}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func authorizeExecutionSignerTool() map[string]any {
	return map[string]any{
		"name":        "atos_authorize_execution_signer",
		"description": "Authorize the (first) execution signer for a Capability owned by the authenticated provider. Accepts only a signer public key and signer ID -- never a private key.",
		"inputSchema": objectSchema([]string{"capability_id", "execution_signer_id", "signer_public_key", "signature_algorithm", "idempotency_key"}, map[string]any{
			"capability_id":       map[string]any{"type": "string", "minLength": 1},
			"capability_version":  map[string]any{"type": "string", "description": "Optional: if set, must match the capability's current version."},
			"execution_signer_id": map[string]any{"type": "string", "minLength": 1},
			"signer_public_key":   map[string]any{"type": "string", "description": "Base64-encoded, optionally \"base64:\"-prefixed public key."},
			"signature_algorithm": map[string]any{"type": "string", "enum": []string{"ed25519"}},
			"valid_from":          map[string]any{"type": "string", "description": "RFC3339. Defaults to now."},
			"valid_until":         map[string]any{"type": "string", "description": "RFC3339. Defaults to one year after valid_from."},
			"idempotency_key":     map[string]any{"type": "string", "minLength": 1},
		}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func rotateExecutionSignerTool() map[string]any {
	return map[string]any{
		"name":        "atos_rotate_execution_signer",
		"description": "Durably orchestrated rotation (authorize-then-revoke, never the reverse) of a Capability's execution signer, owned by the authenticated provider. Never implemented as two independent tool calls -- see docs/IMPLEMENTATION_ROADMAP.md §7.2.2's checkpoint sequence.",
		"inputSchema": objectSchema([]string{"capability_id", "execution_signer_id", "signer_public_key", "signature_algorithm", "idempotency_key"}, map[string]any{
			"capability_id":       map[string]any{"type": "string", "minLength": 1},
			"capability_version":  map[string]any{"type": "string", "description": "Optional: if set, must match the capability's current version."},
			"execution_signer_id": map[string]any{"type": "string", "minLength": 1, "description": "The NEW signer's ID."},
			"signer_public_key":   map[string]any{"type": "string", "description": "The NEW signer's base64-encoded, optionally \"base64:\"-prefixed public key."},
			"signature_algorithm": map[string]any{"type": "string", "enum": []string{"ed25519"}},
			"valid_from":          map[string]any{"type": "string", "description": "RFC3339. Defaults to now."},
			"valid_until":         map[string]any{"type": "string", "description": "RFC3339. Defaults to one year after valid_from."},
			"reason_code":         map[string]any{"type": "string", "description": "Optional reason recorded for the old signer's revocation."},
			"idempotency_key":     map[string]any{"type": "string", "minLength": 1},
		}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func revokeExecutionSignerTool() map[string]any {
	return map[string]any{
		"name":        "atos_revoke_execution_signer",
		"description": "Revoke a Capability's currently authorized execution signer, owned by the authenticated provider. Refused while doing so would leave an active stronger trust mode with no authorized signer -- use rotate instead.",
		"inputSchema": objectSchema([]string{"capability_id", "idempotency_key"}, map[string]any{
			"capability_id":      map[string]any{"type": "string", "minLength": 1},
			"capability_version": map[string]any{"type": "string", "description": "Optional: if set, must match the capability's current version."},
			"reason_code":        map[string]any{"type": "string"},
			"idempotency_key":    map[string]any{"type": "string", "minLength": 1},
		}),
		"outputSchema": map[string]any{"type": "object"},
	}
}

func getExecutionSignerStatusTool() map[string]any {
	return map[string]any{
		"name":        "atos_get_execution_signer_status",
		"description": "Read-only: the current execution signer and any in-progress authorize/rotate/revoke operation's durable checkpoint for a Capability owned by the authenticated provider. current_execution_signer_id remains the old signer until a rotation's checkpoint reaches new_authorized.",
		"inputSchema": objectSchema([]string{"capability_id"}, map[string]any{
			"capability_id": map[string]any{"type": "string", "minLength": 1},
		}),
		"outputSchema": map[string]any{"type": "object"},
	}
}
