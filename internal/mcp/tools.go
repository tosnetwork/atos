package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// toolDefinitions mirrors ~/atos-spec/schemas/mcp-tools.json — kept as a Go
// literal here so the MCP server has no runtime dependency on the spec
// repo. If the two drift, schemas/mcp-tools.json is the source of truth.
var toolDefinitions = []map[string]any{
	{
		"name":        "atos_search",
		"description": "Search and rank ATOS capabilities by natural-language intent and constraints.",
		"inputSchema": map[string]any{
			"type": "object", "required": []string{"query"},
			"properties": map[string]any{
				"query": map[string]any{"type": "string", "minLength": 1},
				"filters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"max_price":       map[string]any{"type": "object", "properties": map[string]any{"amount": map[string]any{"type": "string"}, "currency": map[string]any{"type": "string"}}},
						"delivery_modes":  map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": []string{"instant", "async", "interactive"}}},
						"min_trust_score": map[string]any{"type": "number"},
						"max_latency_ms":  map[string]any{"type": "integer"},
					},
				},
				"limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 20, "default": 5},
			},
		},
	},
	{
		"name":        "atos_get_capability",
		"description": "Get full metadata and schemas for one capability.",
		"inputSchema": map[string]any{"type": "object", "required": []string{"capability_id"}, "properties": map[string]any{"capability_id": map[string]any{"type": "string"}}},
	},
	{
		"name":        "atos_quote",
		"description": "Create a time-limited executable commercial quote for a capability.",
		"inputSchema": map[string]any{"type": "object", "required": []string{"capability_id"}, "properties": map[string]any{
			"capability_id": map[string]any{"type": "string"},
			"constraints":   map[string]any{"type": "object"},
		}},
	},
	{
		"name":        "atos_invoke",
		"description": "Invoke a bounded capability using an executable quote.",
		"inputSchema": map[string]any{"type": "object", "required": []string{"capability_id", "quote_id", "input", "idempotency_key"}, "properties": map[string]any{
			"capability_id":   map[string]any{"type": "string"},
			"quote_id":        map[string]any{"type": "string"},
			"input":           map[string]any{},
			"idempotency_key": map[string]any{"type": "string"},
			"max_wait_ms":     map[string]any{"type": "integer", "minimum": 1000, "maximum": 120000},
			"confirmed":       map[string]any{"type": "boolean", "description": "Reissue with the same idempotency_key and confirmed=true after receiving result_type=input_required."},
		}},
	},
	{
		"name":        "atos_create_job",
		"description": "Create a long-running or interactive ATOS job.",
		"inputSchema": map[string]any{"type": "object", "required": []string{"capability_id", "quote_id", "input", "idempotency_key"}, "properties": map[string]any{
			"capability_id":   map[string]any{"type": "string"},
			"quote_id":        map[string]any{"type": "string"},
			"input":           map[string]any{},
			"idempotency_key": map[string]any{"type": "string"},
			"confirmed":       map[string]any{"type": "boolean", "description": "Reissue with the same idempotency_key and confirmed=true after receiving result_type=input_required."},
		}},
	},
	{
		"name":        "atos_get_job",
		"description": "Get the current state, messages and artifacts of an ATOS job.",
		"inputSchema": map[string]any{"type": "object", "required": []string{"job_id"}, "properties": map[string]any{"job_id": map[string]any{"type": "string"}}},
	},
	{
		"name":        "atos_cancel_job",
		"description": "Request cancellation of a non-terminal ATOS job.",
		"inputSchema": map[string]any{"type": "object", "required": []string{"job_id", "idempotency_key"}, "properties": map[string]any{
			"job_id":          map[string]any{"type": "string"},
			"reason":          map[string]any{"type": "string"},
			"idempotency_key": map[string]any{"type": "string"},
		}},
	},
	{
		"name":        "atos_register_capability",
		"description": "Register a provider capability on ATOS.",
		"inputSchema": map[string]any{"type": "object", "required": []string{"name", "description", "delivery_mode", "input_schema", "output_schema", "pricing"}, "properties": map[string]any{
			"name":          map[string]any{"type": "string"},
			"description":   map[string]any{"type": "string"},
			"delivery_mode": map[string]any{"enum": []string{"instant", "async", "interactive"}},
			"input_schema":  map[string]any{"type": "object"},
			"output_schema": map[string]any{"type": "object"},
			"pricing":       map[string]any{"type": "object"},
			"tags":          map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		}},
	},
	{
		"name":        "atos_update_capability",
		"description": "Update mutable metadata/configuration of a capability owned by the authenticated provider.",
		"inputSchema": map[string]any{"type": "object", "required": []string{"capability_id", "patch"}, "properties": map[string]any{
			"capability_id": map[string]any{"type": "string"},
			"patch":         map[string]any{"type": "object"},
		}},
	},
	{
		"name":        "atos_account",
		"description": "Get account balance, usage and autonomous spending policy.",
		"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
	},
	// Artifact transfer (docs/ARTIFACTS.md) is one tool, not three. From a
	// model's perspective create_upload/complete_upload/get_download_url
	// are one intent ("work with an ATOS artifact"), not three separate
	// ones — collapsing them to a single operation-dispatched tool keeps
	// the default surface at 11 instead of 13 while staying always-visible
	// (file I/O isn't an authorization concern the way admin tools are, so
	// there's no reliable per-caller signal to gate it on instead — see
	// adminToolDefinitions below for the case where gating IS correct).
	{
		"name":        "atos_artifact",
		"description": "Work with ATOS artifacts (binary content): create_upload requests a signed upload target, complete_upload finalizes one into a reusable artifact_id, get_download_url returns a signed download link. Use only when a capability's input/output schema references a file field — bytes never travel through this call itself, only through the signed URL it returns.",
		"inputSchema": map[string]any{
			"type":     "object",
			"required": []string{"operation"},
			"properties": map[string]any{
				"operation":    map[string]any{"type": "string", "enum": []string{"create_upload", "complete_upload", "get_download_url"}},
				"content_type": map[string]any{"type": "string", "description": "create_upload only"},
				"size_bytes":   map[string]any{"type": "integer", "minimum": 1, "description": "create_upload only"},
				"purpose":      map[string]any{"type": "string", "enum": []string{"job_input", "capability_asset"}, "description": "create_upload only"},
				"upload_id":    map[string]any{"type": "string", "description": "complete_upload only"},
				"artifact_id":  map[string]any{"type": "string", "description": "get_download_url only"},
			},
		},
	},
}

// adminToolDefinitions ARE an authorization concern — unlike file
// transfer, only a principal that actually owns at least one capability
// has any use for these, so they are computed into tools/list dynamically
// per request (see server.go's toolsForPrincipal) rather than statically
// merged into toolDefinitions. This is the real MCP mechanism for
// per-caller tool visibility: tools/list is a session-scoped response,
// not a fixed constant, and a server is expected to tailor it to the
// authenticated caller.
var adminToolDefinitions = []map[string]any{
	{
		"name":        "atos_list_my_capabilities",
		"description": "List capabilities owned by the authenticated provider.",
		"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
	},
	{
		"name":        "atos_pause_capability",
		"description": "Pause one of the authenticated provider's own capabilities so it stops matching new searches.",
		"inputSchema": map[string]any{"type": "object", "required": []string{"capability_id"}, "properties": map[string]any{"capability_id": map[string]any{"type": "string"}}},
	},
}

type toolHandler func(ctx context.Context, principalID string, args map[string]any) (any, error)

// dispatch is the full set of callable tools, including admin tools that
// server.go's toolsForPrincipal only advertises in tools/list to
// principals that own a capability. dispatch itself does not re-check
// that condition — the individual handlers (e.g. Pause) already enforce
// real ownership, so gating tools/list is a discoverability nicety, not
// the security boundary.
func (s *Server) dispatch() map[string]toolHandler {
	return map[string]toolHandler{
		"atos_search":               s.toolSearch,
		"atos_get_capability":       s.toolGetCapability,
		"atos_quote":                s.toolQuote,
		"atos_invoke":               s.toolInvoke,
		"atos_create_job":           s.toolCreateJob,
		"atos_get_job":              s.toolGetJob,
		"atos_cancel_job":           s.toolCancelJob,
		"atos_register_capability":  s.toolRegisterCapability,
		"atos_update_capability":    s.toolUpdateCapability,
		"atos_account":              s.toolAccount,
		"atos_list_my_capabilities": s.toolListMyCapabilities,
		"atos_pause_capability":     s.toolPauseCapability,
		"atos_artifact":             s.toolArtifact,
	}
}

func argString(args map[string]any, key string) string {
	v, _ := args[key].(string)
	return v
}

func argInt64(args map[string]any, key string) int64 {
	switch v := args[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	default:
		return 0
	}
}

func argObject(args map[string]any, key string) map[string]any {
	v, _ := args[key].(map[string]any)
	return v
}

func argBool(args map[string]any, key string) bool {
	v, _ := args[key].(bool)
	return v
}

func (s *Server) toolSearch(ctx context.Context, principalID string, args map[string]any) (any, error) {
	in := service.SearchInput{
		Query: argString(args, "query"),
		Limit: int(argInt64(args, "limit")),
	}
	if filters := argObject(args, "filters"); filters != nil {
		if raw, ok := filters["max_price"].(map[string]any); ok {
			amount, _ := raw["amount"].(string)
			currency, _ := raw["currency"].(string)
			if amount != "" {
				in.Filters.MaxPrice = &domain.Money{Amount: amount, Currency: currency}
			}
		}
		if raw, ok := filters["min_trust_score"].(float64); ok {
			in.Filters.MinTrustScore = &raw
		}
		if raw, ok := filters["max_latency_ms"].(float64); ok {
			v := int64(raw)
			in.Filters.MaxLatencyMS = &v
		}
		if raw, ok := filters["delivery_modes"].([]any); ok {
			for _, m := range raw {
				if str, ok := m.(string); ok {
					in.Filters.DeliveryModes = append(in.Filters.DeliveryModes, domain.DeliveryMode(str))
				}
			}
		}
	}

	caps, err := s.Capabilities.Search(ctx, in)
	if err != nil {
		return nil, err
	}
	return map[string]any{"matches": caps, "next_cursor": nil}, nil
}

func (s *Server) toolGetCapability(ctx context.Context, principalID string, args map[string]any) (any, error) {
	return s.Capabilities.Get(ctx, argString(args, "capability_id"))
}

func (s *Server) toolQuote(ctx context.Context, principalID string, args map[string]any) (any, error) {
	in := service.CreateQuoteInput{CapabilityID: argString(args, "capability_id")}
	if constraints := argObject(args, "constraints"); constraints != nil {
		if maxTotal, ok := constraints["max_total"].(map[string]any); ok {
			amount, _ := maxTotal["amount"].(string)
			currency, _ := maxTotal["currency"].(string)
			if amount != "" {
				in.MaxTotal = &domain.Money{Amount: amount, Currency: currency}
			}
		}
	}
	return s.Quotes.Create(ctx, in)
}

func (s *Server) toolInvoke(ctx context.Context, principalID string, args map[string]any) (any, error) {
	result, err := s.Jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID:    principalID,
		CapabilityID:   argString(args, "capability_id"),
		QuoteID:        argString(args, "quote_id"),
		Input:          argObject(args, "input"),
		IdempotencyKey: argString(args, "idempotency_key"),
		MaxWaitMS:      argInt64(args, "max_wait_ms"),
		Confirmed:      argBool(args, "confirmed"),
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"result_type": result.Type, "job": result.Job}, nil
}

func (s *Server) toolCreateJob(ctx context.Context, principalID string, args map[string]any) (any, error) {
	result, err := s.Jobs.CreateJob(ctx, service.SubmitInput{
		PrincipalID:    principalID,
		CapabilityID:   argString(args, "capability_id"),
		QuoteID:        argString(args, "quote_id"),
		Input:          argObject(args, "input"),
		IdempotencyKey: argString(args, "idempotency_key"),
		Confirmed:      argBool(args, "confirmed"),
	})
	if err != nil {
		return nil, err
	}
	return result.Job, nil
}

func (s *Server) toolGetJob(ctx context.Context, principalID string, args map[string]any) (any, error) {
	job, err := s.Jobs.Get(ctx, argString(args, "job_id"))
	if err != nil {
		return nil, err
	}
	if job.PrincipalID != principalID {
		return nil, domain.NewError(domain.ErrPermissionDenied, "not the job's owning principal", false)
	}
	return job, nil
}

func (s *Server) toolCancelJob(ctx context.Context, principalID string, args map[string]any) (any, error) {
	return s.Jobs.Cancel(ctx, argString(args, "job_id"), principalID, argString(args, "reason"), argString(args, "idempotency_key"))
}

func (s *Server) toolRegisterCapability(ctx context.Context, principalID string, args map[string]any) (any, error) {
	var pricing domain.Pricing
	if raw, ok := args["pricing"]; ok {
		b, _ := json.Marshal(raw)
		_ = json.Unmarshal(b, &pricing)
	}
	var tags []string
	if raw, ok := args["tags"].([]any); ok {
		for _, t := range raw {
			if str, ok := t.(string); ok {
				tags = append(tags, str)
			}
		}
	}
	return s.Capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID:   principalID,
		Name:         argString(args, "name"),
		Description:  argString(args, "description"),
		DeliveryMode: domain.DeliveryMode(argString(args, "delivery_mode")),
		InputSchema:  argObject(args, "input_schema"),
		OutputSchema: argObject(args, "output_schema"),
		Pricing:      pricing,
		Tags:         tags,
	})
}

func (s *Server) toolUpdateCapability(ctx context.Context, principalID string, args map[string]any) (any, error) {
	patch := argObject(args, "patch")
	if patch == nil {
		return nil, fmt.Errorf("patch is required")
	}
	return s.Capabilities.Update(ctx, argString(args, "capability_id"), principalID, patch)
}

func (s *Server) toolAccount(ctx context.Context, principalID string, args map[string]any) (any, error) {
	return s.Accounts.Get(ctx, principalID)
}

func (s *Server) toolListMyCapabilities(ctx context.Context, principalID string, args map[string]any) (any, error) {
	caps, err := s.Capabilities.ListByProvider(ctx, principalID)
	if err != nil {
		return nil, err
	}
	return map[string]any{"capabilities": caps}, nil
}

func (s *Server) toolPauseCapability(ctx context.Context, principalID string, args map[string]any) (any, error) {
	return s.Capabilities.Pause(ctx, argString(args, "capability_id"), principalID)
}

// toolArtifact is the single MCP-visible entry point for
// docs/ARTIFACTS.md's three-step signed-URL flow — see toolDefinitions'
// atos_artifact entry for why these were consolidated from three tools
// into one. Each operation validates its own required fields server-side
// rather than relying on the (necessarily loose, since fields are shared
// across operations) top-level JSON Schema to catch it.
func (s *Server) toolArtifact(ctx context.Context, principalID string, args map[string]any) (any, error) {
	switch op := argString(args, "operation"); op {
	case "create_upload":
		return s.artifactCreateUpload(ctx, principalID, args)
	case "complete_upload":
		uploadID := argString(args, "upload_id")
		if uploadID == "" {
			return nil, fmt.Errorf("upload_id is required for operation=complete_upload")
		}
		return s.Artifacts.CompleteUpload(ctx, principalID, uploadID)
	case "get_download_url":
		artifactID := argString(args, "artifact_id")
		if artifactID == "" {
			return nil, fmt.Errorf("artifact_id is required for operation=get_download_url")
		}
		return s.artifactGetDownloadURL(ctx, principalID, artifactID)
	default:
		return nil, fmt.Errorf("unknown operation %q; must be one of create_upload, complete_upload, get_download_url", op)
	}
}

func (s *Server) artifactCreateUpload(ctx context.Context, principalID string, args map[string]any) (any, error) {
	target, err := s.Artifacts.CreateUpload(ctx, service.CreateUploadInput{
		PrincipalID: principalID,
		ContentType: argString(args, "content_type"),
		SizeBytes:   argInt64(args, "size_bytes"),
		Purpose:     argString(args, "purpose"),
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"upload_id":     target.UploadID,
		"upload_url":    target.UploadURL,
		"upload_method": target.UploadMethod,
		"expires_at":    target.ExpiresAt,
	}, nil
}

func (s *Server) artifactGetDownloadURL(ctx context.Context, principalID, artifactID string) (any, error) {
	target, err := s.Artifacts.GetDownloadURL(ctx, principalID, artifactID)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"download_url": target.DownloadURL,
		"expires_at":   target.ExpiresAt,
		"content_type": target.ContentType,
		"size_bytes":   target.SizeBytes,
	}, nil
}
