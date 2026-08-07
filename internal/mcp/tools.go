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
				"query":   map[string]any{"type": "string", "minLength": 1},
				"filters": map[string]any{"type": "object"},
				"limit":   map[string]any{"type": "integer", "minimum": 1, "maximum": 20, "default": 5},
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
}

type toolHandler func(ctx context.Context, principalID string, args map[string]any) (any, error)

func (s *Server) dispatch() map[string]toolHandler {
	return map[string]toolHandler{
		"atos_search":              s.toolSearch,
		"atos_get_capability":      s.toolGetCapability,
		"atos_quote":               s.toolQuote,
		"atos_invoke":              s.toolInvoke,
		"atos_create_job":          s.toolCreateJob,
		"atos_get_job":             s.toolGetJob,
		"atos_cancel_job":          s.toolCancelJob,
		"atos_register_capability": s.toolRegisterCapability,
		"atos_update_capability":   s.toolUpdateCapability,
		"atos_account":             s.toolAccount,
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
	limit := int(argInt64(args, "limit"))
	caps, err := s.Capabilities.Search(ctx, argString(args, "query"), limit)
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
