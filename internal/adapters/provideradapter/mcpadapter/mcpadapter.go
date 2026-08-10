// Package mcpadapter implements provideradapter.ProviderAdapter over
// outbound calls to a third-party provider's MCP server, following the
// same stateless Streamable HTTP JSON-RPC transport internal/mcp.Server
// uses for ATOS's own inbound MCP surface (POST a JSON-RPC 2.0 request,
// read one JSON-RPC 2.0 response).
//
// EndpointRef convention (ATOS-defined, matching the binding's single
// endpoint_ref string field): "<mcp-server-url>#<tool-name>" -- the
// fragment names the specific provider-side MCP tool this Capability binds
// to, since a CapabilityBinding must map to one explicit provider
// operation, never an ambiguous "whatever tools/list happens to return".
//
// Invocation calls tools/call with arguments {job_id, capability_id,
// capability_version, idempotency_key, input, deadline}, exactly mirroring
// httpadapter's wire body shape (minus the transport envelope). A
// successful (isError=false) result's structuredContent is decoded the
// same way httpadapter decodes its response body. MCP itself has no
// protocol-level "look up a prior call's result by id" operation, so Query
// always reports found=false -- this is honest per
// provideradapter.ProviderAdapter's own doc comment, not a shortcut.
package mcpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/domain"
)

const (
	defaultTimeout       = 30 * time.Second
	defaultHealthTimeout = 5 * time.Second
	jsonRPCVersion       = "2.0"
	codeMethodNotFound   = -32601
)

// Config configures an Adapter. Client, if set, overrides the underlying
// *http.Client -- tests point it at an in-process MCP test server.
type Config struct {
	Timeout time.Duration
	Client  *http.Client
}

type Adapter struct {
	client  *http.Client
	timeout time.Duration
}

func New(cfg Config) *Adapter {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	return &Adapter{client: client, timeout: timeout}
}

func (a *Adapter) Transport() domain.EndpointAdapterType { return domain.AdapterMCP }

// splitEndpoint parses the "<url>#<tool-name>" EndpointRef convention.
func splitEndpoint(endpointRef string) (serverURL, tool string, err error) {
	idx := strings.LastIndex(endpointRef, "#")
	if idx < 0 || idx == len(endpointRef)-1 {
		return "", "", fmt.Errorf("mcpadapter: endpoint_ref must be \"<mcp-server-url>#<tool-name>\", got %q", endpointRef)
	}
	serverURL = endpointRef[:idx]
	tool = endpointRef[idx+1:]
	if serverURL == "" || tool == "" {
		return "", "", fmt.Errorf("mcpadapter: endpoint_ref must be \"<mcp-server-url>#<tool-name>\", got %q", endpointRef)
	}
	return serverURL, tool, nil
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type toolCallParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type toolCallResult struct {
	IsError           bool           `json:"isError"`
	StructuredContent map[string]any `json:"structuredContent"`
}

type structuredOutcome struct {
	Status        string         `json:"status"`
	Output        map[string]any `json:"output"`
	Usage         domain.Usage   `json:"usage"`
	FailureReason string         `json:"failure_reason"`
}

func (a *Adapter) call(ctx context.Context, serverURL string, method string, params any) (rpcResponse, error) {
	body, err := json.Marshal(rpcRequest{JSONRPC: jsonRPCVersion, ID: 1, Method: method, Params: params})
	if err != nil {
		return rpcResponse{}, fmt.Errorf("mcpadapter: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL, bytes.NewReader(body))
	if err != nil {
		return rpcResponse{}, fmt.Errorf("mcpadapter: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return rpcResponse{}, fmt.Errorf("mcpadapter: request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return rpcResponse{}, fmt.Errorf("mcpadapter: non-2xx response: %d", resp.StatusCode)
	}
	var rpcResp rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return rpcResponse{}, fmt.Errorf("mcpadapter: malformed JSON-RPC response: %w", err)
	}
	if rpcResp.JSONRPC != jsonRPCVersion {
		return rpcResponse{}, fmt.Errorf("mcpadapter: unexpected jsonrpc version %q", rpcResp.JSONRPC)
	}
	return rpcResp, nil
}

func (a *Adapter) Invoke(ctx context.Context, req provideradapter.InvokeRequest) (provideradapter.InvokeResult, error) {
	if req.IdempotencyKey == "" {
		return provideradapter.InvokeResult{}, errors.New("mcpadapter: idempotency_key is required")
	}
	serverURL, tool, err := splitEndpoint(req.EndpointRef)
	if err != nil {
		return provideradapter.InvokeResult{}, err
	}

	callCtx := ctx
	var cancel context.CancelFunc
	if !req.Deadline.IsZero() {
		callCtx, cancel = context.WithDeadline(ctx, req.Deadline)
		defer cancel()
	}

	args := map[string]any{
		"job_id": req.JobID, "capability_id": req.CapabilityID, "capability_version": req.CapabilityVersion,
		"idempotency_key": req.IdempotencyKey, "input": req.Input,
	}
	if !req.Deadline.IsZero() {
		args["deadline"] = req.Deadline
	}
	rpcResp, err := a.call(callCtx, serverURL, "tools/call", toolCallParams{Name: tool, Arguments: args})
	if err != nil {
		return provideradapter.InvokeResult{}, err
	}
	if rpcResp.Error != nil {
		if rpcResp.Error.Code == codeMethodNotFound {
			return provideradapter.InvokeResult{}, fmt.Errorf("mcpadapter: tool %q not found on provider MCP server: %s", tool, rpcResp.Error.Message)
		}
		return provideradapter.InvokeResult{}, fmt.Errorf("mcpadapter: JSON-RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	return decodeToolCallResult(rpcResp.Result)
}

func decodeToolCallResult(raw json.RawMessage) (provideradapter.InvokeResult, error) {
	var result toolCallResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return provideradapter.InvokeResult{}, fmt.Errorf("mcpadapter: malformed tools/call result: %w", err)
	}
	if result.IsError {
		reason := ""
		if v, ok := result.StructuredContent["error"].(map[string]any); ok {
			if msg, ok := v["message"].(string); ok {
				reason = msg
			}
		}
		return provideradapter.InvokeResult{Status: provideradapter.InvokeFailed, FailureReason: reason}, nil
	}
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		return provideradapter.InvokeResult{}, fmt.Errorf("mcpadapter: re-encode structuredContent: %w", err)
	}
	var outcome structuredOutcome
	if err := json.Unmarshal(encoded, &outcome); err != nil {
		return provideradapter.InvokeResult{}, fmt.Errorf("mcpadapter: malformed structuredContent: %w", err)
	}
	status, err := decodeStatus(outcome.Status)
	if err != nil {
		return provideradapter.InvokeResult{}, err
	}
	return provideradapter.InvokeResult{Status: status, Output: outcome.Output, Usage: outcome.Usage, FailureReason: outcome.FailureReason}, nil
}

func decodeStatus(raw string) (provideradapter.InvokeStatus, error) {
	switch provideradapter.InvokeStatus(raw) {
	case provideradapter.InvokeCompleted, provideradapter.InvokeFailed, provideradapter.InvokePending:
		return provideradapter.InvokeStatus(raw), nil
	default:
		return "", fmt.Errorf("mcpadapter: unknown status %q in provider structuredContent", raw)
	}
}

// Query always reports found=false: the MCP protocol has no built-in
// operation to look up a prior tools/call's outcome by an arbitrary
// caller-chosen identity, so there is nothing honest to return here beyond
// "no record" -- see provideradapter.ProviderAdapter.Query's doc comment,
// which explicitly allows this for stateless request/response-only
// protocols.
func (a *Adapter) Query(ctx context.Context, endpointRef, idempotencyKey string) (provideradapter.InvokeResult, bool, error) {
	return provideradapter.InvokeResult{}, false, nil
}

func (a *Adapter) Cancel(ctx context.Context, endpointRef, idempotencyKey, reason string) error {
	return provideradapter.ErrCancelUnsupported
}

// Health calls tools/list on the MCP server named in endpointRef (a bare
// "tools/list" is safe even for a server that requires no auth) and
// reports pure reachability -- a healthy response only proves the
// transport is usable, never that a specific tool exists or that Verified/
// Native should be considered active; see domain.AdapterHealthCheck's doc
// comment.
func (a *Adapter) Health(ctx context.Context, endpointRef string) domain.AdapterHealthCheck {
	now := time.Now().UTC()
	check := domain.AdapterHealthCheck{Transport: domain.AdapterMCP, EndpointRef: endpointRef, CheckedAt: now}
	serverURL, _, err := splitEndpoint(endpointRef)
	if err != nil {
		check.Status = domain.AdapterHealthUnhealthy
		check.FailureReason = err.Error()
		return check
	}
	healthCtx, cancel := context.WithTimeout(ctx, defaultHealthTimeout)
	defer cancel()
	start := time.Now()
	rpcResp, err := a.call(healthCtx, serverURL, "tools/list", map[string]any{})
	check.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		check.Status = domain.AdapterHealthUnhealthy
		check.FailureReason = err.Error()
		return check
	}
	if rpcResp.Error != nil {
		check.Status = domain.AdapterHealthUnhealthy
		check.FailureReason = rpcResp.Error.Message
		return check
	}
	check.Status = domain.AdapterHealthHealthy
	return check
}

type toolListResult struct {
	Tools []toolDescriptor `json:"tools"`
}

type toolDescriptor struct {
	Name        string         `json:"name"`
	InputSchema map[string]any `json:"inputSchema,omitempty"`
}

// ProbeCertification implements provideradapter.CertificationProbe: unlike
// Health (which only proves tools/list itself is reachable), this proves
// the specific tool this binding names actually exists on the provider's
// server, and -- since tools/list is the one MCP-native place a provider
// can declare a real JSON Schema for its tool's arguments -- cross-checks
// that declared schema against the Capability's own registered
// input_schema. This is genuinely more than reachability, entirely
// side-effect-free (tools/list never invokes anything), and uses no wire
// operation beyond what Health already calls.
func (a *Adapter) ProbeCertification(ctx context.Context, endpointRef string, inputSchema, _ map[string]any) (map[string]any, error) {
	serverURL, tool, err := splitEndpoint(endpointRef)
	if err != nil {
		return nil, err
	}
	probeCtx, cancel := context.WithTimeout(ctx, defaultHealthTimeout)
	defer cancel()
	rpcResp, err := a.call(probeCtx, serverURL, "tools/list", map[string]any{})
	if err != nil {
		return nil, fmt.Errorf("mcpadapter: certification tools/list failed: %w", err)
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("mcpadapter: certification tools/list error: %s", rpcResp.Error.Message)
	}
	var listed toolListResult
	if err := json.Unmarshal(rpcResp.Result, &listed); err != nil {
		return nil, fmt.Errorf("mcpadapter: malformed tools/list result: %w", err)
	}

	var declared map[string]any
	found := false
	for _, t := range listed.Tools {
		if t.Name == tool {
			found, declared = true, t.InputSchema
			break
		}
	}
	if !found {
		return map[string]any{"tool_found": false}, fmt.Errorf("mcpadapter: tool %q not found in provider's tools/list", tool)
	}
	evidence := map[string]any{"tool_found": true, "provider_input_schema_declared": len(declared) > 0}
	if len(declared) == 0 {
		return evidence, nil
	}
	compatible, reason := inputSchemasStructurallyCompatible(inputSchema, declared)
	evidence["provider_input_schema_compatible"] = compatible
	if !compatible {
		return evidence, fmt.Errorf("mcpadapter: provider tool %q declares an input schema incompatible with the registered capability input_schema: %s", tool, reason)
	}
	return evidence, nil
}

// inputSchemasStructurallyCompatible is a bounded structural compatibility
// heuristic, NOT a full JSON Schema subsumption/equivalence check --
// deciding true schema-language equivalence in general is not something
// certification needs to solve. It catches the two most operationally
// dangerous kinds of drift between what ATOS's Capability registration
// promises callers and what the provider's own MCP tool actually declares:
//
//  1. a top-level "type" mismatch (e.g. ATOS promises "object", the
//     provider's tool now declares "string");
//  2. the provider requiring an argument ATOS's own schema does not know
//     about (ATOS would never tell a caller to send it, so every call
//     would be rejected by the provider).
//
// A capability schema with no "properties" restriction is treated as
// permissive (compatible with anything the provider requires), matching
// how an absent "properties"/"required" keyword behaves in JSON Schema
// itself.
func inputSchemasStructurallyCompatible(capabilitySchema, providerSchema map[string]any) (bool, string) {
	if capType, ok := capabilitySchema["type"].(string); ok {
		if provType, ok := providerSchema["type"].(string); ok && provType != capType {
			return false, fmt.Sprintf("capability input_schema type %q does not match provider tool schema type %q", capType, provType)
		}
	}
	capProps, hasCapProps := capabilitySchema["properties"].(map[string]any)
	provRequired, _ := providerSchema["required"].([]any)
	for _, r := range provRequired {
		name, ok := r.(string)
		if !ok {
			continue
		}
		if !hasCapProps {
			// Capability schema places no restriction on properties at
			// all -- permissive, nothing to conflict with.
			continue
		}
		if _, declared := capProps[name]; !declared {
			return false, fmt.Sprintf("provider tool requires argument %q that the registered capability input_schema does not declare", name)
		}
	}
	return true, ""
}
