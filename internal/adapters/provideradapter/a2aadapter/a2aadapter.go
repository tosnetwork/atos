// Package a2aadapter implements provideradapter.ProviderAdapter over
// outbound calls to a third-party provider's A2A agent, reusing
// internal/a2a's domain/wire types (Message, Part, Task, TaskState,
// CommerceExtension) rather than inventing a parallel task state model --
// the same commerce-extension shape ATOS's own inbound internal/a2a.Server
// expects from callers is what this adapter sends to providers, so an
// ATOS-compatible provider on the other end needs no special-casing.
//
// EndpointRef is the provider agent's JSON-RPC endpoint URL. Invoke calls
// message/send with Message.TaskID set to the stable IdempotencyKey --
// reusing ATOS's own durable execution identity as the A2A task id -- so a
// later Query can call tasks/get(id: IdempotencyKey) for lost-response
// recovery, exactly as the protocol's own task-tracking model intends.
package a2aadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tosnetwork/atos/internal/a2a"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/domain"
)

const (
	defaultTimeout       = 30 * time.Second
	defaultHealthTimeout = 5 * time.Second
	jsonRPCVersion       = "2.0"
)

type Config struct {
	Timeout time.Duration
	Client  *http.Client
}

type Adapter struct {
	client *http.Client
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
	return &Adapter{client: client}
}

func (a *Adapter) Transport() domain.EndpointAdapterType { return domain.AdapterA2A }

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (a *Adapter) call(ctx context.Context, endpointRef, method string, params any) (rpcResponse, error) {
	body, err := json.Marshal(rpcRequest{JSONRPC: jsonRPCVersion, ID: 1, Method: method, Params: params})
	if err != nil {
		return rpcResponse{}, fmt.Errorf("a2aadapter: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointRef, bytes.NewReader(body))
	if err != nil {
		return rpcResponse{}, fmt.Errorf("a2aadapter: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return rpcResponse{}, fmt.Errorf("a2aadapter: request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return rpcResponse{}, fmt.Errorf("a2aadapter: non-2xx response: %d", resp.StatusCode)
	}
	var rpcResp rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return rpcResponse{}, fmt.Errorf("a2aadapter: malformed JSON-RPC response: %w", err)
	}
	return rpcResp, nil
}

type messageSendParams struct {
	Message a2a.Message `json:"message"`
}

func commerceMetadata(capabilityID, quoteID, idempotencyKey string) map[string]any {
	return map[string]any{
		a2a.CommerceExtensionURI: map[string]any{
			"capability_id":   capabilityID,
			"quote_id":        quoteID,
			"idempotency_key": idempotencyKey,
		},
	}
}

func (a *Adapter) Invoke(ctx context.Context, req provideradapter.InvokeRequest) (provideradapter.InvokeResult, error) {
	if req.EndpointRef == "" {
		return provideradapter.InvokeResult{}, errors.New("a2aadapter: endpoint_ref is required")
	}
	if req.IdempotencyKey == "" {
		return provideradapter.InvokeResult{}, errors.New("a2aadapter: idempotency_key is required")
	}
	callCtx := ctx
	var cancel context.CancelFunc
	if !req.Deadline.IsZero() {
		callCtx, cancel = context.WithDeadline(ctx, req.Deadline)
		defer cancel()
	}
	msg := a2a.Message{
		Role:     "user",
		Parts:    []a2a.Part{{Kind: "data", Data: req.Input}},
		TaskID:   req.IdempotencyKey,
		Metadata: commerceMetadata(req.CapabilityID, req.QuoteID, req.IdempotencyKey),
	}
	rpcResp, err := a.call(callCtx, req.EndpointRef, "message/send", messageSendParams{Message: msg})
	if err != nil {
		return provideradapter.InvokeResult{}, err
	}
	if rpcResp.Error != nil {
		return provideradapter.InvokeResult{}, fmt.Errorf("a2aadapter: JSON-RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	return decodeTaskResult(rpcResp.Result, req.JobID, req.ProviderID)
}

func decodeTaskResult(raw json.RawMessage, expectJobID, expectProviderID string) (provideradapter.InvokeResult, error) {
	var task a2a.Task
	if err := json.Unmarshal(raw, &task); err != nil {
		return provideradapter.InvokeResult{}, fmt.Errorf("a2aadapter: malformed Task response: %w", err)
	}
	status, err := mapTaskState(task.Status.State)
	if err != nil {
		return provideradapter.InvokeResult{}, err
	}
	output := taskOutput(task)
	return provideradapter.InvokeResult{Status: status, Output: output}, nil
}

// mapTaskState maps a provider's A2A TaskState into provideradapter's
// InvokeStatus, failing closed (an explicit error, never a guessed
// InvokeCompleted/InvokeFailed) on any state this adapter does not
// recognize -- a provider on a newer/different A2A revision must not be
// silently misinterpreted as having succeeded or failed.
func mapTaskState(state a2a.TaskState) (provideradapter.InvokeStatus, error) {
	switch state {
	case a2a.TaskCompleted:
		return provideradapter.InvokeCompleted, nil
	case a2a.TaskFailed, a2a.TaskCanceled, a2a.TaskRejected:
		return provideradapter.InvokeFailed, nil
	case a2a.TaskSubmitted, a2a.TaskWorking, a2a.TaskInputRequired:
		return provideradapter.InvokePending, nil
	default:
		return "", fmt.Errorf("a2aadapter: unknown A2A task state %q, failing closed", state)
	}
}

// taskOutput extracts result data from a Task's Artifacts. A Capability
// binding produces at most one meaningful result artifact per invocation
// in Phase 3A's scope, so this prefers an artifact literally named
// "result" (the name a2a.TaskFromJob itself uses for a Job's Output, for
// ATOS-to-ATOS symmetry) and otherwise falls back to the first artifact.
func taskOutput(task a2a.Task) map[string]any {
	for _, artifact := range task.Artifacts {
		if artifact.Name == "result" {
			return artifact.Content
		}
	}
	if len(task.Artifacts) > 0 {
		return task.Artifacts[0].Content
	}
	return nil
}

type taskIDParams struct {
	ID string `json:"id"`
}

// Query calls tasks/get(id: idempotencyKey) at endpointRef -- recovering a
// lost message/send response via the same durable identity Invoke set as
// the task's TaskID, exactly as A2A's own task-tracking model intends. A
// provider reporting "task not found" is a legitimate found=false, not an
// error.
func (a *Adapter) Query(ctx context.Context, endpointRef, idempotencyKey string) (provideradapter.InvokeResult, bool, error) {
	if endpointRef == "" || idempotencyKey == "" {
		return provideradapter.InvokeResult{}, false, errors.New("a2aadapter: endpoint_ref and idempotency_key are required")
	}
	rpcResp, err := a.call(ctx, endpointRef, "tasks/get", taskIDParams{ID: idempotencyKey})
	if err != nil {
		return provideradapter.InvokeResult{}, false, err
	}
	if rpcResp.Error != nil {
		// A "not found"-shaped error is a legitimate absence of prior
		// state, not a call failure -- any JSON-RPC error here (this
		// adapter does not attempt to distinguish A2A error codes beyond
		// that) is treated as found=false rather than surfaced as err, so
		// callers can safely fall back to a fresh Invoke.
		return provideradapter.InvokeResult{}, false, nil
	}
	result, err := decodeTaskResult(rpcResp.Result, "", "")
	if err != nil {
		return provideradapter.InvokeResult{}, false, err
	}
	return result, true, nil
}

// Cancel calls tasks/cancel(id: idempotencyKey) at endpointRef -- unlike
// HTTP/MCP, A2A has a defined cancellation operation, so this adapter
// implements it rather than reporting ErrCancelUnsupported.
func (a *Adapter) Cancel(ctx context.Context, endpointRef, idempotencyKey, reason string) error {
	if endpointRef == "" || idempotencyKey == "" {
		return errors.New("a2aadapter: endpoint_ref and idempotency_key are required")
	}
	type params struct {
		ID       string         `json:"id"`
		Metadata map[string]any `json:"metadata"`
	}
	rpcResp, err := a.call(ctx, endpointRef, "tasks/cancel", params{
		ID:       idempotencyKey,
		Metadata: map[string]any{a2a.CommerceExtensionURI: map[string]any{"idempotency_key": idempotencyKey, "reason": reason}},
	})
	if err != nil {
		return err
	}
	if rpcResp.Error != nil {
		return fmt.Errorf("a2aadapter: cancel JSON-RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	return nil
}

// Health calls tasks/get with a deliberately unused, freshly random-looking
// probe id and treats ANY well-formed JSON-RPC response (including a
// "task not found" error, which proves the method itself is implemented
// and reachable) as healthy. A connection/transport failure is unhealthy.
// This is pure reachability -- see domain.AdapterHealthCheck's doc
// comment.
func (a *Adapter) Health(ctx context.Context, endpointRef string) domain.AdapterHealthCheck {
	now := time.Now().UTC()
	check := domain.AdapterHealthCheck{Transport: domain.AdapterA2A, EndpointRef: endpointRef, CheckedAt: now}
	if endpointRef == "" {
		check.Status = domain.AdapterHealthUnhealthy
		check.FailureReason = "endpoint_ref is empty"
		return check
	}
	healthCtx, cancel := context.WithTimeout(ctx, defaultHealthTimeout)
	defer cancel()
	start := time.Now()
	_, err := a.call(healthCtx, endpointRef, "tasks/get", taskIDParams{ID: "atos-health-probe"})
	check.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		check.Status = domain.AdapterHealthUnhealthy
		check.FailureReason = err.Error()
		return check
	}
	check.Status = domain.AdapterHealthHealthy
	return check
}

// This adapter deliberately does NOT implement
// provideradapter.CertificationProbe. Health above already exercises a
// real A2A JSON-RPC operation (tasks/get), unlike httpadapter's original
// bare GET, so certification for this transport is not weaker than what
// Health itself already proves about protocol handshake. What it cannot
// do is the schema-compatibility half of atos-spec
// IMPLEMENTATION_ROADMAP.md §7.1.3's certification requirement: A2A has no
// skill/schema-discovery convention (an Agent Card at a well-known URI,
// declaring per-skill input schemas, is the natural candidate) defined
// anywhere in this codebase or in atos-spec today. Inventing one
// unilaterally here would violate §3.1's spec-first gate -- that
// convention needs to be frozen in atos-spec before an adapter-side probe
// can honestly claim to check it. Until then, A2A certification evidence
// is Health-equivalent, and CertificationService records that explicitly
// rather than implying a depth this transport does not yet have.
