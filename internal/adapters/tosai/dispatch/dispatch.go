// Package dispatch implements tosai.Provider by routing a Job's execution
// to the transport its Capability actually binds to: tos-native (and
// human) bindings delegate unchanged to a wrapped native tosai.Provider
// (the mock or tos-protocol RPC client already used for every Phase 0-2C
// Job), while http/mcp/a2a bindings route to the matching
// provideradapter.ProviderAdapter.
//
// This is deliberately the ONLY place JobService's execution boundary
// fans out by transport -- JobService itself still talks to exactly one
// tosai.Provider, exactly as before. There is no second Job execution
// architecture; third-party dispatch lives entirely behind the existing
// abstraction.
package dispatch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/domain"
)

// Provider implements tosai.Provider.
type Provider struct {
	native   tosai.Provider
	resolver *provideradapter.Resolver

	mu sync.Mutex
	// routes is an in-memory fast-path cache from JobID to the binding a
	// prior SubmitJob call resolved, so a same-process GetJob/CancelJob/
	// FetchResult/FetchReceipt call doesn't need the caller to re-supply
	// Binding. Losing this cache (process restart) is always safe: the
	// next recoverProviderExecution call re-reads the Job's own durably
	// frozen Binding (never the Capability's current one) and calls
	// SubmitJob again, which itself always Querys the third-party endpoint
	// by the SAME stable idempotency key before ever Invoking -- so a
	// "lost" route recovers the prior result rather than duplicating the
	// side effect. See
	// InvocationIdentity's doc comment.
	routes map[string]jobRoute
}

type jobRoute struct {
	transport      domain.EndpointAdapterType
	endpointRef    string
	idempotencyKey string
	providerID     string
	capabilityID   string
	capVersion     string
	quoteID        string
	escrowID       string
	principalID    string
	trustMode      domain.TrustMode
	proofProfile   domain.ProofProfile
}

// New builds a dispatching Provider. native handles every Job whose
// Capability binding is tos-native, human, or absent -- exactly the
// current behavior of whichever tosai.Provider cmd/api/main.go already
// selects (mock or the tos-protocol RPC client). resolver supplies the
// third-party transport adapters (http/mcp/a2a); a nil resolver is valid
// and simply means every Job falls back to native (e.g. in tests that
// never register third-party bindings).
func New(native tosai.Provider, resolver *provideradapter.Resolver) *Provider {
	return &Provider{native: native, resolver: resolver, routes: make(map[string]jobRoute)}
}

func (p *Provider) RegisterProvider(ctx context.Context, providerID string, capability domain.Capability) error {
	return p.native.RegisterProvider(ctx, providerID, capability)
}

func (p *Provider) GetProviderStatus(ctx context.Context, providerID string) (bool, error) {
	return p.native.GetProviderStatus(ctx, providerID)
}

// InvocationIdentity derives the stable, durable execution identity for
// jobID -- the same identity is presented to a third-party endpoint on
// every attempt for this Job, whether the first submission or a crash-
// recovery replay, exactly as internal/service/earnings.go's
// payoutIdempotencyKey does for the payout rail.
func InvocationIdentity(jobID string) string {
	return "job:" + jobID + ":v1"
}

// isThirdParty reports whether transport routes through a
// provideradapter.ProviderAdapter rather than the wrapped native
// tosai.Provider.
func isThirdParty(transport domain.EndpointAdapterType) bool {
	switch transport {
	case domain.AdapterHTTP, domain.AdapterMCP, domain.AdapterA2A:
		return true
	default:
		return false
	}
}

func (p *Provider) SubmitJob(ctx context.Context, req tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	if req.Binding == nil || !isThirdParty(req.Binding.Transport) {
		return p.native.SubmitJob(ctx, req)
	}
	binding := *req.Binding
	adapter, ok := p.resolver.For(binding.Transport)
	if !ok {
		return tosai.SubmitJobResult{}, fmt.Errorf("dispatch: no provider adapter registered for transport %q", binding.Transport)
	}
	key := InvocationIdentity(req.JobID)
	route := jobRoute{
		transport: binding.Transport, endpointRef: binding.EndpointRef, idempotencyKey: key,
		providerID: req.ProviderID, capabilityID: req.CapabilityID, capVersion: req.CapabilityVersion,
		quoteID: req.QuoteID, escrowID: req.EscrowID, principalID: req.PrincipalID,
		trustMode: req.TrustMode, proofProfile: req.ProofProfile,
	}

	// Query first: if a prior attempt already reached the third party but
	// this process crashed (or a concurrent caller/replica already
	// submitted) before recording it locally, this recovers the result
	// without risking a second side-effecting call.
	if result, found, err := adapter.Query(ctx, binding.EndpointRef, key); err == nil && found {
		p.remember(req.JobID, route)
		return p.toSubmitResult(req, result), nil
	}

	invokeReq := provideradapter.InvokeRequest{
		JobID: req.JobID, CapabilityID: req.CapabilityID, CapabilityVersion: req.CapabilityVersion,
		ProviderID: req.ProviderID, QuoteID: req.QuoteID, EndpointRef: binding.EndpointRef,
		Input: req.Input, InputCommitment: req.InputCommitment, Deadline: req.ExecutionDeadline,
		IdempotencyKey: key,
	}
	result, err := adapter.Invoke(ctx, invokeReq)
	if err != nil {
		return tosai.SubmitJobResult{}, err
	}
	p.remember(req.JobID, route)
	return p.toSubmitResult(req, result), nil
}

func (p *Provider) remember(jobID string, route jobRoute) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.routes[jobID] = route
}

func (p *Provider) recall(jobID string) (jobRoute, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	route, ok := p.routes[jobID]
	return route, ok
}

func (p *Provider) forget(jobID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.routes, jobID)
}

// toSubmitResult maps a third-party InvokeResult onto tosai.SubmitJobResult.
// A Completed result is given a synthesized, ATOS-self-signed
// ExecutionReceipt -- there is no TOS network signer material for a
// third-party endpoint ATOS does not operate, exactly mirroring how
// internal/adapters/tosai/mock.Provider signs a Managed-mode receipt
// itself. This path is only ever reachable for TrustModeManaged in Phase
// 3A: nothing here (or anywhere else -- see domain.SandboxCertification's
// doc comment) can make ModeSupport[verified/native] Active, so
// ResolveTrustMode can never resolve a third-party binding's Job to a
// stronger mode yet.
func (p *Provider) toSubmitResult(req tosai.SubmitJobRequest, result provideradapter.InvokeResult) tosai.SubmitJobResult {
	switch result.Status {
	case provideradapter.InvokeCompleted:
		receipt := synthesizeReceipt(req, result)
		return tosai.SubmitJobResult{State: domain.JobCompleted, Output: result.Output, Usage: result.Usage, Receipt: receipt}
	case provideradapter.InvokeFailed:
		return tosai.SubmitJobResult{State: domain.JobFailed}
	default: // InvokePending
		return tosai.SubmitJobResult{State: domain.JobWorking}
	}
}

func synthesizeReceipt(req tosai.SubmitJobRequest, result provideradapter.InvokeResult) *domain.ExecutionReceipt {
	now := time.Now().UTC()
	inputHash := req.InputCommitment
	if inputHash == "" {
		inputHash = hashJSON(req.Input)
	}
	outputHash := hashJSON(result.Output)
	usageCommitment := hashJSON(map[string]any{
		"input_bytes": result.Usage.InputBytes, "output_bytes": result.Usage.OutputBytes,
		"input_tokens": result.Usage.InputTokens, "output_tokens": result.Usage.OutputTokens,
		"execution_millis": result.Usage.ExecutionMillis,
	})
	receiptID := "xrcpt_" + hashJSON(map[string]any{"job_id": req.JobID, "idempotency_key": InvocationIdentity(req.JobID)})[:32]
	signerID := "sig_atos_managed_dispatch"
	return &domain.ExecutionReceipt{
		ID: receiptID, QuoteID: req.QuoteID, EscrowID: req.EscrowID, JobID: req.JobID,
		PrincipalID: req.PrincipalID, ProviderID: req.ProviderID,
		CapabilityID: req.CapabilityID, CapabilityVersion: req.CapabilityVersion,
		TrustMode: req.TrustMode, ProofProfile: req.ProofProfile,
		Result: domain.ExecutionSuccess, InputHash: inputHash, OutputHash: outputHash,
		UsageCommitment: usageCommitment, Usage: result.Usage,
		StartedAt: now, CompletedAt: now,
		// ExecutionSignerID is set (required non-empty for
		// toscore.Core.ResolveExecutionSignerAuthorization to authorize
		// it at all) but SignerAuthorizationID/-Ref are deliberately left
		// empty: those are toscore.Core's own authoritative identifiers
		// to issue, not this adapter's to invent. VerifyExecutionReceipt
		// only compares a NON-empty SignerAuthorizationID against the
		// Core's resolved authorization -- leaving it empty asks the Core
		// to be the sole source of truth instead of risking a byte-exact
		// mismatch against a value this adapter has no way to predict.
		ExecutionSignerID:  signerID,
		SignatureAlgorithm: "atos-managed-sha256",
		Signature:          hashJSON(map[string]any{"receipt_id": receiptID, "quote_id": req.QuoteID, "job_id": req.JobID, "input": inputHash, "output": outputHash, "usage": usageCommitment}),
	}
}

func hashJSON(v any) string {
	encoded, _ := json.Marshal(v)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func (p *Provider) GetJob(ctx context.Context, jobID string) (tosai.SubmitJobResult, error) {
	route, ok := p.recall(jobID)
	if !ok {
		return p.native.GetJob(ctx, jobID)
	}
	adapter, ok := p.resolver.For(route.transport)
	if !ok {
		return tosai.SubmitJobResult{}, fmt.Errorf("dispatch: no provider adapter registered for transport %q", route.transport)
	}
	result, found, err := adapter.Query(ctx, route.endpointRef, route.idempotencyKey)
	if err != nil {
		return tosai.SubmitJobResult{}, err
	}
	if !found {
		return tosai.SubmitJobResult{}, domain.NewError(domain.ErrNotFound, fmt.Sprintf("dispatch: no record of job %q at provider", jobID), false)
	}
	return p.toSubmitResult(routeAsRequest(route, jobID), result), nil
}

// routeAsRequest reconstructs just enough of a tosai.SubmitJobRequest from
// a remembered route to synthesize a receipt for a GetJob-recovered
// result -- every field it uses was captured verbatim from the original
// SubmitJob call.
func routeAsRequest(route jobRoute, jobID string) tosai.SubmitJobRequest {
	return tosai.SubmitJobRequest{
		JobID: jobID, QuoteID: route.quoteID, EscrowID: route.escrowID, PrincipalID: route.principalID,
		CapabilityID: route.capabilityID, CapabilityVersion: route.capVersion, ProviderID: route.providerID,
		TrustMode: route.trustMode, ProofProfile: route.proofProfile,
	}
}

func (p *Provider) CancelJob(ctx context.Context, jobID, reason string) error {
	route, ok := p.recall(jobID)
	if !ok {
		return p.native.CancelJob(ctx, jobID, reason)
	}
	adapter, ok := p.resolver.For(route.transport)
	if !ok {
		return fmt.Errorf("dispatch: no provider adapter registered for transport %q", route.transport)
	}
	err := adapter.Cancel(ctx, route.endpointRef, route.idempotencyKey, reason)
	p.forget(jobID)
	if errors.Is(err, provideradapter.ErrCancelUnsupported) {
		// Best-effort, matching tosai/mock.Provider.CancelJob's own
		// always-nil contract -- not every transport can stop an
		// in-flight attempt, and that is not itself an error.
		return nil
	}
	return err
}

func (p *Provider) FetchResult(ctx context.Context, jobID string) (map[string]any, error) {
	result, err := p.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return result.Output, nil
}

func (p *Provider) FetchReceipt(ctx context.Context, jobID string) (*domain.ExecutionReceipt, error) {
	result, err := p.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return result.Receipt, nil
}

// StreamJobEvents makes Provider itself satisfy tosai.Streamer whenever the
// wrapped native provider does, so internal/service.StreamService's own
// type assertion (streamer, ok := s.provider.(tosai.Streamer)) keeps
// working for every tos-native Job exactly as before wrapping native in
// dispatch.New. Third-party (http/mcp/a2a) Jobs do not support streaming in
// Phase 3A -- this reports a clear, honest error for them rather than
// silently no-op'ing or panicking.
func (p *Provider) StreamJobEvents(ctx context.Context, req tosai.StreamJobEventsRequest, onEvent func(domain.JobEvent) error) error {
	streamer, ok := p.native.(tosai.Streamer)
	if !ok {
		return fmt.Errorf("dispatch: underlying native provider does not implement streaming")
	}
	if route, found := p.recall(req.JobID); found && isThirdParty(route.transport) {
		return fmt.Errorf("dispatch: streaming is not supported for third-party (%s) job execution", route.transport)
	}
	return streamer.StreamJobEvents(ctx, req, onEvent)
}
