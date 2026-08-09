// Package mock is a synchronous in-process stand-in for the execution plane.
package mock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/domain"
)

type Provider struct {
	mu        sync.Mutex
	jobs      map[string]tosai.SubmitJobResult
	modes     map[domain.TrustMode]bool
	simulated bool
}

func New() *Provider {
	return newProvider(false, domain.TrustModeManaged)
}

// NewContractFixture creates a deliberately simulated provider that accepts
// every concrete v0.2 mode. It exists only for Phase 0 contract/conformance
// tests; runtime composition continues to use New, which is Managed-only.
func NewContractFixture() *Provider {
	return newProvider(true, domain.TrustModeManaged, domain.TrustModeVerified, domain.TrustModeNative)
}

func newProvider(simulated bool, modes ...domain.TrustMode) *Provider {
	allowed := make(map[domain.TrustMode]bool, len(modes))
	for _, mode := range modes {
		allowed[mode] = true
	}
	return &Provider{jobs: make(map[string]tosai.SubmitJobResult), modes: allowed, simulated: simulated}
}

func (p *Provider) RegisterProvider(ctx context.Context, providerID string, capability domain.Capability) error {
	return nil
}

func (p *Provider) GetProviderStatus(ctx context.Context, providerID string) (bool, error) {
	return true, nil
}

func (p *Provider) SubmitJob(ctx context.Context, req tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	p.mu.Lock()
	if existing, ok := p.jobs[req.JobID]; ok {
		p.mu.Unlock()
		if existing.Receipt != nil && (existing.Receipt.QuoteID != req.QuoteID || existing.Receipt.EscrowID != req.EscrowID || existing.Receipt.PrincipalID != req.PrincipalID || existing.Receipt.CapabilityID != req.CapabilityID) {
			return tosai.SubmitJobResult{}, domain.NewError(domain.ErrIdempotencyConflict, "job replay does not match original execution", false)
		}
		return existing, nil
	}
	p.mu.Unlock()
	if !p.modes[req.TrustMode] {
		return tosai.SubmitJobResult{}, fmt.Errorf("mock tos-ai is not configured for %s mode", req.TrustMode)
	}
	if err := domain.ValidateCommittedTrust(req.TrustMode, req.ProofProfile); err != nil {
		return tosai.SubmitJobResult{}, err
	}
	started := time.Now().UTC()
	output := map[string]any{
		"echo":    req.Input,
		"note":    "mock tos-ai execution — replace with tos-protocol Edge Core before Phase 2",
		"job_id":  req.JobID,
		"handled": req.CapabilityID,
	}
	inputHash := req.InputCommitment
	if inputHash == "" {
		inputHash = hashJSON(req.Input)
	}
	outputHash := hashJSON(output)
	completed := time.Now().UTC()
	executionMillis := completed.Sub(started).Milliseconds()
	if executionMillis < 0 {
		executionMillis = 0
	}
	usage := domain.Usage{
		InputBytes:      jsonSize(req.Input),
		OutputBytes:     jsonSize(output),
		ExecutionMillis: uint64(executionMillis),
	}
	usageCommitment := hashAny(usage)
	receiptID := "xrcpt_" + uuid.NewString()
	signerID := "sig_mock_tos_ai"
	authorizationID := "auth_mock_" + req.CapabilityID
	authorizationRef := "atos:managed:signer:" + signerID
	networkProofRef := ""
	if p.simulated && req.TrustMode != domain.TrustModeManaged {
		authorizationRef = simulatedRef("signer", req.TrustMode, authorizationID)
		networkProofRef = simulatedRef("execution", req.TrustMode, receiptID)
	}
	receipt := &domain.ExecutionReceipt{
		ID:                     receiptID,
		QuoteID:                req.QuoteID,
		EscrowID:               req.EscrowID,
		JobID:                  req.JobID,
		PrincipalID:            req.PrincipalID,
		ProviderID:             req.ProviderID,
		CapabilityID:           req.CapabilityID,
		CapabilityVersion:      req.CapabilityVersion,
		TrustMode:              req.TrustMode,
		ProofProfile:           req.ProofProfile,
		Result:                 domain.ExecutionSuccess,
		InputHash:              inputHash,
		OutputHash:             outputHash,
		UsageCommitment:        usageCommitment,
		Usage:                  usage,
		StartedAt:              started,
		CompletedAt:            completed,
		ExecutionSignerID:      signerID,
		SignerAuthorizationID:  authorizationID,
		SignerAuthorizationRef: authorizationRef,
		SignatureAlgorithm:     "mock-sha256",
		Signature:              mockSignature(receiptID, req.QuoteID, req.JobID, inputHash, outputHash, usageCommitment),
		NetworkProofRef:        networkProofRef,
	}

	result := tosai.SubmitJobResult{
		State: domain.JobCompleted, Output: output, Usage: usage, Receipt: receipt,
	}
	p.mu.Lock()
	p.jobs[req.JobID] = result
	p.mu.Unlock()
	return result, nil
}

func (p *Provider) GetJob(ctx context.Context, jobID string) (tosai.SubmitJobResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	result, ok := p.jobs[jobID]
	if !ok {
		return tosai.SubmitJobResult{}, domain.NewError(domain.ErrNotFound, fmt.Sprintf("mock tosai: unknown job %q", jobID), false)
	}
	return result, nil
}

func (p *Provider) CancelJob(ctx context.Context, jobID, reason string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.jobs, jobID)
	return nil
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

// mockMaxStreamChunkBytes mirrors the bound enforced by the durable stream
// store (internal/store/postgres, internal/store/memory).
const mockMaxStreamChunkBytes = 256 << 10

// StreamJobEvents synthesizes the same STATE -> OutputChunk* -> Terminal
// sequence tos-protocol's real StreamJob RPC replays for a completed Job,
// deterministically re-derived from the stored completion each call and
// mirroring its exact resume contract: a STATE event is always sent first,
// labeled at NextSequence; OutputChunk events start at NextOffset (not
// necessarily zero); NextOffset/ExpectedStreamDigest are validated against
// the retained output before anything is sent. The mock has no genuine
// mid-execution partial output (SubmitJob above always completes
// synchronously), so this is an equivalent local replay rather than a
// network round trip — not a polling-based fake of a live stream.
func (p *Provider) StreamJobEvents(ctx context.Context, req tosai.StreamJobEventsRequest, onEvent func(domain.JobEvent) error) error {
	p.mu.Lock()
	result, ok := p.jobs[req.JobID]
	p.mu.Unlock()
	if !ok {
		return domain.NewError(domain.ErrNotFound, fmt.Sprintf("mock tosai: unknown job %q", req.JobID), false)
	}
	outputBytes, err := json.Marshal(result.Output)
	if err != nil {
		return err
	}
	outputSize := uint64(len(outputBytes))
	outputSum := sha256.Sum256(outputBytes)
	identityDigest := "sha256:" + hex.EncodeToString(outputSum[:])
	if req.NextOffset > outputSize {
		return domain.NewError(domain.ErrStreamCursorMismatch, "next_offset is beyond the retained output", false)
	}
	if req.ExpectedStreamDigest != "" && req.ExpectedStreamDigest != identityDigest {
		return domain.NewError(domain.ErrStreamCursorMismatch, "expected_stream_digest does not match retained output", false)
	}
	if req.NextOffset > 0 && req.ExpectedStreamDigest == "" {
		return domain.NewError(domain.ErrStreamCursorMismatch, "expected_stream_digest is required when resuming output", false)
	}
	chunkSize := req.MaxChunkBytes
	if chunkSize == 0 || chunkSize > mockMaxStreamChunkBytes {
		chunkSize = mockMaxStreamChunkBytes
	}
	now := time.Now().UTC()
	sequence := req.NextSequence
	send := func(event domain.JobEvent) error {
		event.Sequence = sequence
		sequence++
		event.CreatedAt = now
		event.UpstreamRetainedDigest = identityDigest
		return onEvent(event)
	}

	if err := send(domain.JobEvent{JobID: req.JobID, EventType: domain.JobEventState, State: result.State}); err != nil {
		return err
	}
	for offset := req.NextOffset; offset < outputSize; offset += chunkSize {
		end := min(offset+chunkSize, outputSize)
		if err := send(domain.JobEvent{
			JobID: req.JobID, EventType: domain.JobEventOutputChunk,
			State: result.State, Chunk: slices.Clone(outputBytes[offset:end]), Offset: offset,
			TotalOutputBytes: outputSize,
		}); err != nil {
			return err
		}
	}

	usage := result.Usage
	return send(domain.JobEvent{
		JobID: req.JobID, EventType: domain.JobEventTerminal,
		State: result.State, Terminal: true, Usage: &usage,
	})
}

func hashJSON(v map[string]any) string { return hashAny(v) }

func hashAny(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func jsonSize(v any) uint64 {
	b, _ := json.Marshal(v)
	return uint64(len(b))
}

func mockSignature(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
	}
	_, _ = h.Write([]byte(uuid.NewString()))
	return "mock-unsigned:" + hex.EncodeToString(h.Sum(nil))
}

func simulatedRef(kind string, mode domain.TrustMode, id string) string {
	return "simulated:atos-v0.2:" + string(mode) + ":" + kind + ":" + id
}
