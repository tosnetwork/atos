// Package mock is a synchronous, in-process stand-in for a real tos-ai
// provider network. It "executes" a job by echoing its input back as
// output, which is enough to exercise the full ATOS contract end to end
// before any real provider exists — the Phase 0 "mock provider" called for
// in ~/atos-spec/docs/IMPLEMENTATION_ROADMAP.md.
package mock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/domain"
)

type Provider struct {
	mu   sync.Mutex
	jobs map[string]tosai.SubmitJobResult
}

func New() *Provider {
	return &Provider{jobs: make(map[string]tosai.SubmitJobResult)}
}

func (p *Provider) RegisterProvider(ctx context.Context, providerID string, capability domain.Capability) error {
	return nil
}

func (p *Provider) GetProviderStatus(ctx context.Context, providerID string) (bool, error) {
	return true, nil
}

// SubmitJob runs synchronously and always succeeds for Phase 0 — realistic
// failure/latency simulation is out of scope until a real tos-ai network is
// wired in.
func (p *Provider) SubmitJob(ctx context.Context, req tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	started := time.Now()
	output := map[string]any{
		"echo":    req.Input,
		"note":    "mock tos-ai execution — replace with a real provider before Phase 2",
		"job_id":  req.JobID,
		"handled": req.CapabilityID,
	}
	inputHash := hashJSON(req.Input)
	outputHash := hashJSON(output)
	completed := time.Now()

	receipt := &domain.ExecutionReceipt{
		JobID:        req.JobID,
		ProviderID:   req.ProviderID,
		CapabilityID: req.CapabilityID,
		InputHash:    inputHash,
		OutputHash:   outputHash,
		StartedAt:    started,
		CompletedAt:  completed,
		Signature:    mockSignature(req.JobID, inputHash, outputHash),
	}

	result := tosai.SubmitJobResult{
		State:   domain.JobCompleted,
		Output:  output,
		Receipt: receipt,
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
		return tosai.SubmitJobResult{}, fmt.Errorf("mock tosai: unknown job %q", jobID)
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

func hashJSON(v map[string]any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// mockSignature stands in for a real provider signing key. It is
// deterministic and clearly labeled so nobody mistakes it for a real
// cryptographic signature.
func mockSignature(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
	}
	h.Write([]byte(uuid.NewString())) // nonce so repeated calls don't collide
	return "mock-unsigned:" + hex.EncodeToString(h.Sum(nil))
}
