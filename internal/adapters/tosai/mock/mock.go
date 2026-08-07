// Package mock is a synchronous in-process stand-in for the execution plane.
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

func (p *Provider) SubmitJob(ctx context.Context, req tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	if req.TrustMode != domain.TrustModeManaged {
		return tosai.SubmitJobResult{}, fmt.Errorf("mock tos-ai is certified for managed mode only")
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
		SignerAuthorizationRef: "atos:managed:signer:" + signerID,
		SignatureAlgorithm:     "mock-sha256",
		Signature:              mockSignature(receiptID, req.QuoteID, req.JobID, inputHash, outputHash, usageCommitment),
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
