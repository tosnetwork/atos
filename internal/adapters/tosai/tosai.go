// Package tosai defines the execution-only adapter boundary. In the final
// topology ATOS calls tos-protocol Edge Core and Edge Core calls the private
// tos-ai Worker RPC; this interface remains the Phase 0 compatibility seam for
// the in-process execution mock.
package tosai

import (
	"context"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

type SubmitJobRequest struct {
	JobID              string
	QuoteID            string
	EscrowID           string
	PrincipalID        string
	CapabilityID       string
	CapabilityVersion  string
	ProviderID         string
	TrustMode          domain.TrustMode
	ProofProfile       domain.ProofProfile
	Input              map[string]any
	InputCommitment    string
	MaxWaitMS          int64
}

type SubmitJobResult struct {
	State     domain.JobState
	Output    map[string]any
	Artifacts []domain.Artifact
	Usage     domain.Usage
	Receipt   *domain.ExecutionReceipt
}

type Provider interface {
	RegisterProvider(ctx context.Context, providerID string, capability domain.Capability) error
	GetProviderStatus(ctx context.Context, providerID string) (healthy bool, err error)
	SubmitJob(ctx context.Context, req SubmitJobRequest) (SubmitJobResult, error)
	GetJob(ctx context.Context, jobID string) (SubmitJobResult, error)
	CancelJob(ctx context.Context, jobID, reason string) error
	FetchResult(ctx context.Context, jobID string) (map[string]any, error)
	FetchReceipt(ctx context.Context, jobID string) (*domain.ExecutionReceipt, error)
}

var clock = time.Now
