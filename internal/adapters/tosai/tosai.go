// Package tosai defines the ATOS -> tos-ai interface contract from
// ~/atos-spec/docs/ARCHITECTURE.md's "Cross-Service Interface Contracts":
// execution only. No identity, ownership, escrow or settlement logic
// belongs behind this interface — that is tos-core's job (see
// internal/adapters/toscore).
package tosai

import (
	"context"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

// SubmitJobRequest carries everything tos-ai needs to run one job. It
// intentionally does not carry billing/escrow fields — tos-ai executes,
// it does not know or care what the job costs.
type SubmitJobRequest struct {
	JobID        string
	CapabilityID string
	ProviderID   string
	Input        map[string]any
	MaxWaitMS    int64
}

type SubmitJobResult struct {
	State  domain.JobState
	Output map[string]any
	// Receipt is populated once the job reaches a terminal successful
	// state; nil otherwise.
	Receipt *domain.ExecutionReceipt
}

// Provider is the ATOS -> tos-ai interface. Method names match
// docs/ARCHITECTURE.md exactly (RegisterProvider, SubmitJob, GetJob,
// FetchResult, FetchReceipt) so the mapping back to the spec stays
// traceable. StreamJob and CancelJob round out job lifecycle handling that
// the spec's job model requires.
type Provider interface {
	RegisterProvider(ctx context.Context, providerID string, capability domain.Capability) error
	GetProviderStatus(ctx context.Context, providerID string) (healthy bool, err error)

	SubmitJob(ctx context.Context, req SubmitJobRequest) (SubmitJobResult, error)
	GetJob(ctx context.Context, jobID string) (SubmitJobResult, error)
	CancelJob(ctx context.Context, jobID, reason string) error
	FetchResult(ctx context.Context, jobID string) (map[string]any, error)
	FetchReceipt(ctx context.Context, jobID string) (*domain.ExecutionReceipt, error)
}

// clock is overridable in tests; defaults to time.Now.
var clock = time.Now
