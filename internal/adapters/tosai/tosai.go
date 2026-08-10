// Package tosai defines ATOS's execution-facing adapter boundary. In RPC
// mode ATOS calls tos-protocol's public ExecutionGatewayService; tos-protocol
// alone calls the private tos-ai Worker RPC. The in-process mock remains an
// explicit Phase 0 development backend and is never a network-failure fallback.
package tosai

import (
	"context"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

// QuoteExecutionRequest asks tos-protocol for the provider/Edge execution
// quote that ATOS will bind into its own client-facing commercial Quote.
type QuoteExecutionRequest struct {
	Capability        domain.Capability
	InputSummary      map[string]any
	InputCommitment   string
	InputBytes        uint64
	MaxOutputBytes    uint64
	ExecutionDeadline time.Time
	TrustMode         domain.TrustMode
	ProofProfile      domain.ProofProfile
}

// ServiceExecutionQuote is the provider/Edge layer of ATOS's two-layer Quote
// model. It is not itself the client-facing commercial Quote.
type ServiceExecutionQuote struct {
	ID                string
	Reference         string
	ProviderPrice     domain.Money
	ExpiresAt         time.Time
	ExecutionDeadline time.Time
	CapacityRevision  string
	RuntimeRevision   string
	ModelRevision     string
	SignedDigest      string
}

// Quoter is intentionally separate from Provider so the Quote service can be
// configured independently while legacy/mock deployments keep the static
// catalog-price path explicitly.
type Quoter interface {
	QuoteExecution(ctx context.Context, req QuoteExecutionRequest) (ServiceExecutionQuote, error)
}

type SubmitJobRequest struct {
	JobID             string
	InvocationID      string
	QuoteID           string
	ServiceQuoteID    string
	EscrowID          string
	PrincipalID       string
	CapabilityID      string
	CapabilityVersion string
	ProviderID        string
	TrustMode         domain.TrustMode
	ProofProfile      domain.ProofProfile
	Input             map[string]any
	InputCommitment   string
	MaxOutputBytes    uint64
	ExecutionDeadline time.Time
	RetainUntil       time.Time
	MaxWaitMS         int64
	// Binding is the Job's own frozen transport binding (domain.Job.Binding,
	// selected once at Job creation time via domain.SelectBinding and never
	// re-resolved from the Capability's current, possibly-since-updated
	// bindings). A Provider implementation that dispatches by transport
	// (see internal/adapters/tosai/dispatch) uses this directly -- an
	// already-committed Job must always execute against the exact binding
	// it froze, never a live Capability.Bindings lookup. Nil for a
	// Capability with no bindings at all; a Provider that only ever
	// executes tos-native capabilities (the mock, the tos-protocol RPC
	// client) is free to ignore this field entirely.
	Binding *domain.CapabilityBinding
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

// StreamJobEventsRequest resumes a bounded server-streamed read of a Job's
// event sequence starting at NextSequence, matching tos-protocol's
// StreamJobRequest resume fields.
type StreamJobEventsRequest struct {
	JobID                string
	NextSequence         uint64
	NextOffset           uint64
	ExpectedStreamDigest string
	MaxChunkBytes        uint64
}

// Streamer is implemented by execution backends that can (re)play a Job's
// bounded event stream. onEvent is called once per event in sequence order;
// returning an error from onEvent stops the stream early. Not every Provider
// implements this — callers MUST type-assert before use rather than
// depending on it being universally available.
type Streamer interface {
	StreamJobEvents(ctx context.Context, req StreamJobEventsRequest, onEvent func(domain.JobEvent) error) error
}

var clock = time.Now
