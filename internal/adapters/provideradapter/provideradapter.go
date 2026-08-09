// Package provideradapter defines the transport-agnostic boundary between
// ATOS's execution dispatch (internal/adapters/tosai/dispatch) and outbound
// calls to third-party provider endpoints. HTTP, MCP, and A2A each ship a
// concrete ProviderAdapter implementation in their own sub-package; the
// dispatch layer depends only on this interface, never on a
// protocol-specific client directly.
package provideradapter

import (
	"context"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

// InvokeRequest is the transport-agnostic request to execute one attempt of
// a Job against a third-party Capability binding.
type InvokeRequest struct {
	JobID             string
	CapabilityID      string
	CapabilityVersion string
	ProviderID        string
	EndpointRef       string
	Input             map[string]any
	InputCommitment   string
	Deadline          time.Time
	// IdempotencyKey is the stable, durable execution identity for this
	// exact semantic operation. It MUST be derived only from the Job's own
	// durable identity (see dispatch.InvocationIdentity) and MUST NOT be
	// regenerated across ATOS-side retries after a timeout or restart --
	// the same semantic operation always presents the same identity to the
	// third-party endpoint, exactly as internal/service/earnings.go's
	// payoutIdempotencyKey does for the payout rail.
	IdempotencyKey string
}

// InvokeStatus is the outcome of one adapter invocation attempt.
type InvokeStatus string

const (
	// InvokeCompleted means the provider returned a result bound to this
	// exact JobID/CapabilityID/CapabilityVersion/IdempotencyKey.
	InvokeCompleted InvokeStatus = "completed"
	// InvokeFailed means the provider (or the underlying protocol)
	// definitively and safely reports no further progress is possible for
	// this attempt -- never used for "we don't know", only for a
	// confirmed terminal failure.
	InvokeFailed InvokeStatus = "failed"
	// InvokePending means the true outcome is not yet known -- the call
	// may still be in flight, the response was lost, or the protocol is
	// asynchronous and has not settled. Callers MUST treat this as
	// "unknown, not failed" and retry Query using the same
	// IdempotencyKey; an adapter MUST NEVER infer InvokeCompleted or
	// InvokeFailed from an ambiguous signal.
	InvokePending InvokeStatus = "pending"
)

// InvokeResult is the outcome of an Invoke or Query call.
type InvokeResult struct {
	Status        InvokeStatus
	Output        map[string]any
	Usage         domain.Usage
	FailureReason string
}

// ProviderAdapter is the shared abstraction every transport implementation
// (HTTP, MCP, A2A) satisfies. Every method must be safe to call any number
// of times with the same IdempotencyKey without causing a duplicate
// side-effecting call to the underlying provider where the protocol offers
// a way to avoid one.
type ProviderAdapter interface {
	// Invoke attempts, or on retry replays, one execution attempt for req.
	// It is the only method that may cause a new external side effect.
	Invoke(ctx context.Context, req InvokeRequest) (InvokeResult, error)
	// Query looks up the outcome of a previously attempted invocation by
	// its idempotency key, without causing any new side effect. found is
	// false when the adapter has no record of ever attempting this key --
	// a safe signal that Invoke was never called, or never reached the
	// provider, for it. Adapters that talk to a stateless
	// request/response-only protocol (no server-side lookup-by-id) MAY
	// legitimately always report found=false; callers must not treat that
	// as an error.
	Query(ctx context.Context, idempotencyKey string) (result InvokeResult, found bool, err error)
	// Cancel is a best-effort request to stop an in-flight attempt.
	// Adapters that cannot support cancellation for their protocol return
	// ErrCancelUnsupported rather than a fabricated success.
	Cancel(ctx context.Context, idempotencyKey, reason string) error
	// Health performs a lightweight, bounded reachability probe against
	// endpointRef. This is readiness evidence only -- see
	// domain.AdapterHealthCheck's doc comment -- and MUST NEVER be treated
	// by any caller as trust-mode activation authority.
	Health(ctx context.Context, endpointRef string) domain.AdapterHealthCheck
	// Transport identifies which domain.EndpointAdapterType this adapter
	// implements, for Resolver dispatch.
	Transport() domain.EndpointAdapterType
}

// ErrCancelUnsupported is returned by ProviderAdapter.Cancel implementations
// whose underlying protocol has no safe way to stop an in-flight attempt.
var ErrCancelUnsupported = cancelUnsupportedError{}

type cancelUnsupportedError struct{}

func (cancelUnsupportedError) Error() string {
	return "provideradapter: this transport does not support cancellation"
}

// Resolver maps a Capability's transport binding to the ProviderAdapter
// that can invoke it. It is deliberately a thin, static lookup -- routing
// decisions belong to the caller (dispatch), not to the Resolver itself.
type Resolver struct {
	adapters map[domain.EndpointAdapterType]ProviderAdapter
}

// NewResolver builds a Resolver from a set of adapters, keyed by each
// adapter's own declared Transport(). Registering two adapters for the same
// transport is a caller bug; the later one silently wins, matching Go map
// semantics -- callers should not do this.
func NewResolver(adapters ...ProviderAdapter) *Resolver {
	r := &Resolver{adapters: make(map[domain.EndpointAdapterType]ProviderAdapter, len(adapters))}
	for _, a := range adapters {
		if a == nil {
			continue
		}
		r.adapters[a.Transport()] = a
	}
	return r
}

// For returns the adapter registered for transport, if any.
func (r *Resolver) For(transport domain.EndpointAdapterType) (ProviderAdapter, bool) {
	if r == nil {
		return nil, false
	}
	a, ok := r.adapters[transport]
	return a, ok
}
