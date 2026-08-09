// Package mock is an explicit, deterministic test/development
// implementation of payout.Adapter. It never moves real funds and MUST NOT
// be treated as a production payment rail -- callers that need a real one
// must supply their own Adapter implementation behind the same interface.
package mock

import (
	"context"
	"errors"
	"sync"

	"github.com/tosnetwork/atos/internal/adapters/payout"
)

// FailureMode lets tests inject every crash/ambiguity window a real payout
// rail can produce, keyed by IdempotencyKey so each injected failure is
// scoped to one specific attempt.
type FailureMode int

const (
	// FailNone performs the payout normally.
	FailNone FailureMode = iota
	// FailBeforeEffect simulates an error that occurs before the rail
	// records anything (e.g. connection refused). Nothing happened, so a
	// retry with the same key is unconditionally safe.
	FailBeforeEffect
	// FailAmbiguous simulates the rail durably completing the payout but
	// the caller never learning the result (e.g. the response was lost in
	// transit). The Adapter records the payout as paid internally -- so a
	// later Query or replayed Payout call with the same key finds it --
	// but this specific call still returns an error to its caller, exactly
	// as if the response never arrived.
	FailAmbiguous
)

// Adapter is a deterministic in-memory payout.Adapter keyed by
// IdempotencyKey: the first call for a key performs the (simulated) payout,
// and every subsequent call with the same key returns the identical stored
// Result instead of paying again -- the same guarantee a real idempotent
// rail must provide.
type Adapter struct {
	mu      sync.Mutex
	results map[string]payout.Result
	// Inject, if set for a key, is consulted once for that key's first
	// Payout call (then cleared) so tests can force one specific crash
	// window per idempotency key without affecting the retry that follows.
	Inject map[string]FailureMode
}

func New() *Adapter {
	return &Adapter{results: make(map[string]payout.Result)}
}

func (a *Adapter) Payout(ctx context.Context, req payout.Request) (payout.Result, error) {
	if req.IdempotencyKey == "" {
		return payout.Result{}, errors.New("mock payout: idempotency key is required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if existing, ok := a.results[req.IdempotencyKey]; ok {
		return existing, nil
	}
	mode := FailNone
	if a.Inject != nil {
		mode = a.Inject[req.IdempotencyKey]
		delete(a.Inject, req.IdempotencyKey)
	}
	switch mode {
	case FailBeforeEffect:
		return payout.Result{}, errors.New("mock payout: simulated failure before any external effect")
	case FailAmbiguous:
		a.results[req.IdempotencyKey] = payout.Result{Status: payout.StatusPaid, Reference: "mock_payout_" + req.IdempotencyKey}
		return payout.Result{}, errors.New("mock payout: simulated response loss after the rail committed")
	default:
		result := payout.Result{Status: payout.StatusPaid, Reference: "mock_payout_" + req.IdempotencyKey}
		a.results[req.IdempotencyKey] = result
		return result, nil
	}
}

func (a *Adapter) Query(ctx context.Context, idempotencyKey string) (payout.Result, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	result, ok := a.results[idempotencyKey]
	return result, ok, nil
}
