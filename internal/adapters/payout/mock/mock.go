// Package mock is an explicit, deterministic test/development
// implementation of payout.Adapter. It never moves real funds and MUST NOT
// be treated as a production payment rail -- callers that need a real one
// must supply their own Adapter implementation behind the same interface.
package mock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/tosnetwork/atos/internal/adapters/payout"
	"github.com/tosnetwork/atos/internal/domain"
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

// requestDigest summarizes the semantically meaningful fields of a payout
// Request (everything that determines what actually gets paid to whom) so
// a replayed call sharing an IdempotencyKey can be recognized as identical
// versus a substitution attempt -- a caller reusing a key with a different
// amount, provider, or earning is a bug (or worse), and a real idempotent
// rail would reject it rather than silently honoring whichever amount
// happened to arrive first or replaying a stale one for a different
// request.
func requestDigest(req payout.Request) string {
	encoded, _ := json.Marshal(struct {
		EarningID, ProviderID string
		Amount                domain.Money
	}{req.EarningID, req.ProviderID, req.Amount})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

type record struct {
	digest string
	result payout.Result
}

// Adapter is a deterministic in-memory payout.Adapter keyed by
// IdempotencyKey: the first call for a key performs the (simulated) payout,
// and every subsequent call with the same key AND THE SAME REQUEST CONTENT
// returns the identical stored Result instead of paying again -- the same
// guarantee a real idempotent rail must provide. A call reusing a key with
// DIFFERENT request content is rejected rather than silently replayed or
// silently accepted.
type Adapter struct {
	mu      sync.Mutex
	results map[string]record
	// Inject, if set for a key, is consulted once for that key's first
	// Payout call (then cleared) so tests can force one specific crash
	// window per idempotency key without affecting the retry that follows.
	Inject map[string]FailureMode
}

func New() *Adapter {
	return &Adapter{results: make(map[string]record)}
}

func (a *Adapter) Payout(ctx context.Context, req payout.Request) (payout.Result, error) {
	if req.IdempotencyKey == "" {
		return payout.Result{}, errors.New("mock payout: idempotency key is required")
	}
	digest := requestDigest(req)
	a.mu.Lock()
	defer a.mu.Unlock()
	if existing, ok := a.results[req.IdempotencyKey]; ok {
		if existing.digest != digest {
			return payout.Result{}, fmt.Errorf("mock payout: idempotency key %q was already used for a different request (earning_id/provider_id/amount changed)", req.IdempotencyKey)
		}
		return existing.result, nil
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
		a.results[req.IdempotencyKey] = record{digest: digest, result: payout.Result{Status: payout.StatusPaid, Reference: "mock_payout_" + req.IdempotencyKey}}
		return payout.Result{}, errors.New("mock payout: simulated response loss after the rail committed")
	default:
		result := payout.Result{Status: payout.StatusPaid, Reference: "mock_payout_" + req.IdempotencyKey}
		a.results[req.IdempotencyKey] = record{digest: digest, result: result}
		return result, nil
	}
}

func (a *Adapter) Query(ctx context.Context, idempotencyKey string) (payout.Result, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	rec, ok := a.results[idempotencyKey]
	return rec.result, ok, nil
}
