// Package store defines the persistence interfaces the service layer
// depends on. internal/store/memory implements them in-memory for Phase 0.
// A Postgres implementation (Phase 1, "centralized Postgres registry" per
// docs/IMPLEMENTATION_ROADMAP.md) drops in behind the same interfaces
// without touching internal/service.
package store

import (
	"context"
	"errors"

	"github.com/tosnetwork/atos/internal/domain"
)

var ErrNotFound = errors.New("store: not found")

// ErrConflict signals a caller-supplied compare-and-swap precondition (an
// expected prior state) did not hold — e.g. trying to transition a job that
// a concurrent caller already moved to a terminal state.
var ErrConflict = errors.New("store: conflict")

// IdempotencyStatus tracks whether a reserved key's underlying request has
// finished, so a retry that arrives before the first attempt commits does
// not resolve to an empty/missing response, and a retry that arrives after
// a pre-commit failure is not permanently blocked.
type IdempotencyStatus string

const (
	IdempotencyInProgress IdempotencyStatus = "in_progress"
	IdempotencyCompleted  IdempotencyStatus = "completed"
)

// IdempotencyRecord caches the result of a previous committing call so a
// retried request with the same key returns the original outcome instead
// of re-executing (docs/MCP.md "Idempotency").
type IdempotencyRecord struct {
	RequestHash string
	ResponseKey string // opaque pointer the caller resolves (e.g. a job/invocation ID); only meaningful once Status is Completed
	Status      IdempotencyStatus
}

// Method names are qualified per collection (PutQuote, not Put) because
// Store below embeds all of these into one interface — Go does not allow
// embedding two interfaces that declare the same method name with
// different signatures.
type Capabilities interface {
	Put(ctx context.Context, c domain.Capability) error
	Get(ctx context.Context, id string) (domain.Capability, error)
	Search(ctx context.Context, query string, limit int) ([]domain.Capability, error)
	ByProvider(ctx context.Context, providerID string) ([]domain.Capability, error)
}

type Quotes interface {
	PutQuote(ctx context.Context, q domain.Quote) error
	GetQuote(ctx context.Context, id string) (domain.Quote, error)
}

type Escrows interface {
	PutEscrow(ctx context.Context, e domain.Escrow) error
	GetEscrow(ctx context.Context, id string) (domain.Escrow, error)
}

type Receipts interface {
	PutReceipt(ctx context.Context, r domain.Receipt) error
	GetReceipt(ctx context.Context, id string) (domain.Receipt, error)
	ReceiptByJob(ctx context.Context, jobID string) (domain.Receipt, error)
	ReceiptsByPrincipal(ctx context.Context, principalID string) ([]domain.Receipt, error)
}

type Jobs interface {
	PutJob(ctx context.Context, j domain.Job) error
	GetJob(ctx context.Context, id string) (domain.Job, error)
	JobsByPrincipal(ctx context.Context, principalID string) ([]domain.Job, error)
	// UpdateJob atomically applies fn to the job's current stored state (or
	// domain.Job{} with exists=false if it isn't stored yet) and persists
	// whatever fn returns. fn returning an error aborts without persisting
	// — used to implement compare-and-swap state transitions (e.g. "only
	// move to failed if not already terminal") without a read/write race
	// between the execution pipeline and cancellation.
	UpdateJob(ctx context.Context, id string, fn func(j domain.Job, exists bool) (domain.Job, error)) (domain.Job, error)
}

type Accounts interface {
	GetAccount(ctx context.Context, principalID string) (domain.Account, error)
	PutAccount(ctx context.Context, a domain.Account) error
	// UpdateAccount atomically applies fn to the account's current stored
	// state (or a caller-supplied seed with exists=false if it isn't
	// stored yet) and persists whatever fn returns. This is the only safe
	// way to debit/credit — a separate Get+Put race lets two concurrent
	// calls both observe the pre-debit balance.
	UpdateAccount(ctx context.Context, principalID string, seed domain.Account, fn func(a domain.Account, exists bool) (domain.Account, error)) (domain.Account, error)
}

type Artifacts interface {
	PutArtifact(ctx context.Context, a domain.StoredArtifact) error
	GetArtifact(ctx context.Context, id string) (domain.StoredArtifact, error)
}

type Idempotency interface {
	// Reserve atomically claims (principalID, key) in the InProgress state.
	// If the key was already used it returns the prior record and ok=false
	// so the caller can return the cached result (Completed) or a
	// retry-later conflict (still InProgress).
	Reserve(ctx context.Context, principalID, key, requestHash string) (rec IdempotencyRecord, ok bool, err error)
	// Finish marks a reservation Completed with its response_key, so a
	// later replay resolves to the real result.
	Finish(ctx context.Context, principalID, key, responseKey string) error
	// Release removes a reservation entirely. Callers MUST call this on
	// every pre-commit failure (validation, quote lookup, etc.) so a
	// corrected retry is not permanently blocked by a poisoned key.
	Release(ctx context.Context, principalID, key string) error
}

// Store aggregates every collection the service layer needs. Concrete
// implementations (memory, later postgres) satisfy this in one place.
type Store interface {
	Capabilities
	Quotes
	Escrows
	Receipts
	Jobs
	Accounts
	Artifacts
	Idempotency
}
