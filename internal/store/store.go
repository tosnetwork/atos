// Package store defines the persistence interfaces the service layer
// depends on. internal/store/memory implements them in-memory for Phase 0.
// A Postgres implementation (Phase 1, "centralized Postgres registry" per
// docs/IMPLEMENTATION_ROADMAP.md) drops in behind the same interfaces
// without touching internal/service.
package store

import (
	"context"
	"errors"
	"time"

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
	RequestHash  string
	ResponseKey  string // opaque pointer the caller resolves (e.g. a job/invocation ID); only meaningful once Status is Completed
	Status       IdempotencyStatus
	ReservedAt   time.Time
	LeaseExpires time.Time
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
	EscrowByJob(ctx context.Context, jobID string) (domain.Escrow, error)
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
	JobsForRecovery(ctx context.Context, updatedBefore time.Time, limit int) ([]domain.Job, error)
	JobByConfirmationCode(ctx context.Context, userCode string) (domain.Job, error)
	JobByIdempotencyKey(ctx context.Context, principalID, key string) (domain.Job, error)
	// UpdateJob atomically applies fn to the job's current stored state (or
	// domain.Job{} with exists=false if it isn't stored yet) and persists
	// whatever fn returns. fn returning an error aborts without persisting
	// — used to implement compare-and-swap state transitions (e.g. "only
	// move to failed if not already terminal") without a read/write race
	// between the execution pipeline and cancellation.
	UpdateJob(ctx context.Context, id string, fn func(j domain.Job, exists bool) (domain.Job, error)) (domain.Job, error)
	// UpdateJobAndAccount commits one Job checkpoint and one Managed Account
	// mutation in the same storage transaction. This is the Phase 1 economic
	// atomicity boundary: a crash cannot persist a debit/credit without the
	// corresponding durable Job checkpoint, or vice versa.
	UpdateJobAndAccount(
		ctx context.Context, jobID, principalID string, seed domain.Account,
		fn func(job domain.Job, jobExists bool, account domain.Account, accountExists bool) (domain.Job, domain.Account, error),
	) (domain.Job, domain.Account, error)
}

// JobStream persists a Job's resumable stream-event journal. Implementations
// MUST make AppendJobStreamEvent safe across concurrent callers in different
// ATOS processes (e.g. a PostgreSQL advisory transaction lock scoped to the
// Job ID, not an in-process mutex), since the same Job can be ingested and
// streamed from more than one gateway replica.
type JobStream interface {
	// AppendJobStreamEvent durably appends one event to the Job's ordered
	// journal, enforcing:
	//   - idempotent replay: an event identical to the one already stored at
	//     that sequence is a silent no-op;
	//   - ErrStreamSequenceConflict if that sequence already holds different
	//     content ("sequence substitution");
	//   - ErrConflict if the sequence is not exactly the next expected one
	//     (out-of-order/gapped append);
	//   - ErrStreamOffsetInvalid / ErrStreamDigestInvalid if an
	//     JobEventOutputChunk event's offset or cumulative stream digest does
	//     not match what the store independently computes from the prior
	//     durable chunks;
	//   - ErrStreamTerminal if a terminal event was already recorded for
	//     this Job (no events are accepted after terminal).
	AppendJobStreamEvent(ctx context.Context, event domain.JobEvent) error
	// JobStreamEvents returns durable events for jobID with
	// sequence >= fromSequence, oldest first. limit<=0 means no limit.
	JobStreamEvents(ctx context.Context, jobID string, fromSequence uint64, limit int) ([]domain.JobEvent, error)
	// JobStreamCursor returns the current durable resume cursor for jobID.
	// found is false if no event has ever been appended for this Job.
	JobStreamCursor(ctx context.Context, jobID string) (cursor domain.JobStreamCursor, found bool, err error)
	// SetJobStreamUpstreamDigest durably records the execution provider's
	// retained-output identity digest (JobEvent.UpstreamRetainedDigest) the
	// first time it is observed for a Job, creating the cursor row if it
	// does not exist yet. It is safe to call before any event has been
	// appended, and safe to call repeatedly with the same value. A digest
	// is a stable property of a Job's immutable completed output, so
	// observing a *different* non-empty digest for the same Job is a
	// provider-consistency error, not a benign race. Passing an empty
	// digest is a no-op.
	SetJobStreamUpstreamDigest(ctx context.Context, jobID, digest string) error
	// LastJobStreamChunkBefore returns the most recent JobEventOutputChunk
	// event with sequence < beforeSequence, if any. Cumulative offset/digest
	// state only changes on OutputChunk events -- every other event type
	// (STATE, PROOF_STATUS, TERMINAL, ...) passes the current cumulative
	// state through unchanged -- so this is the correct way to recover "the
	// stream's cumulative state as of beforeSequence" regardless of which
	// event type happens to sit immediately before it. found is false if no
	// OutputChunk event exists before beforeSequence (the state is still
	// offset 0 / no digest).
	LastJobStreamChunkBefore(ctx context.Context, jobID string, beforeSequence uint64) (event domain.JobEvent, found bool, err error)
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
	Reserve(ctx context.Context, principalID, key, requestHash string, leaseUntil time.Time) (rec IdempotencyRecord, ok bool, err error)
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
	JobStream
	Accounts
	Artifacts
	Idempotency
}
