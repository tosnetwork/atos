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
	// For a JobEventOutputChunk event carrying a non-empty
	// UpstreamRetainedDigest, the execution provider's retained-output
	// identity digest is also validated/recorded (set-once; a later,
	// different non-empty value is a provider-consistency error) in the
	// exact same transaction/write as the event itself -- never as a
	// separate, independently-committed step, so a crash or a rejected
	// event can never leave the identity digest durably set without the
	// event that justified it.
	AppendJobStreamEvent(ctx context.Context, event domain.JobEvent) error
	// JobStreamEvents returns durable events for jobID with
	// sequence >= fromSequence, oldest first. limit<=0 means no limit.
	JobStreamEvents(ctx context.Context, jobID string, fromSequence uint64, limit int) ([]domain.JobEvent, error)
	// JobStreamCursor returns the current durable resume cursor for jobID.
	// found is false if no event has ever been appended for this Job.
	JobStreamCursor(ctx context.Context, jobID string) (cursor domain.JobStreamCursor, found bool, err error)
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

// Billing persists the durable, auditable metered billing calculation for
// each Job. computeBillingSnapshot (internal/service/billing.go) is a pure
// function of already-durable, immutable inputs (the frozen Quote and the
// verified Execution Receipt), so recomputing and re-persisting an
// identical snapshot for the same JobID is always a safe no-op -- exactly
// what makes it safe to call from crash recovery.
type Billing interface {
	// PutBillingSnapshot idempotently persists snap, keyed by JobID (one
	// snapshot per Job). If a snapshot already exists for this JobID with
	// identical economic content (every field except CalculatedAt),
	// implementations MUST return that stored snapshot with created=false
	// and a nil error -- a safe no-op recomputation. If a snapshot already
	// exists with DIFFERENT economic content, implementations MUST return
	// domain.ErrIdempotencyConflict rather than silently keeping the old
	// value or silently accepting the new one: a Job's billing may never be
	// recomputed to a different result once persisted.
	PutBillingSnapshot(ctx context.Context, snap domain.BillingSnapshot) (stored domain.BillingSnapshot, created bool, err error)
	BillingSnapshotByJob(ctx context.Context, jobID string) (domain.BillingSnapshot, error)
}

// Earnings persists the durable provider earnings ledger and the idempotent
// external-payout state machine folded into each ProviderEarning record.
type Earnings interface {
	// CreateEarning atomically creates a ProviderEarning uniquely bound to
	// e.SettlementID. If an earning already exists for that settlement
	// (enforced by a database uniqueness constraint, not a Get+Put race)
	// with IDENTICAL identity+economic fields (ProviderID, JobID, QuoteID,
	// ReceiptID, SettlementID, CapabilityID, CapabilityVersion,
	// GrossAmount, GatewayFee, NetAmount -- not lifecycle fields like
	// Status/CreatedAt/MaturesAt, which legitimately differ between the
	// original create and a later retry), the existing record is returned
	// with created=false and a nil error -- this is what makes earning
	// creation from crash recovery or a duplicate reconciler sweep safe to
	// retry without ever creating a second earning for the same
	// settlement. If an earning already exists for that settlement with
	// DIFFERENT identity+economic fields, implementations MUST return
	// domain.ErrIdempotencyConflict rather than silently returning the
	// stale row.
	CreateEarning(ctx context.Context, e domain.ProviderEarning) (out domain.ProviderEarning, created bool, err error)
	GetEarning(ctx context.Context, id string) (domain.ProviderEarning, error)
	EarningBySettlement(ctx context.Context, settlementID string) (domain.ProviderEarning, error)
	// EarningByJob returns the ProviderEarning for jobID. Used by the
	// dispute workflow, which is keyed by Job (the principal-facing
	// identity) rather than SettlementID.
	EarningByJob(ctx context.Context, jobID string) (domain.ProviderEarning, error)
	EarningsByProvider(ctx context.Context, providerID string) ([]domain.ProviderEarning, error)
	// EarningsMaturing returns EarningMaturing earnings with matures_at <=
	// before, oldest first, for the maturation sweep.
	EarningsMaturing(ctx context.Context, before time.Time, limit int) ([]domain.ProviderEarning, error)
	// EarningsAvailableForPayout returns EarningAvailable earnings ready to
	// begin the payout state machine.
	EarningsAvailableForPayout(ctx context.Context, limit int) ([]domain.ProviderEarning, error)
	// EarningsPayoutPending returns EarningPayoutPending earnings whose
	// payout_requested_at <= before, for payout crash recovery: a process
	// that died between committing the payout_pending intent and recording
	// its outcome leaves the earning here until reconciliation replays it.
	EarningsPayoutPending(ctx context.Context, before time.Time, limit int) ([]domain.ProviderEarning, error)
	// SettledJobsMissingEarning returns Jobs whose economic settlement is
	// already durably finalized (EconomicState == EconomicSettled) but
	// which have no ProviderEarning yet -- the crash window between
	// committing a settlement and creating its earning. Used by
	// EarningsService's backfill sweep; safe to call repeatedly since
	// RecordSettlement is idempotent.
	SettledJobsMissingEarning(ctx context.Context, limit int) ([]domain.Job, error)
	// UpdateEarning atomically applies fn to the earning's current stored
	// state (or domain.ProviderEarning{} with exists=false if it isn't
	// stored yet) and persists whatever fn returns, mirroring
	// store.Jobs.UpdateJob's compare-and-swap pattern. Implementations MUST
	// reject (domain.ErrIdempotencyConflict) a returned value whose ID, or
	// whose identity/economic fields (ProviderID, SettlementID,
	// GrossAmount, GatewayFee, NetAmount, ...), differ from the existing
	// stored earning -- only lifecycle fields (Status, timestamps, payout
	// checkpoints) may change through this method.
	UpdateEarning(ctx context.Context, id string, fn func(e domain.ProviderEarning, exists bool) (domain.ProviderEarning, error)) (domain.ProviderEarning, error)
}

// Disputes persists the durable Managed-dispute workflow and its economic
// recovery checkpoints. See domain.Dispute's doc comment for the
// immutability contract every implementation must uphold.
type Disputes interface {
	// OpenDispute atomically resolves the ProviderEarning bound to jobID
	// (row-locked in the same transaction the dispute row is created in)
	// and applies build to decide the dispute's initial durable state
	// together with the earning's next state -- see
	// domain.DisputeEconomicState's doc comments for the three possible
	// branches (frozen / pending_payout_resolution / paid) build must
	// choose between based on the earning's current, row-locked status.
	// This is what makes "opening a dispute atomically freezes provider
	// funds" (or correctly defers to pending_payout_resolution/paid when
	// it cannot) a single durable transaction rather than two operations
	// with a crash window between them.
	//
	// If a dispute already exists for jobID, build is never called and the
	// existing dispute is returned with created=false and a nil error --
	// enforced by a database UNIQUE(job_id) constraint, not a
	// service-layer race, so "at most one dispute per Job" holds even
	// under 8+ concurrent callers or two independent ATOS replicas.
	OpenDispute(ctx context.Context, jobID string, build func(earning domain.ProviderEarning, earningExists bool) (domain.Dispute, domain.ProviderEarning, error)) (dispute domain.Dispute, earning domain.ProviderEarning, created bool, err error)
	GetDispute(ctx context.Context, id string) (domain.Dispute, error)
	DisputeByJob(ctx context.Context, jobID string) (domain.Dispute, error)
	DisputeByIdempotencyKey(ctx context.Context, principalID, key string) (domain.Dispute, error)
	DisputesByPrincipal(ctx context.Context, principalID string) ([]domain.Dispute, error)
	DisputesByProvider(ctx context.Context, providerID string) ([]domain.Dispute, error)
	// DisputesUnderReview returns disputes still awaiting a review
	// decision (ReviewStatus opened or under_review), oldest first, for
	// the reviewer queue.
	DisputesUnderReview(ctx context.Context, limit int) ([]domain.Dispute, error)
	// DisputesForRecovery returns disputes whose economic recovery is not
	// yet terminal (see domain.DisputeEconomicState.Terminal) and were
	// last updated before updatedBefore, for the dispute reconciler.
	DisputesForRecovery(ctx context.Context, updatedBefore time.Time, limit int) ([]domain.Dispute, error)
	// UpdateDispute atomically applies fn to the dispute's current stored
	// state and persists whatever fn returns, mirroring UpdateJob/
	// UpdateEarning's compare-and-swap pattern. Implementations MUST
	// reject (domain.ErrIdempotencyConflict) a returned value whose ID or
	// whose identity/economic fields (see domain.Dispute's doc comment)
	// differ from the existing stored dispute -- only lifecycle fields
	// (ReviewStatus, EconomicState, Outcome, ReviewerID, ReasonRejected,
	// timestamps) may change through this method.
	UpdateDispute(ctx context.Context, id string, fn func(d domain.Dispute, exists bool) (domain.Dispute, error)) (domain.Dispute, error)
	// UpdateDisputeAndEarning atomically applies fn to the dispute AND the
	// ProviderEarning it references (both row-locked in the same
	// transaction), for resolution transitions that must durably commit
	// both together in one instruction (reversing/releasing the earning
	// together with the dispute's own checkpoint). Same immutability
	// contract as UpdateDispute for the dispute side, and the same as
	// UpdateEarning for the earning side.
	UpdateDisputeAndEarning(ctx context.Context, disputeID string, fn func(d domain.Dispute, e domain.ProviderEarning, earningExists bool) (domain.Dispute, domain.ProviderEarning, error)) (domain.Dispute, domain.ProviderEarning, error)
	// ResolveDispute atomically row-locks the dispute, the ProviderEarning
	// it references (found by job_id), and the principal's Account (in
	// that order: dispute, then earning, then account) and commits fn's
	// result to all three in one transaction. This is the sole primitive
	// DisputeService.Resolve uses for every outcome (principal, provider,
	// rejected), so a principal-win's earning reversal and account credit
	// can never be split across two transactions with a crash window
	// between them -- there is no intermediate durable state between
	// "frozen" and "refunded" to recover from, because that transition can
	// never be observed partially applied. Same immutability contract as
	// UpdateDispute/UpdateEarning for the dispute/earning sides; fn MUST
	// return an Account whose PrincipalID equals principalID even when
	// leaving the account otherwise unchanged (provider-win/rejected).
	ResolveDispute(ctx context.Context, disputeID, principalID string, seed domain.Account, fn func(d domain.Dispute, e domain.ProviderEarning, earningExists bool, a domain.Account, accountExists bool) (domain.Dispute, domain.ProviderEarning, domain.Account, error)) (domain.Dispute, domain.ProviderEarning, domain.Account, error)
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
	Billing
	Earnings
	Disputes
	Artifacts
	Idempotency
}
