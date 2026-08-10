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
	// QuoteByIdempotencyKey returns the Quote previously created under
	// (principalID, key), the QuoteService.Create counterpart to
	// JobByIdempotencyKey -- used to recover a durable idempotency-record
	// commit that crashed before Finish ran, without minting a second
	// Quote for the same caller-supplied idempotency key.
	QuoteByIdempotencyKey(ctx context.Context, principalID, key string) (domain.Quote, error)
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
	// JobsByProvider returns every Job whose ProviderID matches providerID,
	// newest first -- the provider-side counterpart to JobsByPrincipal, for
	// atos_provider_jobs.
	JobsByProvider(ctx context.Context, providerID string) ([]domain.Job, error)
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
	// begin the payout state machine. Earnings with a non-empty
	// DisputeHoldID are excluded even though their Status is Available --
	// beginPayoutUnderLock would no-op on them anyway, but excluding them
	// here keeps a batch of held earnings from head-of-line-blocking
	// genuinely payable ones behind the reconciler's limit.
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
	// OpenDispute atomically resolves the ProviderEarning bound to
	// settlementID (row-locked, by its real UNIQUE identity -- never by
	// job_id, which carries no uniqueness guarantee -- in the same
	// transaction the dispute row is created in) and applies build to
	// decide the dispute's initial durable state together with the
	// earning's next state -- see domain.DisputeEconomicState's doc
	// comments for the three possible branches (frozen /
	// pending_payout_resolution / paid) build must choose between based on
	// the earning's current, row-locked status. This is what makes
	// "opening a dispute atomically freezes provider funds" (or correctly
	// defers to pending_payout_resolution/paid when it cannot) a single
	// durable transaction rather than two operations with a crash window
	// between them.
	//
	// If a dispute already exists for jobID, build is never called and the
	// existing dispute is returned with created=false and a nil error --
	// enforced by a database UNIQUE(job_id) constraint, not a
	// service-layer race, so "at most one dispute per Job" holds even
	// under 8+ concurrent callers or two independent ATOS replicas.
	OpenDispute(ctx context.Context, jobID, settlementID string, build func(earning domain.ProviderEarning, earningExists bool) (domain.Dispute, domain.ProviderEarning, error)) (dispute domain.Dispute, earning domain.ProviderEarning, created bool, err error)
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
	// ProviderEarning it references -- both row-locked in the same
	// transaction, the earning by the dispute's own immutable EarningID
	// (never re-derived from job_id) -- for resolution transitions that
	// must durably commit both together in one instruction (reversing/
	// releasing the earning together with the dispute's own checkpoint).
	// Same immutability contract as UpdateDispute for the dispute side,
	// and the same as UpdateEarning for the earning side.
	UpdateDisputeAndEarning(ctx context.Context, disputeID string, fn func(d domain.Dispute, e domain.ProviderEarning, earningExists bool) (domain.Dispute, domain.ProviderEarning, error)) (domain.Dispute, domain.ProviderEarning, error)
	// ResolveDispute atomically row-locks the dispute, the ProviderEarning
	// it references (by the dispute's own immutable EarningID, never
	// re-derived from job_id), and the principal's Account (in
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

// ProviderHealth stores the single most recent observed health result per
// (capability_id, capability_version, transport). Health is a point-in-time
// probe with no history worth preserving -- unlike Disputes/Certifications
// there is no idempotency or immutability contract here, only overwrite.
type ProviderHealth interface {
	// PutHealthCheck upserts the most recent health result for the
	// (CapabilityID, CapabilityVersion, Transport) triple in check.
	PutHealthCheck(ctx context.Context, check domain.AdapterHealthCheck) error
	// HealthCheck returns the most recently recorded health for that
	// binding, if any has ever been recorded. found=false is a normal,
	// expected state for a binding never checked yet -- never an error.
	HealthCheck(ctx context.Context, capabilityID, capabilityVersion string, transport domain.EndpointAdapterType) (check domain.AdapterHealthCheck, found bool, err error)
}

// Certifications stores durable, idempotent sandbox-certification records.
// Passing certification is readiness evidence only -- see
// domain.SandboxCertification's doc comment -- and no method here mutates
// domain.ModeSupport or SupportedTrustModes; that activation path does not
// exist yet (Phase 3B).
type Certifications interface {
	// OpenCertification idempotently creates a certification record keyed
	// by (providerID, idempotencyKey): the first call for that pair
	// creates cert (created=true); a replay with the SAME key and
	// identical semantic content (see domain.SandboxCertification's
	// content-hash contract, mirrored on the Postgres implementation via
	// certificationContentHash) returns the existing record unchanged
	// (created=false, nil error); a replay with the SAME key but
	// DIFFERENT content returns domain.ErrIdempotencyConflict.
	OpenCertification(ctx context.Context, providerID string, cert domain.SandboxCertification) (domain.SandboxCertification, bool, error)
	GetCertification(ctx context.Context, id string) (domain.SandboxCertification, error)
	CertificationByIdempotencyKey(ctx context.Context, providerID, key string) (domain.SandboxCertification, error)
	// CertificationsByCapability returns every certification ever opened
	// for capabilityID, newest first.
	CertificationsByCapability(ctx context.Context, capabilityID string) ([]domain.SandboxCertification, error)
	// UpdateCertification atomically applies fn to the certification's
	// current stored state. Implementations MUST reject
	// (domain.ErrIdempotencyConflict) a returned value whose ID or
	// identity fields (ProviderID/CapabilityID/CapabilityVersion/
	// Transport/EndpointRef/IdempotencyKey) differ from the existing
	// stored record -- only Status/FailureReason/Evidence/CompletedAt/
	// UpdatedAt may change through this method, exactly mirroring
	// UpdateDispute's own immutability contract.
	UpdateCertification(ctx context.Context, id string, fn func(c domain.SandboxCertification, exists bool) (domain.SandboxCertification, error)) (domain.SandboxCertification, error)
}

// ExecutionSignerOperations stores the durable execution-signer
// authorize/rotate/revoke journal (atos-spec
// docs/IMPLEMENTATION_ROADMAP.md §7.2.2) -- see
// domain.ExecutionSignerOperation's doc comment for the full checkpoint
// sequence this exists to make crash-recoverable.
type ExecutionSignerOperations interface {
	// OpenSignerOperation idempotently creates an operation record keyed
	// by (providerID, idempotencyKey), mirroring OpenCertification's
	// contract exactly: first call creates op (created=true); a replay
	// with the SAME key and identical identity content returns the
	// existing record unchanged (created=false, nil error); a replay with
	// the SAME key but DIFFERENT identity content returns
	// domain.ErrIdempotencyConflict.
	OpenSignerOperation(ctx context.Context, providerID string, op domain.ExecutionSignerOperation) (domain.ExecutionSignerOperation, bool, error)
	// OpenSignerOperationForCapability combines two steps
	// Authorize/Revoke/Rotate each need when opening a genuinely NEW
	// operation (never for resuming an idempotency-key replay -- see
	// service.ExecutionSignerService.resumeOrConflict) into one atomic
	// sequence under a single lock scoped to (capabilityID,
	// capabilityVersion):
	//
	//  1. Determine the current execution signer -- exactly what
	//     LatestCompletedSignerOperationByCapability plus the
	//     authorize/rotate-vs-revoke derivation in
	//     service.ExecutionSignerService.CurrentSigner would report --
	//     at a single consistent snapshot.
	//  2. Call build with that snapshot to obtain the operation to open,
	//     then open it exactly as OpenSignerOperation would (same
	//     idempotency semantics: created=true on first open,
	//     created=false and the existing record on a same-content
	//     replay, domain.ErrIdempotencyConflict on a same-key
	//     different-content replay).
	//
	// This exists to close a real race: without a lock spanning BOTH
	// steps, two concurrent callers using DIFFERENT idempotency keys
	// (e.g. two independent Rotate calls) can each read the same current
	// signer before either has persisted an operation that would change
	// it, then each independently authorize and complete a DIFFERENT new
	// signer -- both genuinely valid at the remote authority, but only
	// one ever visible as "current" through this store, leaving the
	// other an orphaned authorization no caller can name or revoke.
	// Reading the current signer and opening the operation as two
	// separate store calls (as an earlier version of this codebase did)
	// cannot close this race no matter how each individual call is
	// locked internally, because the race is in the GAP between the two
	// calls, not within either one.
	//
	// Locking the read-then-open sequence alone is ALSO not sufficient:
	// it prevents two concurrent callers from reading current-signer at
	// the exact same instant, but not two callers that read it in quick
	// SUCCESSION, before the first one's operation reaches Completed --
	// the second would still see the same stale current signer and open
	// a second, independently-completable operation against it.
	// Implementations MUST additionally reject a new open with
	// domain.ErrSignerOperationInProgress (retryable) while ANY
	// non-terminal operation already exists for (capabilityID,
	// capabilityVersion), which is what actually closes that window: the
	// second caller is forced to fail and retry after the first either
	// completes (and it will then correctly see the new signer as
	// current) or is itself recovered by the reconciler.
	//
	// build returning an error aborts the whole sequence without opening
	// anything (e.g. "no current signer to rotate").
	OpenSignerOperationForCapability(
		ctx context.Context, providerID, capabilityID, capabilityVersion string,
		build func(currentAuthorizationID, currentExecutionSignerID string, found bool) (domain.ExecutionSignerOperation, error),
	) (op domain.ExecutionSignerOperation, created bool, err error)
	GetSignerOperation(ctx context.Context, id string) (domain.ExecutionSignerOperation, error)
	SignerOperationByIdempotencyKey(ctx context.Context, providerID, key string) (domain.ExecutionSignerOperation, error)
	// LatestSignerOperationByCapability returns the most recently updated
	// operation for capabilityID, if any -- including a non-terminal one
	// still in flight or reconciling. This is the status-query view (so a
	// caller can see "a rotation is in progress"), NOT the basis for
	// "what is the current execution signer" -- see
	// LatestCompletedSignerOperationByCapability for that, which a
	// non-terminal latest operation must never override (§7.2.2: the old
	// signer remains authoritative until a rotation's new signer is
	// durably new_authorized, and MUST NOT appear to have no signer at
	// all just because a later operation is stuck).
	LatestSignerOperationByCapability(ctx context.Context, capabilityID string) (domain.ExecutionSignerOperation, bool, error)
	// LatestCompletedSignerOperationByCapability returns the most
	// recently updated COMPLETED operation for (capabilityID,
	// capabilityVersion), if any -- the sole basis for "what is the
	// current execution signer" (the most recent completed
	// authorize/rotate's NewExecutionSignerID, or none if the most recent
	// completed operation is a revoke). A newer non-terminal operation (in
	// flight or reconciling) never changes this answer until it too
	// reaches Completed.
	//
	// capabilityVersion MUST be filtered in the query itself, not applied
	// afterward by the caller: signer authorization currency is
	// version-scoped (a capability version bump resets it), and a
	// completed operation for a SUPERSEDED version can still have a more
	// recent updated_at than the current version's own completed
	// operation -- e.g. a stuck v1 rotation that a reconciler only
	// finishes recovering after a v2 signer was already authorized and
	// completed. Selecting "most recently updated, completed, ANY
	// version" and rejecting it in Go after the fact (an earlier version
	// of this method did exactly that) answers "no current signer" in
	// that case even though a real, current v2 signer exists -- it never
	// looks past the wrong version's row to find it.
	LatestCompletedSignerOperationByCapability(ctx context.Context, capabilityID, capabilityVersion string) (domain.ExecutionSignerOperation, bool, error)
	// StaleSignerOperations returns up to limit non-terminal operations
	// (checkpoint != completed) last updated before cutoff, oldest first
	// -- the reconciler's sweep query.
	StaleSignerOperations(ctx context.Context, cutoff time.Time, limit int) ([]domain.ExecutionSignerOperation, error)
	// UpdateSignerOperation atomically applies fn to the operation's
	// current stored state. Implementations MUST reject
	// (domain.ErrIdempotencyConflict) a returned value whose ID or
	// identity fields (ProviderID/CapabilityID/CapabilityVersion/Type/
	// IdempotencyKey/new-signer identity/old-authorization identity/
	// RevocationReasonCode) differ from the existing stored record -- only
	// Checkpoint/NewAuthorizationRef/FailureReason/CompletedAt/UpdatedAt
	// may change through this method, exactly mirroring
	// UpdateCertification's own immutability contract. RevocationReasonCode
	// moved into the identity set alongside the fields it was always meant
	// to travel with: it is caller-supplied content for Revoke/Rotate that
	// feeds tos-protocol's own commitment digest, so a replay that
	// supplies a different reason is a different logical request, exactly
	// like a different new-signer identity would be -- advance() (the sole
	// production caller) never touches it, so this tightening changes no
	// existing behavior.
	UpdateSignerOperation(ctx context.Context, id string, fn func(op domain.ExecutionSignerOperation, exists bool) (domain.ExecutionSignerOperation, error)) (domain.ExecutionSignerOperation, error)
}

// OpenTasks is Phase 3C's store surface (atos-spec docs/IMPLEMENTATION_ROADMAP.md
// §7.3). The AcceptanceOperation half of this interface mirrors
// ExecutionSignerOperations deliberately: OpenAcceptanceOperation plays
// exactly the role OpenSignerOperationForCapability plays for Phase 3B,
// including the same two-invariant defense (an in-flight, non-terminal
// operation lock scoped by TaskID, PLUS a caller-idempotency-key dedup
// scoped by (PrincipalID, IdempotencyKey)) for the same reason: locking only
// the read-then-open sequence is not sufficient to prevent two callers who
// read "no winner yet" in quick succession from each independently claiming
// a winner for the same task.
type OpenTasks interface {
	// PutOpenTask inserts a new OpenTask row. Insert-only (ON CONFLICT DO
	// NOTHING on id, mirroring PutQuote) -- every subsequent mutation goes
	// through UpdateOpenTask or OpenAcceptanceOperation's winner-claim step,
	// never a second PutOpenTask.
	PutOpenTask(ctx context.Context, t domain.OpenTask) error
	GetOpenTask(ctx context.Context, id string) (domain.OpenTask, error)
	// OpenTaskByIdempotencyKey recovers a Publish call that committed the
	// OpenTask row but crashed before its idempotency record was marked
	// Finished -- the OpenTask counterpart to JobByIdempotencyKey/
	// QuoteByIdempotencyKey.
	OpenTaskByIdempotencyKey(ctx context.Context, principalID, key string) (domain.OpenTask, error)
	// OpenTasksByPrincipal returns every OpenTask owned by principalID
	// (any status), newest first -- the owner's own "my tasks" view, which
	// unlike the public listing includes Input and every other
	// owner-visible field.
	OpenTasksByPrincipal(ctx context.Context, principalID string) ([]domain.OpenTask, error)
	// ListPublicOpenTasks returns up to limit OpenTasks with Status=Open,
	// newest first, for marketplace search/browse. Callers MUST still call
	// domain.OpenTask.Public() on each result before returning it outside
	// the service layer -- this query filters by status only, it does not
	// redact fields. An expired-but-still-Open row (ExpiresAt passed, no
	// reconciler sweep has caught up yet) may still appear here; callers
	// that care must check Expired(now) themselves, exactly like
	// domain.Quote.Expired's lazy-check convention.
	ListPublicOpenTasks(ctx context.Context, limit int) ([]domain.OpenTask, error)
	// UpdateOpenTask atomically applies fn to the task's current stored
	// state (or domain.OpenTask{} with exists=false if it isn't stored yet)
	// and persists whatever fn returns -- the compare-and-swap primitive
	// for Cancel and expiry-sweep transitions. Implementations MUST NOT
	// use this method to claim a winner (set AcceptedProposalID) --
	// OpenAcceptanceOperation is the only path that may do that, since only
	// it also enforces the non-terminal-operation and idempotency-key
	// invariants in the same atomic step.
	UpdateOpenTask(ctx context.Context, id string, fn func(t domain.OpenTask, exists bool) (domain.OpenTask, error)) (domain.OpenTask, error)

	// PutOpenTaskProposal inserts a new proposal row. Insert-only, like
	// PutOpenTask.
	PutOpenTaskProposal(ctx context.Context, p domain.OpenTaskProposal) error
	GetOpenTaskProposal(ctx context.Context, id string) (domain.OpenTaskProposal, error)
	// OpenTaskProposalByIdempotencyKey mirrors OpenTaskByIdempotencyKey,
	// scoped by (providerID, key) instead of (principalID, key) -- a
	// proposal's caller identity is the applying provider, not the task
	// owner.
	OpenTaskProposalByIdempotencyKey(ctx context.Context, providerID, key string) (domain.OpenTaskProposal, error)
	// ProposalsByTask returns every proposal submitted against taskID
	// (including withdrawn ones -- the service layer, not this query,
	// decides what a given viewer may see), newest first.
	ProposalsByTask(ctx context.Context, taskID string) ([]domain.OpenTaskProposal, error)
	// UpdateOpenTaskProposal is Withdraw's CAS primitive, exactly like
	// UpdateOpenTask is Cancel's. Implementations MUST NOT allow this
	// method to change ID/TaskID/ProviderID/CapabilityID/CapabilityVersion/
	// ProposalIdempotencyKey -- only WithdrawnAt/UpdatedAt may change
	// through this method.
	UpdateOpenTaskProposal(ctx context.Context, id string, fn func(p domain.OpenTaskProposal, exists bool) (domain.OpenTaskProposal, error)) (domain.OpenTaskProposal, error)

	// OpenAcceptanceOperation is Accept's single atomic entry point. Under
	// one lock scoped to taskID, it must:
	//
	//  1. Reject (domain.ErrOpenTaskAcceptanceInProgress, retryable) if a
	//     non-terminal (checkpoint not in {completed,failed})
	//     AcceptanceOperation already exists for taskID -- the same
	//     "in-flight lock" hasNonTerminalSignerOperationTx enforces for
	//     Phase 3B, and for the identical reason: reading "no winner yet"
	//     and opening a new operation are not atomic with each other
	//     unless something also forbids a second opener while the first
	//     is still in flight.
	//  2. Load the current OpenTask row (the snapshot build observes).
	//  3. Call build(task) to construct the operation to open. build MUST
	//     NOT call back into the store (a nested call would deadlock the
	//     in-memory implementation's single non-reentrant lock, and would
	//     read a separate, non-transactional snapshot through the
	//     Postgres implementation's connection pool instead of
	//     participating in the same transaction) -- every validation that
	//     depends on something OTHER than this task snapshot (proposal
	//     ownership/version, capability-still-active) must be done by the
	//     caller BEFORE calling this method, using its own store calls;
	//     build itself checks only task.Status==Open / not Expired
	//     against the snapshot it was handed, and returns a definitive
	//     (non-retryable) error to abort the whole sequence without
	//     opening or claiming anything.
	//  4. In the SAME transaction as the operation insert: claim the
	//     winner by setting task.AcceptedProposalID=op.ProposalID and
	//     task.Status=Accepted. This is what makes "exactly one accepted
	//     proposal" a database guarantee rather than an in-process
	//     check -- the claim and the operation's opening can never
	//     observably happen without each other.
	//
	// Idempotency-key replay semantics (scoped by (op.PrincipalID,
	// op.IdempotencyKey) as build returns them) mirror
	// OpenSignerOperationForCapability's own openSignerOperationTx step
	// exactly: same key + identical identity content -> the existing
	// operation, created=false; same key + different content ->
	// domain.ErrIdempotencyConflict. This check runs AFTER build(), not
	// before -- see the implementation's doc comment for why a genuine
	// SEQUENTIAL replay (the caller's original call already completed) must
	// instead be caught by service.OpenTaskService.Accept calling
	// AcceptanceOperationByIdempotencyKey BEFORE ever calling this method,
	// exactly mirroring service.ExecutionSignerService.Authorize's
	// resumeOrConflict-before-Open pattern -- build()'s own validation
	// (task.Status==Open) is not safe to run against a task a prior,
	// already-completed call to THIS SAME idempotency key already moved
	// past Open.
	OpenAcceptanceOperation(
		ctx context.Context, taskID string,
		build func(task domain.OpenTask) (domain.AcceptanceOperation, error),
	) (op domain.AcceptanceOperation, task domain.OpenTask, created bool, err error)
	GetAcceptanceOperation(ctx context.Context, id string) (domain.AcceptanceOperation, error)
	AcceptanceOperationByIdempotencyKey(ctx context.Context, principalID, key string) (domain.AcceptanceOperation, error)
	// AcceptanceOperationByTask returns the most recently updated operation
	// for taskID, if any -- including a non-terminal one still in flight.
	// A task can have more than one operation across its lifetime only if
	// an earlier one reached Failed and reopened the task (see
	// domain.AcceptanceFailed's doc comment); this always returns the
	// latest.
	AcceptanceOperationByTask(ctx context.Context, taskID string) (domain.AcceptanceOperation, bool, error)
	// StaleAcceptanceOperations returns up to limit non-terminal operations
	// last updated before cutoff, oldest first -- the reconciler's sweep
	// query, mirroring StaleSignerOperations exactly.
	StaleAcceptanceOperations(ctx context.Context, cutoff time.Time, limit int) ([]domain.AcceptanceOperation, error)
	// UpdateAcceptanceOperation atomically applies fn to the operation's
	// current stored state. Implementations MUST reject
	// (domain.ErrIdempotencyConflict) a returned value whose ID or identity
	// fields (TaskID/ProposalID/PrincipalID/ProviderID/CapabilityID/
	// CapabilityVersion/IdempotencyKey) differ from the existing stored
	// record -- only Checkpoint/QuoteID/JobID/FailureReason/CompletedAt/
	// UpdatedAt may change through this method, mirroring
	// UpdateSignerOperation's own immutability contract. This method never
	// touches the OpenTask row itself -- reaching Completed or Failed is
	// reflected onto the OpenTask (BoundQuoteID/BoundJobID/Status) by the
	// service layer's own subsequent UpdateOpenTask call, since only the
	// service layer knows the full reopen-on-Failed transition.
	UpdateAcceptanceOperation(ctx context.Context, id string, fn func(op domain.AcceptanceOperation, exists bool) (domain.AcceptanceOperation, error)) (domain.AcceptanceOperation, error)
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
	ProviderHealth
	Certifications
	ExecutionSignerOperations
	OpenTasks
	Idempotency
}
