# ATOS v0.2 Implementation Status

This document tracks the Go gateway against `tosnetwork/atos-spec` v0.2.

## Phase 0 — Contract First

**Code status: ✅ complete.**

Implemented and tested:

- ✅ one Capability/Quote/Invocation/Job/Escrow/Receipt/Settlement model shared by REST, MCP and A2A;
- ✅ `requested_trust_mode` separated from concrete `trust_mode`;
- ✅ `auto` accepted only as pre-Quote policy and rejected as committed state;
- ✅ normative `managed`/no-profile, `verified`/`tos_verified_v1` and `native`/`tos_native_v1` pairs;
- ✅ provider `requested_trust_modes` separated from derived active `supported_trust_modes`;
- ✅ immutable Quote mode/profile propagation through Job, Escrow, Receipt and Settlement;
- ✅ no execution-time mode override and no silent downgrade;
- ✅ explicit delegated execution-signer fields and proof references;
- ✅ federation-safe identity and commitment fields modeled without moving bulk or private payloads on-chain;
- ✅ schema, OpenAPI and conformance tests, including one common Quote API shape across all three concrete resolved modes.

Remaining Phase 0 work is maintenance rather than missing behavior: future schema changes must keep generated artifacts and conformance vectors synchronized.

## Phase 1 — Codex-First Managed MVP

**Code status: ✅ complete.**

Implemented and tested:

- ✅ embedded installable Skill served from `/skills/atos/SKILL.md`;
- ✅ OAuth-style Device Authorization with pending approval, browser consent, polling intervals, `slow_down`, denial, expiry and bounded codes;
- ✅ scoped access tokens, rotating refresh tokens, revocation and durable owner-private bbolt auth state;
- ✅ trusted consent decisions derive the principal from the authenticated `X-ATOS-Principal-ID` boundary rather than a caller-supplied JSON field;
- ✅ stateless Streamable HTTP MCP with nine deterministic ordinary consumer tools;
- ✅ Capability search/retrieval, Commercial Quote, invocation, jobs and account;
- ✅ PostgreSQL capability/search/business storage with complete v0.2 JSON payloads;
- ✅ configurable Managed internal-credit balance, per-call limit and daily limit;
- ✅ server-issued, exact-request-bound spending confirmations for calls above automatic policy limits;
- ✅ rejection of caller-supplied confirmation booleans;
- ✅ Managed reservation, execution, signed receipt, verification and settlement;
- ✅ explicit `ATOS_TOS_BACKEND=mock|rpc` selection with no failure fallback;
- ✅ real `atos -> tos-protocol -> private tos-ai Worker` Managed RPC path, verified end-to-end against a live tos-protocol RPC server (`TestATOSConnectRPCManagedLifecycle`), not only the deterministic mock backend;
- ✅ signed-URL Artifact transport with ownership, size and expiry enforcement;
- ✅ idempotency leases, stale-reservation recovery and a unique `(principal_id, idempotency_key)` Job constraint;
- ✅ production configuration gates requiring PostgreSQL, HTTPS public URL, explicit user approval, durable auth state and the RPC backend;
- ✅ a full HTTP acceptance test from Skill retrieval and authorization through search, Quote, payment-policy handling, invocation, Receipt/settlement and exact balance mutation.

### Crash-safe Managed economic state machine

**Status: ✅ complete**, independently re-reviewed and verified against a live PostgreSQL 16 instance.

Managed economic mutations now use explicit durable private checkpoints:

```text
none
  -> debited
  -> escrow_pending
  -> escrow_reserved
  -> settlement_pending
  -> settled

failure/cancellation path:
  escrow_pending / escrow_reserved
  -> release_pending
  -> released
```

The checkpoints are intentionally internal implementation state rather than new public ATOS protocol states.

Crash-safety guarantees implemented by the gateway include:

- ✅ Account debit and its Job `debited` checkpoint commit in one storage transaction;
- ✅ Account refund/credit and the corresponding terminal Job checkpoint commit in one storage transaction;
- ✅ PostgreSQL transaction-scoped advisory locks serialize Account, Job and idempotency mutation identities even before a logical row exists;
- ✅ `working` is not persisted until the exact escrow has been created/recovered and its reference is durable;
- ✅ `escrow_pending` means an external CreateEscrow result may be ambiguous and therefore MUST be recovered by idempotent replay rather than guessed or immediately refunded;
- ✅ Job-to-Escrow is uniquely recoverable through the durable `job_id` relation, backed by a database-level unique constraint (`escrows_job_id_uidx`) that rejects a duplicate escrow even under a concurrent-writer race;
- ✅ escrow create, release and settlement adapters support stable replay semantics, in both the mock backend and the `tos-protocol` RPC adapter;
- ✅ the exact verified Execution Receipt is privately persisted before the `settlement_pending` external effect;
- ✅ a lost settlement response can be recovered after process restart without re-executing provider work;
- ✅ a lost release response can be replayed without applying the Account refund twice;
- ✅ settlement/refund finalization and Account mutation are atomic locally;
- ✅ an immediate startup sweep plus a periodic reconciler (`RunReconciler`, wired into `cmd/api/main.go`) resumes stale `submitted`, `working`, `canceling` and `reconciling` Jobs;
- ✅ ambiguous external outcomes remain visibly `reconciling` and fail closed instead of being converted into a false terminal success/failure;
- ✅ permanent CI runs the crash-recovery service tests against PostgreSQL 16 in addition to the in-memory failure-injection suite.

Failure-injection coverage includes:

```text
lost CreateEscrow response after the escrow side effect committed
lost SettleJob response after settlement committed
lost ReleaseEscrow response after release committed
process restart with settlement_pending + persisted Execution Receipt
provider unavailable after that restart
debited Job recovered before escrow creation
stale Job discovery by the periodic reconciliation scan
repeated recovery proving no double debit or double credit
```

### RPC transport-layer idempotency and Quote/Job binding (cross-repo)

**Status: ✅ complete.** Two independent-review findings against the live `tos-protocol` RPC path were root-caused and fixed, both verified end-to-end rather than only against the mock backend:

- ✅ `tos-protocol`'s mutation idempotency digest previously included transport metadata (`request_id`, `trace_id`, `deadline_unix_millis`), which ATOS regenerates on every retry — so a retry after a lost RPC response could incorrectly return `IDEMPOTENCY_CONFLICT` instead of replaying the original durable result. Fixed in `tosnetwork/tos-protocol#12` (merged to `main` at `1585abf8`): the digest now excludes transport metadata while still binding `caller_id`, `idempotency_key`, and all business fields, so a semantic replay returns the original result and a semantic substitution still conflicts. Regression-tested by `TestAtomicMutationReplayIgnoresTransportContextAcrossRestart`.
- ✅ ATOS's `CommitQuote` call sent `quote.UnderlyingServiceQuoteRef` (a distinct commitment reference) on the wire as `underlying_service_quote_ref`, while `tos-protocol`'s `SubmitJob` requires that field to equal the `service_quote_id` presented at submission — an unconditional mismatch that failed every RPC-backed job after `CommitQuote` succeeded. Fixed in `internal/adapters/tosprotocol/core.go`; this had been latent since Phase 0 and was only exercised once ATOS's `tos-protocol` pin was current enough to hit `SubmitJob`'s binding check.

Both fixes are merged to `main` in both repositories (`tosnetwork/atos` PR #3 at `21b03ac`, `tosnetwork/tos-protocol` PR #12 at `1585abf8`), and `TestATOSConnectRPCManagedLifecycle` — the full Managed lifecycle test against a live `tos-protocol` RPC server and private Worker — passes.

## Runtime boundary

```text
Agent client
    -> REST / MCP / A2A
    -> ATOS Commercial Quote and Managed account policy
    -> durable economic checkpoint
    -> tos-protocol ExecutionGatewayService
    -> private Unix-socket tos-ai Worker
    -> signed Execution Receipt
    -> verification
    -> durable settlement checkpoint
    -> Managed settlement
    -> atomic Account/Job finalization
```

The deterministic mock backend remains available only as an explicit local-development/test deployment. Production configuration rejects it.

## Completion boundary

“Phase 0/1 code complete” means the roadmap contracts, services, persistence, authorization, Managed economy, crash recovery and executable acceptance criteria are implemented and permanently regression-tested. It does not mean a public production environment has automatically been provisioned. A hosted launch still requires operator-controlled domains, certificates, PostgreSQL, secrets, backups, monitoring, billing policy, support and incident response.

Verified and Native work continues in later roadmap phases. Stronger-mode requests remain fail-closed unless the configured TOS backends provide the required guarantees.

## Phase 3C — Open Task Marketplace

**Code status: ✅ complete.**

Implements `atos-spec/docs/IMPLEMENTATION_ROADMAP.md` §7.3. An `OpenTask` is a
**demand-side marketplace object, not a new commercial contract** and not a
parallel pricing/escrow/execution/receipt/settlement system. Its entire
purpose is to select exactly one winning provider proposal and hand off to
the existing pipeline unchanged:

```text
publish -> open -> proposals -> exactly one accepted proposal
  -> immutable Quote/Job binding -> normal Job/Receipt/settlement/dispute lifecycle
```

**Authoritative source of price/trust-mode/proof requirements**: always
`QuoteService.Create`, called at acceptance time with the task owner as
principal, the winning proposal's Capability ID/version, and the task's own
`requested_trust_mode`/`proof_requirements`/`max_total` as constraints —
exactly as if the owner had called `POST /v1/quotes` directly. A provider's
`proposed_price` (if supplied at all) is recorded purely as a
non-authoritative UI hint; it is never read by the acceptance path.
`JobService.CreateJob` is likewise the only path that ever creates a Job.
Neither `QuoteService` nor `JobService`'s pricing/trust-mode/proof algorithms
were duplicated or modified beyond adding an optional idempotency key to
`QuoteService.Create` (see below) — `internal/service/open_task.go` calls
them exactly like any other caller.

### Domain model (`internal/domain/open_task.go`)

- `OpenTask` — owner, title/description, frozen `Input` (used verbatim as
  the eventual Job input), `RequestedTrustMode`/`ProofRequirements`/
  `MaxTotal` (the owner's own constraints), `ExpiresAt`, `Status`
  (`open`/`accepted`/`fulfilled`/`cancelled`/`expired`), and once accepted,
  `AcceptedProposalID`/`BoundQuoteID`/`BoundJobID`. `Expired(now)` is
  checked lazily wherever it matters (Propose, Accept, every read path) —
  there is no background sweep that mutates a stored row's `Status` to
  `expired`; `OpenTaskService.view` promotes it at read time instead.
- `OpenTaskProposal` — provider's application, binding an exact
  `CapabilityID`/`CapabilityVersion` frozen at Propose time (never
  caller-supplied, never silently re-derived from a later Capability
  update). Deliberately has **no stored accepted/rejected status** — that
  is derived by comparing a proposal's ID against its task's
  `AcceptedProposalID`, keeping Accept O(1) regardless of how many
  proposals a popular task accumulates. `WithdrawnAt` is the one piece of
  proposal state that genuinely cannot be derived.
- `AcceptanceOperation` — the durable winner-selection → Quote → Job
  binding journal, one record per acceptance attempt, mirroring
  `domain.ExecutionSignerOperation`'s checkpoint-journal pattern from
  Phase 3B exactly:

  ```text
  winner_claimed -> quote_binding_pending -> quote_bound
    -> job_binding_pending -> job_bound -> completed
  (any step) -> reconciling (ambiguous outcome, resumable)
  (any step) -> failed (definitive rejection, reopens the task)
  ```

  `winner_claimed` (not `intent_persisted`) is where a fresh operation's
  journal begins: the store-level `OpenAcceptanceOperation` call that opens
  this record durably claims the winner (`OpenTask.AcceptedProposalID`) in
  the *same* transaction as the row insert, so the claim and the journal's
  existence can never be observed apart from each other.

### Database-level invariants (`migrations/012_phase3c_open_tasks.sql`)

- `open_tasks`, `open_task_proposals`, `acceptance_operations` tables, each
  with a relational-column-plus-jsonb-payload shape mirroring `quotes`/
  `jobs`.
- `open_tasks_principal_idempotency_key_idx` / `open_task_proposals_provider_idempotency_key_idx`:
  partial unique indexes on `(principal_id|provider_id, idempotency_key)`,
  mirroring `jobs_principal_idempotency_key_uidx` from Phase 1.
- `acceptance_operations` carries a plain `UNIQUE (principal_id, idempotency_key)`
  constraint (the caller's own Accept-request identity) **plus two more**,
  the actual "exactly one accepted proposal / exactly one bound Job per
  task" guarantee:
  - `idx_acceptance_operations_task_nonterminal`: partial unique index on
    `task_id` where `checkpoint NOT IN ('completed','failed')` — at most one
    in-flight acceptance per task, enforced by Postgres, not an in-process
    lock.
  - `idx_acceptance_operations_task_completed`: partial unique index on
    `task_id` where `checkpoint='completed'` — at most one *ever-completed*
    acceptance per task, defense in depth beyond the non-terminal index
    above.
- `store.OpenTasks.OpenAcceptanceOperation` (implemented for both `memory`
  and `postgres`) is the sole path that can claim a winner: under locks
  scoped to both `task_id` and `proposal_id` (the same locks
  `UpdateOpenTask`/`WithdrawOpenTaskProposal`/`CompleteAcceptance`/
  `FailAcceptance` all share, so Accept is race-free against a concurrent
  Cancel or Withdraw, not merely against a concurrent Accept), it rejects a
  second opener while any non-terminal operation exists, loads the current
  `OpenTask`/`OpenTaskProposal` snapshots, calls a caller-supplied
  `build(task, proposal)` closure to construct the operation (which must
  not itself call back into the store — see the doc comments on that
  method for why: a nested store call would deadlock the in-memory
  implementation's single mutex, and would read a non-transactional
  snapshot through the Postgres implementation's separate pool connection),
  then commits the operation insert and the `OpenTask` winner-claim update
  in the same transaction.
- `quotes.principal_id`/`quotes.idempotency_key` columns and
  `quotes_principal_idempotency_key_idx` were added in this same migration:
  `QuoteService.Create` gained an **optional** `IdempotencyKey` field
  (`CreateQuoteInput.IdempotencyKey`, empty for every pre-existing caller,
  which keeps their always-fresh-Quote behavior unchanged byte-for-byte) so
  Accept's own Quote-binding step can be genuinely idempotent, using the
  identical Reserve/Finish/Release-plus-crash-recovery-lookup pattern
  `JobService.submit` already uses.

### Idempotency and crash recovery

`Publish`/`Propose` are simple idempotent creates via the generic
`store.Idempotency` primitive (Reserve → do the work → Finish, with a
crash-recovery lookup via `OpenTaskByIdempotencyKey`/
`OpenTaskProposalByIdempotencyKey` for a commit that landed but was never
marked Finished) — exactly `JobService.submit`'s own pattern.

`Accept` mirrors `ExecutionSignerService.Authorize`'s
resume-before-open discipline precisely: it looks up
`AcceptanceOperationByIdempotencyKey` **first**, before ever touching
anything that depends on the task's current (possibly already-mutated-by-
this-same-operation) state, and resumes driving that operation forward on a
hit. Only a genuinely new idempotency key reaches
`OpenAcceptanceOperation`. Once an operation exists, `driveAcceptance` walks
its checkpoint forward exactly like `ExecutionSignerService`'s `drive*`
methods: each Quote/Job creation call uses a checkpoint-derived, stable
idempotency key (`AcceptanceOperation.NewQuoteIdempotencyKey()`/
`NewJobIdempotencyKey()`, both deterministic functions of the operation's
own ID, never the caller's Accept-request key and never regenerated per
retry) so a retry after a lost response resumes the *same* Quote/Job rather
than minting a second one. An ambiguous remote outcome marks the operation
`reconciling` and returns a retryable error; a **definitive** rejection
(stale Capability version, Capability paused, quote/job validation failure)
marks it `failed` and reopens the task (`AcceptedProposalID` cleared,
`Status` back to `open`) via a CAS update that only fires while the task is
still `Accepted` with that exact proposal claimed — so it can never clobber
newer state.

`OpenTaskService.RunReconciler`/`ReconcileStaleOperations` sweep every
non-terminal `AcceptanceOperation` older than a staleness threshold and
resume it exactly the way a client retry would — the same pattern
`ExecutionSignerService.RunReconciler` already uses, wired into
`cmd/api/main.go` alongside the other five reconcilers.

### REST (`internal/httpapi/open_tasks.go`) and MCP (`internal/mcp/open_task_tools.go`)

Both surfaces dispatch into the exact same `OpenTaskService` methods — there
is one set of lifecycle/redaction/idempotency rules, not two.

| REST | MCP tool | Scope |
|---|---|---|
| `POST /v1/open-tasks` | `atos_publish_open_task` | `open_tasks:write` |
| `GET /v1/open-tasks` (`?mine=true` for the owner's own tasks) | `atos_search_open_tasks` | `open_tasks:read` |
| `GET /v1/open-tasks/{task_id}` | `atos_get_open_task` | `open_tasks:read` |
| `POST /v1/open-tasks/{task_id}/cancel` | `atos_cancel_open_task` | `open_tasks:write` |
| `POST /v1/open-tasks/{task_id}/proposals` | `atos_apply_to_open_task` | `open_task_proposals:write` |
| `GET /v1/open-tasks/{task_id}/proposals` | `atos_list_open_task_proposals` | `open_tasks:read` |
| `POST .../proposals/{proposal_id}/withdraw` | `atos_withdraw_open_task_proposal` | `open_task_proposals:write` |
| `POST .../proposals/{proposal_id}/accept` | `atos_accept_open_task_proposal` | `open_tasks:write` |

`open_tasks:read`/`open_tasks:write` are default consumer scopes (publishing
and accepting a proposal for one's own task is an ordinary consumer action,
like creating a Job). `open_task_proposals:write` is explicit-grant-only,
mirroring `provider_jobs:deliver`'s pattern — applying to fulfill someone
else's task is a distinct provider-role action. Every MCP tool re-checks
scope at call time and is silently absent from `tools/list` for a caller who
lacks it (never revealed as existing-but-forbidden).

Visibility: `OpenTask.Input` and a proposal's `Message` are visible only to
the task owner (both) and, for a proposal, its own submitting provider
(`Message` only) — every other caller/listing sees the redacted
`.Public()` shape. Withdrawing a proposal is not part of the roadmap's
explicit endpoint list but was implemented anyway since the domain model
already needed a real (non-derived) `WithdrawnAt` field; see "Known
follow-ups" below.

### Tests

- Lifecycle, idempotency (replay + same-key-different-content conflict +
  RequestId/TraceId-independent digests), security/policy (non-owner
  accept/cancel, provider impersonation, provider-supplied price/trust-mode
  never authoritative, stale/paused Capability version rejected, withdrawn
  proposal rejected, public listing redaction), and crash recovery (Quote
  succeeded-but-checkpoint-lost, Job succeeded-but-checkpoint-lost,
  reconciler resumes a stuck `reconciling` operation, definitive failure
  reopens the task) all in `internal/service/open_task_*_test.go`, against
  the in-memory store.
- Store parity: `internal/store/memory/open_task_test.go` and
  `internal/store/postgres/open_task_test.go` exercise the same CRUD/
  in-flight-guard contract against both backends.
- Multi-instance concurrency against real PostgreSQL 16 (not a single-
  process mutex): `TestOpenAcceptanceOperation_ConcurrentAcceptHasSingleWinner`
  (store layer, `internal/store/postgres/open_task_test.go`) and
  `TestOpenTaskService_TwoRealPostgresInstancesConvergeToOneAccept`
  (service layer, `internal/service/open_task_multiinstance_postgres_test.go`,
  two independent `*postgres.Store` connections/service stacks, ≥2
  providers, N concurrent Accept attempts) both assert exactly one accepted
  proposal, one bound Quote, one bound Job, and zero Jobs created for the
  losing proposal.
- REST/MCP scope enforcement and ownership: `internal/httpapi/open_tasks_test.go`,
  `internal/mcp/open_task_tools_test.go`.

### Known follow-ups (not addressed in this change; `atos-spec` was not modified)

- A bug found and fixed during initial development, worth noting for
  anyone building on `domain.Quote`/`domain.Job`-adjacent types: a
  `map[string]any` field tagged `json:"...,omitempty"` that round-trips
  through the Postgres store's jsonb-payload convention silently collapses
  an explicitly-empty (but non-nil) map into `nil` on read-back, because
  Go's `omitempty` treats a zero-length map the same as a nil one.
  `domain.OpenTask.Input` was fixed by dropping `,omitempty`; any future
  jsonb-payload-backed field of map/slice type should default to no
  `omitempty` unless "explicitly empty" and "absent" are genuinely meant
  to be indistinguishable.

### Manual review round (post-implementation, pre-merge)

A manual code review of the Draft PR found and this change fixed three P0s
and three P1s, all verified by reading the affected code directly and
reproduced/closed with new tests (`go test -race` against real PostgreSQL
16 for every concurrency claim below):

- **P0: Accept and Cancel were not actually serialized.**
  `OpenAcceptanceOperation` locked `"open-task-acceptance"+taskID` while
  `UpdateOpenTask` (Cancel's primitive) locked a DIFFERENT
  `"open-task"+id` key and read the task row without `FOR UPDATE` --
  different PostgreSQL advisory lock keys provide no mutual exclusion, so
  a concurrent Accept and Cancel could both read a stale `open` snapshot
  and both commit. Fixed by having `OpenAcceptanceOperation` also acquire
  the `"open-task"` lock and read the row `FOR UPDATE`, the same
  lock/row-lock `UpdateOpenTask`, `CompleteAcceptance` and
  `FailAcceptance` all now share. Regression test:
  `TestOpenAcceptanceOperation_ConcurrentWithCancelConverges`.
- **P0: a crash between "operation Completed" and "task Fulfilled" (or
  between "operation Failed" and "task reopened") permanently stranded
  the task.** Both transitions were two separate store calls/commits;
  `Completed`/`Failed` are terminal and therefore excluded from the
  reconciler's stale-operation sweep, so nothing would ever revisit an
  operation stuck in that gap. Fixed by two new store methods,
  `CompleteAcceptance` and `FailAcceptance`, each performing its
  checkpoint transition AND the corresponding `OpenTask` projection
  (Fulfilled+bindings, or reopened-to-Open) in ONE database transaction --
  see their doc comments in `internal/store/store.go`.
- **P0: a Capability version bump between winner-claim and the actual
  Quote call could silently bind a different version than the one the
  proposal/operation both still claim.** `CreateQuoteInput` gained an
  optional `ExpectedCapabilityVersion` field; `QuoteService.Create`
  refuses (`domain.ErrQuoteMismatch`, non-retryable) if the live
  Capability's version no longer matches, and `driveAcceptance` now passes
  `op.CapabilityVersion`. A rejection here reopens the task via the same
  `FailAcceptance` path as any other definitive acceptance failure.
  Regression test: `TestOpenTaskAcceptRejectsCapabilityVersionDriftDuringResume`.
- **P1: Withdraw and Accept raced on the same proposal row with no shared
  lock**, so a proposal could end up simultaneously withdrawn and claimed
  as the task's accepted winner. Fixed with a new store method,
  `WithdrawOpenTaskProposal`, that locks `"open-task-proposal"+proposalID`
  (the same lock `OpenAcceptanceOperation` now also takes before reading
  the proposal it's about to claim) and re-checks the task's
  `AcceptedProposalID` under `"open-task"+task_id` before allowing the
  withdrawal to commit. `OpenTaskService.Withdraw` now just delegates to
  it. Regression test:
  `TestWithdrawOpenTaskProposal_ConcurrentWithAcceptConverges`.
- **P1: Publish/Propose validated live state (expires_at-in-the-future,
  task still open, capability still active) BEFORE the idempotency
  replay/crash-recovery lookup**, so a legitimate retry of an
  already-successful call could spuriously fail once real state moved on
  (time passing past the original `expires_at`; the task being accepted
  by a different proposal; the capability being paused) instead of
  replaying the original result. Fixed by reordering both methods to run
  the Reserve/replay/crash-recovery gates FIRST, exactly mirroring
  `Accept`'s own (already-correct) discipline; `Propose`'s idempotency
  digest also no longer includes the live-resolved Capability version
  (only caller-supplied content), since that field drifting independently
  of caller intent must never make a genuine replay hash differently.
  Regression tests: `TestOpenTaskPublishReplaySucceedsAfterOriginalExpiresAtElapses`,
  `TestOpenTaskProposeReplaySucceedsAfterTaskAccepted`.
- **P1: an omitted MCP `limit` argument meant opposite things to the two
  store backends** (memory: unbounded; Postgres: `LIMIT 0`, zero rows),
  so `atos_search_open_tasks` with no `limit` returned every open task
  under one backend and none under the other. Fixed by centralizing the
  default/max clamp inside `OpenTaskService.ListPublic` (removing the
  REST handler's separate, now-redundant default) rather than duplicating
  it per transport. Regression test:
  `TestOpenTaskListPublicDefaultLimitParityAgainstPostgres`.
- **P2: `OpenTaskProposal.Public()` did not clear `ProposedPrice`**, so a
  provider's price hint -- meant to be visible only to the task owner and
  the submitting provider, per the same rule already applied to
  `Message` -- leaked to any caller with plain `open_tasks:read`. Fixed.
- **Self-dealing**: neither `Propose` nor `Accept` checked that the
  proposing/accepting identity differed from the task owner. Added an
  explicit guard to both (Propose rejects up front; Accept re-checks as
  defense in depth), per this codebase's own
  self-referential-operation-check discipline.

All of the above are covered by tests that fail on the pre-fix code and
pass after -- see `internal/store/postgres/open_task_test.go` and
`internal/service/open_task_review_fixes_test.go`.

### Second review round (independent Codex review of the fix commit)

A second, independent review (OpenAI Codex, run against the fix commit
above) found two further real issues in the fixes themselves:

- **P1: `OpenAcceptanceOperation` and `WithdrawOpenTaskProposal` acquired
  the shared `"open-task"`/`"open-task-proposal"` advisory locks in
  OPPOSITE orders** (task-then-proposal vs proposal-then-task) --
  textbook conditions for a PostgreSQL deadlock (one transaction holds
  task and waits on proposal while the other holds proposal and waits on
  task), which Postgres resolves by aborting one side with a raw,
  unclassified `40P01` error rather than a clean domain rejection. The
  existing concurrency regression tests didn't catch it because they
  discarded every error from the racing goroutines. Fixed:
  `WithdrawOpenTaskProposal` now does a lock-free preview read of the
  proposal (safe -- `TaskID` is one of the identity fields
  `UpdateOpenTaskProposal` already protects as immutable) purely to learn
  which task lock to acquire FIRST, then locks task before proposal,
  matching `OpenAcceptanceOperation`'s order exactly. Both concurrency
  tests were also strengthened to assert every observed error is an
  expected, classified `*domain.Error` rather than silently discarding
  errors -- the assertion that would have caught the deadlock directly.
- **P1: the idempotency crash-recovery lookup path in `Publish`,
  `Propose`, and `QuoteService.Create` trusted an already-committed
  object as a valid replay without comparing its content against the
  current request.** The gap: if a prior attempt's `PutOpenTask`/
  `PutOpenTaskProposal`/`PutQuote` succeeded but that SAME attempt's own
  `Finish` call then failed for any reason short of the process dying,
  the deferred `Release` hard-deletes the `idempotency_records` row
  entirely (not just marks it stale) -- a LATER call reusing the same key
  with genuinely different content would find no reservation at all,
  reserve fresh, hit the crash-recovery lookup, and silently receive the
  OLD object back instead of `ErrIdempotencyConflict`. Fixed by comparing
  a content digest before trusting the lookup: `Publish`/`Propose`
  recompute `hashRequest(...)` from the existing object's own
  already-stored fields (no schema change needed, since `OpenTask`/
  `OpenTaskProposal` losslessly retain every field the digest covers);
  `QuoteService.Create` persists the digest directly on a new
  `domain.Quote.IdempotencyRequestHash` field instead, since `Quote`'s
  stored representation does not losslessly preserve every raw input
  (e.g. `InputSummary` is only kept as a commitment). Regression tests
  reproduce the exact end state by calling the real creation path once
  and then calling `store.Idempotency.Release` directly -- the same
  primitive the abandoned attempt's own cleanup would have called --
  rather than hand-constructing a fake row:
  `TestOpenTaskPublishContentValidatedAfterAbandonedReservation`,
  `TestOpenTaskProposeContentValidatedAfterAbandonedReservation`,
  `TestQuoteCreateContentValidatedAfterAbandonedReservation` (each also
  asserts the identical-content case still replays correctly).

This is the same class of finding both review rounds kept surfacing:
correctness at the exact boundary between two store calls that are not
themselves atomic. Every fix in both rounds either makes that boundary
genuinely atomic (one transaction) or makes the recovery path re-verify
content instead of trusting position alone.

### Third review round

A third review (against commit `38d1ed1`, one commit before the lock-order
and digest fixes above were pushed) re-reported the same lock-ordering
deadlock and digest-validation gaps, this time backed by running the
Withdraw/Accept concurrency test 30x against real PostgreSQL and grepping
the *server's own log* for `deadlock detected` (not just trusting the Go
test's pass/fail) -- a stronger verification method than the previous
round used. Both findings were already fixed by the commit this round
predated; re-verified at current `HEAD` using the same stronger method
(50 iterations, `deadlock_timeout=200ms`, `log_lock_waits=on`, grepping
the container's actual log output): zero deadlocks, zero errors of any
kind logged by PostgreSQL across all 50 runs.

The same round also found two genuinely new, smaller issues in the
fixes' own test coverage and store contract, both fixed:

- **P2: the self-dealing regression test didn't actually exercise
  Accept's new guard.** `TestOpenTaskAcceptRejectsSelfDealing` called
  Accept with a DIFFERENT `PrincipalID` than the task's actual owner, so
  the pre-existing "not the task owner" check fired first and the test
  would still have passed even if Accept's self-dealing guard were
  deleted entirely. Since `Propose` already refuses to create a
  self-dealing proposal in the first place, the fixed test inserts one
  directly through the store (bypassing `Propose`, simulating a proposal
  that predates the guard) and calls Accept AS the task's real owner, so
  the ownership check passes and the self-dealing check is what actually
  fires -- plus asserts no `AcceptanceOperation` was created at all.
- **P2: `UpdateAcceptanceOperation`'s documented contract ("MUST NOT set
  a terminal checkpoint") was not actually enforced by either store
  implementation.** Nothing in the current call graph violates this
  (`advanceAcceptance` never targets `Completed`/`Failed`;
  `CompleteAcceptance`/`FailAcceptance` are the only paths that reach
  them), but an unenforced contract is one accidental future call away
  from silently reintroducing the exact split-commit bug those two
  methods exist to close. Both `memory` and `postgres` now reject a
  `Completed`/`Failed` target checkpoint from `UpdateAcceptanceOperation`
  with `ErrIdempotencyConflict`, with a conformance-style assertion added
  to both stores' existing tests.

### Fourth review round

A fourth review (re-verifying commit `22ca289`, this round's own terminal-
checkpoint guard above) found that the guard as first written was too
strict: `advanceAcceptance`'s own CAS design (`internal/service/open_task.go`)
has a no-op branch -- `if current.Checkpoint.Terminal() || current.Checkpoint
!= expectedFrom { return current, nil }` -- that returns an ALREADY-terminal
`current` completely unchanged, exactly reproducing a stale worker (a client
retry, or the periodic reconciler) safely converging after a DIFFERENT
worker already completed or failed the same operation. The guard added in
the previous round rejected ANY `next.Checkpoint` equal to `Completed`/
`Failed` unconditionally, including this exact legitimate no-op -- so two
concurrent drivers racing the same operation (explicitly a supported
scenario: "multiple service instances must be able to safely concurrently
reconcile") could have the SLOWER one receive a spurious
`ErrIdempotencyConflict` even though the operation it was driving forward
had, in fact, already completed successfully. The same overly-narrow check
also had a reverse gap: it only inspected `next.Checkpoint`, never
`current.Checkpoint`, so nothing stopped a caller from moving an
ALREADY-terminal operation back to a non-terminal one.

Fixed with the semantics the review specified: once `current.Checkpoint`
is terminal, `UpdateAcceptanceOperation` now ignores whatever `fn`
computed entirely and returns the STORED `current` value unconditionally
(no write) -- this is what makes a stale-worker no-op succeed correctly
(the store never even compares `next`, so there is nothing to spuriously
reject) while also making terminal-to-non-terminal revival structurally
impossible (the store never looks at `next.Checkpoint` once `current` is
terminal, so there is no path for a rogue `next` to be applied). The
narrower rejection ("must not set a terminal checkpoint") now only fires
when `current` is genuinely non-terminal and `next` tries to become
terminal directly through this method -- the actual protection this guard
was meant to provide.

Four new regression tests (`TestMemoryUpdateAcceptanceOperation_TerminalIsImmutable`
/ `TestUpdateAcceptanceOperation_TerminalIsImmutable`, one per store) cover
exactly the scenarios the review specified: an unchanged update against an
already-terminal operation succeeds; an attempted revival back to
non-terminal is silently ignored; a concurrent stale driver's advance
(with a stale `expectedFrom` that no longer matches, because a different
driver already completed the operation) converges without error; and the
pre-existing non-terminal-to-terminal rejection (already covered by
`TestMemoryOpenAcceptanceOperation_InFlightGuard`/
`TestOpenAcceptanceOperation_RejectsSecondAttemptWhileFirstInFlight`)
still holds.

This same round also re-verified, using the stronger
server-log-inspection method from the third round, that the lock-ordering
fix and idempotency digest fix from earlier rounds remain correct at the
current commit -- both were already fixed and nothing regressed.

### Fifth review round

An automated GitHub Codex review (`chatgpt-codex-connector[bot]`) had left
three inline findings against commit `38d1ed1` that predated all four
review rounds above. Checked each against the current commit individually:

1. **P2, lock order for accept vs withdraw** -- already fixed by the
   second review round (`WithdrawOpenTaskProposal`'s lock-free preview
   read + task-before-proposal ordering). Confirmed `OpenAcceptanceOperation`
   and `WithdrawOpenTaskProposal` still acquire `"open-task"` before
   `"open-task-proposal"` identically. No change needed.
2. **P1, expired tasks consuming the ListPublic page limit** -- still
   present, not fixed by any prior round. `ListPublicOpenTasks` filtered by
   `status='open'` only and applied `LIMIT` before the caller's lazy
   `Expired(now)` check ever ran. Since expiry is lazy (no reconciler flips
   Status away from Open), a batch of newest rows that all happen to be
   expired-but-still-stored-Open would consume the entire limit window,
   and `OpenTaskService.ListPublic` would filter every one of them out
   client-side -- hiding older rows that are still genuinely open, even
   though they exist and would fit within the requested limit. Fixed by
   pushing the expiry filter into the store query itself (`AND expires_at
   > $2`, both stores), so `LIMIT` is only ever applied to rows that are
   actually still live. `store.OpenTasks.ListPublicOpenTasks` now takes
   `now time.Time`. New tests:
   `TestListPublicOpenTasks_ExcludesExpiredBeforeApplyingLimit` /
   `TestMemoryListPublicOpenTasks_ExcludesExpiredBeforeApplyingLimit`
   (older genuinely-open task must not be hidden behind newer expired
   rows).
3. **P2, proposal insertion not conditional on the task remaining open** --
   also still present. `PutOpenTaskProposal` was a plain unconditional
   insert with no lock and no task-state validation of its own;
   `OpenTaskService.Propose`'s live-state check ran against an unlocked
   `GetOpenTask` read, so a concurrent `Accept`/`Cancel` committing in the
   gap before the insert would let a provider receive a successful
   proposal against a task that, by the time the row actually landed, was
   no longer open. Fixed with a new store method,
   `CreateOpenTaskProposal(ctx, taskID, now, build)`, that locks
   `"open-task"+taskID` (the same lock key `OpenAcceptanceOperation`/
   `UpdateOpenTask`/`WithdrawOpenTaskProposal` already use), re-reads the
   task fresh, and refuses (`ErrOpenTaskNotOpen`, without calling `build`)
   if it is not `Open` and not expired -- all inside one transaction with
   the insert. `Propose` still does its capability lookup and an initial
   fast-path task check *before* calling this (kept out of `build` for the
   same reason `Accept` keeps Capability lookup out of
   `OpenAcceptanceOperation`'s `build`: a nested store call from `build`
   would deadlock the in-memory store's single mutex and would not
   participate in the Postgres transaction), but the actual
   correctness-critical check now happens fresh under lock, atomically
   with the insert. New tests:
   `TestCreateOpenTaskProposal_RejectsOnAlreadyClosedTask` /
   `TestMemoryCreateOpenTaskProposal_RejectsOnAlreadyClosedTask`
   (deterministic: cancel the task first, then prove `CreateOpenTaskProposal`
   refuses without ever invoking `build` and without inserting anything --
   the exact validation the old unconditional insert never had), plus
   `TestCreateOpenTaskProposal_ConcurrentWithCancelConverges` (Postgres-only
   concurrency stress: races proposal creation against cancellation,
   asserting every result is either a created proposal or a properly
   classified `ErrOpenTaskNotOpen`, and that the persisted proposal count
   matches exactly the count of calls that reported success).

Verification: `gofmt`/`go vet`/`go build` clean; full `go test ./... -race
-count=1` clean against a freshly created, never-reused Postgres 16
instance (a full-suite run against a long-lived container reused across
many manual verification runs this session produced two unrelated false
failures -- `TestOpenAcceptanceOperation_ConcurrentAcceptHasSingleWinner`
and `TestEarningsService_TwoRealPostgresInstancesConvergeToOnePayout` --
both traced to accumulated rows from earlier runs exceeding those tests'
own `LIMIT`/count assumptions, confirmed via `git stash` bisection and a
side-by-side fresh-container rerun to be pure test-fixture pollution, not
product bugs); deadlock re-check using the third round's methodology
(`deadlock_timeout=200ms`, `log_lock_waits=on`, grepping the server log)
across all open-task/proposal/acceptance concurrency and immutability
tests at `-count=20` (160 sub-test runs) against a fresh container --
160/160 pass, 0 occurrences of `deadlock detected`.
