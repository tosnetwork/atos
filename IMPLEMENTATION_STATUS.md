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
  and `postgres`) is the sole path that can claim a winner: under one lock
  scoped to `task_id`, it rejects a second opener while any non-terminal
  operation exists, loads the current `OpenTask` snapshot, calls a
  caller-supplied `build(task)` closure to construct the operation (which
  must not itself call back into the store — see the doc comments on that
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

- **Proposal withdrawal** (`atos_withdraw_open_task_proposal` /
  `POST .../withdraw`) is an implementation addition beyond
  `atos-spec/docs/IMPLEMENTATION_ROADMAP.md` §7.3's explicit endpoint list.
  `OpenTaskService.Withdraw` checks the task's current accepted-proposal
  state before its CAS update but does not hold the same `task_id`-scoped
  lock `OpenAcceptanceOperation` does, so there is a narrow window where a
  concurrent Accept can claim the same proposal a Withdraw call is
  in flight against; the accepted binding always wins (it is committed
  first and is never unwound by a later `WithdrawnAt` write) but the
  proposal can end up simultaneously "accepted" and carrying a
  `WithdrawnAt` timestamp. `effectiveProposalStatus` resolves this in favor
  of `accepted`, so no incorrect status is ever reported, but `atos-spec`
  does not yet define withdrawal's semantics formally — worth
  standardizing there before other implementations add the same endpoint.
- A bug found and fixed by this change, worth noting for anyone building
  on `domain.Quote`/`domain.Job`-adjacent types: a `map[string]any` field
  tagged `json:"...,omitempty"` that round-trips through the Postgres
  store's jsonb-payload convention silently collapses an explicitly-empty
  (but non-nil) map into `nil` on read-back, because Go's `omitempty`
  treats a zero-length map the same as a nil one. `domain.OpenTask.Input`
  was fixed by dropping `,omitempty`; any future jsonb-payload-backed field
  of map/slice type should default to no `omitempty` unless "explicitly
  empty" and "absent" are genuinely meant to be indistinguishable.
