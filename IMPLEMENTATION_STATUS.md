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
