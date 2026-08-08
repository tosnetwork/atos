# ATOS v0.2 Implementation Status

This document tracks the Go gateway against `tosnetwork/atos-spec` v0.2.

## Phase 0 — Contract First

**Code status: complete.**

Implemented and tested:

- one Capability/Quote/Invocation/Job/Escrow/Receipt/Settlement model shared by REST, MCP and A2A;
- `requested_trust_mode` separated from concrete `trust_mode`;
- `auto` accepted only as pre-Quote policy and rejected as committed state;
- normative `managed`/no-profile, `verified`/`tos_verified_v1` and `native`/`tos_native_v1` pairs;
- provider `requested_trust_modes` separated from derived active `supported_trust_modes`;
- immutable Quote mode/profile propagation through Job, Escrow, Receipt and Settlement;
- no execution-time mode override and no silent downgrade;
- explicit delegated execution-signer fields and proof references;
- federation-safe identity and commitment fields modeled without moving bulk or private payloads on-chain;
- schema, OpenAPI and conformance tests, including one common Quote API shape across all three concrete resolved modes.

Remaining Phase 0 work is maintenance rather than missing behavior: future schema changes must keep generated artifacts and conformance vectors synchronized.

## Phase 1 — Codex-First Managed MVP

**Code status: complete.**

Implemented and tested:

- embedded installable Skill served from `/skills/atos/SKILL.md`;
- OAuth-style Device Authorization with pending approval, browser consent, polling intervals, `slow_down`, denial, expiry and bounded codes;
- scoped access tokens, rotating refresh tokens, revocation and durable owner-private bbolt auth state;
- stateless Streamable HTTP MCP with nine deterministic ordinary consumer tools;
- Capability search/retrieval, Commercial Quote, invocation, jobs and account;
- PostgreSQL capability/search/business storage with complete v0.2 JSON payloads;
- configurable Managed internal-credit balance, per-call limit and daily limit;
- server-issued, exact-request-bound spending confirmations for calls above automatic policy limits;
- rejection of caller-supplied confirmation booleans;
- Managed reservation, execution, signed receipt, verification and settlement;
- explicit `ATOS_TOS_BACKEND=mock|rpc` selection with no failure fallback;
- real `atos -> tos-protocol -> private tos-ai Worker` Managed RPC path;
- signed-URL Artifact transport with ownership, size and expiry enforcement;
- idempotency leases, stale-reservation recovery and a unique `(principal_id, idempotency_key)` Job constraint;
- production configuration gates requiring PostgreSQL, HTTPS public URL, explicit user approval, durable auth state and the RPC backend;
- a full HTTP acceptance test from Skill retrieval and authorization through search, Quote, payment-policy handling, invocation, Receipt/settlement and exact balance mutation.

## Runtime boundary

```text
Agent client
    -> REST / MCP / A2A
    -> ATOS Commercial Quote and Managed account policy
    -> tos-protocol ExecutionGatewayService
    -> private Unix-socket tos-ai Worker
    -> signed Execution Receipt
    -> verification and Managed settlement
```

The deterministic mock backend remains available only as an explicit local-development/test deployment. Production configuration rejects it.

## Completion boundary

“Phase 0/1 code complete” means the roadmap contracts, services, persistence, authorization, Managed economy and acceptance tests are implemented. It does not mean a public production environment has automatically been provisioned. A hosted launch still requires operator-controlled domains, certificates, PostgreSQL, secrets, backups, monitoring, billing policy, support and incident response.

Verified and Native work continues in later roadmap phases. Stronger-mode requests remain fail-closed unless the configured TOS backends provide the required guarantees.
