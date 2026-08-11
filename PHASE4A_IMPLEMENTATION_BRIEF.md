# Phase 4A Implementation Brief

## Production Identity and Capability Ownership Activation

Status: implementation required. This document is a work brief, not evidence
that Phase 4A has been delivered.

The canonical requirements are the latest `tosnetwork/atos-spec` `main`, in
particular `docs/IMPLEMENTATION_ROADMAP.md` Phase 4A and the companion
architecture, authentication, capability, API, MCP, A2A and proof-profile
specifications. If this brief conflicts with a normative specification, the
normative specification wins.

## Objective

Implement the production TOS-backed path:

```text
authenticated gateway principal/provider
    -> global TOS Agent Identity
    -> exact Capability ownership
    -> exact manifest/version commitment
    -> current execution-signer authorization
    -> TOS-backed ActivationAuthority decision
```

A Capability may advertise `verified` only while every required identity,
ownership, version, manifest, signer, network and finality checkpoint is current.
Missing, stale, revoked, transferred, cross-network or uncertain evidence must
fail closed.

## Repository and branch discipline

Work from current `main` in dedicated worktrees. Use the coordinated branch name
`agent/phase4a-production-identity-ownership` in every repository that requires
changes:

- `tosnetwork/atos-spec`
- `tosnetwork/atos`
- `tosnetwork/tos-protocol`
- `tosnetwork/tos`

Inspect repository instructions and uncommitted changes before acting. Do not
modify another contributor's branch or worktree. Record exact compatible commits
for all cross-repository changes. Do not merge to `main` as part of this work.

Freeze missing public contracts in `atos-spec` before implementing them. Code
must not silently become the specification.

## Scope boundary

This phase implements gateway-mediated Verified identity and ownership. It does
not implement:

- Phase 4B's complete Quote/escrow/execution/settlement path;
- Phase 4C's complete portable proof package;
- Phase 5 Wallet-Native stateless gateway authorization;
- Native resolution or gateway federation;
- Phase 7A Managed financial-ledger integration.

`principal_id` remains gateway-local. Passkey or Device Authorization proves
control of an ATOS account, not automatic control of a claimed TOS identity.

## Required deliverables

### 1. Production Agent identity binding

Implement a durable, auditable binding between an authenticated gateway
principal/provider and a real TOS Agent Identity. The binding must include the
TOS network/genesis domain, identity version, authority/reference, status,
finality/freshness, timestamps, idempotency identity and reconciliation state.

Never trust a caller-supplied Agent ID by itself. Establish binding authority
through a frozen and verifiable TOS-backed mechanism. Handle rotation,
revocation, recovery, stale references and network mismatch explicitly.

Keep these identities distinct:

```text
gateway principal
provider
global TOS Agent Identity
execution signer
```

### 2. Provider and Capability ownership resolution

Provider identity must derive from authentication and stored ownership, never a
request-body `provider_id`. Scope does not imply ownership, and ownership does
not grant scope.

For every Capability requesting Verified support, establish:

```text
current TOS Agent Identity
    -> owns the provider
    -> owns the exact Capability
    -> owns the exact Capability version and manifest commitment
```

Reject wrong owner, transferred ownership, revoked identity, wrong Capability,
wrong version, provisional local IDs, stale/unfinalized evidence and mixed TOS
networks.

### 3. Canonical manifest/version anchoring

Freeze and implement deterministic manifest commitment semantics, including:

- encoding and commitment version;
- domain separation;
- global Capability ID and owner identity;
- exact Capability version;
- immutable execution binding or commitment;
- relevant schemas and immutable commercial/trust fields;
- network identity and sequence/reference.

Ship cross-repository deterministic vectors. A version, owner, binding, manifest,
signer or network change must invalidate current readiness and require a new
authoritative evaluation. Historical Quotes and Jobs remain immutable.

### 4. Network, genesis and finality binding

Identity, ownership, manifest, signer authorization and activation must refer to
the same configured TOS network. Validate network ID, genesis/chain domain,
protocol domain, endpoint identity and finality policy. Reject testnet/mainnet
mixing, failover to a different network, stale forks and endpoint disagreement.

### 5. TOS-backed ActivationAuthority

Keep the existing fail-closed authority as the safe default. Add a production
TOS-backed implementation that grants Verified activation only when all of the
following are current and agree:

```text
provider identity
Capability ownership
exact manifest/version commitment
required execution-signer authorization
TOS network/domain
required finality
```

Provider intent, health and sandbox certification are readiness evidence, not
activation authority. Timeouts and uncertain evidence deny activation. Never
silently downgrade to Managed.

Preserve the existing legal lifecycle:

```text
requested -> pending -> active -> suspended -> unsupported
```

Loss of current evidence suspends future Verified availability through the
existing state machine; it must not rewrite historical transactions.

### 6. Durable external-operation journal

Every TOS side effect follows:

```text
durable stable intent
    -> external operation with the same semantic identity
    -> durable observed outcome
```

Use stable idempotency identities and content digests. Identical retries
converge; changed semantics conflict. Persist uncertain outcomes, reconcile lost
responses, and survive crashes, restarts and multiple replicas. Reuse the proven
Phase 3B signer-operation journal and `tos-protocol` commitment patterns rather
than creating a weaker mechanism.

### 7. Public and operator surfaces

After contracts are frozen, expose only the required authenticated REST/MCP
and operator operations for:

- identity-binding status and mutation;
- Capability ownership status;
- manifest commitment/status;
- Phase 4A readiness;
- TOS-backed activation evaluation;
- rotation, revocation, refresh and reconciliation where applicable.

Define exact scopes, roles, ownership checks, idempotency, errors and redaction.
Admin-only authority must not be obtainable through ordinary self-service
authorization. Keep REST and MCP behavior coherent; add A2A operations only when
the normative profile requires them.

### 8. Persistence, caching and freshness

Add real PostgreSQL migrations and matching memory-store behavior. Enforce
uniqueness, ownership, version, network and idempotency constraints. Use CAS or
equivalent transactional updates for concurrent mutations.

Cached TOS evidence is never authority. Bind cache entries to network,
checkpoint, finality, observed time and expiry. Define negative caching,
reorg/failover handling and refresh behavior. Stale or uncertain evidence cannot
keep Verified active.

## Security review requirements

Explicitly test and defend against:

- principal-to-TOS-identity impersonation;
- caller-selected provider/owner IDs;
- confused-deputy authorization;
- cross-network and cross-domain replay;
- stale or transferred ownership;
- old keys remaining valid after rotation/revocation;
- Capability version and manifest TOCTOU;
- signer authorization for the wrong version;
- malicious or misconfigured RPC endpoints;
- test authority reachable in production;
- self-service acquisition of administrative scopes;
- idempotency-key reuse with changed content;
- lost-response duplicate effects;
- multi-replica activation races;
- cache poisoning and chain reorganization;
- secret, private-key, credential or internal-error leakage.

Correctness of durable state must not depend on a process-local mutex.

## Acceptance tests

Use fresh PostgreSQL 16, at least two independent service/store instances, a
real `tos-protocol` server and a real TOS localnet. Interface mocks may support
fault injection but cannot be the only protocol evidence.

The positive end-to-end test must demonstrate:

```text
create/resolve TOS Agent Identity
-> bind authenticated provider
-> register/resolve exact Capability ownership
-> commit exact manifest/version
-> authorize exact-version execution signer
-> record readiness evidence
-> TOS-backed ActivationAuthority evaluates
-> pending becomes active
-> supported_trust_modes includes verified
```

The negative matrix must cover wrong network, missing/revoked identity, wrong or
transferred owner, wrong Capability version, changed manifest, missing/revoked
signer, unfinalized reference, unavailable RPC, lost response and concurrent
conflicting operations. Every case must keep Verified inactive or suspend it
through the legal state machine, without silently falling back to Managed.

Run in every affected repository, as applicable:

```text
git diff --check
gofmt -l .
go vet ./...
go build ./...
go test ./... -race -count=1
```

Report tests that were not run and the exact missing dependency. Do not hide
failures with skips, weaker assertions or build tags.

## Completion report

The final report must include:

1. branches and exact commits for every repository;
2. the cross-repository compatibility matrix;
3. reused foundations versus new implementation;
4. identity and ownership models;
5. manifest encoding and vectors;
6. network/genesis/finality enforcement;
7. ActivationAuthority and operation-journal behavior;
8. migrations, constraints and public scopes;
9. real PostgreSQL, `tos-protocol` and TOS localnet results;
10. crash, lost-response, multi-replica, rotation, transfer and version tests;
11. remaining Phase 4B/4C or production-infrastructure work;
12. a checklist against every Phase 4A acceptance requirement.

Do not claim Phase 4A complete until the production code path uses real
TOS-backed identity, ownership, manifest and activation evidence and the real
protocol/failure tests pass.
