# Claude Code Task — ATOS Phase 3B: Provider Trust Readiness

You are implementing **Phase 3B — Provider Trust Readiness** for ATOS.

This is a production-quality, multi-repository implementation task. Do not stop at analysis, TODOs, design notes, scaffolding, or acceptance Markdown. Inspect the current repositories, freeze any genuinely missing public contract in `atos-spec`, implement the real code, add migrations where necessary, add crash/concurrency/integration tests, run the required verification commands, and report exactly what was completed.

---

## 1. Repositories and architectural ownership

Relevant repositories:

```text
tosnetwork/atos
tosnetwork/atos-spec
tosnetwork/tos-protocol
tosnetwork/tos-ai
tosnetwork/tos
```

Architecture ownership:

```text
atos
= public REST/MCP/A2A gateway
= auth
= provider control plane
= Capability/readiness projections
= commercial policy
= durable orchestration/reconciliation
= provider-facing trust UX

tos-protocol
= typed trust/execution boundary
= identity
= Capability ownership/commitment integration
= signer authorization
= TOS trust/economic/proof integration

tos-ai
= execution/data plane only
= bounded workers/adapters
= NOT trust authority
= NOT signer-authorization authority

tos
= finalized TOS identity/commitment/economic/proof state

atos-spec
= normative public contract
= schemas
= RPC/protobuf contracts
= proof profiles
= implementation roadmap
```

Preserve this rule:

```text
tos-ai executes.
tos-protocol trusts, proves, and settles.
atos orchestrates and exposes the product/control plane.
```

Ordinary `atos` business code MUST NOT directly import TOS consensus/node clients.

Signer authorization/revocation MUST go through the existing `tos-core` / `tos-protocol` trust boundary.

Do not create a parallel trust engine.

---

## 2. Authoritative documents

Before editing code, read at minimum:

```text
atos-spec/docs/IMPLEMENTATION_ROADMAP.md
atos-spec/docs/ARCHITECTURE_V0.2.md
atos-spec/docs/PROOF_PROFILES.md
atos-spec/docs/TOS_RPC.md
atos-spec/docs/MCP.md
atos-spec/docs/API.md
atos-spec/docs/AUTH.md
atos-spec/docs/CAPABILITIES.md
atos-spec/docs/THIRD_PARTY_EXECUTION_PLANE.md
atos-spec/proto/atos/tos/v1/trust.proto
atos-spec/IMPLEMENTATION_STATUS.md
```

Also inspect current implementations in:

```text
atos/internal/domain/
atos/internal/service/
atos/internal/store/
atos/internal/adapters/
atos/internal/httpapi/
atos/internal/mcp/
atos/cmd/api/

tos-protocol/
```

Particularly inspect:

```text
ModeSupport
ModeSupportEntry
ModeAvailability
AdapterHealthCheck
SandboxCertification

TrustService
AuthorizeExecutionSigner
RevokeExecutionSigner
ResolveExecutionSignerAuthorization

Capability registration/update
provider ownership checks
PostgreSQL transaction/locking primitives
existing reconcilers
existing idempotency implementation
tos-protocol RPC adapter
production configuration gates
```

Do not assume roadmap historical notes exactly describe current HEAD. Inspect actual HEAD first.

---

## 3. Phase 3B mission

Implement **Phase 3B — Provider Trust Readiness**.

The objective is:

```text
make provider trust readiness and execution-signer operations
production-operable

WITHOUT

allowing readiness evidence or provider self-assertion
to self-activate Verified or Native
```

Required deliverables:

```text
1. complete provider-facing mode-support lifecycle UX

   requested
      -> pending
      -> active
      -> suspended
      -> unsupported

2. public per-mode availability/readiness projection
   alongside active supported_trust_modes

3. execution-signer authorization workflow

4. execution-signer rotation workflow

5. execution-signer revocation workflow

6. durable pending/reconciliation checkpoints
   for all external signer mutations

7. explicit activation-authority boundary

8. restart-safe reconciliation

9. multi-replica correctness

10. production wiring of the Phase 3B control-plane paths
```

This is NOT Phase 4.

Do NOT implement production Verified transaction activation merely to make Phase 3B tests pass.

---

## 4. Absolute trust-mode invariant

This invariant is non-negotiable:

```text
provider requested Verified/Native
+ provider says it supports Verified/Native
+ endpoint healthy
+ sandbox certification passed
+ execution signer authorized

MUST NOT imply

ModeSupport[verified/native].Status = active

and MUST NOT imply

supported_trust_modes += verified/native
```

Providers may request stronger modes.

Readiness systems may produce evidence.

Readiness systems may drive legitimate operational states such as:

```text
requested
pending
suspended
unsupported
```

But they are NOT the authority capable of declaring Verified or Native cryptographically/economically active.

Before the Phase 4 activation path is production-ready:

```text
Verified -> fail closed
Native   -> fail closed
```

even when every Phase 3B readiness signal is green.

No hidden fallback. No temporary activation shortcut. No development behavior that leaks into production.

---

## 5. Preserve the existing distinction

Preserve these independent concepts:

```text
RequestedTrustModes
    = what the provider wants

ModeSupport
    = authoritative lifecycle/activation state

SupportedTrustModes
    = derived set of ACTIVE concrete modes

ModeAvailability
    = operational/readiness projection

Health
    = transport observation

SandboxCertification
    = readiness evidence

ExecutionSignerAuthorization
    = signer trust evidence

ActivationAuthority
    = the authority capable of making a stronger mode active
```

Do not collapse them.

In particular:

```text
ModeAvailability.Ready != ModeSupport.Active
```

and:

```text
SupportedTrustModes
MUST continue to be derived from authoritative active ModeSupport entries.
```

Never make `supported_trust_modes` directly writable by a provider request.

---

## 6. Step 0 — Current-state audit

Before implementation, perform a focused audit covering:

```text
A. What Phase 3B domain types already exist?
B. What persistence already exists?
C. Which REST/MCP provider/admin operations already exist?
D. Which scopes already exist?
E. How capability ownership is currently enforced?
F. How ModeSupport currently transitions?
G. Whether ModeAvailability is currently public or internal-only?
H. Whether HealthService/CertificationService have production call sites now?
I. How tos-protocol currently persists signer authorization?
J. Whether Authorize/Revoke are already replay-safe across restart?
K. Whether a signer-operation reconciler already exists?
L. Whether PostgreSQL advisory locking/transaction primitives can be reused?
```

Do not redo Phase 3A components that are already correct.

Only modify Phase 3A code where Phase 3B requires production wiring or fixes a real prerequisite.

---

## 7. Specification-first gate

If Phase 3B public semantics are not completely frozen, update `atos-spec` FIRST.

Do not make implementation code the de facto specification.

Before introducing a public operation, freeze:

```text
request shape
response shape
authorization scope
provider/resource ownership rule
idempotency identity
state transitions
error semantics
public projection semantics
secret/redaction rules
```

This particularly applies to:

```text
mode request/update operations
mode availability/status retrieval
signer authorize
signer rotate
signer revoke
signer operation status/recovery
```

If REST/MCP operation names are missing from the normative specification, define the smallest coherent Phase 3B surface consistent with existing API/MCP conventions.

Do NOT invent an A2A provider-admin protocol merely for symmetry unless the normative A2A contract requires it.

Treat provider-facing UX in this Go gateway primarily as REST/MCP/control-plane workflow unless an existing frontend/provider dashboard is already present.

Do not introduce a new frontend framework as part of Phase 3B.

---

## 8. Mode-support state machine

Define and enforce the exact legal transition graph.

Start from the existing states:

```text
requested
pending
active
suspended
unsupported
```

The implementation must explicitly identify the AUTHORITY for every transition.

At minimum, preserve this model:

```text
Provider:
    may request a mode
    may stop requesting a mode where contract permits
    may NEVER directly set active

Readiness/control plane:
    may compute evidence
    may move operational readiness toward pending
    may cause suspension where policy requires
    may NEVER grant stronger-mode activation authority

Activation authority:
    is the only source capable of authorizing
    Verified/Native active state
```

The exact transition matrix must be documented in `atos-spec`.

Reject illegal transitions rather than normalizing them silently.

A provider-supplied field such as:

```json
{"status":"active"}
```

must never activate a stronger mode.

---

## 9. ActivationAuthority abstraction

Introduce or formalize a narrow activation-authority boundary in `atos` if one does not already exist.

The business service must not contain ad-hoc rules such as:

```go
if healthy && certified && signerExists {
    active = true
}
```

Instead, stronger-mode activation must depend on an explicit authority result conceptually similar to:

```text
ActivationAuthority
    Evaluate(provider, capability, version, mode)
        ->
    authoritative activation decision / evidence
```

Exact Go names should follow current code style.

For production Phase 3B, the absence of a complete Phase 4 authority MUST return a fail-closed result for Verified/Native.

A test-only authority may be used to exercise lifecycle logic, but production configuration must not accidentally enable it.

Do not implement fake on-chain activation merely to satisfy this phase.

---

## 10. Public per-mode availability

Make per-mode availability operationally useful and public through the appropriate existing Capability/provider read surfaces.

The public projection must allow a provider/operator/client to distinguish:

```text
Was this mode requested?
What is its lifecycle status?
Is it active/quotable?
Is an eligible transport healthy?
Is the health evidence fresh?
Has this exact capability version/binding passed certification?
Is required signer authorization current?
Is stronger-mode activation authority satisfied?
Why is the mode not available?
```

Do not expose private credentials or sensitive endpoint configuration.

Reuse the current `ModeAvailability` model where appropriate rather than creating overlapping read models.

Extend it only where Phase 3B semantics require additional information.

The final public representation should be deterministic and schema-defined.

It should explain at least:

```text
requested but not ready
ready but not authorized for activation
active
suspended
unsupported
stale readiness evidence
missing signer authorization
```

without falsely presenting readiness as trust.

---

## 11. Freshness/version binding

All readiness evidence must remain bound to immutable execution identity.

At minimum:

```text
provider_id
capability_id
capability_version
binding identity
transport
endpoint_ref where appropriate
```

A Capability version change must not accidentally inherit current readiness from another version.

A certification for version N must not certify N+1.

A signer authorization scoped to version N must not silently become authorization for N+1 unless the normative contract explicitly says so.

Do not introduce wildcard signer scope as a shortcut.

---

## 12. Execution signer operations

Implement a production control plane for:

```text
authorize
rotate
revoke
```

The existing trust RPC contract includes:

```text
AuthorizeExecutionSigner
RevokeExecutionSigner
ResolveExecutionSignerAuthorization
```

Reuse these semantics.

Do NOT allow ATOS provider business logic to write trust-side signer state directly.

---

## 13. Signer authorization rules

Authorization must be bound to the authenticated provider and target Capability.

Provider identity comes from authentication.

Never trust `provider_id` supplied in request JSON as authority.

Scope and ownership are BOTH required:

```text
correct OAuth/API scope
AND
authenticated provider owns target Capability
```

A provider must not authorize a signer for another provider, another provider's Capability, or an arbitrary Capability version unless explicitly allowed by the normative ownership contract.

Signer public key / signer ID validation must be strict and bounded.

There must be no API accepting a signer private key.

Never log private keys, secrets, bearer credentials, or unredacted sensitive signer material.

---

## 14. Durable signer-operation state machine

Signer mutations are external trust-side effects.

They MUST use the same class of crash-safe pattern already used by the Managed economic state machine:

```text
durable stable intent
    ->
external operation with stable semantic identity
    ->
durable observed/completed result
```

Do not implement an unsafe `RPC()` followed by `store result` with no recovery checkpoint.

Introduce or reuse a durable signer-operation journal/state machine.

It must preserve enough information to recover:

```text
operation/action ID
provider ID
capability ID
capability version
operation type
old signer identity where applicable
new signer identity where applicable
canonical semantic request digest
stable idempotency identity
current checkpoint
authorization reference
revocation reference
last deterministic error
timestamps
```

Never persist signer private key material.

---

## 15. Stable idempotency

Every external signer mutation must use a stable idempotency identity.

Required property:

```text
same caller
+ same idempotency key
+ same canonical semantic request
=
same semantic result
```

and:

```text
same caller
+ same idempotency key
+ changed semantic request
=
idempotency_conflict
```

Transport metadata MUST NOT change the semantic identity.

Retries may regenerate request IDs, trace IDs, and RPC deadline metadata, but must reuse the same business mutation identity.

Follow the already-fixed `tos-protocol` mutation-idempotency conventions rather than introducing another digest model.

---

## 16. Authorization crash recovery

Prove these cases:

```text
1. crash after durable local intent but before RPC

2. AuthorizeExecutionSigner commits remotely,
   response is lost

3. response arrives,
   process crashes before local completion is persisted

4. process restarts

5. reconciler replays/re-resolves operation

6. exactly one semantic authorization exists

7. local projection converges
```

A timeout is not proof that authorization failed.

Do not mark it failed simply because an RPC response was lost.

Ambiguous external state must remain `pending` or `reconciling` until deterministically resolved.

---

## 17. Revocation crash recovery

Apply the same semantics to revocation.

Prove:

```text
lost RevokeExecutionSigner response
restart before local completion
duplicate replay
two-replica replay
```

A signer must never become locally revoked merely because an outbound request timed out.

Persist the remote revocation proof/reference where available.

---

## 18. Safe rotation protocol

Do NOT implement rotation as:

```text
revoke old
authorize new
```

because a crash between those operations could leave the Capability without its intended signer.

Use a safe, explicitly documented transition.

The intended safety principle is:

```text
old known-good signer remains authoritative
while new signer authorization is uncertain

authorize new signer
    ->
confirm new authorization
    ->
perform the defined cutover
    ->
revoke old signer
    ->
confirm old revocation
    ->
rotation complete
```

If the TOS trust model temporarily contains both old and new authorizations during safe rotation, that overlap must be explicit and well-defined.

ATOS MUST NOT advertise the new signer as current before its authorization is confirmed.

ATOS MUST NOT irrecoverably discard the old signer before the new authorization is confirmed.

Do not pretend the rotation is complete until all required remote/local checkpoints are durable.

A crash at ANY boundary must converge after restart.

---

## 19. Rotation checkpoints

Use explicit durable checkpoints equivalent to:

```text
intent_persisted

new_authorization_pending
new_authorized

cutover_pending

old_revocation_pending
old_revoked

completed
```

plus an explicit `reconciling` or equivalent recoverable state where a remote outcome is uncertain.

Do not copy these exact enum names if the project has a better established convention, but preserve the semantics.

---

## 20. Multi-replica correctness

Correctness MUST NOT depend on a process-local mutex.

Critical signer and mode-support operations must work when two ATOS replicas use the same PostgreSQL database.

Reuse PostgreSQL transaction/advisory-lock primitives already established in ATOS.

Add real PostgreSQL 16 tests with TWO independent service/store instances for at least:

```text
same authorization requested concurrently
same revocation requested concurrently
same rotation replayed concurrently
conflicting signer mutation on same Capability/version
provider mode request racing with readiness reconciliation
reconciler racing with live request
```

The result must converge to one legal durable state.

No duplicate external semantic mutation. No split-brain signer projection.

---

## 21. Reconciler

Add signer operations to the startup + periodic reconciliation architecture.

Do not create an unrelated background framework if the existing `RunReconciler` infrastructure can be extended cleanly.

On startup, recover stale signer operations.

Periodically scan recoverable states.

Bound batch size, concurrency, deadlines, and retry behavior.

Avoid unbounded goroutines.

A deterministic permanent semantic error may become terminal.

An ambiguous transport/external outcome must remain recoverable.

---

## 22. Health/certification production wiring

Inspect the current state of `HealthService` and `CertificationService` and their provider/admin entry points.

If they still exist only as unit-tested services with no production call sites, Phase 3B must wire the minimum spec-defined control-plane paths needed to make readiness operational.

Do NOT add direct arbitrary network probing from the ATOS business layer.

Health/certification must continue to use the bounded third-party execution/probing boundary established in Phase 3A:

```text
ATOS control plane
    ->
tos-protocol
    ->
tos-ai third-party execution/probing plane
```

with the operator allowlist remaining fail-closed.

Do not create a second HTTP/MCP/A2A probe implementation.

---

## 23. Mode suspension semantics

Freeze exact semantics for operational degradation.

If a stronger mode was legitimately activated by the proper authority and later readiness becomes invalid, the system must be capable of representing it as operationally unavailable/suspended without destroying underlying historical trust evidence.

Examples include:

```text
health becomes stale
endpoint becomes unhealthy
certification is no longer current for the Capability version
required signer authorization is revoked
```

Restoration from suspended to active MUST still require the required activation-authority condition.

A green readiness check alone must never perform unauthorized reactivation.

Test this using a test activation authority; do not implement Phase 4 production activation.

---

## 24. Managed Mode must remain stable

Do not break Managed Mode.

Managed is a permanent product mode.

Phase 3B stronger-mode readiness must not:

```text
remove Managed
force TOS signer authorization for Managed
require Phase 4 infrastructure for Managed calls
change existing Managed Quote semantics
change billing/dispute behavior
```

Existing Phase 1/2/3A acceptance tests must remain green.

---

## 25. Public/API/MCP authorization

Provider trust operations must follow the existing authentication architecture.

For every provider/admin operation test:

```text
no scope
scope only
provider role only
correct provider + scope
wrong provider
wrong capability
revoked authorization
changed authorization
```

`tools/list` visibility remains separate from `tools/call` authorization.

Do not trust tool visibility as the security boundary.

Every actual call must re-check scope, role, resource ownership, and current authorization.

Do not add Phase 3B signer management to the ordinary nine-tool consumer surface.

Provider/admin tools should only be visible when authorization makes them relevant.

---

## 26. No secrets in public projections

Inspect all new JSON/OpenAPI/MCP responses.

Never expose:

```text
provider endpoint credentials
Authorization headers
private endpoint configuration
signer private keys
internal RPC credentials
operator allowlist secrets
database internals
raw sensitive backend errors
```

Public signer state may expose only information intentionally frozen by the normative public contract.

---

## 27. Persistence and migrations

If new durable tables/columns/indexes are required:

```text
add proper PostgreSQL migrations
add memory-store equivalent behavior
add store interfaces
add transactional primitives
add uniqueness constraints
```

Do not rely on application-only uniqueness where the database can enforce the invariant.

Run migrations from a fresh PostgreSQL 16 database.

Ensure restart from already-applied previous migrations also works.

---

## 28. Important failure-injection tests

At minimum add deterministic failure injection for:

```text
lost AuthorizeExecutionSigner response after remote commit
lost RevokeExecutionSigner response after remote commit
crash after local signer intent
crash after remote authorize but before local checkpoint
crash after new signer authorization during rotation
crash immediately before old signer revocation
lost old-signer revocation response
restart during rotation
repeated reconciler execution
two replicas recovering the same signer operation
same idempotency key + changed signer request
wrong provider/capability/version binding
stale Capability version
provider self-asserted active mode
health + certification + signer authorization without activation authority
```

The last case MUST prove:

```text
verified/native is still NOT in supported_trust_modes
```

---

## 29. Required lifecycle acceptance scenario

Implement an end-to-end Phase 3B acceptance test approximately equivalent to:

```text
1. provider owns Capability C version N

2. Managed is active

3. provider requests Verified

4. Verified becomes requested/pending,
   NOT active

5. provider binding is healthy

6. exact version/binding passes sandbox certification

7. provider authorizes execution signer S1

8. signer authorization is confirmed

9. public readiness shows all relevant evidence green

10. Verified remains NOT active because
    Phase 4 activation authority is unavailable

11. rotate S1 -> S2

12. inject crash/lost response during rotation

13. restart ATOS

14. reconciliation converges to S2 according to
    the frozen rotation semantics

15. revoke S2 with a lost RPC response

16. restart again

17. reconciliation confirms final revocation state

18. at no point did provider-controlled evidence
    activate Verified or Native
```

Run this against real PostgreSQL.

Where practical, use the real ConnectRPC `tos-protocol` service, not only a Go-interface mock.

Mocks remain appropriate for deterministic fault injection.

---

## 30. Activation-authority positive test

Also prove the opposite direction at the domain/service level.

Using a TEST-ONLY activation authority:

```text
requested
+ required readiness
+ signer authorization
+ authoritative activation decision
=
active
```

Then prove `supported_trust_modes` is derived correctly.

This ensures Phase 3B does not merely hard-code Verified as always disabled.

It must instead build the correct authority boundary that Phase 4 can later satisfy.

Production Phase 3B configuration must remain fail-closed.

---

## 31. Capability version change test

Test:

```text
Capability version N:
    health green
    certification passed
    signer authorized

Capability updated -> version N+1
```

Then verify N+1 does NOT automatically inherit inappropriate current evidence from N.

The public readiness projection for N+1 must accurately explain what is now missing/stale.

Historical N evidence must remain auditable where the data model requires it.

Do not mutate old execution history.

---

## 32. Quote/Job immutability

Do not change the semantics of already committed Quote/Job history while implementing Phase 3B.

Existing Quotes and Jobs keep their frozen:

```text
provider
capability
capability version
binding
trust mode
proof profile
economic terms
```

A provider changing readiness or signer configuration must not rewrite a historical Receipt or reinterpret an old Quote.

Receipt verification should use signer authorization semantics applicable to the relevant execution time/version, not simply whatever signer is current now.

---

## 33. tos-protocol work

Audit the current `TrustService` implementation.

Do not change protobuf unnecessarily.

If existing RPCs are sufficient:

```text
AuthorizeExecutionSigner
RevokeExecutionSigner
ResolveExecutionSignerAuthorization
```

reuse them.

If Phase 3B requires a genuinely missing cross-repository semantic operation, update `atos-spec` first, then the implementation.

Do not add a `RotateExecutionSigner` RPC merely for convenience if rotation can safely and normatively be implemented as durable ATOS orchestration of authorize + revoke.

Only add a new RPC when the existing contract cannot satisfy required atomicity/recovery semantics.

Audit `tos-protocol` signer persistence for:

```text
stable semantic idempotency
restart persistence
canonical request digest
same-key substitution rejection
provider/capability/version binding
authorization validity interval
revocation durability
replay
```

Fix real deficiencies if found and add tests there.

---

## 34. tos-ai scope

Do not modify `tos-ai` merely because this phase concerns providers.

Execution-signer authorization belongs to the trust plane.

Modify `tos-ai` only if a verified, necessary Phase 3B readiness integration gap exists.

Do not move wallet ownership, trust activation, signer authorization, or settlement authority into `tos-ai`.

---

## 35. Observability

Add useful structured observability for signer operations and mode lifecycle without leaking secrets.

Useful dimensions may include:

```text
operation type
provider ID
capability ID
capability version
mode
checkpoint
result category
reconciliation attempt
```

Bound cardinality according to existing observability conventions.

Never log private key material.

---

## 36. Error semantics

Use stable structured errors consistent with the existing ATOS model.

Distinguish where appropriate:

```text
unauthorized
forbidden
ownership mismatch
invalid transition
trust mode unavailable
activation authority unavailable
signer already authorized
signer not authorized
idempotency conflict
stale capability version
operation pending/reconciling
remote unavailable
permanent trust-side rejection
```

Do not expose raw internal RPC/database errors directly to users.

A transient RPC timeout must not be represented as a permanent trust rejection.

---

## 37. Test levels

Use all appropriate levels.

### Domain tests

Test:

```text
transition matrix
active-mode derivation
availability projection
activation-authority separation
rotation state machine
```

### Store tests

Test:

```text
idempotency
semantic conflict
atomic state transitions
uniqueness
recovery scanning
```

against memory and PostgreSQL where applicable.

### Service tests

Test:

```text
authorization
ownership
mode lifecycle
signer lifecycle
reconciliation
```

### HTTP/MCP tests

Test:

```text
schemas
authorization scopes
resource ownership
visibility vs call authorization
public readiness projection
```

### RPC integration tests

Use real ConnectRPC `tos-protocol` where possible.

### PostgreSQL multi-replica tests

Use two independently constructed Store/service instances.

### Race tests

Required.

---

## 38. Existing invariants must remain green

Do not regress:

```text
Phase 0 trust-mode semantics
Managed accounting
Managed crash-safe economic state machine
Quote continuity/binding freeze
third-party execution placement
HTTP/MCP/A2A bounded execution
schema validation
health/certification version binding
metered billing
provider earnings
disputes
payout holds
artifact security
StreamJob recovery
Device Authorization
ordinary MCP nine-tool compactness
```

Provider/admin Phase 3B operations must not bloat the ordinary consumer tool list.

---

## 39. Documentation updates

After implementation, update documentation truthfully.

At minimum inspect/update:

```text
atos-spec/docs/IMPLEMENTATION_ROADMAP.md
atos-spec/IMPLEMENTATION_STATUS.md
atos/IMPLEMENTATION_STATUS.md
README/docs where public behavior changed
OpenAPI/schema files if changed
MCP schema/spec if changed
```

Do NOT mark Phase 3B complete until its success criteria actually pass.

Do not describe skipped tests as passed.

---

## 40. Required verification commands

On every modified Go repository run on the exact final HEAD:

```bash
git diff --stat origin/main...HEAD
git diff --name-only origin/main...HEAD
git diff --check origin/main...HEAD

gofmt -l .
go vet ./...
go build ./...
go test ./... -race -count=1
```

If `gofmt -l .` prints modified Go files, fix them.

For PostgreSQL-backed changes:

```text
start/use PostgreSQL 16
apply ALL migrations from a fresh database
run relevant PostgreSQL integration tests
run multi-replica tests
```

Run real RPC integration tests required by changed code.

Clearly distinguish:

```text
executed and passed
executed and failed
skipped because dependency/environment absent
known pre-existing failure reproduced on main
CI job failed to start
actual test failure
```

---

## 41. Cross-repository verification

If more than one repository changes, verify compatible exact commits together.

Do not claim Phase 3B complete based only on isolated mock tests in `atos`.

The acceptance path must exercise the actual contract between:

```text
atos
    ->
tos-protocol TrustService
```

for signer mutation and resolution.

No source-level mock may be the only evidence that the cross-repository path works.

---

## 42. Git discipline

Before changing anything:

```bash
git status
git branch --show-current
git log -5 --oneline
```

Preserve unrelated user changes.

Do not reset, discard, overwrite, or reformat unrelated work.

Prefer focused commits by repository/work package.

Do not merge to `main` automatically.

Do not fabricate commit IDs or test results.

---

## 43. Phase 3B success criteria

You may report Phase 3B complete only when ALL of these are true:

```text
provider mode-support lifecycle is operational
requested/pending/active/suspended/unsupported semantics are explicit
providers cannot directly activate stronger modes
public per-mode availability exists
readiness remains separate from activation
signer authorization is operational
signer rotation is operational
signer revocation is operational
signer mutations have durable intent checkpoints
lost responses are replay/reconciliation safe
restart recovery works
rotation survives crash at every external-call boundary
revocation survives crash at every external-call boundary
two replicas converge on one legal state
idempotency substitution conflicts correctly
Capability-version binding is enforced
readiness evidence does not leak across versions
signer evidence does not silently leak across versions
health + certification + signer alone cannot activate Verified
health + certification + signer alone cannot activate Native
activation authority is explicit and testable
production stronger-mode activation remains fail-closed before Phase 4
Managed Mode remains unaffected
real PostgreSQL tests pass
real tos-protocol RPC integration passes
go test ./... -race -count=1 passes
```

The most important regression test is:

```text
provider self-assertion
+ healthy transport
+ passed sandbox certification
+ authorized signer

!=

Verified/Native active
```

---

## 44. Non-goals

Do NOT implement as part of this task:

```text
Phase 3C Open Task Marketplace
Phase 4 production Verified activation
full tos_verified_v1 independent verifier
production TOS escrow changes unrelated to signer readiness
Native decentralized resolution
gateway federation
new semantic reputation system
new provider execution engine
new direct HTTP/MCP/A2A path from ATOS business services
new frontend framework
```

Do not broaden Phase 3B into Phase 4.

---

## 45. Final report format

At the end give a concrete engineering report with:

### A. Current-state findings

What already existed and what was actually missing.

### B. Specification changes

Every changed `atos-spec` file and why.

### C. ATOS implementation

Domain, store, service, HTTP/MCP, reconciliation, configuration, and migration changes.

### D. tos-protocol implementation

Any trust/idempotency/recovery changes.

### E. Other repositories

Explain explicitly whether `tos-ai` or `tos` changed and why.

### F. Crash-safety proof

List every failure boundary tested.

### G. Multi-replica proof

Explain PostgreSQL concurrency scenarios actually run.

### H. Activation-authority proof

Show tests proving:

```text
green readiness without authority -> NOT active

green readiness + trusted test authority -> active
```

### I. Commands actually executed

Include exact commands and outcomes.

### J. Remaining gaps

List only real remaining gaps.

Do not hide known limitations.

### K. Diff summary

Show:

```bash
git diff --stat origin/main...HEAD
git diff --name-only origin/main...HEAD
```

for every modified repository.

---

## 46. Final instruction

Implement Phase 3B as a durable production control-plane feature, not a demo.

The architectural principle to preserve throughout implementation is:

```text
Readiness is evidence.
Signer authorization is evidence.
Provider intent is intent.

None of them is stronger-mode activation authority.
```

And for all trust-side mutations:

```text
persist intent first
use stable semantic identity
perform external effect
persist observed result
reconcile uncertainty after restart
```

Do not declare completion until the real implementation, real migrations, real PostgreSQL concurrency tests, real `tos-protocol` RPC tests, race tests, and fail-closed activation tests all pass.
