# Lessons from Phase 4A: Testing Practices to Prevent Recurring Bug Classes

Phase 4A (production identity binding, capability ownership activation) went
through 10 rounds of independent code review after implementation, which
found roughly 33 real, confirmed bugs. This is not a one-time cleanup log —
it documents *recurring bug classes* and the specific testing practices that
would have caught them earlier, so future work in this repo builds these
practices in from the start instead of discovering them the same way again.

## What kept slipping through, and why

### 1. Incomplete input-validation coverage on any new validated-input surface

The `-identity-seed-file` bootstrap mechanism went through 7 separate fixes
across 5 review rounds: format validation, in-batch uniqueness, cross-run
uniqueness against persisted state, and controller-address cardinality were
each found one at a time, in separate rounds, on the same function.

**Root cause:** validation was written by enumerating cases as they came to
mind, not by systematically listing every dimension a field needs checked
against.

**Practice:** before considering input validation for any new surface (a
seed/config file, an RPC request body) complete, write out the full
field × dimension table first:

```text
For each field: required? / valid format? / unique within this request or
batch? / unique against already-persisted state? / referentially valid
against other data it must resolve through?
```

One test per populated cell. Do not consider validation done until the table
has no empty cells that should be checked.

### 2. Idempotent-replay "truth signal" chosen incorrectly

`CreatePrincipalBinding`/`RevokePrincipalBinding`'s idempotent-replay
handling was wrong in four separate ways across rounds 1, 7, 8, and 10:
re-validating current state on a replay that shouldn't re-validate it,
being unable to distinguish "already revoked" from "never bound" on a
retry with a fresh idempotency key, inferring a boolean result from a
secondary field (ref non-empty) instead of using the RPC's own authoritative
boolean, and a status endpoint silently losing revocation history entirely.

**Root cause:** the state space for an idempotent operation is (operation
type) × (outcome) × (same-key retry vs. fresh-key retry vs. first attempt),
reasoned about one dimension at a time instead of drawn out as a full matrix
before writing tests.

**Practice:** for every idempotent RPC/operation, draw the full transition
table before writing tests. At minimum include: first attempt, same-key
retry after success, same-key retry after ambiguous failure, **fresh-key
retry after success** (this exact cell caused 3+ of the bugs above and is
the easiest one to forget), and a pure status/read query after each terminal
state. One test per cell.

### 3. Version-pinning / TOCTOU across concurrent mutations

`CapabilityService.Update`'s second CAS write could return a concurrently-
superseding caller's data instead of the caller's own; a version bump left
an already-Active stronger trust mode advertised without re-evaluation;
`TOSBackedActivationAuthority.Evaluate`'s signer check silently re-resolved
against the capability's current version instead of the version pinned by
every other check in the same call.

**Root cause:** these bugs are only reachable by actually interleaving two
writes. Most tests called one operation and asserted the result; genuine
concurrent-interleaving tests (a wrapped store/core that injects a second
write mid-flow) were only introduced reactively, in the last two review
rounds, after a reviewer had already found the bug by hand.

**Practice:** every CAS-guarded mutation gets at least one test that forces
a second write to land between the read and the write — proactively, when
the CAS code is written, not after a reviewer flags the missing coverage.
Use a wrapped store/core injection to make the interleaving deterministic
rather than relying on real goroutines and timing.

### 4. Field plumbing lost across adapter/service/transport layers

A revocation reference field failed to propagate through the response
chain; a `created` boolean was silently discarded between the RPC client
and the REST/MCP response; a `Revoked` flag was derived from the wrong
signal after being threaded through incompletely.

**Root cause:** every new field added to a proto/RPC response has to be
manually re-threaded through several layers (mock, real client, service,
REST, MCP). Go's type system does not catch a layer that forgets to read a
field — the zero value is a valid value, so this compiles cleanly and only
fails at runtime, silently.

**Practice:** when adding a new field to any response type, immediately
write one test that asserts a **non-zero-value** reaches the outermost
caller, exercised through every layer the field passes through, before
moving on to the next feature. Do not trust "it compiles" as a signal that
plumbing is complete.

### 5. Code contradicting its own documented invariant

`identity_bindings:read`'s own declaration comment stated it "carries no
ownership precondition" — precisely the criterion for inclusion in the
`adminScopes` set — but it was never added to that set.

**Practice:** whenever a comment asserts an invariant ("this scope has no
ownership check," "this field is mutually exclusive with that one"), treat
writing the comment as a prompt to also check whether some mechanism is
supposed to mechanically enforce it, and verify it actually does.

### 6. Behavioral divergence between the memory and Postgres store backends

`ActiveByMode(mode, limit)` treated `limit<=0` as "unbounded" in the memory
store and as "return zero rows" in Postgres — the same interface, opposite
behavior for the same input.

**Root cause:** two backends exist for test convenience, but most tests
only exercised one of them (memory), so backend parity on edge-case inputs
was never explicitly checked.

**Practice:** any `store.Store` method gets a shared, table-driven test
(edge cases included — zero/negative limits, empty strings, etc.) run
against *both* backends with identical inputs, not separate ad hoc tests
per backend.

### 7. Validation logic copy-pasted instead of centralized

A capability-identity validation guard was duplicated into two RPC-client
call sites and missing from a third.

**Practice:** the moment a validation check is copy-pasted into a *second*
call site, that is the signal to push it down into the shared function all
callers go through — do not wait for a third occurrence or a reviewer to
notice the gap.

### 8. Reconcilers/background sweeps tested only against their real dependency

A periodic suspension sweep ran unconditionally regardless of which
`ActivationAuthority` was wired; against the fail-closed default (a
realistic misconfiguration, not a corner case) it would have mass-suspended
every already-active capability on its first sweep.

**Practice:** any reconciler/background sweep whose behavior depends on
which implementation of a dependency is wired needs a test against the
**fail-closed/default** dependency, not only against the fully-configured
real one. Misconfiguration is the common case in production, not an edge
case.

## Why 10 review rounds kept finding new things

Some of this is genuine emergent complexity: the round-8 bug did not exist
until round 1 and round 7's individually-correct fixes were both in place —
review can only find a bug once the code that has it exists. But most of it
is a gap between "verify this specific fix still works" (what I did after
each round) and "re-audit the whole surface with fresh eyes" (what an
independent review round does). Phase 4A was also built incrementally under
repeated "finish it in one pass" pressure across many sessions, including a
subagent run that exceeded its audit-only mandate and implemented a large
chunk directly — more layers built quickly means more implicit cross-fix
assumptions that only a systematic re-audit surfaces.

**Structural takeaway:** run multi-angle review (`/code-review` or
equivalent) as a routine part of active development — after every 2-3
substantial pieces of new mutation/concurrency-sensitive logic — rather
than saving it all for one large pass at the end. The later a bug in this
list was found, the more other code had already been built on top of the
wrong assumption, and the more expensive the fix became.
