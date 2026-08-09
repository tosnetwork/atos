# Phase 2 / Phase 3 Design Invariants

- A concrete Quote trust mode is immutable and never silently downgraded.
- Account-changing transitions use the existing atomic Job/Account transaction boundary.
- External mutations are journaled before dispatch and support exact idempotent replay.
- Ambiguous outcomes remain recoverable, never guessed terminal.
- Provider earnings derive only from verified settlement usage and mature exactly once.
- Managed disputes freeze unresolved earnings and resolve by one atomic economic transition.
- Provider certification and signer lifecycle do not self-activate stronger trust modes.
- Streaming cursors bind sequence, offset, and digest and reject substitution.
