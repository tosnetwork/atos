-- Phase 4A: production TOS Agent Identity binding for gateway
-- principals/providers, and Capability ownership/manifest anchoring state.
-- See atos-spec docs/IMPLEMENTATION_ROADMAP.md §8.1 and
-- docs/AUTH.md's "Agent Identity Migration" section.

-- principal_identity_bindings is CURRENT STATE (one row per principal_id,
-- overwritten on rebind), not a journal -- mirrors passkey_accounts'
-- shape. status='revoked' rows are deleted rather than kept (matching
-- tos-protocol's own bucketPrincipalBindings semantics: presence means
-- currently bound); revocation history for operator visibility lives in
-- identity_binding_operations below, not here.
CREATE TABLE IF NOT EXISTS principal_identity_bindings (
    principal_id       TEXT PRIMARY KEY,
    agent_id           TEXT NOT NULL,
    network            TEXT NOT NULL,
    binding_ref        TEXT NOT NULL,
    bound_at           TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_principal_identity_bindings_agent
    ON principal_identity_bindings (agent_id);

-- identity_binding_operations is the durable external-operation journal
-- (docs/IMPLEMENTATION_ROADMAP.md's "durable stable intent -> external
-- operation -> durable observed outcome" rule) for the two tos-protocol
-- IdentityService mutations. Unlike Phase 3B's execution_signer_operations,
-- CreatePrincipalBinding/RevokePrincipalBinding are each ONE atomic,
-- idempotent tos-protocol RPC call (no multi-step cutover sequence), so
-- this journal only needs two checkpoints, not execution_signer's full
-- intent_persisted -> ... -> completed walk: 'intent_persisted' (this
-- process is about to call, or has called and lost the response for, the
-- tos-protocol RPC) and 'completed' (the local principal_identity_bindings
-- row is durably consistent with the confirmed remote outcome). A crash
-- between those two checkpoints is safe to retry with the SAME
-- idempotency_key: tos-protocol's own atomicMutation replays its original
-- response for that key rather than double-anchoring or double-revoking.
CREATE TABLE IF NOT EXISTS identity_binding_operations (
    id                     TEXT PRIMARY KEY,
    principal_id           TEXT NOT NULL,
    -- type: bind | revoke.
    type                   TEXT NOT NULL,
    -- checkpoint: intent_persisted | completed | reconciling.
    checkpoint             TEXT NOT NULL,
    idempotency_key        TEXT NOT NULL,
    agent_id               TEXT NOT NULL DEFAULT '',
    reason_code            TEXT NOT NULL DEFAULT '',
    -- binding_ref is the opaque TOS reference for this operation's
    -- anchored fact: for type='bind', CreatePrincipalBinding's binding_ref;
    -- for type='revoke', RevokePrincipalBinding's revocation_ref. The two
    -- types never populate it simultaneously (mirrors agent_id/reason_code
    -- above), and ref_network carries the network component for both.
    binding_ref            TEXT NOT NULL DEFAULT '',
    ref_network            TEXT NOT NULL DEFAULT '',
    -- created is type='bind'-only: whether CreatePrincipalBinding reported
    -- a genuinely NEW binding versus an idempotent replay of an
    -- already-existing same-principal/same-agent one (docs/API.md §9A's
    -- documented created=false no-op case) -- distinct from checkpoint
    -- reaching 'completed', which is true in both cases.
    created                BOOLEAN NOT NULL DEFAULT FALSE,
    -- revoked is type='revoke'-only: RevokePrincipalBinding's own
    -- authoritative revoked bool, stored directly rather than inferred from
    -- binding_ref being non-empty (revoked/revocation_ref are independent
    -- RPC response fields with no wire-level guarantee they always agree).
    revoked                BOOLEAN NOT NULL DEFAULT FALSE,
    -- content_hash summarizes the identity fields that must never change
    -- once an operation is opened (principal_id/type/idempotency_key/
    -- agent_id/reason_code) -- mirrors execution_signer_operations'
    -- content_hash role exactly.
    content_hash           TEXT NOT NULL,
    failure_reason         TEXT NOT NULL DEFAULT '',
    created_at             TIMESTAMPTZ NOT NULL,
    completed_at           TIMESTAMPTZ,
    updated_at             TIMESTAMPTZ NOT NULL,
    UNIQUE (principal_id, idempotency_key)
);

-- Drives the reconciler's stale-operation sweep: every non-terminal
-- operation (checkpoint <> 'completed'), oldest first -- identical index
-- shape to idx_execution_signer_operations_pending.
CREATE INDEX IF NOT EXISTS idx_identity_binding_operations_pending
    ON identity_binding_operations (updated_at ASC)
    WHERE checkpoint <> 'completed';

-- Capability ownership/manifest anchoring state (atos-spec
-- docs/CAPABILITIES.md §1's "ownership":{"status","network","commitment"}
-- object). One row per capability_id + version, immutable once committed
-- (a version's manifest/ownership never changes -- a new version gets a
-- new row, matching manifest_commitment's own immutability rule).
CREATE TABLE IF NOT EXISTS capability_ownership_commitments (
    capability_id      TEXT NOT NULL,
    version            TEXT NOT NULL,
    provider_id        TEXT NOT NULL,
    network            TEXT NOT NULL,
    manifest_commitment TEXT NOT NULL,
    ownership_commitment TEXT NOT NULL,
    committed_at        TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (capability_id, version)
);
