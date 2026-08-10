-- Phase 3B: execution-signer authorize/rotate/revoke durable journal.
--
-- tos-protocol's TrustService.AuthorizeExecutionSigner/RevokeExecutionSigner
-- RPCs are each synchronous and atomic-or-nothing -- neither has an
-- async/pending concept of its own. All crash recovery for the multi-step
-- authorize/rotate/revoke sequence (docs/IMPLEMENTATION_ROADMAP.md §7.2.2's
-- frozen checkpoint model) is atos's own responsibility, and this table is
-- where that durable state lives -- see internal/domain/execution_signer_operation.go's
-- doc comment for the full checkpoint sequence and per-Type field usage.
CREATE TABLE IF NOT EXISTS execution_signer_operations (
    id                       TEXT PRIMARY KEY,
    provider_id              TEXT NOT NULL,
    capability_id            TEXT NOT NULL,
    capability_version       TEXT NOT NULL,
    -- type: authorize | revoke | rotate.
    type                     TEXT NOT NULL,
    -- checkpoint: intent_persisted | new_authorization_pending |
    -- new_authorized | cutover_pending | old_revocation_pending |
    -- old_revoked | completed | reconciling.
    checkpoint               TEXT NOT NULL,
    idempotency_key          TEXT NOT NULL,
    new_authorization_id     TEXT NOT NULL DEFAULT '',
    new_execution_signer_id  TEXT NOT NULL DEFAULT '',
    new_signer_public_key    BYTEA,
    new_signature_algorithm  TEXT NOT NULL DEFAULT '',
    new_valid_from           TIMESTAMPTZ,
    new_valid_until          TIMESTAMPTZ,
    new_authorization_ref    TEXT NOT NULL DEFAULT '',
    old_authorization_id     TEXT NOT NULL DEFAULT '',
    old_execution_signer_id  TEXT NOT NULL DEFAULT '',
    revocation_reason_code   TEXT NOT NULL DEFAULT '',
    failure_reason           TEXT NOT NULL DEFAULT '',
    -- content_hash summarizes the identity fields that must never change
    -- once an operation is opened (provider/capability/version/type/
    -- idempotency_key/new signer identity/old authorization identity) --
    -- not the lifecycle fields (checkpoint, refs, failure_reason,
    -- completed_at, updated_at), which legitimately change over the
    -- operation's life. Mirrors sandbox_certifications.content_hash's role.
    content_hash              TEXT NOT NULL,
    created_at                TIMESTAMPTZ NOT NULL,
    completed_at              TIMESTAMPTZ,
    updated_at                TIMESTAMPTZ NOT NULL,
    UNIQUE (provider_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_execution_signer_operations_capability
    ON execution_signer_operations (capability_id, updated_at DESC, id ASC);

-- Drives the reconciler's stale-operation sweep: every non-terminal
-- operation (checkpoint != 'completed'), oldest first.
CREATE INDEX IF NOT EXISTS idx_execution_signer_operations_pending
    ON execution_signer_operations (updated_at ASC)
    WHERE checkpoint <> 'completed';
