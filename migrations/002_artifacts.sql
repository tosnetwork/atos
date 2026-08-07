-- Artifact metadata for the signed-URL file transfer flow
-- (~/atos-spec/docs/ARTIFACTS.md). The actual bytes live wherever
-- internal/adapters/storage.Provider puts them (local disk in Phase 0/1,
-- an S3-compatible bucket later) -- this table only tracks ownership and
-- lifecycle state, never content.

CREATE TABLE IF NOT EXISTS artifacts (
    id                  TEXT PRIMARY KEY,
    owner_principal_id  TEXT NOT NULL,
    content_type        TEXT NOT NULL,
    size_bytes          BIGINT NOT NULL,
    sha256              TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    expires_at          TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS artifacts_owner_principal_id_idx ON artifacts (owner_principal_id);
