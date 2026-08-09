-- Phase 2A: durable resumable StreamJob event journal.
-- "offset" is a reserved SQL keyword, so the byte cursor column is named
-- byte_offset instead.
CREATE TABLE IF NOT EXISTS job_stream_events (
    job_id             TEXT NOT NULL,
    sequence           BIGINT NOT NULL CHECK (sequence >= 0),
    event_type         TEXT NOT NULL,
    state               TEXT NOT NULL DEFAULT '',
    chunk               BYTEA CHECK (chunk IS NULL OR octet_length(chunk) <= 262144),
    byte_offset         BIGINT NOT NULL DEFAULT 0,
    total_output_bytes  BIGINT NOT NULL DEFAULT 0,
    stream_digest       TEXT NOT NULL DEFAULT '',
    terminal            BOOLEAN NOT NULL DEFAULT false,
    usage               JSONB,
    proof_status        JSONB,
    error_code          TEXT NOT NULL DEFAULT '',
    content_hash        TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, sequence)
);

-- Denormalized per-Job resume cursor kept in sync, in the same transaction,
-- with every job_stream_events append. digest_state carries the marshaled
-- incremental sha256 hasher state (encoding.BinaryMarshaler) so resuming a
-- cumulative stream digest never requires re-reading every prior chunk.
-- upstream_digest is the execution provider's retained-output identity
-- digest (tos-protocol's digestMessage(stored.Output)), captured the first
-- time it is observed. It is a different concept from stream_digest above
-- (which is ATOS's own independently-computed progressive per-chunk
-- cumulative digest): it never changes for a given Job and exists only so a
-- resumed ingestion pull can supply it back to the provider as
-- expected_stream_digest.
CREATE TABLE IF NOT EXISTS job_stream_cursors (
    job_id          TEXT PRIMARY KEY,
    next_sequence   BIGINT NOT NULL DEFAULT 0,
    next_offset     BIGINT NOT NULL DEFAULT 0,
    stream_digest   TEXT NOT NULL DEFAULT '',
    digest_state    BYTEA,
    terminal        BOOLEAN NOT NULL DEFAULT false,
    upstream_digest TEXT NOT NULL DEFAULT '',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
