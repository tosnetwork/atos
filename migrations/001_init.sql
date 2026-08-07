-- Phase 1 Postgres schema for ATOS. Mirrors internal/domain exactly: one
-- table per store.Store collection, jsonb for nested value objects
-- (Money/Pricing/SLA/Trust/SpendPolicy) so their shape can evolve without a
-- migration every time a new optional field is added.

CREATE TABLE IF NOT EXISTS capabilities (
    id             TEXT PRIMARY KEY,
    provider_id    TEXT NOT NULL,
    name           TEXT NOT NULL,
    description    TEXT NOT NULL,
    version        TEXT NOT NULL,
    tags           JSONB NOT NULL DEFAULT '[]',
    modalities     JSONB NOT NULL DEFAULT '[]',
    delivery_mode  TEXT NOT NULL,
    input_schema   JSONB NOT NULL,
    output_schema  JSONB NOT NULL,
    pricing        JSONB NOT NULL,
    sla            JSONB NOT NULL DEFAULT '{}',
    trust          JSONB NOT NULL DEFAULT '{}',
    status         TEXT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS capabilities_provider_id_idx ON capabilities (provider_id);
CREATE INDEX IF NOT EXISTS capabilities_status_idx ON capabilities (status);

CREATE TABLE IF NOT EXISTS quotes (
    id                     TEXT PRIMARY KEY,
    capability_id          TEXT NOT NULL,
    capability_version     TEXT NOT NULL,
    price                  JSONB NOT NULL,
    expires_at             TIMESTAMPTZ NOT NULL,
    requires_confirmation  BOOLEAN NOT NULL DEFAULT FALSE,
    terms_hash             TEXT NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS escrows (
    id             TEXT PRIMARY KEY,
    quote_id       TEXT NOT NULL,
    principal_id   TEXT NOT NULL,
    provider_id    TEXT NOT NULL,
    capability_id  TEXT NOT NULL,
    reserved       JSONB NOT NULL,
    status         TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    expires_at     TIMESTAMPTZ NOT NULL,
    settled_at     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS receipts (
    id             TEXT PRIMARY KEY,
    quote_id       TEXT NOT NULL,
    escrow_id      TEXT NOT NULL,
    job_id         TEXT NOT NULL DEFAULT '',
    principal_id   TEXT NOT NULL,
    charged        JSONB NOT NULL,
    refunded       JSONB NOT NULL,
    status         TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS receipts_job_id_idx ON receipts (job_id);
CREATE INDEX IF NOT EXISTS receipts_principal_id_idx ON receipts (principal_id);

CREATE TABLE IF NOT EXISTS jobs (
    id                       TEXT PRIMARY KEY,
    capability_id            TEXT NOT NULL,
    quote_id                 TEXT NOT NULL,
    escrow_id                TEXT NOT NULL DEFAULT '',
    principal_id             TEXT NOT NULL,
    state                    TEXT NOT NULL,
    input                    JSONB NOT NULL DEFAULT '{}',
    output                   JSONB,
    artifacts                JSONB NOT NULL DEFAULT '[]',
    idempotency_key          TEXT NOT NULL DEFAULT '',
    failure_reason           TEXT NOT NULL DEFAULT '',
    created_at               TIMESTAMPTZ NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL,
    estimated_completion_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS jobs_principal_id_idx ON jobs (principal_id);

CREATE TABLE IF NOT EXISTS accounts (
    principal_id   TEXT PRIMARY KEY,
    balance        JSONB NOT NULL,
    spend_policy   JSONB NOT NULL
);

-- One row per (principal_id, key): the same idempotency_key from two
-- different principals must not collide (see internal/store's
-- composite-key convention, mirrored here as a composite primary key
-- instead of a concatenated string).
CREATE TABLE IF NOT EXISTS idempotency_records (
    principal_id   TEXT NOT NULL,
    key            TEXT NOT NULL,
    request_hash   TEXT NOT NULL,
    response_key   TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL,
    PRIMARY KEY (principal_id, key)
);
