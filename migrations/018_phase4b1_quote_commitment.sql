CREATE TABLE IF NOT EXISTS quote_commitment_operations (
    quote_id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    checkpoint TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS quote_commitment_operations_idempotency_idx ON quote_commitment_operations(principal_id,idempotency_key) WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS quote_commitment_operations_checkpoint_idx
    ON quote_commitment_operations (checkpoint, updated_at);
