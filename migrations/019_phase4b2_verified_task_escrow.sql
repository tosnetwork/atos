CREATE TABLE escrow_operations (
    job_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('reserve','release')),
    operation_id TEXT NOT NULL UNIQUE,
    quote_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    checkpoint TEXT NOT NULL CHECK (checkpoint IN ('intent_persisted','reconciling','authority_reserved','authority_released','projection_persisted','completed')),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (job_id, kind)
);

CREATE INDEX escrow_operations_recovery_idx
    ON escrow_operations (updated_at, job_id, kind)
    WHERE checkpoint <> 'completed';
