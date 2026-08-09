-- Phase 2B: metered billing and provider earnings.
--
-- billing_snapshots is the durable, auditable record of one Job's metered
-- billing calculation. It is a deterministic function of the frozen Quote
-- terms and the verified Execution Receipt usage, so job_id is the primary
-- key: recomputing and re-persisting an identical snapshot for the same Job
-- is always a safe no-op (see internal/store.Billing.PutBillingSnapshot).
CREATE TABLE IF NOT EXISTS billing_snapshots (
    job_id              TEXT PRIMARY KEY,
    quote_id            TEXT NOT NULL,
    receipt_id          TEXT NOT NULL,
    provider_id         TEXT NOT NULL,
    capability_id       TEXT NOT NULL,
    capability_version  TEXT NOT NULL,
    trust_mode          TEXT NOT NULL,
    usage               JSONB NOT NULL,
    usage_commitment    TEXT NOT NULL DEFAULT '',
    pricing_model       TEXT NOT NULL DEFAULT '',
    pricing_terms_hash  TEXT NOT NULL DEFAULT '',
    gross_charge        JSONB NOT NULL,
    provider_gross      JSONB NOT NULL,
    gateway_fee         JSONB NOT NULL,
    principal_refund    JSONB NOT NULL,
    calculated_at       TIMESTAMPTZ NOT NULL,
    -- content_hash summarizes the semantically meaningful economic fields
    -- (everything above except calculated_at) so a replayed
    -- PutBillingSnapshot for the same job_id can be recognized as an
    -- identical recomputation (idempotent no-op) versus a computation that
    -- produced different economic content for the same Job, which is
    -- rejected rather than silently ignored. Mirrors
    -- job_stream_events.content_hash's role for stream events.
    content_hash        TEXT NOT NULL,
    payload             JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_billing_snapshots_provider ON billing_snapshots (provider_id);

-- provider_earnings is the durable ledger of provider earnings. Each row is
-- uniquely bound to the settlement that produced it: settlement_id is
-- UNIQUE, so a retried or reconciled attempt to create an earning for a
-- settlement that already has one is rejected at the database level, not
-- just by application logic (see internal/store.Earnings.CreateEarning,
-- which relies on ON CONFLICT (settlement_id) DO NOTHING to make earning
-- creation an idempotent, exactly-once operation under concurrent callers).
--
-- payout_idempotency_key is likewise UNIQUE (partial index, since it is
-- empty until the earning enters payout_pending) so two concurrent workers
-- can never durably record two different external payout attempts for the
-- same earning, even if the CAS on status were somehow bypassed.
--
-- The payout_* columns are intentionally not part of the public JSON
-- contract (domain.ProviderEarning tags them json:"-") -- they are recovery
-- checkpoints for the idempotent external-payout state machine, not
-- protocol state a caller should observe or depend on.
CREATE TABLE IF NOT EXISTS provider_earnings (
    id                      TEXT PRIMARY KEY,
    provider_id             TEXT NOT NULL,
    job_id                  TEXT NOT NULL,
    quote_id                TEXT NOT NULL,
    receipt_id              TEXT NOT NULL,
    settlement_id           TEXT NOT NULL,
    capability_id           TEXT NOT NULL,
    capability_version      TEXT NOT NULL,
    gross_amount            JSONB NOT NULL,
    gateway_fee             JSONB NOT NULL,
    net_amount              JSONB NOT NULL,
    status                  TEXT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL,
    matures_at              TIMESTAMPTZ NOT NULL,
    available_at            TIMESTAMPTZ,
    payout_requested_at     TIMESTAMPTZ,
    payout_reference        TEXT NOT NULL DEFAULT '',
    paid_at                 TIMESTAMPTZ,
    payout_idempotency_key  TEXT NOT NULL DEFAULT '',
    payout_attempts         INT NOT NULL DEFAULT 0,
    payout_last_attempt_at  TIMESTAMPTZ,
    payout_failure_reason   TEXT NOT NULL DEFAULT '',
    -- content_hash summarizes the identity+economic fields that must never
    -- differ for a given settlement_id (provider/job/quote/receipt id and
    -- the gross/fee/net split) -- not lifecycle fields like status or
    -- timestamps, which legitimately change over the earning's life. A
    -- CreateEarning conflict on settlement_id with a DIFFERENT content_hash
    -- is rejected rather than silently returning the stale row.
    content_hash            TEXT NOT NULL,
    payload                 JSONB NOT NULL,
    UNIQUE (settlement_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_provider_earnings_payout_key
    ON provider_earnings (payout_idempotency_key)
    WHERE payout_idempotency_key <> '';

CREATE INDEX IF NOT EXISTS idx_provider_earnings_provider ON provider_earnings (provider_id, created_at DESC, id ASC);
CREATE INDEX IF NOT EXISTS idx_provider_earnings_maturing ON provider_earnings (status, matures_at) WHERE status = 'maturing';
CREATE INDEX IF NOT EXISTS idx_provider_earnings_available ON provider_earnings (status) WHERE status = 'available';
CREATE INDEX IF NOT EXISTS idx_provider_earnings_payout_pending ON provider_earnings (status, payout_requested_at) WHERE status = 'payout_pending';

-- Supports the earnings backfill sweep's scan for settled Jobs that do not
-- yet have a corresponding provider_earnings row (the crash window between
-- committing a settlement and creating its earning). See
-- store.Earnings.SettledJobsMissingEarning.
CREATE INDEX IF NOT EXISTS idx_jobs_settled_scan ON jobs (updated_at, id) WHERE economic_state = 'settled';
