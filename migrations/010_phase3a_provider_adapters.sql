-- Phase 3A: Provider adapters.
--
-- Two durable readiness-evidence concerns, neither of which is trust-mode
-- activation authority (Phase 3B): provider_health_checks (a lightweight,
-- overwritten-in-place transport reachability probe per Capability
-- binding) and sandbox_certifications (a durable, idempotent, immutable-
-- once-opened certification workflow per Capability binding). See
-- internal/domain/provider_readiness.go's doc comments for the full
-- readiness-vs-activation invariant this schema exists to support.

-- provider_health_checks holds only the single most recent observation per
-- binding -- health has no history worth preserving, unlike
-- sandbox_certifications below.
CREATE TABLE IF NOT EXISTS provider_health_checks (
    capability_id       TEXT NOT NULL,
    capability_version  TEXT NOT NULL,
    transport           TEXT NOT NULL,
    endpoint_ref        TEXT NOT NULL,
    status              TEXT NOT NULL,
    latency_ms          BIGINT NOT NULL DEFAULT 0,
    failure_reason      TEXT NOT NULL DEFAULT '',
    checked_at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (capability_id, capability_version, transport)
);

-- sandbox_certifications is a durable, idempotent readiness-evidence
-- record. UNIQUE(provider_id, idempotency_key) is what makes "at most one
-- certification per idempotency key" a database guarantee under
-- concurrent openers or two independent ATOS replicas -- see
-- store.Certifications.OpenCertification.
CREATE TABLE IF NOT EXISTS sandbox_certifications (
    id                  TEXT PRIMARY KEY,
    provider_id         TEXT NOT NULL,
    capability_id       TEXT NOT NULL,
    capability_version  TEXT NOT NULL,
    transport           TEXT NOT NULL,
    endpoint_ref        TEXT NOT NULL,
    -- status: pending | passed | failed. Passing certification is
    -- readiness evidence ONLY -- see domain.SandboxCertification's doc
    -- comment. No code path may derive ModeSupport activation from it.
    status              TEXT NOT NULL,
    idempotency_key     TEXT NOT NULL,
    failure_reason      TEXT NOT NULL DEFAULT '',
    evidence            JSONB NOT NULL DEFAULT '{}',
    -- content_hash summarizes the identity fields that must never change
    -- once a certification is opened (provider/capability/version/
    -- transport/endpoint_ref/idempotency_key) -- not the lifecycle fields
    -- (status, failure_reason, evidence, completed_at, updated_at) which
    -- legitimately change over the certification's life. Mirrors
    -- disputes.content_hash's role.
    content_hash        TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL,
    UNIQUE (provider_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_sandbox_certifications_capability
    ON sandbox_certifications (capability_id, created_at DESC, id ASC);
