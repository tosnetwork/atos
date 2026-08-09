-- Phase 2C: Managed disputes.
--
-- disputes is a Managed-mode economic/legal workflow layered on top of an
-- already-completed, already-settled Job. It never rewrites
-- billing_snapshots, receipts, quotes, or jobs -- those remain immutable
-- historical evidence -- it only ever adds new durable state describing
-- what happened to the ProviderEarning they produced.
--
-- provider_earnings has no index on job_id as of migration 008 (every
-- existing lookup goes through id or settlement_id); the dispute workflow
-- is Job-keyed (the principal-facing identity), so OpenDispute and
-- UpdateDisputeAndEarning both need an efficient, row-lockable lookup by
-- job_id -- see internal/store.Earnings.EarningByJob.
CREATE INDEX IF NOT EXISTS idx_provider_earnings_job ON provider_earnings (job_id);

-- job_id is UNIQUE: at most one dispute may ever exist for a given Job.
-- This is what makes "at most one dispute per settlement" a database
-- guarantee under 8+ concurrent openers or two independent ATOS replicas,
-- not a service-layer race (see internal/store.Disputes.OpenDispute).
CREATE TABLE IF NOT EXISTS disputes (
    id                      TEXT PRIMARY KEY,
    principal_id            TEXT NOT NULL,
    provider_id             TEXT NOT NULL,
    job_id                  TEXT NOT NULL,
    quote_id                TEXT NOT NULL,
    capability_id           TEXT NOT NULL,
    receipt_id              TEXT NOT NULL,
    settlement_id           TEXT NOT NULL,
    earning_id              TEXT NOT NULL,
    charged_amount          JSONB NOT NULL,
    original_refund_amount  JSONB NOT NULL,
    reason                  TEXT NOT NULL,
    description             TEXT NOT NULL DEFAULT '',
    evidence                JSONB NOT NULL DEFAULT '[]',
    idempotency_key         TEXT NOT NULL DEFAULT '',
    -- review_status is the human-review lifecycle: opened -> under_review
    -- -> resolved_for_principal | resolved_for_provider | rejected.
    review_status           TEXT NOT NULL,
    -- economic_state is the durable economic-recovery checkpoint for the
    -- disputed ProviderEarning, kept deliberately separate from
    -- review_status -- see domain.DisputeEconomicState's doc comment for
    -- why a review outcome and its economic recovery are not always
    -- reached in lockstep (e.g. resolved_for_principal against an
    -- already-paid earning reaches economic_state=clawback_required, not
    -- refunded).
    economic_state           TEXT NOT NULL DEFAULT '',
    outcome                  TEXT NOT NULL DEFAULT '',
    reviewer_id               TEXT NOT NULL DEFAULT '',
    reason_rejected           TEXT NOT NULL DEFAULT '',
    -- dispute_policy_hash is copied from the disputed Quote at open time,
    -- so resolution always applies the policy (dispute window, etc.) that
    -- specific Quote actually committed to, never whatever ATOS's current
    -- global policy happens to be by the time the dispute is resolved.
    dispute_policy_hash       TEXT NOT NULL DEFAULT '',
    -- content_hash summarizes the identity+economic fields that must never
    -- change once a dispute is created (principal/provider/job/quote/
    -- receipt/settlement/earning id, charged/refund amounts, reason,
    -- description, evidence, dispute_policy_hash) -- not the lifecycle
    -- fields (review_status, economic_state, outcome, reviewer_id,
    -- timestamps) which legitimately change over the dispute's life.
    -- Mirrors provider_earnings.content_hash's role.
    content_hash              TEXT NOT NULL,
    opened_at                 TIMESTAMPTZ NOT NULL,
    under_review_at           TIMESTAMPTZ,
    resolved_at                TIMESTAMPTZ,
    updated_at                 TIMESTAMPTZ NOT NULL,
    payload                    JSONB NOT NULL,
    UNIQUE (job_id)
);

CREATE INDEX IF NOT EXISTS idx_disputes_principal ON disputes (principal_id, opened_at DESC, id ASC);
CREATE INDEX IF NOT EXISTS idx_disputes_provider ON disputes (provider_id, opened_at DESC, id ASC);

-- Supports crash recovery between a successful OpenDispute commit and the
-- idempotency-record Finish call that follows it, mirroring
-- store.Jobs.JobByIdempotencyKey's role for job submission.
CREATE INDEX IF NOT EXISTS idx_disputes_idempotency ON disputes (principal_id, idempotency_key) WHERE idempotency_key <> '';

-- Supports the reviewer queue: disputes still awaiting a review decision.
CREATE INDEX IF NOT EXISTS idx_disputes_under_review
    ON disputes (opened_at ASC, id ASC)
    WHERE review_status IN ('opened', 'under_review');

-- Supports DisputesForRecovery's reconciliation scan: disputes whose
-- disputed earning was payout_pending/ambiguous at open time and whose
-- outcome must still be resolved (see domain.DisputeEconomicState's doc
-- comments). Resolution's principal-win path (earning reversal + account
-- credit + dispute terminal checkpoint) is a single atomic transaction
-- (store.Disputes.ResolveDispute), so it has no intermediate durable
-- state to reconcile here.
CREATE INDEX IF NOT EXISTS idx_disputes_recovery
    ON disputes (updated_at ASC, id ASC)
    WHERE economic_state = 'pending_payout_resolution';
