-- Phase 3C: Open Task Marketplace (atos-spec docs/IMPLEMENTATION_ROADMAP.md
-- §7.3). An OpenTask is a demand-side marketplace object, not a parallel
-- commercial contract -- acceptance still binds to the existing quotes/jobs
-- tables below, it never writes escrow/receipt/settlement state directly.

-- QuoteService.Create gains an optional idempotency key (Phase 3C's
-- acceptance flow needs a genuinely idempotent Quote-creation entry point
-- to resume safely after a crash between "Quote committed" and "idempotency
-- record marked finished"). principal_id/idempotency_key are promoted to
-- real columns (mirroring jobs.idempotency_key) so QuoteByIdempotencyKey can
-- use an index instead of scanning the payload jsonb blob.
ALTER TABLE quotes ADD COLUMN IF NOT EXISTS principal_id    TEXT NOT NULL DEFAULT '';
ALTER TABLE quotes ADD COLUMN IF NOT EXISTS idempotency_key TEXT NOT NULL DEFAULT '';

-- Only one row can exist per (principal_id, idempotency_key) pair once a key
-- is actually supplied; quotes created without one (idempotency_key = '')
-- are exempt so the many pre-Phase-3C callers that never set a key keep
-- minting distinct rows exactly as before.
CREATE UNIQUE INDEX IF NOT EXISTS quotes_principal_idempotency_key_idx
    ON quotes (principal_id, idempotency_key)
    WHERE idempotency_key <> '';

-- open_tasks: the demand-side marketplace object. payload carries the full
-- domain.OpenTask (including Input, RequestedTrustMode, ProofRequirements --
-- the owner-only fields domain.OpenTask.Public() strips before any response
-- leaves the service layer), mirroring the quotes/jobs jsonb-payload
-- convention so new optional fields never need a migration. The columns
-- below duplicate only what needs to be queried or constrained directly.
CREATE TABLE IF NOT EXISTS open_tasks (
    id                     TEXT PRIMARY KEY,
    principal_id           TEXT NOT NULL,
    title                  TEXT NOT NULL,
    -- status: open | accepted | fulfilled | cancelled | expired.
    status                 TEXT NOT NULL,
    expires_at             TIMESTAMPTZ NOT NULL,
    accepted_proposal_id   TEXT NOT NULL DEFAULT '',
    bound_quote_id         TEXT NOT NULL DEFAULT '',
    bound_job_id           TEXT NOT NULL DEFAULT '',
    idempotency_key        TEXT NOT NULL DEFAULT '',
    created_at             TIMESTAMPTZ NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL,
    payload                JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_open_tasks_principal ON open_tasks (principal_id, created_at DESC);

-- Drives ListPublicOpenTasks (marketplace browse/search): every currently
-- Open task, newest first. A task past its own ExpiresAt but not yet swept
-- to Expired can still appear here -- see ListPublicOpenTasks's doc
-- comment, this is a status filter only, not an expiry filter.
CREATE INDEX IF NOT EXISTS idx_open_tasks_status ON open_tasks (status, created_at DESC) WHERE status = 'open';

CREATE UNIQUE INDEX IF NOT EXISTS open_tasks_principal_idempotency_key_idx
    ON open_tasks (principal_id, idempotency_key)
    WHERE idempotency_key <> '';

-- open_task_proposals: a provider's application to fulfill an OpenTask.
-- Deliberately has NO status column for accepted/rejected -- see
-- domain.OpenTaskProposal's doc comment, that is derived from
-- open_tasks.accepted_proposal_id, never stored redundantly here.
-- withdrawn_at is the one piece of state that genuinely cannot be derived.
CREATE TABLE IF NOT EXISTS open_task_proposals (
    id                   TEXT PRIMARY KEY,
    task_id              TEXT NOT NULL REFERENCES open_tasks (id),
    provider_id          TEXT NOT NULL,
    capability_id        TEXT NOT NULL,
    capability_version   TEXT NOT NULL,
    idempotency_key      TEXT NOT NULL DEFAULT '',
    withdrawn_at         TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL,
    payload              JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_open_task_proposals_task ON open_task_proposals (task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_open_task_proposals_provider ON open_task_proposals (provider_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS open_task_proposals_provider_idempotency_key_idx
    ON open_task_proposals (provider_id, idempotency_key)
    WHERE idempotency_key <> '';

-- acceptance_operations: the durable winner-selection -> Quote -> Job
-- binding journal (domain.AcceptanceOperation), mirroring
-- execution_signer_operations exactly, including the same dual
-- defense-in-depth this codebase already uses for "at most one in-flight
-- mutation" invariants:
--
--  * idx_acceptance_operations_task_nonterminal: at most one NON-TERMINAL
--    operation per task -- what OpenAcceptanceOperation's in-flight guard
--    enforces at the service layer is ALSO a database guarantee here, not
--    just an in-process check (a task's Failed operation reopens the task,
--    so more than one operation total per task over its lifetime is
--    expected and allowed -- only one at a time in flight).
--  * idx_acceptance_operations_task_completed: at most one COMPLETED
--    operation per task, ever -- this is the actual "exactly one Job is
--    ever bound to this task" guarantee; a task cannot be fulfilled twice
--    even by two operations that were somehow both allowed to reach
--    Completed (which the non-terminal index above already prevents from
--    happening concurrently, but this is the belt to that suspenders).
CREATE TABLE IF NOT EXISTS acceptance_operations (
    id                   TEXT PRIMARY KEY,
    task_id              TEXT NOT NULL REFERENCES open_tasks (id),
    proposal_id          TEXT NOT NULL REFERENCES open_task_proposals (id),
    principal_id         TEXT NOT NULL,
    provider_id          TEXT NOT NULL,
    capability_id        TEXT NOT NULL,
    capability_version   TEXT NOT NULL,
    -- checkpoint: intent_persisted | winner_claimed | quote_binding_pending
    -- | quote_bound | job_binding_pending | job_bound | completed | failed
    -- | reconciling.
    checkpoint           TEXT NOT NULL,
    idempotency_key      TEXT NOT NULL,
    quote_id             TEXT NOT NULL DEFAULT '',
    job_id               TEXT NOT NULL DEFAULT '',
    failure_reason       TEXT NOT NULL DEFAULT '',
    created_at           TIMESTAMPTZ NOT NULL,
    completed_at         TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ NOT NULL,
    UNIQUE (principal_id, idempotency_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_acceptance_operations_task_nonterminal
    ON acceptance_operations (task_id)
    WHERE checkpoint NOT IN ('completed', 'failed');

CREATE UNIQUE INDEX IF NOT EXISTS idx_acceptance_operations_task_completed
    ON acceptance_operations (task_id)
    WHERE checkpoint = 'completed';

-- Drives the reconciler's stale-operation sweep, mirroring
-- idx_execution_signer_operations_pending exactly.
CREATE INDEX IF NOT EXISTS idx_acceptance_operations_pending
    ON acceptance_operations (updated_at ASC)
    WHERE checkpoint NOT IN ('completed', 'failed');
