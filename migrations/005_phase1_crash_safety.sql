-- Phase 1 crash-safe Managed economy.
-- Internal checkpoints intentionally live outside the public Job JSON payload.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS economic_state TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS pending_credit JSONB;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS reconciliation_target TEXT NOT NULL DEFAULT '';

ALTER TABLE escrows ADD COLUMN IF NOT EXISTS job_id TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS escrows_job_id_uidx
  ON escrows (job_id) WHERE job_id <> '';

CREATE INDEX IF NOT EXISTS jobs_recovery_scan_idx
  ON jobs (updated_at, id)
  WHERE state IN ('submitted','working','canceling','reconciling');
