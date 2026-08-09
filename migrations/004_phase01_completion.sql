-- Phase 0/1 completion hardening.
--
-- Idempotency reservations are leased so a process crash cannot leave a key
-- permanently poisoned in the in_progress state. Spend confirmations remain
-- part of the authoritative Job payload; this expression index supports the
-- operator/user decision endpoint without creating a second mutable record.

ALTER TABLE idempotency_records
  ADD COLUMN IF NOT EXISTS reserved_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE idempotency_records
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS jobs_confirmation_user_code_idx
  ON jobs ((payload #>> '{confirmation,user_code}'))
  WHERE payload ? 'confirmation';

CREATE UNIQUE INDEX IF NOT EXISTS jobs_principal_idempotency_key_uidx
  ON jobs (principal_id, idempotency_key)
  WHERE idempotency_key <> '';
