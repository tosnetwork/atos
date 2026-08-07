-- Preserve indexed/query-critical columns from the Phase 0 schema while
-- storing the complete versioned v0.2 object. This prevents trust-mode,
-- proof-profile, signer, settlement and Artifact fields from disappearing
-- across process restarts as the protocol evolves.

ALTER TABLE capabilities ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE quotes       ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE escrows      ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE receipts     ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE jobs         ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE accounts     ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE artifacts    ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS capabilities_supported_trust_modes_gin
  ON capabilities USING gin ((payload->'supported_trust_modes'));
CREATE INDEX IF NOT EXISTS jobs_trust_mode_idx
  ON jobs ((payload->>'trust_mode'));
CREATE INDEX IF NOT EXISTS receipts_trust_mode_idx
  ON receipts ((payload->>'trust_mode'));
