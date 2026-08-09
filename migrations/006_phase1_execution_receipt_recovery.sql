-- Persist private execution evidence required to replay an ambiguous settlement.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS execution_receipt JSONB;
