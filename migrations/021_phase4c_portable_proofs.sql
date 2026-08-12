CREATE TABLE proof_package_operations (
  id text PRIMARY KEY,
  receipt_id text NOT NULL UNIQUE,
  job_id text NOT NULL,
  quote_id text NOT NULL,
  escrow_id text NOT NULL,
  principal_id text NOT NULL,
  semantic_digest text NOT NULL,
  package_digest text NOT NULL DEFAULT '',
  canonical_cbor bytea NOT NULL DEFAULT ''::bytea,
  checkpoint text NOT NULL CHECK (checkpoint IN ('intent_persisted','reconciling','canonical_observed','projection_persisted','completed')),
  last_error text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  completed_at timestamptz
);
CREATE INDEX proof_package_operations_stale_idx ON proof_package_operations(updated_at,id) WHERE checkpoint <> 'completed';
