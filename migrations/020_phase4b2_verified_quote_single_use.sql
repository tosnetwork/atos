-- A finalized Verified Quote is an economic authorization for exactly one
-- Job/TaskEscrow. Enforce the claim in PostgreSQL so independent ATOS replicas
-- cannot both pass a service-layer check.
CREATE UNIQUE INDEX jobs_verified_quote_once_idx
    ON jobs (quote_id)
    WHERE payload->>'trust_mode' = 'verified';
