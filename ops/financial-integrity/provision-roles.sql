\set ON_ERROR_STOP on
-- Run as a cluster administrator after creating the database. Passwords are
-- assigned out-of-band; this file deliberately contains no credentials.
DO $$ BEGIN
  CREATE ROLE atos_financial_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  CREATE ROLE atos_migration LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  CREATE ROLE atos_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  CREATE ROLE atos_financial_audit LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

GRANT atos_financial_owner TO atos_migration;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO atos_runtime, atos_financial_audit;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO atos_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO atos_runtime;
GRANT SELECT ON financial_events, financial_projections, financial_chain_state,
  financial_batches, financial_integrity_state, financial_integrity_incidents
  TO atos_financial_audit;
ALTER DEFAULT PRIVILEGES FOR ROLE atos_financial_owner IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE ON TABLES TO atos_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE atos_financial_owner IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO atos_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE atos_financial_owner IN SCHEMA public
  GRANT SELECT ON TABLES TO atos_financial_audit;

-- Neither normal runtime nor audit credentials may bypass append-only
-- triggers, alter schema, truncate evidence, or delete rows.
REVOKE DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA public FROM atos_runtime;
REVOKE CREATE ON SCHEMA public FROM atos_runtime, atos_financial_audit;
