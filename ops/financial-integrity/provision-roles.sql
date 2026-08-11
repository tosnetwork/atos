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
DO $$ BEGIN
  CREATE ROLE atos_backup LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOBYPASSRLS;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  CREATE ROLE atos_break_glass NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- Re-applying the script also repairs drift in role attributes; CREATE IF
-- ABSENT alone would silently preserve a previously over-privileged role.
ALTER ROLE atos_financial_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE atos_migration LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE atos_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE atos_financial_audit LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE atos_backup LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOBYPASSRLS;
ALTER ROLE atos_break_glass NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS VALID UNTIL 'infinity';

GRANT atos_financial_owner TO atos_migration;
GRANT atos_financial_owner TO atos_break_glass;
ALTER SCHEMA public OWNER TO atos_financial_owner;

-- Adopt existing objects during an upgrade. Tables precede their owned
-- sequences because PostgreSQL requires both ends to have the same owner.
DO $$ DECLARE item record; kind text; BEGIN
  FOR item IN SELECT c.relkind,n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
              WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S')
              ORDER BY CASE c.relkind WHEN 'r' THEN 1 WHEN 'p' THEN 1 WHEN 'v' THEN 2 WHEN 'm' THEN 2 ELSE 3 END,
                       c.oid LOOP
    kind := CASE item.relkind WHEN 'S' THEN 'SEQUENCE' WHEN 'v' THEN 'VIEW'
            WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE 'TABLE' END;
    EXECUTE format('ALTER %s %I.%I OWNER TO atos_financial_owner',kind,item.nspname,item.relname);
  END LOOP;
END $$;
DO $$ DECLARE item record; BEGIN
  FOR item IN SELECT n.nspname,p.proname,pg_get_function_identity_arguments(p.oid) AS args
              FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' LOOP
    EXECUTE format('ALTER FUNCTION %I.%I(%s) OWNER TO atos_financial_owner',item.nspname,item.proname,item.args);
  END LOOP;
END $$;
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
REVOKE UPDATE ON financial_integrity_state,financial_integrity_incidents FROM atos_runtime;
REVOKE ALL ON FUNCTION enter_financial_safe_mode(TEXT,TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION enter_financial_safe_mode(TEXT,TEXT) TO atos_runtime;
REVOKE CREATE ON SCHEMA public FROM atos_runtime, atos_financial_audit;

-- The break-glass role is inert until two operators grant it a time-bounded
-- LOGIN credential. It inherits owner authority only for that recorded window.
