\set ON_ERROR_STOP on
SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls
FROM pg_roles WHERE rolname IN ('atos_runtime','atos_migration','atos_financial_audit','atos_backup','atos_break_glass')
ORDER BY rolname;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='atos_runtime' AND
      (rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls)) THEN
    RAISE EXCEPTION 'atos_runtime has privileged role attributes';
  END IF;
  IF has_schema_privilege('atos_runtime','public','CREATE') THEN
    RAISE EXCEPTION 'atos_runtime can create schema objects';
  END IF;
  IF has_table_privilege('atos_runtime','financial_events','DELETE') OR
     has_table_privilege('atos_runtime','financial_batches','TRUNCATE') THEN
    RAISE EXCEPTION 'atos_runtime can destroy financial evidence';
  END IF;
  IF has_table_privilege('atos_runtime','financial_integrity_state','UPDATE') OR
     has_table_privilege('atos_runtime','financial_integrity_incidents','UPDATE') THEN
    RAISE EXCEPTION 'atos_runtime can clear or resolve financial integrity state directly';
  END IF;
  IF NOT has_function_privilege('atos_runtime','enter_financial_safe_mode(text,text)','EXECUTE') THEN
    RAISE EXCEPTION 'atos_runtime cannot enter financial safe mode';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='atos_break_glass' AND rolcanlogin) THEN
    RAISE EXCEPTION 'atos_break_glass must be NOLOGIN outside an active incident';
  END IF;
  IF pg_has_role('atos_runtime','atos_financial_owner','MEMBER') OR
     pg_has_role('atos_financial_audit','atos_financial_owner','MEMBER') OR
     pg_has_role('atos_backup','atos_financial_owner','MEMBER') THEN
    RAISE EXCEPTION 'runtime, audit, or backup role inherited financial ownership';
  END IF;
END $$;
