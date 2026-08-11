\set ON_ERROR_STOP on
SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls
FROM pg_roles WHERE rolname IN ('atos_runtime','atos_migration','atos_financial_audit')
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
END $$;
