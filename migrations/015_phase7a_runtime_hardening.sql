-- Runtime credentials may raise financial safe mode, but clearing it is an
-- audited migration/break-glass operation. SECURITY DEFINER is required after
-- direct UPDATE is revoked from the runtime role.
CREATE OR REPLACE FUNCTION enter_financial_safe_mode(p_reason TEXT, p_incident_id TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_reason IS NULL OR length(p_reason) NOT BETWEEN 1 AND 256 OR
       p_incident_id IS NULL OR length(p_incident_id) NOT BETWEEN 1 AND 128 THEN
        RAISE EXCEPTION 'invalid financial safe-mode transition';
    END IF;
    UPDATE public.financial_integrity_state
       SET safe_mode=TRUE, reason=p_reason, incident_id=p_incident_id,
           entered_at=COALESCE(entered_at,now()), updated_at=now()
     WHERE singleton=TRUE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'financial integrity singleton is missing';
    END IF;
END;
$$;
REVOKE ALL ON FUNCTION enter_financial_safe_mode(TEXT,TEXT) FROM PUBLIC;
