ALTER TABLE financial_batches
    ADD COLUMN IF NOT EXISTS ledger_evidence JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE OR REPLACE FUNCTION protect_sealed_financial_batch() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' AND OLD.state IN ('retained','anchored') THEN
        RAISE EXCEPTION 'retained financial batch is append-only';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.state IN ('retained','anchored') AND (
        NEW.batch_id IS DISTINCT FROM OLD.batch_id OR
        NEW.batch_sequence IS DISTINCT FROM OLD.batch_sequence OR
        NEW.first_sequence IS DISTINCT FROM OLD.first_sequence OR
        NEW.last_sequence IS DISTINCT FROM OLD.last_sequence OR
        NEW.commitment_count IS DISTINCT FROM OLD.commitment_count OR
        NEW.previous_batch_id IS DISTINCT FROM OLD.previous_batch_id OR
        NEW.previous_merkle_root IS DISTINCT FROM OLD.previous_merkle_root OR
        NEW.merkle_root IS DISTINCT FROM OLD.merkle_root OR
        NEW.manifest_digest IS DISTINCT FROM OLD.manifest_digest OR
        NEW.manifest_cbor IS DISTINCT FROM OLD.manifest_cbor OR
        NEW.manifest IS DISTINCT FROM OLD.manifest OR
        NEW.ledger_evidence IS DISTINCT FROM OLD.ledger_evidence OR
        NEW.signing_key_id IS DISTINCT FROM OLD.signing_key_id OR
        NEW.signature_envelope IS DISTINCT FROM OLD.signature_envelope OR
        NEW.retained_object_key IS DISTINCT FROM OLD.retained_object_key OR
        NEW.retained_version_id IS DISTINCT FROM OLD.retained_version_id
    ) THEN
        RAISE EXCEPTION 'retained financial batch immutable fields cannot change';
    END IF;
    IF TG_OP = 'UPDATE' AND NOT (
        (OLD.state = 'created' AND NEW.state IN ('created','signed')) OR
        (OLD.state = 'signed' AND NEW.state IN ('signed','retained')) OR
        (OLD.state = 'retained' AND NEW.state IN ('retained','anchored')) OR
        (OLD.state = 'anchored' AND NEW.state = 'anchored')
    ) THEN
        RAISE EXCEPTION 'financial batch state cannot move backwards or skip a sealing stage';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.state = 'anchored' AND NEW.anchor_id IS DISTINCT FROM OLD.anchor_id THEN
        RAISE EXCEPTION 'anchored financial batch identity cannot change';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.state = 'retained' AND NEW.state = 'anchored' AND
       (OLD.anchor_id <> '' OR NEW.anchor_id = '') THEN
        RAISE EXCEPTION 'financial batch anchor transition requires one immutable anchor identity';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
