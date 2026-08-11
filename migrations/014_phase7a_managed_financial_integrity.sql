-- Phase 7A: durable financial intents, commitment chain, independently
-- retained batches, and fail-closed integrity state. Blnk remains the only
-- mutable balance authority; these tables contain evidence and projections.

CREATE TABLE financial_chain_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    next_sequence BIGINT NOT NULL DEFAULT 1 CHECK (next_sequence > 0),
    last_commitment TEXT NOT NULL DEFAULT 'sha256:0000000000000000000000000000000000000000000000000000000000000000'
        CHECK (last_commitment ~ '^sha256:[0-9a-f]{64}$'),
    next_batch_sequence BIGINT NOT NULL DEFAULT 1 CHECK (next_batch_sequence > 0),
    last_batch_id TEXT NOT NULL DEFAULT '',
    last_batch_root TEXT NOT NULL DEFAULT 'sha256:0000000000000000000000000000000000000000000000000000000000000000'
        CHECK (last_batch_root ~ '^sha256:[0-9a-f]{64}$'),
    last_anchor_id TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO financial_chain_state(singleton) VALUES (TRUE) ON CONFLICT DO NOTHING;

CREATE TABLE financial_events (
    idempotency_identity TEXT PRIMARY KEY CHECK (length(idempotency_identity) BETWEEN 1 AND 512),
    semantic_digest TEXT NOT NULL CHECK (semantic_digest ~ '^sha256:[0-9a-f]{64}$'),
    event_id TEXT NOT NULL UNIQUE CHECK (length(event_id) BETWEEN 1 AND 128),
    event_type TEXT NOT NULL,
    gateway_id TEXT NOT NULL,
    network_id TEXT NOT NULL,
    sequence BIGINT NOT NULL UNIQUE CHECK (sequence > 0),
    previous_commitment TEXT NOT NULL CHECK (previous_commitment ~ '^sha256:[0-9a-f]{64}$'),
    commitment_digest TEXT NOT NULL UNIQUE CHECK (commitment_digest ~ '^sha256:[0-9a-f]{64}$'),
    canonical_cbor BYTEA NOT NULL,
    ledger_reference TEXT NOT NULL UNIQUE,
    ledger_transaction_id TEXT NOT NULL UNIQUE,
    source_indicator TEXT NOT NULL,
    destination_indicator TEXT NOT NULL,
    asset TEXT NOT NULL,
    decimals SMALLINT NOT NULL CHECK (decimals BETWEEN 0 AND 18),
    atomic_amount NUMERIC(78,0) NOT NULL CHECK (atomic_amount >= 0),
    allow_overdraft BOOLEAN NOT NULL DEFAULT FALSE,
    commitment JSONB NOT NULL,
    state TEXT NOT NULL DEFAULT 'intent' CHECK (state IN ('intent','submitting','finalized','conflict')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT NOT NULL DEFAULT '',
    ledger_response JSONB,
    batch_id TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    finalized_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((state = 'finalized') = (finalized_at IS NOT NULL))
);
CREATE INDEX financial_events_recovery_idx ON financial_events(state, sequence);
CREATE UNIQUE INDEX financial_events_one_reversal_per_event
    ON financial_events ((commitment->>'reverses_event_id'))
    WHERE event_type='compensating_reversal';

CREATE TABLE financial_projections (
    account_code TEXT NOT NULL,
    account_owner_id TEXT NOT NULL,
    asset TEXT NOT NULL,
    atomic_balance NUMERIC(78,0) NOT NULL,
    last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(account_code, account_owner_id, asset)
);

CREATE TABLE financial_integrity_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    safe_mode BOOLEAN NOT NULL DEFAULT FALSE,
    reason TEXT NOT NULL DEFAULT '',
    incident_id TEXT NOT NULL DEFAULT '',
    entered_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO financial_integrity_state(singleton) VALUES (TRUE) ON CONFLICT DO NOTHING;

CREATE TABLE financial_integrity_incidents (
    incident_id TEXT PRIMARY KEY,
    classification TEXT NOT NULL,
    expected JSONB NOT NULL,
    observed JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    resolution TEXT NOT NULL DEFAULT ''
);

CREATE TABLE financial_batches (
    batch_id TEXT PRIMARY KEY,
    batch_sequence BIGINT NOT NULL UNIQUE CHECK (batch_sequence > 0),
    first_sequence BIGINT NOT NULL,
    last_sequence BIGINT NOT NULL CHECK (last_sequence >= first_sequence),
    commitment_count INTEGER NOT NULL CHECK (commitment_count BETWEEN 1 AND 4096),
    previous_batch_id TEXT NOT NULL,
    previous_merkle_root TEXT NOT NULL CHECK (previous_merkle_root ~ '^sha256:[0-9a-f]{64}$'),
    merkle_root TEXT NOT NULL CHECK (merkle_root ~ '^sha256:[0-9a-f]{64}$'),
    manifest_digest TEXT NOT NULL UNIQUE CHECK (manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    manifest_cbor BYTEA NOT NULL,
    manifest JSONB NOT NULL,
    signing_key_id TEXT NOT NULL DEFAULT '',
    signature_envelope JSONB,
    retained_object_key TEXT NOT NULL DEFAULT '',
    retained_version_id TEXT NOT NULL DEFAULT '',
    anchor_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'created' CHECK (state IN ('created','signed','retained','anchored')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE financial_reconciler_leases (
    lease_name TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    cursor BIGINT NOT NULL DEFAULT 0 CHECK (cursor >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION protect_finalized_financial_evidence() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' AND OLD.state = 'finalized' THEN
        RAISE EXCEPTION 'finalized financial event is append-only';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.state = 'finalized' AND (
        NEW.idempotency_identity IS DISTINCT FROM OLD.idempotency_identity OR
        NEW.semantic_digest IS DISTINCT FROM OLD.semantic_digest OR
        NEW.event_id IS DISTINCT FROM OLD.event_id OR
        NEW.event_type IS DISTINCT FROM OLD.event_type OR
        NEW.gateway_id IS DISTINCT FROM OLD.gateway_id OR
        NEW.network_id IS DISTINCT FROM OLD.network_id OR
        NEW.sequence IS DISTINCT FROM OLD.sequence OR
        NEW.previous_commitment IS DISTINCT FROM OLD.previous_commitment OR
        NEW.commitment_digest IS DISTINCT FROM OLD.commitment_digest OR
        NEW.canonical_cbor IS DISTINCT FROM OLD.canonical_cbor OR
        NEW.ledger_reference IS DISTINCT FROM OLD.ledger_reference OR
        NEW.ledger_transaction_id IS DISTINCT FROM OLD.ledger_transaction_id OR
        NEW.source_indicator IS DISTINCT FROM OLD.source_indicator OR
        NEW.destination_indicator IS DISTINCT FROM OLD.destination_indicator OR
        NEW.asset IS DISTINCT FROM OLD.asset OR
        NEW.decimals IS DISTINCT FROM OLD.decimals OR
        NEW.atomic_amount IS DISTINCT FROM OLD.atomic_amount OR
        NEW.allow_overdraft IS DISTINCT FROM OLD.allow_overdraft OR
        NEW.commitment IS DISTINCT FROM OLD.commitment OR
        NEW.state IS DISTINCT FROM OLD.state OR
        NEW.finalized_at IS DISTINCT FROM OLD.finalized_at OR
        NEW.ledger_response IS DISTINCT FROM OLD.ledger_response
    ) THEN
        RAISE EXCEPTION 'finalized financial event immutable fields cannot change';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.batch_id <> '' AND NEW.batch_id IS DISTINCT FROM OLD.batch_id THEN
        RAISE EXCEPTION 'batched financial event cannot change batch';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER financial_events_append_only
BEFORE UPDATE OR DELETE ON financial_events
FOR EACH ROW EXECUTE FUNCTION protect_finalized_financial_evidence();

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
    IF TG_OP = 'UPDATE' AND OLD.state = 'anchored' AND
        NEW.anchor_id IS DISTINCT FROM OLD.anchor_id THEN
        RAISE EXCEPTION 'anchored financial batch identity cannot change';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.state = 'retained' AND NEW.state = 'anchored' AND
        (OLD.anchor_id <> '' OR NEW.anchor_id = '') THEN
        RAISE EXCEPTION 'financial batch anchor transition requires one immutable anchor identity';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER financial_batches_append_only
BEFORE UPDATE OR DELETE ON financial_batches
FOR EACH ROW EXECUTE FUNCTION protect_sealed_financial_batch();
