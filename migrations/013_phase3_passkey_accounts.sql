-- Human account authentication (passkey/WebAuthn) -- atos-spec
-- docs/AUTH.md's "Human Account Authentication (Passkey/WebAuthn)" section.
-- Modeled on tosnetwork/atos-aidrop's proven schema, without the
-- wallet-specific ledger/referral/encryption-at-rest concerns (a WebAuthn
-- public key is not secret material -- the private key never leaves the
-- authenticator -- so it is stored in plaintext here, unlike
-- atos-aidrop's extra defense-in-depth for its own threat model).

CREATE TABLE passkey_accounts (
    principal_id TEXT PRIMARY KEY,
    display_handle TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE passkey_credentials (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES passkey_accounts(principal_id) ON DELETE CASCADE,
    credential_id BYTEA NOT NULL,
    public_key BYTEA NOT NULL,
    attestation_type TEXT NOT NULL DEFAULT '',
    attestation_format TEXT NOT NULL DEFAULT '',
    transports TEXT NOT NULL DEFAULT '',
    aaguid BYTEA,
    flags SMALLINT NOT NULL DEFAULT 0,
    sign_count BIGINT NOT NULL DEFAULT 0,
    clone_warning BOOLEAN NOT NULL DEFAULT FALSE,
    backup_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    backup_state BOOLEAN NOT NULL DEFAULT FALSE,
    nickname TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX passkey_credentials_credential_id_idx ON passkey_credentials(credential_id);
CREATE INDEX passkey_credentials_principal_idx ON passkey_credentials(principal_id);

-- Ephemeral ceremony state exchanged between a begin/finish pair.
-- principal_id is nullable: a discoverable login ceremony resolves the
-- account from the assertion response itself, and a signup ceremony has
-- no account row yet at begin time -- both carry their own identifying
-- data inside session_data instead (see internal/service/passkey.go).
CREATE TABLE passkey_ceremonies (
    id TEXT PRIMARY KEY,
    principal_id TEXT REFERENCES passkey_accounts(principal_id) ON DELETE CASCADE,
    purpose TEXT NOT NULL CHECK (purpose IN ('registration', 'login', 'signup')),
    session_data JSONB NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX passkey_ceremonies_expires_idx ON passkey_ceremonies(expires_at);
