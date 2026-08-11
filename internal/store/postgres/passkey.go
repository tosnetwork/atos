package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// CreatePasskeyAccountWithCredential inserts the account and its first
// credential in one transaction -- an account row must never durably exist
// without a credential able to authenticate it (see the interface doc
// comment in internal/store/store.go). Either both rows commit or neither
// does; a mid-transaction failure (including the credential_id unique
// constraint) rolls the account insert back too.
func (s *Store) CreatePasskeyAccountWithCredential(ctx context.Context, a domain.PasskeyAccount, c domain.WebAuthnCredentialRecord) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx, `
		INSERT INTO passkey_accounts (principal_id, display_handle, created_at)
		VALUES ($1,$2,$3)
		ON CONFLICT DO NOTHING
	`, a.PrincipalID, a.DisplayHandle, a.CreatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrConflict
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO passkey_credentials (`+webAuthnCredentialColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
	`, c.ID, c.PrincipalID, c.CredentialID, c.PublicKey, c.AttestationType, c.AttestationFormat, c.Transports,
		c.AAGUID, int16(c.Flags), c.SignCount, c.CloneWarning, c.BackupEligible, c.BackupState, c.Nickname, c.CreatedAt, c.LastUsedAt); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func scanPasskeyAccount(row pgx.Row) (domain.PasskeyAccount, error) {
	var a domain.PasskeyAccount
	if err := row.Scan(&a.PrincipalID, &a.DisplayHandle, &a.CreatedAt); err != nil {
		return domain.PasskeyAccount{}, err
	}
	return a, nil
}

func (s *Store) PasskeyAccountByPrincipalID(ctx context.Context, principalID string) (domain.PasskeyAccount, error) {
	a, err := scanPasskeyAccount(s.pool.QueryRow(ctx, `
		SELECT principal_id, display_handle, created_at FROM passkey_accounts WHERE principal_id=$1
	`, principalID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.PasskeyAccount{}, store.ErrNotFound
	}
	return a, err
}

func (s *Store) PasskeyAccountByDisplayHandle(ctx context.Context, handle string) (domain.PasskeyAccount, error) {
	a, err := scanPasskeyAccount(s.pool.QueryRow(ctx, `
		SELECT principal_id, display_handle, created_at FROM passkey_accounts WHERE display_handle=$1
	`, handle))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.PasskeyAccount{}, store.ErrNotFound
	}
	return a, err
}

const webAuthnCredentialColumns = `id, principal_id, credential_id, public_key, attestation_type, attestation_format, transports, aaguid, flags, sign_count, clone_warning, backup_eligible, backup_state, nickname, created_at, last_used_at`

func (s *Store) SaveWebAuthnCredential(ctx context.Context, c domain.WebAuthnCredentialRecord) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO passkey_credentials (`+webAuthnCredentialColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
	`, c.ID, c.PrincipalID, c.CredentialID, c.PublicKey, c.AttestationType, c.AttestationFormat, c.Transports,
		c.AAGUID, int16(c.Flags), c.SignCount, c.CloneWarning, c.BackupEligible, c.BackupState, c.Nickname, c.CreatedAt, c.LastUsedAt)
	return err
}

func scanWebAuthnCredential(row pgx.Row) (domain.WebAuthnCredentialRecord, error) {
	var c domain.WebAuthnCredentialRecord
	var flags int16
	if err := row.Scan(&c.ID, &c.PrincipalID, &c.CredentialID, &c.PublicKey, &c.AttestationType, &c.AttestationFormat, &c.Transports,
		&c.AAGUID, &flags, &c.SignCount, &c.CloneWarning, &c.BackupEligible, &c.BackupState, &c.Nickname, &c.CreatedAt, &c.LastUsedAt); err != nil {
		return domain.WebAuthnCredentialRecord{}, err
	}
	c.Flags = byte(flags)
	return c, nil
}

func (s *Store) WebAuthnCredentialsByPrincipalID(ctx context.Context, principalID string) ([]domain.WebAuthnCredentialRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+webAuthnCredentialColumns+` FROM passkey_credentials WHERE principal_id=$1
	`, principalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.WebAuthnCredentialRecord
	for rows.Next() {
		c, err := scanWebAuthnCredential(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) WebAuthnCredentialViews(ctx context.Context, principalID string) ([]domain.WebAuthnCredentialRecord, error) {
	return s.WebAuthnCredentialsByPrincipalID(ctx, principalID)
}

func (s *Store) TouchWebAuthnCredential(ctx context.Context, id string, signCount uint32, cloneWarning, backupState bool) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE passkey_credentials SET sign_count=$2, clone_warning=$3, backup_state=$4, last_used_at=$5
		WHERE id=$1
	`, id, signCount, cloneWarning, backupState, time.Now().UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) CreateWebAuthnCeremony(ctx context.Context, c domain.WebAuthnCeremony) error {
	var principalID *string
	if c.PrincipalID != "" {
		principalID = &c.PrincipalID
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO passkey_ceremonies (id, principal_id, purpose, session_data, expires_at, created_at)
		VALUES ($1,$2,$3,$4,$5,$6)
	`, c.ID, principalID, string(c.Purpose), c.SessionData, c.ExpiresAt, c.CreatedAt)
	return err
}

func (s *Store) ConsumeWebAuthnCeremony(ctx context.Context, id string, purpose domain.WebAuthnCeremonyPurpose) (domain.WebAuthnCeremony, error) {
	var c domain.WebAuthnCeremony
	var principalID *string
	var purposeStr string
	err := s.pool.QueryRow(ctx, `
		DELETE FROM passkey_ceremonies WHERE id=$1 AND purpose=$2
		RETURNING id, principal_id, purpose, session_data, expires_at, created_at
	`, id, string(purpose)).Scan(&c.ID, &principalID, &purposeStr, &c.SessionData, &c.ExpiresAt, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.WebAuthnCeremony{}, store.ErrNotFound
	}
	if err != nil {
		return domain.WebAuthnCeremony{}, err
	}
	if principalID != nil {
		c.PrincipalID = *principalID
	}
	c.Purpose = domain.WebAuthnCeremonyPurpose(purposeStr)
	if !time.Now().UTC().Before(c.ExpiresAt) {
		return domain.WebAuthnCeremony{}, store.ErrNotFound
	}
	return c, nil
}

func (s *Store) PurgeExpiredWebAuthnCeremonies(ctx context.Context, cutoff time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM passkey_ceremonies WHERE expires_at < $1`, cutoff)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
