package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func TestPasskeyAccountCRUD(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	principalID := "prn_pg_" + randSuffix()
	handle := "HANDLE" + randSuffix()
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := s.CreatePasskeyAccount(ctx, domain.PasskeyAccount{PrincipalID: principalID, DisplayHandle: handle, CreatedAt: now}); err != nil {
		t.Fatalf("CreatePasskeyAccount: %v", err)
	}

	byID, err := s.PasskeyAccountByPrincipalID(ctx, principalID)
	if err != nil {
		t.Fatalf("PasskeyAccountByPrincipalID: %v", err)
	}
	if byID.DisplayHandle != handle {
		t.Fatalf("display_handle = %s, want %s", byID.DisplayHandle, handle)
	}

	byHandle, err := s.PasskeyAccountByDisplayHandle(ctx, handle)
	if err != nil {
		t.Fatalf("PasskeyAccountByDisplayHandle: %v", err)
	}
	if byHandle.PrincipalID != principalID {
		t.Fatalf("principal_id = %s, want %s", byHandle.PrincipalID, principalID)
	}

	if _, err := s.PasskeyAccountByPrincipalID(ctx, "prn_does_not_exist_"+randSuffix()); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound for unknown principal, got %v", err)
	}
}

func TestPasskeyAccount_DisplayHandleCollisionConflicts(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	handle := "DUPHANDLE" + randSuffix()
	if err := s.CreatePasskeyAccount(ctx, domain.PasskeyAccount{PrincipalID: "prn_pg_a_" + randSuffix(), DisplayHandle: handle, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("first CreatePasskeyAccount: %v", err)
	}
	err := s.CreatePasskeyAccount(ctx, domain.PasskeyAccount{PrincipalID: "prn_pg_b_" + randSuffix(), DisplayHandle: handle, CreatedAt: time.Now().UTC()})
	if err != store.ErrConflict {
		t.Fatalf("expected ErrConflict for a duplicate display_handle, got %v", err)
	}
}

func TestWebAuthnCredential_SaveAndTouch(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	principalID := "prn_pg_cred_" + randSuffix()
	if err := s.CreatePasskeyAccount(ctx, domain.PasskeyAccount{PrincipalID: principalID, DisplayHandle: "CREDH" + randSuffix(), CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("CreatePasskeyAccount: %v", err)
	}

	record := domain.WebAuthnCredentialRecord{
		ID: "pkcred_pg_" + randSuffix(), PrincipalID: principalID,
		CredentialID: []byte("cred-id-bytes-" + randSuffix()), PublicKey: []byte("public-key-bytes"),
		AttestationType: "none", AttestationFormat: "packed", Transports: "internal,hybrid",
		AAGUID: []byte("0123456789abcdef"), Flags: 5, SignCount: 1,
		Nickname: "My Passkey", CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	if err := s.SaveWebAuthnCredential(ctx, record); err != nil {
		t.Fatalf("SaveWebAuthnCredential: %v", err)
	}

	records, err := s.WebAuthnCredentialsByPrincipalID(ctx, principalID)
	if err != nil {
		t.Fatalf("WebAuthnCredentialsByPrincipalID: %v", err)
	}
	if len(records) != 1 || records[0].Nickname != "My Passkey" || records[0].SignCount != 1 {
		t.Fatalf("records = %+v", records)
	}

	if err := s.TouchWebAuthnCredential(ctx, record.ID, 7, true, true); err != nil {
		t.Fatalf("TouchWebAuthnCredential: %v", err)
	}
	touched, err := s.WebAuthnCredentialsByPrincipalID(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if touched[0].SignCount != 7 || !touched[0].CloneWarning || !touched[0].BackupState || touched[0].LastUsedAt == nil {
		t.Fatalf("after touch: %+v", touched[0])
	}

	if err := s.TouchWebAuthnCredential(ctx, "pkcred_does_not_exist_"+randSuffix(), 1, false, false); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound touching an unknown credential, got %v", err)
	}
}

func TestWebAuthnCeremony_ConsumeIsSingleUse(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	ceremonyID := "pkcer_pg_" + randSuffix()
	if err := s.CreateWebAuthnCeremony(ctx, domain.WebAuthnCeremony{
		ID: ceremonyID, Purpose: domain.CeremonyLogin, SessionData: []byte(`{"x":1}`),
		ExpiresAt: time.Now().UTC().Add(5 * time.Minute), CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("CreateWebAuthnCeremony: %v", err)
	}

	consumed, err := s.ConsumeWebAuthnCeremony(ctx, ceremonyID, domain.CeremonyLogin)
	if err != nil {
		t.Fatalf("ConsumeWebAuthnCeremony: %v", err)
	}
	// jsonb round-trips content, not exact text formatting (Postgres
	// reserializes with its own canonical whitespace) -- compare
	// semantically, not byte-for-byte.
	var decoded struct {
		X int `json:"x"`
	}
	if err := json.Unmarshal(consumed.SessionData, &decoded); err != nil || decoded.X != 1 {
		t.Fatalf("session_data = %s (decode err = %v)", consumed.SessionData, err)
	}

	if _, err := s.ConsumeWebAuthnCeremony(ctx, ceremonyID, domain.CeremonyLogin); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound consuming an already-consumed ceremony, got %v", err)
	}
}

func TestWebAuthnCeremony_WrongPurposeIsRejected(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	ceremonyID := "pkcer_pg_" + randSuffix()
	if err := s.CreateWebAuthnCeremony(ctx, domain.WebAuthnCeremony{
		ID: ceremonyID, Purpose: domain.CeremonySignup, SessionData: []byte(`{}`),
		ExpiresAt: time.Now().UTC().Add(5 * time.Minute), CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("CreateWebAuthnCeremony: %v", err)
	}
	if _, err := s.ConsumeWebAuthnCeremony(ctx, ceremonyID, domain.CeremonyLogin); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound consuming under the wrong purpose, got %v", err)
	}
	// The ceremony must still be consumable under its real purpose --
	// a wrong-purpose attempt must not have deleted it.
	if _, err := s.ConsumeWebAuthnCeremony(ctx, ceremonyID, domain.CeremonySignup); err != nil {
		t.Fatalf("ConsumeWebAuthnCeremony (correct purpose): %v", err)
	}
}

func TestWebAuthnCeremony_ExpiredIsRejected(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	ceremonyID := "pkcer_pg_" + randSuffix()
	if err := s.CreateWebAuthnCeremony(ctx, domain.WebAuthnCeremony{
		ID: ceremonyID, Purpose: domain.CeremonyLogin, SessionData: []byte(`{}`),
		ExpiresAt: time.Now().UTC().Add(-time.Minute), CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("CreateWebAuthnCeremony: %v", err)
	}
	if _, err := s.ConsumeWebAuthnCeremony(ctx, ceremonyID, domain.CeremonyLogin); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound consuming an expired ceremony, got %v", err)
	}
}

func TestPurgeExpiredWebAuthnCeremonies(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	expiredID := "pkcer_pg_expired_" + randSuffix()
	freshID := "pkcer_pg_fresh_" + randSuffix()
	now := time.Now().UTC()
	if err := s.CreateWebAuthnCeremony(ctx, domain.WebAuthnCeremony{ID: expiredID, Purpose: domain.CeremonyLogin, SessionData: []byte(`{}`), ExpiresAt: now.Add(-time.Hour), CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateWebAuthnCeremony(ctx, domain.WebAuthnCeremony{ID: freshID, Purpose: domain.CeremonyLogin, SessionData: []byte(`{}`), ExpiresAt: now.Add(time.Hour), CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	removed, err := s.PurgeExpiredWebAuthnCeremonies(ctx, now)
	if err != nil {
		t.Fatalf("PurgeExpiredWebAuthnCeremonies: %v", err)
	}
	if removed < 1 {
		t.Fatalf("removed = %d, want at least 1", removed)
	}
	if _, err := s.ConsumeWebAuthnCeremony(ctx, freshID, domain.CeremonyLogin); err != nil {
		t.Fatalf("fresh ceremony must survive the purge: %v", err)
	}
}
