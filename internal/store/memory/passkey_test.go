package memory

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func testCredentialRecord(principalID, credentialID string) domain.WebAuthnCredentialRecord {
	return domain.WebAuthnCredentialRecord{
		ID: "pkcred_" + credentialID, PrincipalID: principalID,
		CredentialID: []byte(credentialID), PublicKey: []byte("public-key"),
		Nickname: "Passkey", CreatedAt: time.Now().UTC(),
	}
}

func TestCreatePasskeyAccountWithCredential_Atomic(t *testing.T) {
	ctx := context.Background()
	s := New()

	if err := s.CreatePasskeyAccountWithCredential(ctx, domain.PasskeyAccount{PrincipalID: "prn_1", DisplayHandle: "HANDLE1", CreatedAt: time.Now().UTC()}, testCredentialRecord("prn_1", "cred-1")); err != nil {
		t.Fatalf("CreatePasskeyAccountWithCredential: %v", err)
	}

	account, err := s.PasskeyAccountByPrincipalID(ctx, "prn_1")
	if err != nil {
		t.Fatalf("PasskeyAccountByPrincipalID: %v", err)
	}
	if account.DisplayHandle != "HANDLE1" {
		t.Fatalf("display_handle = %s", account.DisplayHandle)
	}
	credentials, err := s.WebAuthnCredentialsByPrincipalID(ctx, "prn_1")
	if err != nil {
		t.Fatal(err)
	}
	if len(credentials) != 1 {
		t.Fatalf("credentials = %+v, want exactly 1", credentials)
	}
}

func TestCreatePasskeyAccountWithCredential_DisplayHandleCollisionRollsBackCredential(t *testing.T) {
	ctx := context.Background()
	s := New()

	if err := s.CreatePasskeyAccountWithCredential(ctx, domain.PasskeyAccount{PrincipalID: "prn_a", DisplayHandle: "DUP", CreatedAt: time.Now().UTC()}, testCredentialRecord("prn_a", "cred-a")); err != nil {
		t.Fatalf("first account: %v", err)
	}
	err := s.CreatePasskeyAccountWithCredential(ctx, domain.PasskeyAccount{PrincipalID: "prn_b", DisplayHandle: "DUP", CreatedAt: time.Now().UTC()}, testCredentialRecord("prn_b", "cred-b"))
	if err != store.ErrConflict {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	// The rejected account's credential must not have been committed
	// either -- an account must never exist without a working credential,
	// and a rejected account must not exist with an orphaned credential.
	if _, err := s.PasskeyAccountByPrincipalID(ctx, "prn_b"); err != store.ErrNotFound {
		t.Fatalf("expected the conflicting account to not exist, got %v", err)
	}
	credentials, err := s.WebAuthnCredentialsByPrincipalID(ctx, "prn_b")
	if err != nil {
		t.Fatal(err)
	}
	if len(credentials) != 0 {
		t.Fatalf("credentials = %+v, want none for the rejected account", credentials)
	}
}

func TestCreatePasskeyAccountWithCredential_PrincipalIDCollision(t *testing.T) {
	ctx := context.Background()
	s := New()

	if err := s.CreatePasskeyAccountWithCredential(ctx, domain.PasskeyAccount{PrincipalID: "prn_x", DisplayHandle: "H1", CreatedAt: time.Now().UTC()}, testCredentialRecord("prn_x", "cred-x1")); err != nil {
		t.Fatalf("first account: %v", err)
	}
	err := s.CreatePasskeyAccountWithCredential(ctx, domain.PasskeyAccount{PrincipalID: "prn_x", DisplayHandle: "H2", CreatedAt: time.Now().UTC()}, testCredentialRecord("prn_x", "cred-x2"))
	if err != store.ErrConflict {
		t.Fatalf("expected ErrConflict for a duplicate principal_id, got %v", err)
	}
}

// TestCreatePasskeyAccountWithCredential_CredentialIDCollision is a
// regression test for a real P2: the atomic write path checked
// principal_id/display_handle uniqueness but never the credential's own
// row ID or its actual WebAuthn CredentialID bytes, unlike Postgres (which
// enforces both via schema constraints) -- a colliding credential used to
// be silently overwritten instead of rejected.
func TestCreatePasskeyAccountWithCredential_CredentialIDCollision(t *testing.T) {
	ctx := context.Background()
	s := New()

	if err := s.CreatePasskeyAccountWithCredential(ctx, domain.PasskeyAccount{PrincipalID: "prn_c1", DisplayHandle: "HC1", CreatedAt: time.Now().UTC()}, testCredentialRecord("prn_c1", "shared-cred-id")); err != nil {
		t.Fatalf("first account: %v", err)
	}
	// A different account, different display handle, but the same
	// underlying WebAuthn CredentialID bytes (e.g. two ceremonies racing
	// against a cloned/replayed attestation).
	err := s.CreatePasskeyAccountWithCredential(ctx, domain.PasskeyAccount{PrincipalID: "prn_c2", DisplayHandle: "HC2", CreatedAt: time.Now().UTC()}, testCredentialRecord("prn_c2", "shared-cred-id"))
	if err != store.ErrConflict {
		t.Fatalf("expected ErrConflict for a duplicate CredentialID, got %v", err)
	}
	// The first account's own credential must be untouched -- not silently
	// overwritten by the rejected second attempt.
	if _, err := s.PasskeyAccountByPrincipalID(ctx, "prn_c2"); err != store.ErrNotFound {
		t.Fatalf("expected the rejected second account to not exist, got %v", err)
	}
	credentials, err := s.WebAuthnCredentialsByPrincipalID(ctx, "prn_c1")
	if err != nil {
		t.Fatal(err)
	}
	if len(credentials) != 1 {
		t.Fatalf("prn_c1's credentials = %+v, want exactly the original one, untouched", credentials)
	}
}

// TestSaveWebAuthnCredential_CredentialIDCollision proves the same
// uniqueness check applies to the standalone "add another passkey to an
// already-existing account" path, not just the atomic signup path.
func TestSaveWebAuthnCredential_CredentialIDCollision(t *testing.T) {
	ctx := context.Background()
	s := New()

	if err := s.CreatePasskeyAccountWithCredential(ctx, domain.PasskeyAccount{PrincipalID: "prn_add", DisplayHandle: "HADD", CreatedAt: time.Now().UTC()}, testCredentialRecord("prn_add", "shared-cred-id-2")); err != nil {
		t.Fatalf("account: %v", err)
	}
	duplicate := testCredentialRecord("prn_add", "shared-cred-id-2")
	duplicate.ID = "pkcred_different_row_id"
	if err := s.SaveWebAuthnCredential(ctx, duplicate); err != store.ErrConflict {
		t.Fatalf("expected ErrConflict for a duplicate CredentialID via SaveWebAuthnCredential, got %v", err)
	}
}
