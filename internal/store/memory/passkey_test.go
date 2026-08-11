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
