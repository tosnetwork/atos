package memory

import (
	"context"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func (s *Store) CreatePasskeyAccount(ctx context.Context, a domain.PasskeyAccount) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.passkeyAccounts[a.PrincipalID]; exists {
		return store.ErrConflict
	}
	if _, exists := s.passkeyHandles[a.DisplayHandle]; exists {
		return store.ErrConflict
	}
	s.passkeyAccounts[a.PrincipalID] = a
	s.passkeyHandles[a.DisplayHandle] = a.PrincipalID
	return nil
}

func (s *Store) PasskeyAccountByPrincipalID(ctx context.Context, principalID string) (domain.PasskeyAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.passkeyAccounts[principalID]
	if !ok {
		return domain.PasskeyAccount{}, store.ErrNotFound
	}
	return a, nil
}

func (s *Store) PasskeyAccountByDisplayHandle(ctx context.Context, handle string) (domain.PasskeyAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	principalID, ok := s.passkeyHandles[handle]
	if !ok {
		return domain.PasskeyAccount{}, store.ErrNotFound
	}
	return s.passkeyAccounts[principalID], nil
}

func (s *Store) SaveWebAuthnCredential(ctx context.Context, c domain.WebAuthnCredentialRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.passkeyCredentials[c.ID] = c
	return nil
}

func (s *Store) WebAuthnCredentialsByPrincipalID(ctx context.Context, principalID string) ([]domain.WebAuthnCredentialRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.WebAuthnCredentialRecord
	for _, c := range s.passkeyCredentials {
		if c.PrincipalID == principalID {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s *Store) WebAuthnCredentialViews(ctx context.Context, principalID string) ([]domain.WebAuthnCredentialRecord, error) {
	return s.WebAuthnCredentialsByPrincipalID(ctx, principalID)
}

func (s *Store) TouchWebAuthnCredential(ctx context.Context, id string, signCount uint32, cloneWarning, backupState bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.passkeyCredentials[id]
	if !ok {
		return store.ErrNotFound
	}
	c.SignCount = signCount
	c.CloneWarning = cloneWarning
	c.BackupState = backupState
	now := time.Now().UTC()
	c.LastUsedAt = &now
	s.passkeyCredentials[id] = c
	return nil
}

func (s *Store) CreateWebAuthnCeremony(ctx context.Context, c domain.WebAuthnCeremony) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.passkeyCeremonies[c.ID] = c
	return nil
}

func (s *Store) ConsumeWebAuthnCeremony(ctx context.Context, id string, purpose domain.WebAuthnCeremonyPurpose) (domain.WebAuthnCeremony, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.passkeyCeremonies[id]
	if !ok || c.Purpose != purpose {
		return domain.WebAuthnCeremony{}, store.ErrNotFound
	}
	delete(s.passkeyCeremonies, id)
	if !time.Now().UTC().Before(c.ExpiresAt) {
		return domain.WebAuthnCeremony{}, store.ErrNotFound
	}
	return c, nil
}

func (s *Store) PurgeExpiredWebAuthnCeremonies(ctx context.Context, cutoff time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for id, c := range s.passkeyCeremonies {
		if c.ExpiresAt.Before(cutoff) {
			delete(s.passkeyCeremonies, id)
			removed++
		}
	}
	return removed, nil
}
