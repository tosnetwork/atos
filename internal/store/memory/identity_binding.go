package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func (s *Store) CurrentPrincipalBinding(ctx context.Context, principalID string) (domain.PrincipalIdentityBinding, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.principalBindings[principalID]
	return b, ok, nil
}

func (s *Store) PutPrincipalBinding(ctx context.Context, b domain.PrincipalIdentityBinding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.principalBindings[b.PrincipalID] = b
	return nil
}

func (s *Store) DeletePrincipalBinding(ctx context.Context, principalID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.principalBindings, principalID)
	return nil
}

// identityBindingOperationContentHash summarizes the identity fields that
// must never change once an operation is opened -- mirrors
// signerOperationContentHash's role exactly, simplified for identity
// binding's single-step operations.
func identityBindingOperationContentHash(op domain.IdentityBindingOperation) string {
	encoded, _ := json.Marshal(struct {
		PrincipalID    string
		Type           domain.IdentityBindingOperationType
		IdempotencyKey string
		AgentID        string
		ReasonCode     string
	}{op.PrincipalID, op.Type, op.IdempotencyKey, op.AgentID, op.ReasonCode})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func identityBindingOpIdemKey(principalID, key string) string {
	return principalID + ":" + key
}

func (s *Store) OpenIdentityBindingOperation(ctx context.Context, principalID string, op domain.IdentityBindingOperation) (domain.IdentityBindingOperation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idemKey := identityBindingOpIdemKey(principalID, op.IdempotencyKey)
	if existingID, ok := s.identityBindingOpByIdemKey[idemKey]; ok {
		existing := s.identityBindingOps[existingID]
		if identityBindingOperationContentHash(existing) != identityBindingOperationContentHash(op) {
			return domain.IdentityBindingOperation{}, false, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with different identity-binding operation content", false)
		}
		return existing, false, nil
	}
	s.identityBindingOps[op.ID] = op
	s.identityBindingOpByIdemKey[idemKey] = op.ID
	return op, true, nil
}

func (s *Store) GetIdentityBindingOperation(ctx context.Context, id string) (domain.IdentityBindingOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.identityBindingOps[id]
	if !ok {
		return domain.IdentityBindingOperation{}, store.ErrNotFound
	}
	return op, nil
}

func (s *Store) IdentityBindingOperationByIdempotencyKey(ctx context.Context, principalID, key string) (domain.IdentityBindingOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.identityBindingOpByIdemKey[identityBindingOpIdemKey(principalID, key)]
	if !ok {
		return domain.IdentityBindingOperation{}, store.ErrNotFound
	}
	return s.identityBindingOps[id], nil
}

func (s *Store) StaleIdentityBindingOperations(ctx context.Context, cutoff time.Time, limit int) ([]domain.IdentityBindingOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.IdentityBindingOperation
	for _, op := range s.identityBindingOps {
		if op.Checkpoint.Terminal() || op.UpdatedAt.After(cutoff) {
			continue
		}
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.Before(out[j].UpdatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) UpdateIdentityBindingOperation(ctx context.Context, id string, fn func(domain.IdentityBindingOperation, bool) (domain.IdentityBindingOperation, error)) (domain.IdentityBindingOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.identityBindingOps[id]
	next, err := fn(current, exists)
	if err != nil {
		return domain.IdentityBindingOperation{}, err
	}
	if exists {
		if next.ID != current.ID {
			return domain.IdentityBindingOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "identity-binding operation update must not change the operation id", false)
		}
		if identityBindingOperationContentHash(current) != identityBindingOperationContentHash(next) {
			return domain.IdentityBindingOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "identity-binding operation update must not change identity fields", false)
		}
	}
	s.identityBindingOps[id] = next
	return next, nil
}

func ownershipCommitmentKey(capabilityID, version string) string {
	return capabilityID + "@" + version
}

func (s *Store) PutCapabilityOwnershipCommitment(ctx context.Context, c domain.CapabilityOwnershipCommitment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := ownershipCommitmentKey(c.CapabilityID, c.Version)
	if existing, ok := s.ownershipCommitments[key]; ok {
		if existing.ProviderID != c.ProviderID || existing.ManifestCommitment != c.ManifestCommitment ||
			existing.OwnershipCommitment != c.OwnershipCommitment || existing.Network != c.Network {
			return domain.NewError(domain.ErrIdempotencyConflict, "capability version is already committed with different ownership or manifest", false)
		}
		return nil
	}
	s.ownershipCommitments[key] = c
	return nil
}

func (s *Store) CapabilityOwnershipCommitmentByVersion(ctx context.Context, capabilityID, version string) (domain.CapabilityOwnershipCommitment, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.ownershipCommitments[ownershipCommitmentKey(capabilityID, version)]
	return c, ok, nil
}
