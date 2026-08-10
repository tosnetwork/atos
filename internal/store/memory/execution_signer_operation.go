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

// signerOperationContentHash summarizes the identity fields that must
// never change once an operation is opened -- mirrors
// certificationContentHash's role for domain.SandboxCertification.
//
// NewAuthorizationID is deliberately EXCLUDED: it is generated fresh by
// this service on every call (like op.ID, also excluded), never supplied
// by the caller, so it must not be treated as caller-identity -- including
// it made every replay of the same logical request hash differently and
// spuriously conflict, since a second call generates a new random value
// even when everything the caller actually provided is identical.
//
// NewValidFrom/NewValidUntil are hashed as UnixMicro, matching the
// Postgres implementation's identical function exactly (there, this
// avoids a same-content replay hashing differently once a value has
// round-tripped through a microsecond-precision timestamptz column; kept
// consistent here even though the in-memory store has no such precision
// loss of its own, so both backends behave identically).
func signerOperationContentHash(op domain.ExecutionSignerOperation) string {
	encoded, _ := json.Marshal(struct {
		ProviderID, CapabilityID, CapabilityVersion   string
		Type                                          domain.SignerOperationType
		IdempotencyKey                                string
		NewExecutionSignerID                          string
		NewSignerPublicKey                            []byte
		NewSignatureAlgorithm                         string
		NewValidFromUnixMicro, NewValidUntilUnixMicro int64
		OldAuthorizationID, OldExecutionSignerID      string
	}{
		op.ProviderID, op.CapabilityID, op.CapabilityVersion, op.Type,
		op.IdempotencyKey, op.NewExecutionSignerID,
		op.NewSignerPublicKey, op.NewSignatureAlgorithm, op.NewValidFrom.UnixMicro(), op.NewValidUntil.UnixMicro(),
		op.OldAuthorizationID, op.OldExecutionSignerID,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func signerOpIdemKey(providerID, key string) string {
	return providerID + ":" + key
}

func (s *Store) OpenSignerOperation(ctx context.Context, providerID string, op domain.ExecutionSignerOperation) (domain.ExecutionSignerOperation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idemKey := signerOpIdemKey(providerID, op.IdempotencyKey)
	if existingID, ok := s.signerOpByIdemKey[idemKey]; ok {
		existing := s.signerOperations[existingID]
		if signerOperationContentHash(existing) != signerOperationContentHash(op) {
			return domain.ExecutionSignerOperation{}, false, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with different execution-signer operation content", false)
		}
		return existing, false, nil
	}
	s.signerOperations[op.ID] = op
	s.signerOpByIdemKey[idemKey] = op.ID
	return op, true, nil
}

func (s *Store) GetSignerOperation(ctx context.Context, id string) (domain.ExecutionSignerOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.signerOperations[id]
	if !ok {
		return domain.ExecutionSignerOperation{}, store.ErrNotFound
	}
	return op, nil
}

func (s *Store) SignerOperationByIdempotencyKey(ctx context.Context, providerID, key string) (domain.ExecutionSignerOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.signerOpByIdemKey[signerOpIdemKey(providerID, key)]
	if !ok {
		return domain.ExecutionSignerOperation{}, store.ErrNotFound
	}
	return s.signerOperations[id], nil
}

func (s *Store) LatestSignerOperationByCapability(ctx context.Context, capabilityID string) (domain.ExecutionSignerOperation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest domain.ExecutionSignerOperation
	found := false
	for _, op := range s.signerOperations {
		if op.CapabilityID != capabilityID {
			continue
		}
		if !found || op.UpdatedAt.After(latest.UpdatedAt) {
			latest, found = op, true
		}
	}
	return latest, found, nil
}

func (s *Store) LatestCompletedSignerOperationByCapability(ctx context.Context, capabilityID string) (domain.ExecutionSignerOperation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest domain.ExecutionSignerOperation
	found := false
	for _, op := range s.signerOperations {
		if op.CapabilityID != capabilityID || !op.Checkpoint.Terminal() {
			continue
		}
		if !found || op.UpdatedAt.After(latest.UpdatedAt) {
			latest, found = op, true
		}
	}
	return latest, found, nil
}

func (s *Store) StaleSignerOperations(ctx context.Context, cutoff time.Time, limit int) ([]domain.ExecutionSignerOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.ExecutionSignerOperation
	for _, op := range s.signerOperations {
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

func (s *Store) UpdateSignerOperation(ctx context.Context, id string, fn func(domain.ExecutionSignerOperation, bool) (domain.ExecutionSignerOperation, error)) (domain.ExecutionSignerOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.signerOperations[id]
	next, err := fn(current, exists)
	if err != nil {
		return domain.ExecutionSignerOperation{}, err
	}
	if exists {
		if next.ID != current.ID {
			return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "execution-signer operation update must not change the operation id", false)
		}
		if signerOperationContentHash(current) != signerOperationContentHash(next) {
			return domain.ExecutionSignerOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "execution-signer operation update must not change identity fields", false)
		}
	}
	s.signerOperations[id] = next
	return next, nil
}
