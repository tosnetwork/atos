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
//
// RevocationReasonCode is caller-supplied content for Revoke/Rotate (it is
// forwarded verbatim into the RevokeExecutionSigner request tos-protocol
// hashes into its own commitment digest) and must be part of this
// operation's identity like every other caller-supplied field: an
// idempotency-key replay that supplies a different reason code is a
// different logical request and must conflict, not silently keep whatever
// reason the first call happened to persist.
//
// NewValidFromExplicit/NewValidUntilExplicit are hashed alongside the
// values themselves: whether the caller explicitly supplied the field is
// itself part of this operation's identity, not just the value when
// present -- a replay that OMITS a field the original call explicitly
// supplied is a different request (see
// service.ExecutionSignerService.resumeOrConflict's doc comment), not a
// legitimate transport-retry shape, and must conflict here too.
func signerOperationContentHash(op domain.ExecutionSignerOperation) string {
	encoded, _ := json.Marshal(struct {
		ProviderID, CapabilityID, CapabilityVersion   string
		Type                                          domain.SignerOperationType
		IdempotencyKey                                string
		NewExecutionSignerID                          string
		NewSignerPublicKey                            []byte
		NewSignatureAlgorithm                         string
		NewValidFromUnixMicro, NewValidUntilUnixMicro int64
		NewValidFromExplicit, NewValidUntilExplicit   bool
		OldAuthorizationID, OldExecutionSignerID      string
		RevocationReasonCode                          string
	}{
		op.ProviderID, op.CapabilityID, op.CapabilityVersion, op.Type,
		op.IdempotencyKey, op.NewExecutionSignerID,
		op.NewSignerPublicKey, op.NewSignatureAlgorithm, op.NewValidFrom.UnixMicro(), op.NewValidUntil.UnixMicro(),
		op.NewValidFromExplicit, op.NewValidUntilExplicit,
		op.OldAuthorizationID, op.OldExecutionSignerID,
		op.RevocationReasonCode,
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
	return s.openSignerOperationLocked(providerID, op)
}

// openSignerOperationLocked is OpenSignerOperation's body, factored out so
// OpenSignerOperationForCapability can reuse it while already holding
// s.mu -- acquiring it a second time would deadlock, sync.Mutex is not
// re-entrant. Caller must hold s.mu.
func (s *Store) openSignerOperationLocked(providerID string, op domain.ExecutionSignerOperation) (domain.ExecutionSignerOperation, bool, error) {
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

// currentSignerLocked derives "the currently authorized execution signer"
// for (capabilityID, capabilityVersion) exactly like
// service.ExecutionSignerService.CurrentSigner does from
// LatestCompletedSignerOperationByCapability's result -- duplicated here
// (rather than shared across the service/store package boundary) because
// OpenSignerOperationForCapability needs this same derivation available
// UNDER THE LOCK, at a snapshot consistent with the operation it is about
// to open, not as a separate store call the service layer could only
// make before or after that lock is held. Caller must hold s.mu.
func (s *Store) currentSignerLocked(capabilityID, capabilityVersion string) (authorizationID, executionSignerID string, found bool) {
	var latest domain.ExecutionSignerOperation
	latestFound := false
	for _, op := range s.signerOperations {
		if op.CapabilityID != capabilityID || op.CapabilityVersion != capabilityVersion || !op.Checkpoint.Terminal() {
			continue
		}
		if !latestFound || op.UpdatedAt.After(latest.UpdatedAt) {
			latest, latestFound = op, true
		}
	}
	if !latestFound {
		return "", "", false
	}
	switch latest.Type {
	case domain.SignerOperationAuthorize, domain.SignerOperationRotate:
		return latest.NewAuthorizationID, latest.NewExecutionSignerID, true
	default: // revoke
		return "", "", false
	}
}

// hasNonTerminalSignerOperationLocked reports whether a non-terminal
// signer operation already exists for (capabilityID, capabilityVersion) --
// the "at most one in-flight signer mutation per capability version"
// invariant OpenSignerOperationForCapability enforces. Caller must hold
// s.mu.
func (s *Store) hasNonTerminalSignerOperationLocked(capabilityID, capabilityVersion string) bool {
	for _, op := range s.signerOperations {
		if op.CapabilityID == capabilityID && op.CapabilityVersion == capabilityVersion && !op.Checkpoint.Terminal() {
			return true
		}
	}
	return false
}

// OpenSignerOperationForCapability -- see the interface doc comment
// (internal/store/store.go) for why this must be one atomic sequence
// rather than two separate store calls.
//
// Locking the read-then-open sequence alone is NOT sufficient on its
// own: it prevents two concurrent callers from reading current-signer at
// the exact same instant, but does nothing about two callers that read it
// in QUICK SUCCESSION, before the first one's operation has reached
// Completed -- the second would still see the same stale "current"
// signer and open a second, independently-completable operation against
// it. Rejecting a new open while ANY non-terminal operation already
// exists for this capability version is what actually closes that
// window.
func (s *Store) OpenSignerOperationForCapability(
	ctx context.Context, providerID, capabilityID, capabilityVersion string,
	build func(currentAuthorizationID, currentExecutionSignerID string, found bool) (domain.ExecutionSignerOperation, error),
) (domain.ExecutionSignerOperation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hasNonTerminalSignerOperationLocked(capabilityID, capabilityVersion) {
		return domain.ExecutionSignerOperation{}, false, domain.NewError(domain.ErrSignerOperationInProgress,
			"a signer mutation is already in progress for this capability version", true)
	}
	currentAuthorizationID, currentExecutionSignerID, found := s.currentSignerLocked(capabilityID, capabilityVersion)
	op, err := build(currentAuthorizationID, currentExecutionSignerID, found)
	if err != nil {
		return domain.ExecutionSignerOperation{}, false, err
	}
	return s.openSignerOperationLocked(providerID, op)
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

func (s *Store) LatestCompletedSignerOperationByCapability(ctx context.Context, capabilityID, capabilityVersion string) (domain.ExecutionSignerOperation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest domain.ExecutionSignerOperation
	found := false
	for _, op := range s.signerOperations {
		if op.CapabilityID != capabilityID || op.CapabilityVersion != capabilityVersion || !op.Checkpoint.Terminal() {
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
