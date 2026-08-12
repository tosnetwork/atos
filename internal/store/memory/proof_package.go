package memory

import (
	"context"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
	"sort"
	"time"
)

func (s *Store) OpenProofPackageOperation(_ context.Context, op domain.ProofPackageOperation) (domain.ProofPackageOperation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id := s.proofPackageByReceipt[op.ReceiptID]; id != "" {
		e := s.proofPackageOperations[id]
		if e.SemanticDigest != op.SemanticDigest {
			return domain.ProofPackageOperation{}, false, domain.NewError(domain.ErrIdempotencyConflict, "proof identity reused with changed semantics", false)
		}
		return e, false, nil
	}
	s.proofPackageOperations[op.ID] = op
	s.proofPackageByReceipt[op.ReceiptID] = op.ID
	return op, true, nil
}
func (s *Store) GetProofPackageOperation(_ context.Context, id string) (domain.ProofPackageOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.proofPackageOperations[id]
	if !ok {
		return op, store.ErrNotFound
	}
	return op, nil
}
func (s *Store) ProofPackageOperationByReceipt(_ context.Context, id string) (domain.ProofPackageOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	oid := s.proofPackageByReceipt[id]
	op, ok := s.proofPackageOperations[oid]
	if !ok {
		return op, store.ErrNotFound
	}
	return op, nil
}
func (s *Store) UpdateProofPackageOperation(_ context.Context, id string, fn func(domain.ProofPackageOperation) (domain.ProofPackageOperation, error)) (domain.ProofPackageOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.proofPackageOperations[id]
	if !ok {
		return old, store.ErrNotFound
	}
	next, e := fn(old)
	if e != nil {
		return old, e
	}
	if next.ID != old.ID || next.ReceiptID != old.ReceiptID || next.JobID!=old.JobID||next.QuoteID!=old.QuoteID||next.EscrowID!=old.EscrowID||next.PrincipalID!=old.PrincipalID||next.SemanticDigest != old.SemanticDigest || (old.PackageDigest!=""&&next.PackageDigest!=old.PackageDigest) || !old.Checkpoint.CanAdvance(next.Checkpoint) {
		return old, store.ErrConflict
	}
	s.proofPackageOperations[id] = next
	return next, nil
}
func (s *Store) StaleProofPackageOperations(_ context.Context, cut time.Time, limit int) ([]domain.ProofPackageOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.ProofPackageOperation
	for _, op := range s.proofPackageOperations {
		if !op.Checkpoint.Terminal() && op.UpdatedAt.Before(cut) {
			out = append(out, op)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.Before(out[j].UpdatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
