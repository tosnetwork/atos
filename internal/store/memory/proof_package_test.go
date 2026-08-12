package memory

import (
	"context"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
	"testing"
	"time"
)

func TestProofPackageOperationConvergesConflictsAndNeverRegresses(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.Now().UTC()
	seed := domain.ProofPackageOperation{ID: "proofop-1", ReceiptID: "receipt-1", JobID: "job-1", QuoteID: "quote-1", EscrowID: "esc-1", PrincipalID: "principal-1", SemanticDigest: "sha256:one", Checkpoint: domain.ProofPackageIntentPersisted, CreatedAt: now, UpdatedAt: now}
	if _, created, e := s.OpenProofPackageOperation(ctx, seed); e != nil || !created {
		t.Fatalf("open=%v %v", created, e)
	}
	if _, created, e := s.OpenProofPackageOperation(ctx, seed); e != nil || created {
		t.Fatalf("replay=%v %v", created, e)
	}
	changed := seed
	changed.SemanticDigest = "sha256:two"
	if _, _, e := s.OpenProofPackageOperation(ctx, changed); e == nil {
		t.Fatal("changed semantics accepted")
	}
	for _, c := range []domain.ProofPackageCheckpoint{domain.ProofPackageReconciling, domain.ProofPackageCanonicalObserved, domain.ProofPackageProjectionPersisted, domain.ProofPackageCompleted} {
		if _, e := s.UpdateProofPackageOperation(ctx, seed.ID, func(o domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
			o.Checkpoint = c
			return o, nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := s.UpdateProofPackageOperation(ctx, seed.ID, func(o domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
		o.Checkpoint = domain.ProofPackageReconciling
		return o, nil
	}); e != store.ErrConflict {
		t.Fatalf("terminal regression err=%v", e)
	}
}
