package postgres_test

import (
	"context"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
	"testing"
	"time"
)

func TestProofPackageTwoPoolsConvergeAndFenceTerminal(t *testing.T) {
	a := openTestStore(t)
	b := openTestStore(t)
	x := randSuffix()
	ctx := context.Background()
	now := time.Now().UTC()
	seed := domain.ProofPackageOperation{ID: "proofop_" + x, ReceiptID: "receipt_" + x, JobID: "job_" + x, QuoteID: "quote_" + x, EscrowID: "esc_" + x, PrincipalID: "principal_" + x, SemanticDigest: "sha256:" + x, Checkpoint: domain.ProofPackageIntentPersisted, CreatedAt: now, UpdatedAt: now}
	if _, created, e := a.OpenProofPackageOperation(ctx, seed); e != nil || !created {
		t.Fatalf("open=%v %v", created, e)
	}
	if got, created, e := b.OpenProofPackageOperation(ctx, seed); e != nil || created || got.ID != seed.ID {
		t.Fatalf("replica=%+v %v %v", got, created, e)
	}
	for _, c := range []domain.ProofPackageCheckpoint{domain.ProofPackageReconciling, domain.ProofPackageCanonicalObserved, domain.ProofPackageProjectionPersisted, domain.ProofPackageCompleted} {
		if _, e := a.UpdateProofPackageOperation(ctx, seed.ID, func(o domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
			o.Checkpoint = c
			o.UpdatedAt = time.Now().UTC()
			return o, nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := b.UpdateProofPackageOperation(ctx, seed.ID, func(o domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
		o.Checkpoint = domain.ProofPackageReconciling
		return o, nil
	}); e != store.ErrConflict {
		t.Fatalf("terminal regression err=%v", e)
	}
}
