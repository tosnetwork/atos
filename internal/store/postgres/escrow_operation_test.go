package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

func TestEscrowOperationTwoHandlesConvergeAndTerminalDoesNotRegress(t *testing.T) {
	a := openTestStore(t)
	b := openTestStore(t)
	ctx := context.Background()
	suffix := randSuffix()
	job := "job_escrow_op_" + suffix
	now := time.Now().UTC()
	seed := domain.EscrowOperation{ID: "op_" + suffix, Kind: domain.EscrowOperationReserve, JobID: job, QuoteID: "quote_" + suffix, PrincipalID: "principal_" + suffix, RequestDigest: "sha256:" + suffix, Checkpoint: domain.EscrowOperationIntentPersisted, CreatedAt: now, UpdatedAt: now}
	if _, created, err := a.OpenEscrowOperation(ctx, seed); err != nil || !created {
		t.Fatalf("open: created=%v err=%v", created, err)
	}
	if got, created, err := b.OpenEscrowOperation(ctx, seed); err != nil || created || got.ID != seed.ID {
		t.Fatalf("replica open: got=%+v created=%v err=%v", got, created, err)
	}
	for _, step := range []domain.EscrowOperationCheckpoint{domain.EscrowOperationReconciling, domain.EscrowOperationAuthorityReserved, domain.EscrowOperationProjectionPersisted, domain.EscrowOperationCompleted} {
		if _, err := a.UpdateEscrowOperation(ctx, job, domain.EscrowOperationReserve, func(op domain.EscrowOperation) (domain.EscrowOperation, error) {
			op.Checkpoint = step
			op.UpdatedAt = time.Now().UTC()
			return op, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := b.UpdateEscrowOperation(ctx, job, domain.EscrowOperationReserve, func(op domain.EscrowOperation) (domain.EscrowOperation, error) {
		op.Checkpoint = domain.EscrowOperationReconciling
		op.LastError = "stale"
		return op, nil
	})
	if err != nil || got.Checkpoint != domain.EscrowOperationCompleted || got.LastError != "" {
		t.Fatalf("terminal regressed: %+v err=%v", got, err)
	}
}
