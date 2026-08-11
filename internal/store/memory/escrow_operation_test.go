package memory

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func TestEscrowOperationIsMonotonicAndCompletedIsTerminal(t *testing.T) {
	s := New()
	now := time.Now().UTC()
	seed := domain.EscrowOperation{ID: "op", Kind: domain.EscrowOperationReserve, JobID: "job", QuoteID: "quote", PrincipalID: "principal", RequestDigest: "sha256:x", Checkpoint: domain.EscrowOperationIntentPersisted, CreatedAt: now, UpdatedAt: now}
	if _, created, err := s.OpenEscrowOperation(context.Background(), seed); err != nil || !created {
		t.Fatalf("open created=%v err=%v", created, err)
	}
	steps := []domain.EscrowOperationCheckpoint{domain.EscrowOperationReconciling, domain.EscrowOperationAuthorityReserved, domain.EscrowOperationProjectionPersisted, domain.EscrowOperationCompleted}
	for _, step := range steps {
		if _, err := s.UpdateEscrowOperation(context.Background(), "job", domain.EscrowOperationReserve, func(op domain.EscrowOperation) (domain.EscrowOperation, error) {
			op.Checkpoint = step
			op.UpdatedAt = time.Now().UTC()
			return op, nil
		}); err != nil {
			t.Fatalf("advance %s: %v", step, err)
		}
	}
	op, err := s.UpdateEscrowOperation(context.Background(), "job", domain.EscrowOperationReserve, func(op domain.EscrowOperation) (domain.EscrowOperation, error) {
		op.Checkpoint = domain.EscrowOperationReconciling
		op.LastError = "stale replica"
		return op, nil
	})
	if err != nil || op.Checkpoint != domain.EscrowOperationCompleted || op.LastError != "" {
		t.Fatalf("terminal regressed: %+v err=%v", op, err)
	}
}

func TestEscrowOperationRejectsIllegalAuthorityTransition(t *testing.T) {
	s := New()
	now := time.Now().UTC()
	_, _, _ = s.OpenEscrowOperation(context.Background(), domain.EscrowOperation{ID: "op", Kind: domain.EscrowOperationReserve, JobID: "job", Checkpoint: domain.EscrowOperationIntentPersisted, CreatedAt: now, UpdatedAt: now})
	_, err := s.UpdateEscrowOperation(context.Background(), "job", domain.EscrowOperationReserve, func(op domain.EscrowOperation) (domain.EscrowOperation, error) {
		op.Checkpoint = domain.EscrowOperationAuthorityReleased
		return op, nil
	})
	if err != store.ErrConflict {
		t.Fatalf("err=%v want conflict", err)
	}
}
