package memory

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// TestMemoryOpenTaskCRUD is the memory-store half of Phase 3C's store
// parity requirement: PutOpenTask/GetOpenTask/OpenTaskByIdempotencyKey
// must behave identically to the Postgres implementation (see
// internal/store/postgres/open_task_test.go).
func TestMemoryOpenTaskCRUD(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now().UTC()
	task := domain.OpenTask{
		ID: "task_mem_1", PrincipalID: "prn_1", Title: "t", Input: map[string]any{"a": 1},
		Status: domain.OpenTaskOpen, ExpiresAt: now.Add(time.Hour),
		PublicationIdempotencyKey: "idem-1", CreatedAt: now, UpdatedAt: now,
	}
	if err := s.PutOpenTask(ctx, task); err != nil {
		t.Fatalf("PutOpenTask: %v", err)
	}
	got, err := s.GetOpenTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetOpenTask: %v", err)
	}
	if got.Title != task.Title || got.PrincipalID != task.PrincipalID {
		t.Fatalf("round-tripped task mismatch: %+v", got)
	}
	byIdem, err := s.OpenTaskByIdempotencyKey(ctx, "prn_1", "idem-1")
	if err != nil {
		t.Fatalf("OpenTaskByIdempotencyKey: %v", err)
	}
	if byIdem.ID != task.ID {
		t.Fatalf("OpenTaskByIdempotencyKey returned %q, want %q", byIdem.ID, task.ID)
	}
	if _, err := s.OpenTaskByIdempotencyKey(ctx, "prn_1", "no-such-key"); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	byPrincipal, err := s.OpenTasksByPrincipal(ctx, "prn_1")
	if err != nil {
		t.Fatalf("OpenTasksByPrincipal: %v", err)
	}
	if len(byPrincipal) != 1 || byPrincipal[0].ID != task.ID {
		t.Fatalf("OpenTasksByPrincipal = %+v", byPrincipal)
	}

	public, err := s.ListPublicOpenTasks(ctx, 10)
	if err != nil {
		t.Fatalf("ListPublicOpenTasks: %v", err)
	}
	if len(public) != 1 || public[0].ID != task.ID {
		t.Fatalf("ListPublicOpenTasks = %+v", public)
	}

	updated, err := s.UpdateOpenTask(ctx, task.ID, func(current domain.OpenTask, exists bool) (domain.OpenTask, error) {
		if !exists {
			t.Fatal("expected task to exist")
		}
		current.Status = domain.OpenTaskCancelled
		return current, nil
	})
	if err != nil {
		t.Fatalf("UpdateOpenTask: %v", err)
	}
	if updated.Status != domain.OpenTaskCancelled {
		t.Fatalf("updated status = %s, want cancelled", updated.Status)
	}
	publicAfterCancel, err := s.ListPublicOpenTasks(ctx, 10)
	if err != nil {
		t.Fatalf("ListPublicOpenTasks after cancel: %v", err)
	}
	if len(publicAfterCancel) != 0 {
		t.Fatalf("cancelled task should not appear in public listing: %+v", publicAfterCancel)
	}
}

func TestMemoryOpenTaskProposalCRUD(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now().UTC()
	p := domain.OpenTaskProposal{
		ID: "prop_mem_1", TaskID: "task_mem_1", ProviderID: "agt_1",
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0", Message: "hi",
		ProposalIdempotencyKey: "propidem-1", CreatedAt: now, UpdatedAt: now,
	}
	if err := s.PutOpenTaskProposal(ctx, p); err != nil {
		t.Fatalf("PutOpenTaskProposal: %v", err)
	}
	got, err := s.GetOpenTaskProposal(ctx, p.ID)
	if err != nil {
		t.Fatalf("GetOpenTaskProposal: %v", err)
	}
	if got.Message != p.Message {
		t.Fatalf("round-tripped proposal mismatch: %+v", got)
	}
	byIdem, err := s.OpenTaskProposalByIdempotencyKey(ctx, "agt_1", "propidem-1")
	if err != nil {
		t.Fatalf("OpenTaskProposalByIdempotencyKey: %v", err)
	}
	if byIdem.ID != p.ID {
		t.Fatalf("OpenTaskProposalByIdempotencyKey returned %q, want %q", byIdem.ID, p.ID)
	}
	byTask, err := s.ProposalsByTask(ctx, "task_mem_1")
	if err != nil {
		t.Fatalf("ProposalsByTask: %v", err)
	}
	if len(byTask) != 1 || byTask[0].ID != p.ID {
		t.Fatalf("ProposalsByTask = %+v", byTask)
	}

	withdrawn, err := s.UpdateOpenTaskProposal(ctx, p.ID, func(current domain.OpenTaskProposal, exists bool) (domain.OpenTaskProposal, error) {
		now := time.Now().UTC()
		current.WithdrawnAt = &now
		return current, nil
	})
	if err != nil {
		t.Fatalf("UpdateOpenTaskProposal: %v", err)
	}
	if withdrawn.WithdrawnAt == nil {
		t.Fatal("expected WithdrawnAt to be set")
	}

	// Identity fields must be protected against mutation through Update.
	_, err = s.UpdateOpenTaskProposal(ctx, p.ID, func(current domain.OpenTaskProposal, exists bool) (domain.OpenTaskProposal, error) {
		current.ProviderID = "agt_someone_else"
		return current, nil
	})
	if err == nil {
		t.Fatal("expected an error when changing ProviderID through UpdateOpenTaskProposal")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}

// TestMemoryOpenAcceptanceOperation_InFlightGuard is the memory-store half
// of the "at most one non-terminal AcceptanceOperation per task" invariant
// -- see TestOpenAcceptanceOperation_RejectsSecondAttemptWhileFirstInFlight
// in the Postgres package for the real-concurrency version of this same
// contract.
func TestMemoryOpenAcceptanceOperation_InFlightGuard(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now().UTC()
	taskID := "task_mem_accept_1"
	if err := s.PutOpenTask(ctx, domain.OpenTask{
		ID: taskID, PrincipalID: "prn_1", Title: "t", Status: domain.OpenTaskOpen,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTask: %v", err)
	}
	propID := "prop_mem_accept_1"
	if err := s.PutOpenTaskProposal(ctx, domain.OpenTaskProposal{
		ID: propID, TaskID: taskID, ProviderID: "agt_1", CapabilityID: "cap_1", CapabilityVersion: "1.0.0",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTaskProposal: %v", err)
	}

	build := func(idemKey string) func(domain.OpenTask) (domain.AcceptanceOperation, error) {
		return func(task domain.OpenTask) (domain.AcceptanceOperation, error) {
			if task.Status != domain.OpenTaskOpen {
				return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskNotOpen, "not open", false)
			}
			n := time.Now().UTC()
			return domain.AcceptanceOperation{
				ID: "accop_mem_" + idemKey, TaskID: taskID, ProposalID: propID,
				PrincipalID: task.PrincipalID, ProviderID: "agt_1", CapabilityID: "cap_1", CapabilityVersion: "1.0.0",
				Checkpoint: domain.AcceptanceWinnerClaimed, IdempotencyKey: idemKey, CreatedAt: n, UpdatedAt: n,
			}, nil
		}
	}

	first, claimedTask, created, err := s.OpenAcceptanceOperation(ctx, taskID, build("first"))
	if err != nil || !created {
		t.Fatalf("first OpenAcceptanceOperation: created=%v err=%v", created, err)
	}
	if claimedTask.Status != domain.OpenTaskAccepted || claimedTask.AcceptedProposalID != propID {
		t.Fatalf("winner claim not reflected on task: %+v", claimedTask)
	}

	_, _, created, err = s.OpenAcceptanceOperation(ctx, taskID, build("second"))
	if err == nil {
		t.Fatalf("expected ErrOpenTaskAcceptanceInProgress, got created=%v", created)
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrOpenTaskAcceptanceInProgress {
		t.Fatalf("expected ErrOpenTaskAcceptanceInProgress, got %v", err)
	}

	// The same idempotency key as the first attempt resumes via the
	// caller's own AcceptanceOperationByIdempotencyKey lookup.
	resumed, err := s.AcceptanceOperationByIdempotencyKey(ctx, first.PrincipalID, first.IdempotencyKey)
	if err != nil {
		t.Fatalf("AcceptanceOperationByIdempotencyKey: %v", err)
	}
	if resumed.ID != first.ID {
		t.Fatalf("resumed ID = %q, want %q", resumed.ID, first.ID)
	}

	// UpdateAcceptanceOperation identity protection.
	_, err = s.UpdateAcceptanceOperation(ctx, first.ID, func(op domain.AcceptanceOperation, exists bool) (domain.AcceptanceOperation, error) {
		op.ProposalID = "prop_someone_else"
		return op, nil
	})
	if err == nil {
		t.Fatal("expected an error when changing ProposalID through UpdateAcceptanceOperation")
	}
}
