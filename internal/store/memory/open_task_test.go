package memory

import (
	"context"
	"fmt"
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

	public, err := s.ListPublicOpenTasks(ctx, 10, now)
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
	publicAfterCancel, err := s.ListPublicOpenTasks(ctx, 10, now)
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

	build := func(idemKey string) func(domain.OpenTask, domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
		return func(task domain.OpenTask, proposal domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
			if task.Status != domain.OpenTaskOpen {
				return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskNotOpen, "not open", false)
			}
			n := time.Now().UTC()
			return domain.AcceptanceOperation{
				ID: "accop_mem_" + idemKey, TaskID: taskID, ProposalID: proposal.ID,
				PrincipalID: task.PrincipalID, ProviderID: proposal.ProviderID, CapabilityID: proposal.CapabilityID, CapabilityVersion: proposal.CapabilityVersion,
				Checkpoint: domain.AcceptanceWinnerClaimed, IdempotencyKey: idemKey, CreatedAt: n, UpdatedAt: n,
			}, nil
		}
	}

	first, claimedTask, created, err := s.OpenAcceptanceOperation(ctx, taskID, propID, build("first"))
	if err != nil || !created {
		t.Fatalf("first OpenAcceptanceOperation: created=%v err=%v", created, err)
	}
	if claimedTask.Status != domain.OpenTaskAccepted || claimedTask.AcceptedProposalID != propID {
		t.Fatalf("winner claim not reflected on task: %+v", claimedTask)
	}

	_, _, created, err = s.OpenAcceptanceOperation(ctx, taskID, propID, build("second"))
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

	// UpdateAcceptanceOperation must refuse to set a terminal checkpoint --
	// only CompleteAcceptance/FailAcceptance may, since those atomically
	// pair the transition with the OpenTask projection/reopen. See the
	// interface doc comment in internal/store/store.go.
	for _, terminal := range []domain.AcceptanceCheckpoint{domain.AcceptanceCompleted, domain.AcceptanceFailed} {
		_, err := s.UpdateAcceptanceOperation(ctx, first.ID, func(op domain.AcceptanceOperation, exists bool) (domain.AcceptanceOperation, error) {
			op.Checkpoint = terminal
			return op, nil
		})
		if err == nil {
			t.Fatalf("expected UpdateAcceptanceOperation to refuse setting checkpoint=%s", terminal)
		}
		derr, ok := err.(*domain.Error)
		if !ok || derr.Code != domain.ErrIdempotencyConflict {
			t.Fatalf("expected ErrIdempotencyConflict for checkpoint=%s, got %v", terminal, err)
		}
	}
}

// TestMemoryUpdateAcceptanceOperation_TerminalIsImmutable proves the
// corrected semantics of UpdateAcceptanceOperation once an operation
// reaches a terminal checkpoint (an earlier version of this guard
// unconditionally rejected any terminal next.Checkpoint, which broke
// advanceAcceptance's own stale-worker CAS no-op -- a worker observing an
// operation a DIFFERENT worker already completed would get a spurious
// ErrIdempotencyConflict instead of safely converging):
//  1. an update whose fn returns current UNCHANGED (exactly what
//     advanceAcceptance's own CAS no-op branch produces) succeeds, not
//     ErrIdempotencyConflict;
//  2. an update that tries to move a terminal operation back to a
//     non-terminal checkpoint is silently ignored, never applied;
//  3. a concurrent stale driver holding a pre-completion snapshot (an
//     expectedFrom that no longer matches, because a different driver
//     already completed the operation) converges without error -- the
//     exact "two concurrent reconcilers" scenario this whole journal
//     exists to support.
func TestMemoryUpdateAcceptanceOperation_TerminalIsImmutable(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now().UTC()
	taskID := "task_mem_terminal_immutable"
	propID := "prop_mem_terminal_immutable"
	if err := s.PutOpenTask(ctx, domain.OpenTask{
		ID: taskID, PrincipalID: "prn_1", Title: "t", Status: domain.OpenTaskOpen,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTask: %v", err)
	}
	if err := s.PutOpenTaskProposal(ctx, domain.OpenTaskProposal{
		ID: propID, TaskID: taskID, ProviderID: "agt_1", CapabilityID: "cap_1", CapabilityVersion: "1.0.0",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTaskProposal: %v", err)
	}
	op, _, created, err := s.OpenAcceptanceOperation(ctx, taskID, propID, func(task domain.OpenTask, proposal domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
		n := time.Now().UTC()
		return domain.AcceptanceOperation{
			ID: "accop_terminal_immutable", TaskID: taskID, ProposalID: propID,
			PrincipalID: task.PrincipalID, ProviderID: proposal.ProviderID,
			CapabilityID: proposal.CapabilityID, CapabilityVersion: proposal.CapabilityVersion,
			Checkpoint: domain.AcceptanceJobBound, QuoteID: "q_1", JobID: "job_1",
			IdempotencyKey: "idem_terminal_immutable", CreatedAt: n, UpdatedAt: n,
		}, nil
	})
	if err != nil || !created {
		t.Fatalf("OpenAcceptanceOperation: created=%v err=%v", created, err)
	}
	completed, _, err := s.CompleteAcceptance(ctx, op.ID)
	if err != nil {
		t.Fatalf("CompleteAcceptance: %v", err)
	}
	if completed.Checkpoint != domain.AcceptanceCompleted {
		t.Fatalf("checkpoint = %s, want completed", completed.Checkpoint)
	}

	// Case 1: a stale worker's CAS no-op must succeed, not error.
	noop, err := s.UpdateAcceptanceOperation(ctx, op.ID, func(current domain.AcceptanceOperation, exists bool) (domain.AcceptanceOperation, error) {
		return current, nil // exactly what advanceAcceptance's own no-op branch does
	})
	if err != nil {
		t.Fatalf("expected a no-op update against a terminal operation to succeed, got: %v", err)
	}
	if noop.Checkpoint != domain.AcceptanceCompleted {
		t.Fatalf("no-op update changed checkpoint to %s", noop.Checkpoint)
	}

	// Case 2: an attempted revival must be silently ignored.
	revived, err := s.UpdateAcceptanceOperation(ctx, op.ID, func(current domain.AcceptanceOperation, exists bool) (domain.AcceptanceOperation, error) {
		current.Checkpoint = domain.AcceptanceReconciling
		return current, nil
	})
	if err != nil {
		t.Fatalf("expected an attempted revival to be silently ignored (no error), got: %v", err)
	}
	if revived.Checkpoint != domain.AcceptanceCompleted {
		t.Fatalf("terminal operation was revived to checkpoint=%s", revived.Checkpoint)
	}
	stored, err := s.GetAcceptanceOperation(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetAcceptanceOperation: %v", err)
	}
	if stored.Checkpoint != domain.AcceptanceCompleted {
		t.Fatalf("stored operation was revived to checkpoint=%s", stored.Checkpoint)
	}

	// Case 3: a concurrent stale driver's advance (expectedFrom no longer
	// matches, because this operation was already completed by someone
	// else) must converge without error -- exactly advanceAcceptance's
	// own CAS check reproduced here.
	staleAdvance, err := s.UpdateAcceptanceOperation(ctx, op.ID, func(current domain.AcceptanceOperation, exists bool) (domain.AcceptanceOperation, error) {
		if !exists {
			return domain.AcceptanceOperation{}, domain.NewError(domain.ErrNotFound, "not found", false)
		}
		if current.Checkpoint.Terminal() || current.Checkpoint != domain.AcceptanceJobBound {
			return current, nil
		}
		current.Checkpoint = domain.AcceptanceCompleted
		return current, nil
	})
	if err != nil {
		t.Fatalf("expected a stale driver's advance against an already-completed operation to converge without error, got: %v", err)
	}
	if staleAdvance.Checkpoint != domain.AcceptanceCompleted {
		t.Fatalf("stale advance result checkpoint = %s, want completed", staleAdvance.Checkpoint)
	}
}

// TestMemoryListPublicOpenTasks_ExcludesExpiredBeforeApplyingLimit is the
// memory-store counterpart of the Postgres regression test with the same
// name: an older, genuinely open task must not be hidden behind newer
// rows that are lazily expired (Status still Open, ExpiresAt already
// passed) consuming the limit window.
func TestMemoryListPublicOpenTasks_ExcludesExpiredBeforeApplyingLimit(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now().UTC()

	olderOpenID := "task_mem_older_open"
	if err := s.PutOpenTask(ctx, domain.OpenTask{
		ID: olderOpenID, PrincipalID: "prn_1", Title: "older, still genuinely open",
		Status: domain.OpenTaskOpen, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("PutOpenTask(older open): %v", err)
	}

	const expiredCount = 3
	for i := 0; i < expiredCount; i++ {
		id := fmt.Sprintf("task_mem_expired_%d", i)
		if err := s.PutOpenTask(ctx, domain.OpenTask{
			ID: id, PrincipalID: "prn_1", Title: "newer, but expired -- no sweep yet",
			Status: domain.OpenTaskOpen, ExpiresAt: now.Add(-time.Minute),
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("PutOpenTask(expired %d): %v", i, err)
		}
	}

	tasks, err := s.ListPublicOpenTasks(ctx, expiredCount, now)
	if err != nil {
		t.Fatalf("ListPublicOpenTasks: %v", err)
	}
	found := false
	for _, task := range tasks {
		if task.ID == olderOpenID {
			found = true
		}
		if task.Status == domain.OpenTaskOpen && task.Expired(now) {
			t.Fatalf("ListPublicOpenTasks returned an expired-but-stored-open row: %+v", task)
		}
	}
	if !found {
		t.Fatalf("older genuinely-open task was hidden behind %d newer expired rows consuming the limit window; got %+v", expiredCount, tasks)
	}
}

// TestMemoryCreateOpenTaskProposal_RejectsOnAlreadyClosedTask is the
// memory-store counterpart of the Postgres regression test with the same
// name: CreateOpenTaskProposal must refuse (without calling build, without
// inserting) once the task is no longer open.
func TestMemoryCreateOpenTaskProposal_RejectsOnAlreadyClosedTask(t *testing.T) {
	ctx := context.Background()
	s := New()
	taskID := "task_mem_closed_propose"
	now := time.Now().UTC()

	if err := s.PutOpenTask(ctx, domain.OpenTask{
		ID: taskID, PrincipalID: "prn_1", Title: "closed before propose lands",
		Status: domain.OpenTaskOpen, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTask: %v", err)
	}
	if _, err := s.UpdateOpenTask(ctx, taskID, func(t domain.OpenTask, exists bool) (domain.OpenTask, error) {
		t.Status = domain.OpenTaskCancelled
		t.UpdatedAt = time.Now().UTC()
		return t, nil
	}); err != nil {
		t.Fatalf("cancel via UpdateOpenTask: %v", err)
	}

	buildCalled := false
	proposalID := "otprop_closed_mem"
	_, err := s.CreateOpenTaskProposal(ctx, taskID, now, func(task domain.OpenTask) (domain.OpenTaskProposal, error) {
		buildCalled = true
		n := time.Now().UTC()
		return domain.OpenTaskProposal{
			ID: proposalID, TaskID: taskID, ProviderID: "provider_1",
			CapabilityID: "cap_1", CapabilityVersion: "v1",
			CreatedAt: n, UpdatedAt: n,
		}, nil
	})
	if err == nil {
		t.Fatal("expected CreateOpenTaskProposal to refuse a proposal against an already-cancelled task")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrOpenTaskNotOpen {
		t.Fatalf("expected ErrOpenTaskNotOpen, got %v", err)
	}
	if buildCalled {
		t.Fatal("build must not be invoked when the task is not open")
	}
	if _, err := s.GetOpenTaskProposal(ctx, proposalID); err != store.ErrNotFound {
		t.Fatalf("expected no proposal row to have been inserted, got err=%v", err)
	}
}
