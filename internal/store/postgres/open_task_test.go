package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

// TestOpenAcceptanceOperation_ConcurrentAcceptHasSingleWinner is Phase 3C's
// store-layer proof of atos-spec §7.3's core invariant: N concurrent Accept
// attempts against real Postgres (not the in-memory store's single mutex)
// yield exactly one created=true AcceptanceOperation and exactly one
// AcceptedProposalID durably claimed on the OpenTask -- see
// store.OpenTasks.OpenAcceptanceOperation's doc comment for why the
// non-terminal-operation guard and the winner-claim must be one atomic
// sequence.
func TestOpenAcceptanceOperation_ConcurrentAcceptHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	taskID := "task_pg_" + suffix
	now := time.Now().UTC()

	if err := s.PutOpenTask(ctx, domain.OpenTask{
		ID: taskID, PrincipalID: "principal_" + suffix, Title: "concurrency smoke test",
		Status: domain.OpenTaskOpen, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTask: %v", err)
	}

	const proposalCount = 3
	proposalIDs := make([]string, proposalCount)
	for i := 0; i < proposalCount; i++ {
		proposalIDs[i] = fmt.Sprintf("prop_%s_%d", suffix, i)
		if err := s.PutOpenTaskProposal(ctx, domain.OpenTaskProposal{
			ID: proposalIDs[i], TaskID: taskID, ProviderID: fmt.Sprintf("provider_%s_%d", suffix, i),
			CapabilityID: "cap_" + suffix, CapabilityVersion: "v1",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("PutOpenTaskProposal[%d]: %v", i, err)
		}
	}

	const attempts = 16
	var wg sync.WaitGroup
	type result struct {
		created bool
		err     error
		propID  string
	}
	results := make(chan result, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		propID := proposalIDs[i%proposalCount]
		go func(i int) {
			defer wg.Done()
			op, _, created, err := s.OpenAcceptanceOperation(ctx, taskID, propID, func(task domain.OpenTask, proposal domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
				if task.Status != domain.OpenTaskOpen {
					return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskNotOpen, "task is not open", false)
				}
				n := time.Now().UTC()
				return domain.AcceptanceOperation{
					ID: fmt.Sprintf("accop_%s_%d", suffix, i), TaskID: taskID, ProposalID: proposal.ID,
					PrincipalID: task.PrincipalID, ProviderID: proposal.ProviderID,
					CapabilityID: proposal.CapabilityID, CapabilityVersion: proposal.CapabilityVersion,
					Checkpoint:     domain.AcceptanceIntentPersisted,
					IdempotencyKey: fmt.Sprintf("idem_%s_%d", suffix, i),
					CreatedAt:      n, UpdatedAt: n,
				}, nil
			})
			results <- result{created, err, op.ProposalID}
		}(i)
	}
	wg.Wait()
	close(results)

	createdCount := 0
	inProgressCount := 0
	var winnerProposal string
	for r := range results {
		switch {
		case r.err == nil && r.created:
			createdCount++
			winnerProposal = r.propID
		case r.err != nil:
			if derr, ok := r.err.(*domain.Error); ok && derr.Code == domain.ErrOpenTaskAcceptanceInProgress {
				inProgressCount++
				continue
			}
			t.Fatalf("unexpected error: %v", r.err)
		default:
			t.Fatalf("created=false with no error is not a valid outcome: %+v", r)
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want exactly 1", createdCount)
	}
	if createdCount+inProgressCount != attempts {
		t.Fatalf("created(%d)+in_progress(%d) != attempts(%d)", createdCount, inProgressCount, attempts)
	}

	task, err := s.GetOpenTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetOpenTask: %v", err)
	}
	if task.Status != domain.OpenTaskAccepted {
		t.Fatalf("task status = %s, want accepted", task.Status)
	}
	if task.AcceptedProposalID != winnerProposal {
		t.Fatalf("task.AcceptedProposalID = %q, want %q (the created=true winner)", task.AcceptedProposalID, winnerProposal)
	}

	nonTerminal, err := s.StaleAcceptanceOperations(ctx, now.Add(24*time.Hour), 100)
	if err != nil {
		t.Fatalf("StaleAcceptanceOperations: %v", err)
	}
	found := 0
	for _, op := range nonTerminal {
		if op.TaskID == taskID {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("exactly one non-terminal AcceptanceOperation should exist for this task, found %d", found)
	}
}

// TestOpenAcceptanceOperation_RejectsSecondAttemptWhileFirstInFlight proves
// a DIFFERENT idempotency key (not a replay of the same request) is
// rejected while the first operation is still non-terminal, even after the
// race above resolves -- a distinct, deterministic (non-concurrent)
// assertion of the same invariant.
func TestOpenAcceptanceOperation_RejectsSecondAttemptWhileFirstInFlight(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	taskID := "task_pg_seq_" + suffix
	now := time.Now().UTC()

	if err := s.PutOpenTask(ctx, domain.OpenTask{
		ID: taskID, PrincipalID: "principal_" + suffix, Title: "sequential smoke test",
		Status: domain.OpenTaskOpen, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTask: %v", err)
	}
	propID := "prop_" + suffix
	if err := s.PutOpenTaskProposal(ctx, domain.OpenTaskProposal{
		ID: propID, TaskID: taskID, ProviderID: "provider_" + suffix,
		CapabilityID: "cap_" + suffix, CapabilityVersion: "v1",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTaskProposal: %v", err)
	}

	build := func(idemKey string) func(domain.OpenTask, domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
		return func(task domain.OpenTask, proposal domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
			if task.Status != domain.OpenTaskOpen {
				return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskNotOpen, "task is not open", false)
			}
			n := time.Now().UTC()
			return domain.AcceptanceOperation{
				ID: "accop_seq_" + idemKey, TaskID: taskID, ProposalID: proposal.ID,
				PrincipalID: task.PrincipalID, ProviderID: proposal.ProviderID,
				CapabilityID: proposal.CapabilityID, CapabilityVersion: proposal.CapabilityVersion,
				Checkpoint: domain.AcceptanceIntentPersisted, IdempotencyKey: idemKey,
				CreatedAt: n, UpdatedAt: n,
			}, nil
		}
	}

	first, _, created, err := s.OpenAcceptanceOperation(ctx, taskID, propID, build("idem_first_"+suffix))
	if err != nil {
		t.Fatalf("first OpenAcceptanceOperation: %v", err)
	}
	if !created {
		t.Fatal("expected created=true for the first attempt")
	}

	_, _, created, err = s.OpenAcceptanceOperation(ctx, taskID, propID, build("idem_second_"+suffix))
	if err == nil {
		t.Fatalf("expected ErrOpenTaskAcceptanceInProgress for a second, different-key attempt, got created=%v", created)
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrOpenTaskAcceptanceInProgress {
		t.Fatalf("expected ErrOpenTaskAcceptanceInProgress, got %v", err)
	}

	// The SAME idempotency key as the first attempt must still resolve via
	// the caller's own AcceptanceOperationByIdempotencyKey pre-check
	// (service.OpenTaskService.Accept's job, not this store method's) --
	// proven directly here since the store exposes that lookup.
	resumed, err := s.AcceptanceOperationByIdempotencyKey(ctx, first.PrincipalID, first.IdempotencyKey)
	if err != nil {
		t.Fatalf("AcceptanceOperationByIdempotencyKey: %v", err)
	}
	if resumed.ID != first.ID {
		t.Fatalf("resumed operation ID = %q, want %q", resumed.ID, first.ID)
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

// TestOpenAcceptanceOperation_ConcurrentWithCancelConverges is the real-
// Postgres regression test for the P0 finding that OpenAcceptanceOperation
// and UpdateOpenTask (Cancel's primitive) previously took DIFFERENT
// advisory locks on the same task row ("open-task-acceptance" vs
// "open-task"), so a concurrent Accept and Cancel could both observe a
// stale "open" snapshot and both commit -- an accepted AcceptanceOperation
// coexisting with a Cancelled task. Both now share the "open-task" lock and
// FOR UPDATE the same row, so exactly one of the two must win regardless of
// how many goroutines race.
func TestOpenAcceptanceOperation_ConcurrentWithCancelConverges(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	taskID := "task_pg_cancel_" + suffix
	now := time.Now().UTC()

	if err := s.PutOpenTask(ctx, domain.OpenTask{
		ID: taskID, PrincipalID: "principal_" + suffix, Title: "accept vs cancel race",
		Status: domain.OpenTaskOpen, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTask: %v", err)
	}
	propID := "prop_" + suffix
	if err := s.PutOpenTaskProposal(ctx, domain.OpenTaskProposal{
		ID: propID, TaskID: taskID, ProviderID: "provider_" + suffix,
		CapabilityID: "cap_" + suffix, CapabilityVersion: "v1",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTaskProposal: %v", err)
	}

	const attempts = 12
	var wg sync.WaitGroup
	type outcome struct {
		created bool
		err     error
	}
	results := make(chan outcome, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		if i%2 == 0 {
			go func(i int) {
				defer wg.Done()
				_, _, created, err := s.OpenAcceptanceOperation(ctx, taskID, propID, func(task domain.OpenTask, proposal domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
					if task.Status != domain.OpenTaskOpen {
						return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskNotOpen, "not open", false)
					}
					n := time.Now().UTC()
					return domain.AcceptanceOperation{
						ID: fmt.Sprintf("accop_cancel_%s_%d", suffix, i), TaskID: taskID, ProposalID: proposal.ID,
						PrincipalID: task.PrincipalID, ProviderID: proposal.ProviderID,
						CapabilityID: proposal.CapabilityID, CapabilityVersion: proposal.CapabilityVersion,
						Checkpoint: domain.AcceptanceWinnerClaimed, IdempotencyKey: fmt.Sprintf("idem_cancel_%s_%d", suffix, i),
						CreatedAt: n, UpdatedAt: n,
					}, nil
				})
				results <- outcome{created, err}
			}(i)
		} else {
			go func() {
				defer wg.Done()
				_, err := s.UpdateOpenTask(ctx, taskID, func(t domain.OpenTask, exists bool) (domain.OpenTask, error) {
					if !exists {
						return t, domain.NewError(domain.ErrNotFound, "not found", false)
					}
					if t.Status != domain.OpenTaskOpen {
						return t, domain.NewError(domain.ErrOpenTaskNotOpen, "not open", false)
					}
					t.Status = domain.OpenTaskCancelled
					t.UpdatedAt = time.Now().UTC()
					return t, nil
				})
				results <- outcome{false, err}
			}()
		}
	}
	wg.Wait()
	close(results)

	acceptedCount := 0
	for r := range results {
		if r.created {
			acceptedCount++
			continue
		}
		if r.err == nil {
			continue
		}
		derr, ok := r.err.(*domain.Error)
		if !ok {
			t.Fatalf("unexpected non-domain error: %v", r.err)
		}
		if derr.Code != domain.ErrOpenTaskNotOpen && derr.Code != domain.ErrOpenTaskAcceptanceInProgress {
			t.Fatalf("unexpected domain error code: %v", derr)
		}
	}
	if acceptedCount > 1 {
		t.Fatalf("more than one accept succeeded: %d", acceptedCount)
	}

	finalTask, err := s.GetOpenTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetOpenTask: %v", err)
	}
	nonTerminalOps, err := s.StaleAcceptanceOperations(ctx, now.Add(24*time.Hour), 100)
	if err != nil {
		t.Fatalf("StaleAcceptanceOperations: %v", err)
	}
	opsForTask := 0
	for _, op := range nonTerminalOps {
		if op.TaskID == taskID {
			opsForTask++
		}
	}
	// The core invariant: the final status must be self-consistent with
	// whether a winner was actually claimed -- never "cancelled" while an
	// AcceptanceOperation exists for it (the exact P0 bug this test
	// targets), and never "accepted" with zero operations.
	switch finalTask.Status {
	case domain.OpenTaskAccepted:
		if opsForTask != 1 || acceptedCount != 1 {
			t.Fatalf("task accepted but opsForTask=%d acceptedCount=%d, want 1/1", opsForTask, acceptedCount)
		}
	case domain.OpenTaskCancelled:
		if opsForTask != 0 || acceptedCount != 0 {
			t.Fatalf("task cancelled but opsForTask=%d acceptedCount=%d -- Accept and Cancel both won (the P0 bug)", opsForTask, acceptedCount)
		}
	default:
		t.Fatalf("unexpected final task status: %s", finalTask.Status)
	}
}

// TestWithdrawOpenTaskProposal_ConcurrentWithAcceptConverges is the real-
// Postgres regression test for the P1 finding that Withdraw and Accept
// raced against the SAME proposal row without a shared lock, so a proposal
// could end up simultaneously withdrawn and claimed as the task's accepted
// winner. WithdrawOpenTaskProposal and OpenAcceptanceOperation now both
// lock "open-task-proposal"+proposalID and FOR UPDATE the same row, so
// exactly one of the two must be authoritative.
func TestWithdrawOpenTaskProposal_ConcurrentWithAcceptConverges(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	taskID := "task_pg_wd_" + suffix
	now := time.Now().UTC()

	if err := s.PutOpenTask(ctx, domain.OpenTask{
		ID: taskID, PrincipalID: "principal_" + suffix, Title: "accept vs withdraw race",
		Status: domain.OpenTaskOpen, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTask: %v", err)
	}
	propID := "prop_" + suffix
	providerID := "provider_" + suffix
	if err := s.PutOpenTaskProposal(ctx, domain.OpenTaskProposal{
		ID: propID, TaskID: taskID, ProviderID: providerID,
		CapabilityID: "cap_" + suffix, CapabilityVersion: "v1",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTaskProposal: %v", err)
	}

	const attempts = 12
	var wg sync.WaitGroup
	type outcome struct {
		created bool
		err     error
	}
	results := make(chan outcome, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		if i%2 == 0 {
			go func(i int) {
				defer wg.Done()
				_, _, created, err := s.OpenAcceptanceOperation(ctx, taskID, propID, func(task domain.OpenTask, proposal domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
					if task.Status != domain.OpenTaskOpen {
						return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskNotOpen, "not open", false)
					}
					if proposal.WithdrawnAt != nil {
						return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskProposalWithdrawn, "withdrawn", false)
					}
					n := time.Now().UTC()
					return domain.AcceptanceOperation{
						ID: fmt.Sprintf("accop_wd_%s_%d", suffix, i), TaskID: taskID, ProposalID: proposal.ID,
						PrincipalID: task.PrincipalID, ProviderID: proposal.ProviderID,
						CapabilityID: proposal.CapabilityID, CapabilityVersion: proposal.CapabilityVersion,
						Checkpoint: domain.AcceptanceWinnerClaimed, IdempotencyKey: fmt.Sprintf("idem_wd_%s_%d", suffix, i),
						CreatedAt: n, UpdatedAt: n,
					}, nil
				})
				results <- outcome{created, err}
			}(i)
		} else {
			go func() {
				defer wg.Done()
				_, err := s.WithdrawOpenTaskProposal(ctx, propID, providerID)
				results <- outcome{false, err}
			}()
		}
	}
	wg.Wait()
	close(results)

	// Every error observed during the race must be one of the expected,
	// classified domain errors -- never a raw/unclassified error. This is
	// the assertion that actually catches the lock-ordering deadlock this
	// test targets: two transactions taking the "open-task" and
	// "open-task-proposal" advisory locks in opposite orders can deadlock,
	// and PostgreSQL surfaces that as a raw driver error (SQLSTATE
	// 40P01), not a *domain.Error -- a prior version of this test
	// discarded every error and would have silently passed through a
	// deadlock as just another "lost the race" outcome.
	acceptedCount := 0
	for r := range results {
		if r.created {
			acceptedCount++
			continue
		}
		if r.err == nil {
			continue // Withdraw succeeding (or a no-op re-withdraw) is a valid outcome.
		}
		derr, ok := r.err.(*domain.Error)
		if !ok {
			t.Fatalf("unexpected non-domain error (possible lock-ordering deadlock): %v", r.err)
		}
		switch derr.Code {
		case domain.ErrOpenTaskNotOpen, domain.ErrOpenTaskProposalWithdrawn, domain.ErrOpenTaskAcceptanceInProgress:
			// expected: the losing side of the race.
		default:
			t.Fatalf("unexpected domain error code: %v", derr)
		}
	}
	if acceptedCount > 1 {
		t.Fatalf("more than one accept succeeded: %d", acceptedCount)
	}

	finalProposal, err := s.GetOpenTaskProposal(ctx, propID)
	if err != nil {
		t.Fatalf("GetOpenTaskProposal: %v", err)
	}
	finalTask, err := s.GetOpenTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetOpenTask: %v", err)
	}

	// The core invariant this test targets: a proposal must never end up
	// BOTH withdrawn AND claimed as the task's accepted winner.
	if finalProposal.WithdrawnAt != nil && finalTask.AcceptedProposalID == propID {
		t.Fatalf("proposal is both withdrawn and accepted as winner -- the exact race this test targets: proposal=%+v task=%+v", finalProposal, finalTask)
	}
	if acceptedCount == 1 && finalTask.AcceptedProposalID != propID {
		t.Fatal("an accept succeeded but the task does not show it as the accepted proposal")
	}
}

// TestUpdateAcceptanceOperation_TerminalIsImmutable is the Postgres
// counterpart to the identically-named memory-store test -- see its doc
// comment for the full rationale. Proves the corrected semantics: a stale
// worker's CAS no-op against an already-terminal operation succeeds (not
// ErrIdempotencyConflict), an attempted revival back to non-terminal is
// silently ignored, and a concurrent stale driver's advance (expectedFrom
// no longer matching, because a different driver already completed the
// operation) converges without error.
func TestUpdateAcceptanceOperation_TerminalIsImmutable(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	taskID := "task_pg_terminal_immutable_" + suffix
	propID := "prop_pg_terminal_immutable_" + suffix
	now := time.Now().UTC()

	if err := s.PutOpenTask(ctx, domain.OpenTask{
		ID: taskID, PrincipalID: "principal_" + suffix, Title: "terminal immutability test",
		Status: domain.OpenTaskOpen, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTask: %v", err)
	}
	if err := s.PutOpenTaskProposal(ctx, domain.OpenTaskProposal{
		ID: propID, TaskID: taskID, ProviderID: "provider_" + suffix,
		CapabilityID: "cap_" + suffix, CapabilityVersion: "v1",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("PutOpenTaskProposal: %v", err)
	}

	op, _, created, err := s.OpenAcceptanceOperation(ctx, taskID, propID, func(task domain.OpenTask, proposal domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
		n := time.Now().UTC()
		return domain.AcceptanceOperation{
			ID: "accop_terminal_immutable_" + suffix, TaskID: taskID, ProposalID: propID,
			PrincipalID: task.PrincipalID, ProviderID: proposal.ProviderID,
			CapabilityID: proposal.CapabilityID, CapabilityVersion: proposal.CapabilityVersion,
			Checkpoint: domain.AcceptanceJobBound, QuoteID: "q_" + suffix, JobID: "job_" + suffix,
			IdempotencyKey: "idem_terminal_immutable_" + suffix, CreatedAt: n, UpdatedAt: n,
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

	noop, err := s.UpdateAcceptanceOperation(ctx, op.ID, func(current domain.AcceptanceOperation, exists bool) (domain.AcceptanceOperation, error) {
		return current, nil
	})
	if err != nil {
		t.Fatalf("expected a no-op update against a terminal operation to succeed, got: %v", err)
	}
	if noop.Checkpoint != domain.AcceptanceCompleted {
		t.Fatalf("no-op update changed checkpoint to %s", noop.Checkpoint)
	}

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
