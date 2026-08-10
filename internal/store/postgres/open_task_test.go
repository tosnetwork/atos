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
	results := make(chan bool, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		if i%2 == 0 {
			go func(i int) {
				defer wg.Done()
				_, _, created, _ := s.OpenAcceptanceOperation(ctx, taskID, propID, func(task domain.OpenTask, proposal domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
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
				results <- created
			}(i)
		} else {
			go func() {
				defer wg.Done()
				_, _ = s.UpdateOpenTask(ctx, taskID, func(t domain.OpenTask, exists bool) (domain.OpenTask, error) {
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
				results <- false
			}()
		}
	}
	wg.Wait()
	close(results)

	acceptedCount := 0
	for created := range results {
		if created {
			acceptedCount++
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
	results := make(chan bool, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		if i%2 == 0 {
			go func(i int) {
				defer wg.Done()
				_, _, created, _ := s.OpenAcceptanceOperation(ctx, taskID, propID, func(task domain.OpenTask, proposal domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
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
				results <- created
			}(i)
		} else {
			go func() {
				defer wg.Done()
				_, _ = s.WithdrawOpenTaskProposal(ctx, propID, providerID)
				results <- false
			}()
		}
	}
	wg.Wait()
	close(results)

	acceptedCount := 0
	for created := range results {
		if created {
			acceptedCount++
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
