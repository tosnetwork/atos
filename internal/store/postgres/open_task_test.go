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
			op, _, created, err := s.OpenAcceptanceOperation(ctx, taskID, func(task domain.OpenTask) (domain.AcceptanceOperation, error) {
				if task.Status != domain.OpenTaskOpen {
					return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskNotOpen, "task is not open", false)
				}
				n := time.Now().UTC()
				return domain.AcceptanceOperation{
					ID: fmt.Sprintf("accop_%s_%d", suffix, i), TaskID: taskID, ProposalID: propID,
					PrincipalID: task.PrincipalID, ProviderID: "irrelevant-for-this-test",
					CapabilityID: "cap_" + suffix, CapabilityVersion: "v1",
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

	build := func(idemKey string) func(domain.OpenTask) (domain.AcceptanceOperation, error) {
		return func(task domain.OpenTask) (domain.AcceptanceOperation, error) {
			if task.Status != domain.OpenTaskOpen {
				return domain.AcceptanceOperation{}, domain.NewError(domain.ErrOpenTaskNotOpen, "task is not open", false)
			}
			n := time.Now().UTC()
			return domain.AcceptanceOperation{
				ID: "accop_seq_" + idemKey, TaskID: taskID, ProposalID: propID,
				PrincipalID: task.PrincipalID, ProviderID: "irrelevant-for-this-test",
				CapabilityID: "cap_" + suffix, CapabilityVersion: "v1",
				Checkpoint: domain.AcceptanceIntentPersisted, IdempotencyKey: idemKey,
				CreatedAt: n, UpdatedAt: n,
			}, nil
		}
	}

	first, _, created, err := s.OpenAcceptanceOperation(ctx, taskID, build("idem_first_"+suffix))
	if err != nil {
		t.Fatalf("first OpenAcceptanceOperation: %v", err)
	}
	if !created {
		t.Fatal("expected created=true for the first attempt")
	}

	_, _, created, err = s.OpenAcceptanceOperation(ctx, taskID, build("idem_second_"+suffix))
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
