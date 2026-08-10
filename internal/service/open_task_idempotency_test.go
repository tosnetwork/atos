package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// TestOpenTaskPublishIdempotentReplayReturnsSameTask proves the exact
// replay contract: same key + same content -> the original task, never a
// second one.
func TestOpenTaskPublishIdempotentReplayReturnsSameTask(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	in := service.PublishOpenTaskInput{
		PrincipalID: "prn_idem", Title: "task", Input: map[string]any{"a": 1},
		ExpiresAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "publish-idem-1",
	}
	first, err := openTasks.Publish(ctx, in)
	if err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	second, err := openTasks.Publish(ctx, in)
	if err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay minted a different task: %q vs %q", second.ID, first.ID)
	}
}

// TestOpenTaskPublishSameKeyDifferentContentConflicts proves the digest
// excludes nothing that should matter (title/description/input/trust mode/
// proof requirements/max_total/expires_at) and a genuine content change
// under a reused key is rejected, not silently resumed.
func TestOpenTaskPublishSameKeyDifferentContentConflicts(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	base := service.PublishOpenTaskInput{
		PrincipalID: "prn_idem2", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-idem-2",
	}
	if _, err := openTasks.Publish(ctx, base); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	changed := base
	changed.Title = "a different task"
	_, err := openTasks.Publish(ctx, changed)
	if err == nil {
		t.Fatal("expected a conflict for a reused key with different content")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}

// TestOpenTaskProposeIdempotentReplayReturnsSameProposal mirrors the
// Publish idempotency test for Propose.
func TestOpenTaskProposeIdempotentReplayReturnsSameProposal(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)
	cap := registerCapability(t, h, "agt_idem_propose", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_idem3", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-idem-3",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	in := service.ProposeInput{
		ProviderID: "agt_idem_propose", TaskID: task.ID, CapabilityID: cap.ID, Message: "hi",
		IdempotencyKey: "propose-idem-3",
	}
	first, err := openTasks.Propose(ctx, in)
	if err != nil {
		t.Fatalf("first Propose: %v", err)
	}
	second, err := openTasks.Propose(ctx, in)
	if err != nil {
		t.Fatalf("second Propose: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay minted a different proposal: %q vs %q", second.ID, first.ID)
	}

	changed := in
	changed.Message = "a different message"
	_, err = openTasks.Propose(ctx, changed)
	if err == nil {
		t.Fatal("expected a conflict for a reused proposal key with different content")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}

// TestOpenTaskAcceptIdempotentReplayReturnsSameBinding proves a full,
// already-completed Accept replayed with the same (task_id, proposal_id,
// idempotency_key) returns the exact same Quote/Job binding, and that the
// SAME key with a DIFFERENT proposal_id or task_id is rejected as a
// conflict rather than silently accepted against a different target.
func TestOpenTaskAcceptIdempotentReplayReturnsSameBinding(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_idem_accept", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_idem4", Title: "task", Input: map[string]any{},
		ExpiresAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "publish-idem-4",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_idem_accept", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-idem-4",
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	// The second proposal must be submitted before the first is accepted --
	// once Accept succeeds the task leaves Open and a later Propose would
	// correctly be refused for an unrelated reason (ErrOpenTaskNotOpen),
	// which would defeat this test's purpose.
	otherCap := registerCapability(t, h, "agt_idem_accept_2", "2.00")
	otherProposal, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_idem_accept_2", TaskID: task.ID, CapabilityID: otherCap.ID, IdempotencyKey: "propose-idem-4b",
	})
	if err != nil {
		t.Fatalf("Propose other: %v", err)
	}

	in := service.AcceptProposalInput{
		PrincipalID: "prn_idem4", TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: "accept-idem-4",
	}
	firstTask, firstOp, err := openTasks.Accept(ctx, in)
	if err != nil {
		t.Fatalf("first Accept: %v", err)
	}
	secondTask, secondOp, err := openTasks.Accept(ctx, in)
	if err != nil {
		t.Fatalf("second Accept: %v", err)
	}
	if secondOp.ID != firstOp.ID || secondOp.QuoteID != firstOp.QuoteID || secondOp.JobID != firstOp.JobID {
		t.Fatalf("replay produced different bindings: %+v vs %+v", secondOp, firstOp)
	}
	if secondTask.BoundJobID != firstTask.BoundJobID {
		t.Fatalf("replay bound a different job: %q vs %q", secondTask.BoundJobID, firstTask.BoundJobID)
	}

	// Same key, different proposal_id -- must conflict, not silently accept
	// a second proposal for the same logical idempotency identity.
	_, _, err = openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_idem4", TaskID: task.ID, ProposalID: otherProposal.ID, IdempotencyKey: "accept-idem-4",
	})
	if err == nil {
		t.Fatal("expected a conflict for the same idempotency_key against a different proposal_id")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}
