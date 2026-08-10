package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// TestOpenTaskLifecycleHappyPath is Phase 3C's end-to-end smoke test:
// publish -> propose -> accept -> Quote/Job bound -> task Fulfilled, reusing
// the exact same harness/registerCapability helpers job_test.go already
// uses so this exercises the real quote -> job pipeline, not a stub.
func TestOpenTaskLifecycleHappyPath(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_provider", "1.00")

	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_owner", Title: "summarize this doc",
		Input:          map[string]any{"doc": "hello world"},
		ExpiresAt:      time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-1",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if task.Status != domain.OpenTaskOpen {
		t.Fatalf("status = %s, want open", task.Status)
	}

	proposal, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_provider", TaskID: task.ID, CapabilityID: cap.ID,
		Message: "I can do this", IdempotencyKey: "propose-1",
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if proposal.CapabilityVersion != cap.Version {
		t.Fatalf("proposal capability_version = %q, want %q", proposal.CapabilityVersion, cap.Version)
	}

	updatedTask, op, err := openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_owner", TaskID: task.ID, ProposalID: proposal.ID,
		IdempotencyKey: "accept-1",
	})
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if op.Checkpoint != domain.AcceptanceCompleted {
		t.Fatalf("checkpoint = %s, want completed", op.Checkpoint)
	}
	if updatedTask.Status != domain.OpenTaskFulfilled {
		t.Fatalf("status = %s, want fulfilled", updatedTask.Status)
	}
	if updatedTask.BoundQuoteID == "" || updatedTask.BoundJobID == "" {
		t.Fatalf("expected bound quote/job ids, got %+v", updatedTask)
	}
	if updatedTask.AcceptedProposalID != proposal.ID {
		t.Fatalf("accepted_proposal_id = %q, want %q", updatedTask.AcceptedProposalID, proposal.ID)
	}

	quote, err := h.quotes.Get(ctx, updatedTask.BoundQuoteID)
	if err != nil {
		t.Fatalf("Get bound quote: %v", err)
	}
	if quote.PrincipalID != "prn_owner" || quote.CapabilityID != cap.ID {
		t.Fatalf("bound quote does not match expected principal/capability: %+v", quote)
	}

	job, err := h.jobs.Get(ctx, updatedTask.BoundJobID)
	if err != nil {
		t.Fatalf("Get bound job: %v", err)
	}
	if job.PrincipalID != "prn_owner" || job.QuoteID != quote.ID {
		t.Fatalf("bound job does not match expected principal/quote: %+v", job)
	}

	// Idempotent replay of the exact same Accept request must return the
	// same result without creating a second Quote/Job.
	replayTask, replayOp, err := openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_owner", TaskID: task.ID, ProposalID: proposal.ID,
		IdempotencyKey: "accept-1",
	})
	if err != nil {
		t.Fatalf("replay Accept: %v", err)
	}
	if replayOp.ID != op.ID || replayOp.QuoteID != op.QuoteID || replayOp.JobID != op.JobID {
		t.Fatalf("replay produced a different operation/quote/job: %+v vs %+v", replayOp, op)
	}
	if replayTask.BoundJobID != updatedTask.BoundJobID {
		t.Fatalf("replay bound a different job: %q vs %q", replayTask.BoundJobID, updatedTask.BoundJobID)
	}
}

// TestOpenTaskAcceptRejectsSecondProposalOnceWon proves a losing proposal
// can never later create a Job -- rule (11) from the Phase 3C spec.
func TestOpenTaskAcceptRejectsSecondProposalOnceWon(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_provider", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_owner", Title: "task", Input: map[string]any{}, ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-2",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p1, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_provider", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-a",
	})
	if err != nil {
		t.Fatalf("Propose 1: %v", err)
	}
	cap2 := registerCapability(t, h, "agt_provider_2", "2.00")
	p2, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_provider_2", TaskID: task.ID, CapabilityID: cap2.ID, IdempotencyKey: "propose-b",
	})
	if err != nil {
		t.Fatalf("Propose 2: %v", err)
	}

	if _, _, err := openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_owner", TaskID: task.ID, ProposalID: p1.ID, IdempotencyKey: "accept-a",
	}); err != nil {
		t.Fatalf("Accept p1: %v", err)
	}

	_, _, err = openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_owner", TaskID: task.ID, ProposalID: p2.ID, IdempotencyKey: "accept-b",
	})
	if err == nil {
		t.Fatal("expected accepting a second proposal to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrOpenTaskNotOpen {
		t.Fatalf("expected ErrOpenTaskNotOpen, got %v", err)
	}
}

// TestOpenTaskCancelRefusedAfterAccept proves rule (12): an accepted winner
// is never silently discarded by a racing/late cancel.
func TestOpenTaskCancelRefusedAfterAccept(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_provider", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_owner", Title: "task", Input: map[string]any{}, ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-3",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_provider", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-c",
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, _, err := openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_owner", TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: "accept-c",
	}); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	_, err = openTasks.Cancel(ctx, service.CancelOpenTaskInput{PrincipalID: "prn_owner", TaskID: task.ID})
	if err == nil {
		t.Fatal("expected cancel to be refused once accepted")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrOpenTaskNotOpen {
		t.Fatalf("expected ErrOpenTaskNotOpen, got %v", err)
	}
}

// TestOpenTaskCancelThenProposeRejected proves a cancelled task refuses new
// proposals.
func TestOpenTaskCancelThenProposeRejected(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_provider", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_owner", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-4",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := openTasks.Cancel(ctx, service.CancelOpenTaskInput{PrincipalID: "prn_owner", TaskID: task.ID}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	_, err = openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_provider", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-d",
	})
	if err == nil {
		t.Fatal("expected propose against a cancelled task to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrOpenTaskNotOpen {
		t.Fatalf("expected ErrOpenTaskNotOpen, got %v", err)
	}
}

// TestOpenTaskProposeRejectsNonOwningProvider proves ProviderID is sourced
// only from the authenticated caller, never trusted from the request.
func TestOpenTaskProposeRejectsNonOwningProvider(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_provider", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_owner", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-5",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_, err = openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_impostor", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-e",
	})
	if err == nil {
		t.Fatal("expected propose from a non-owning provider to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrPermissionDenied {
		t.Fatalf("expected ErrPermissionDenied, got %v", err)
	}
}

// TestOpenTaskAcceptRejectsNonOwner proves only the task owner may accept.
func TestOpenTaskAcceptRejectsNonOwner(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_provider", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_owner", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-6",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_provider", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-f",
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	_, _, err = openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_impostor", TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: "accept-f",
	})
	if err == nil {
		t.Fatal("expected accept from a non-owner to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrPermissionDenied {
		t.Fatalf("expected ErrPermissionDenied, got %v", err)
	}
}

// TestOpenTaskAcceptRejectsStaleCapabilityVersion proves a proposal whose
// bound Capability version has since changed is refused, never silently
// rebound.
func TestOpenTaskAcceptRejectsStaleCapabilityVersion(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_provider", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_owner", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-7",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_provider", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-g",
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	if _, err := h.capabilities.Update(ctx, cap.ID, "agt_provider", map[string]any{
		"pricing": map[string]any{"model": "fixed", "price_hint": map[string]any{"amount": "2.00", "currency": "USD"}},
	}, "update-stale-test"); err != nil {
		t.Fatalf("Update capability: %v", err)
	}

	_, _, err = openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_owner", TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: "accept-g",
	})
	if err == nil {
		t.Fatal("expected accept against a stale capability version to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrOpenTaskProposalStale {
		t.Fatalf("expected ErrOpenTaskProposalStale, got %v", err)
	}
}
