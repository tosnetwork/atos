package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// setupOpenTaskForRecovery publishes a task and one proposal, returning
// enough to manually drive a crash-recovery scenario against the real
// store -- shared setup for the tests below.
func setupOpenTaskForRecovery(t *testing.T, h harness, openTasks *service.OpenTaskService, suffix string) (domain.OpenTask, domain.OpenTaskProposal) {
	t.Helper()
	ctx := context.Background()
	cap := registerCapability(t, h, "agt_provider_"+suffix, "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_owner_" + suffix, Title: "task", Input: map[string]any{},
		ExpiresAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "publish-" + suffix,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_provider_" + suffix, TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-" + suffix,
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	return task, p
}

// TestOpenTaskAcceptRecoversQuoteSucceededButCheckpointLost is the crash-
// recovery test the Phase 3C spec explicitly requires: the underlying Quote
// call genuinely succeeds (a real Quote is committed via QuoteService), but
// the local bookkeeping that would have advanced the AcceptanceOperation's
// checkpoint to quote_bound never happens -- a lost response, not "the
// request never arrived". A retry (Accept called again with the same
// idempotency key) must resume and bind the SAME Quote, never mint a
// second one.
func TestOpenTaskAcceptRecoversQuoteSucceededButCheckpointLost(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)
	task, p := setupOpenTaskForRecovery(t, h, openTasks, "lostquote")

	idemKey := "accept-lostquote"
	now := time.Now().UTC()
	seed := domain.AcceptanceOperation{
		ID: "accop_lostquote", TaskID: task.ID, ProposalID: p.ID,
		PrincipalID: task.PrincipalID, ProviderID: p.ProviderID,
		CapabilityID: p.CapabilityID, CapabilityVersion: p.CapabilityVersion,
		Checkpoint: domain.AcceptanceQuoteBindingPending, IdempotencyKey: idemKey,
		CreatedAt: now, UpdatedAt: now,
	}
	opened, _, created, err := h.store().OpenAcceptanceOperation(ctx, task.ID, p.ID, func(domain.OpenTask, domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
		return seed, nil
	})
	if err != nil {
		t.Fatalf("OpenAcceptanceOperation: %v", err)
	}
	if !created {
		t.Fatal("expected created=true for a fresh operation")
	}

	// The REAL underlying call: QuoteService.Create genuinely commits a
	// Quote under the exact idempotency key driveAcceptance would use --
	// including ExpectedCapabilityVersion, since driveAcceptance's own
	// resumed call passes op.CapabilityVersion and the idempotency digest
	// must match for this to be a genuine same-request replay rather than
	// a spurious conflict.
	realQuote, err := h.quotes.Create(ctx, service.CreateQuoteInput{
		PrincipalID: task.PrincipalID, CapabilityID: p.CapabilityID, InputSummary: task.Input,
		IdempotencyKey: opened.NewQuoteIdempotencyKey(), ExpectedCapabilityVersion: p.CapabilityVersion,
	})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	// Deliberately do NOT advance the operation's checkpoint/QuoteID here --
	// this is the lost response: the remote effect (the Quote row) is
	// durable, but the caller never recorded having seen it succeed.

	finalTask, finalOp, err := openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: task.PrincipalID, TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: idemKey,
	})
	if err != nil {
		t.Fatalf("resumed Accept: %v", err)
	}
	if finalOp.Checkpoint != domain.AcceptanceCompleted {
		t.Fatalf("checkpoint = %s, want completed", finalOp.Checkpoint)
	}
	if finalOp.QuoteID != realQuote.ID {
		t.Fatalf("resumed operation bound quote %q, want the pre-existing %q", finalOp.QuoteID, realQuote.ID)
	}
	if finalTask.BoundQuoteID != realQuote.ID {
		t.Fatalf("task bound quote = %q, want %q", finalTask.BoundQuoteID, realQuote.ID)
	}

	// No duplicate Quote was minted for this idempotency key.
	again, err := h.store().QuoteByIdempotencyKey(ctx, task.PrincipalID, opened.NewQuoteIdempotencyKey())
	if err != nil {
		t.Fatalf("QuoteByIdempotencyKey: %v", err)
	}
	if again.ID != realQuote.ID {
		t.Fatalf("QuoteByIdempotencyKey returned a different quote: %q vs %q", again.ID, realQuote.ID)
	}
}

// TestOpenTaskAcceptRecoversJobSucceededButCheckpointLost is the Job-step
// counterpart: the Quote is already bound, and the underlying Job call
// genuinely succeeds, but the checkpoint advance to job_bound is lost. A
// retry must resume and bind the SAME Job.
func TestOpenTaskAcceptRecoversJobSucceededButCheckpointLost(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)
	task, p := setupOpenTaskForRecovery(t, h, openTasks, "lostjob")

	idemKey := "accept-lostjob"
	now := time.Now().UTC()
	seed := domain.AcceptanceOperation{
		ID: "accop_lostjob", TaskID: task.ID, ProposalID: p.ID,
		PrincipalID: task.PrincipalID, ProviderID: p.ProviderID,
		CapabilityID: p.CapabilityID, CapabilityVersion: p.CapabilityVersion,
		Checkpoint: domain.AcceptanceQuoteBindingPending, IdempotencyKey: idemKey,
		CreatedAt: now, UpdatedAt: now,
	}
	opened, _, created, err := h.store().OpenAcceptanceOperation(ctx, task.ID, p.ID, func(domain.OpenTask, domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
		return seed, nil
	})
	if err != nil {
		t.Fatalf("OpenAcceptanceOperation: %v", err)
	}
	if !created {
		t.Fatal("expected created=true")
	}

	realQuote, err := h.quotes.Create(ctx, service.CreateQuoteInput{
		PrincipalID: task.PrincipalID, CapabilityID: p.CapabilityID, InputSummary: task.Input,
		IdempotencyKey: opened.NewQuoteIdempotencyKey(),
	})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	// Durably advance to job_binding_pending with the real quote recorded --
	// this part of the sequence DID persist correctly.
	if _, err := h.store().UpdateAcceptanceOperation(ctx, opened.ID, func(op domain.AcceptanceOperation, exists bool) (domain.AcceptanceOperation, error) {
		op.Checkpoint = domain.AcceptanceJobBindingPending
		op.QuoteID = realQuote.ID
		op.UpdatedAt = time.Now().UTC()
		return op, nil
	}); err != nil {
		t.Fatalf("UpdateAcceptanceOperation: %v", err)
	}

	// The REAL underlying call: JobService.CreateJob genuinely commits a Job.
	result, err := h.jobs.CreateJob(ctx, service.SubmitInput{
		PrincipalID: task.PrincipalID, CapabilityID: p.CapabilityID, QuoteID: realQuote.ID,
		Input: task.Input, IdempotencyKey: opened.NewJobIdempotencyKey(),
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	realJobID := result.Job.ID
	// Deliberately do NOT advance the operation to job_bound -- the lost
	// response: the Job row is durable, the operation's bookkeeping is not.

	finalTask, finalOp, err := openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: task.PrincipalID, TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: idemKey,
	})
	if err != nil {
		t.Fatalf("resumed Accept: %v", err)
	}
	if finalOp.Checkpoint != domain.AcceptanceCompleted {
		t.Fatalf("checkpoint = %s, want completed", finalOp.Checkpoint)
	}
	if finalOp.JobID != realJobID {
		t.Fatalf("resumed operation bound job %q, want the pre-existing %q", finalOp.JobID, realJobID)
	}
	if finalTask.BoundJobID != realJobID {
		t.Fatalf("task bound job = %q, want %q", finalTask.BoundJobID, realJobID)
	}

	again, err := h.store().JobByIdempotencyKey(ctx, task.PrincipalID, opened.NewJobIdempotencyKey())
	if err != nil {
		t.Fatalf("JobByIdempotencyKey: %v", err)
	}
	if again.ID != realJobID {
		t.Fatalf("JobByIdempotencyKey returned a different job: %q vs %q", again.ID, realJobID)
	}
}

// TestOpenTaskReconcilerResumesStuckOperation proves the reconciler sweep
// (RunReconciler/ReconcileStaleOperations) converges an abandoned
// Reconciling operation to Completed on its own, without any client ever
// retrying -- the "service restarts and continues" / "caller never
// retried" case.
func TestOpenTaskReconcilerResumesStuckOperation(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)
	task, p := setupOpenTaskForRecovery(t, h, openTasks, "reconcile")

	idemKey := "accept-reconcile"
	now := time.Now().UTC()
	seed := domain.AcceptanceOperation{
		ID: "accop_reconcile", TaskID: task.ID, ProposalID: p.ID,
		PrincipalID: task.PrincipalID, ProviderID: p.ProviderID,
		CapabilityID: p.CapabilityID, CapabilityVersion: p.CapabilityVersion,
		Checkpoint: domain.AcceptanceQuoteBindingPending, IdempotencyKey: idemKey,
		CreatedAt: now, UpdatedAt: now,
	}
	opened, _, created, err := h.store().OpenAcceptanceOperation(ctx, task.ID, p.ID, func(domain.OpenTask, domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
		return seed, nil
	})
	if err != nil {
		t.Fatalf("OpenAcceptanceOperation: %v", err)
	}
	if !created {
		t.Fatal("expected created=true")
	}
	realQuote, err := h.quotes.Create(ctx, service.CreateQuoteInput{
		PrincipalID: task.PrincipalID, CapabilityID: p.CapabilityID, InputSummary: task.Input,
		IdempotencyKey: opened.NewQuoteIdempotencyKey(),
	})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	// Mark Reconciling (ambiguous outcome) with the Quote already recorded,
	// last updated an hour ago -- stale enough for the sweep to pick up.
	if _, err := h.store().UpdateAcceptanceOperation(ctx, opened.ID, func(op domain.AcceptanceOperation, exists bool) (domain.AcceptanceOperation, error) {
		op.Checkpoint = domain.AcceptanceReconciling
		op.QuoteID = realQuote.ID
		op.UpdatedAt = time.Now().UTC().Add(-time.Hour)
		return op, nil
	}); err != nil {
		t.Fatalf("UpdateAcceptanceOperation: %v", err)
	}

	if err := openTasks.ReconcileStaleOperations(ctx, time.Now().UTC(), 100); err != nil {
		t.Fatalf("ReconcileStaleOperations: %v", err)
	}

	recovered, err := h.store().GetAcceptanceOperation(ctx, opened.ID)
	if err != nil {
		t.Fatalf("GetAcceptanceOperation: %v", err)
	}
	if recovered.Checkpoint != domain.AcceptanceCompleted {
		t.Fatalf("checkpoint after reconcile = %s, want completed", recovered.Checkpoint)
	}
	if recovered.QuoteID != realQuote.ID {
		t.Fatalf("reconciled operation bound a different quote: %q vs %q", recovered.QuoteID, realQuote.ID)
	}
	if recovered.JobID == "" {
		t.Fatal("expected a job to be bound after reconciliation")
	}

	finalTask, err := h.store().GetOpenTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetOpenTask: %v", err)
	}
	if finalTask.Status != domain.OpenTaskFulfilled {
		t.Fatalf("task status after reconcile = %s, want fulfilled", finalTask.Status)
	}

	// Running the sweep again over an already-converged operation must be
	// harmless (no error, no state change).
	if err := openTasks.ReconcileStaleOperations(ctx, time.Now().UTC(), 100); err != nil {
		t.Fatalf("second ReconcileStaleOperations: %v", err)
	}
}

// TestOpenTaskAcceptDefinitiveFailureReopensTask proves a definitive
// (non-retryable) failure encountered AFTER the winner has already been
// durably claimed -- here, the capability is paused in the window between
// the winner claim and the actual Quote call -- marks the operation Failed
// and reopens the task (AcceptedProposalID cleared, Status back to Open)
// rather than stranding it Accepted forever. Accept's own pre-checks
// happen before any operation is opened (see Accept's doc comment), so this
// test opens the operation directly through the store to land past that
// point before pausing the capability, exactly reproducing the narrow
// window Accept's own TOCTOU-accepting design leaves open.
func TestOpenTaskAcceptDefinitiveFailureReopensTask(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)
	task, p := setupOpenTaskForRecovery(t, h, openTasks, "reopen")

	idemKey := "accept-reopen"
	now := time.Now().UTC()
	seed := domain.AcceptanceOperation{
		ID: "accop_reopen", TaskID: task.ID, ProposalID: p.ID,
		PrincipalID: task.PrincipalID, ProviderID: p.ProviderID,
		CapabilityID: p.CapabilityID, CapabilityVersion: p.CapabilityVersion,
		Checkpoint: domain.AcceptanceQuoteBindingPending, IdempotencyKey: idemKey,
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, created, err := h.store().OpenAcceptanceOperation(ctx, task.ID, p.ID, func(domain.OpenTask, domain.OpenTaskProposal) (domain.AcceptanceOperation, error) {
		return seed, nil
	}); err != nil || !created {
		t.Fatalf("OpenAcceptanceOperation: created=%v err=%v", created, err)
	}
	claimed, err := h.store().GetOpenTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetOpenTask: %v", err)
	}
	if claimed.Status != domain.OpenTaskAccepted {
		t.Fatalf("task status = %s, want accepted", claimed.Status)
	}

	if _, err := h.capabilities.Update(ctx, p.CapabilityID, p.ProviderID, map[string]any{"status": "paused"}, "pause-reopen"); err != nil {
		t.Fatalf("pause capability: %v", err)
	}

	// The resumeOrConflict lookup finds the manually-opened operation and
	// drives it forward directly -- QuoteService.Create now genuinely fails
	// (capability paused), a definitive, non-retryable rejection.
	_, _, err = openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: task.PrincipalID, TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: idemKey,
	})
	if err == nil {
		t.Fatal("expected accept against a paused capability to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrCapabilityUnavailable {
		t.Fatalf("expected ErrCapabilityUnavailable, got %v", err)
	}

	failedOp, err := h.store().GetAcceptanceOperation(ctx, seed.ID)
	if err != nil {
		t.Fatalf("GetAcceptanceOperation: %v", err)
	}
	if failedOp.Checkpoint != domain.AcceptanceFailed {
		t.Fatalf("checkpoint = %s, want failed", failedOp.Checkpoint)
	}

	reopened, err := h.store().GetOpenTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetOpenTask: %v", err)
	}
	if reopened.Status != domain.OpenTaskOpen {
		t.Fatalf("task status = %s, want open (reopened after definitive failure)", reopened.Status)
	}
	if reopened.AcceptedProposalID != "" {
		t.Fatalf("expected AcceptedProposalID cleared on reopen, got %q", reopened.AcceptedProposalID)
	}
}
