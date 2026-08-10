package service_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

// TestOpenTaskAcceptRejectsCapabilityVersionDriftDuringResume is the
// regression test for the P0 finding that a resumed/reconciled Quote
// creation call never passed or checked the Capability version the
// AcceptanceOperation had frozen -- a version bump landing between
// winner-claim and the actual Quote call could silently price and bind a
// DIFFERENT version than the one the proposal/operation both still claim.
// QuoteService.Create's CreateQuoteInput.ExpectedCapabilityVersion now
// closes this: the operation is manually opened at quote_binding_pending
// (as if a first attempt already claimed the winner), the capability is
// then bumped to a new version, and the resumed Accept call must refuse
// (definitive, non-retryable) rather than silently bind the new version.
func TestOpenTaskAcceptRejectsCapabilityVersionDriftDuringResume(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)
	task, p := setupOpenTaskForRecovery(t, h, openTasks, "versiondrift")

	idemKey := "accept-versiondrift"
	now := time.Now().UTC()
	seed := domain.AcceptanceOperation{
		ID: "accop_versiondrift", TaskID: task.ID, ProposalID: p.ID,
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

	// Bump the capability's version (a pricing change bumps the minor
	// version -- see capability.go's Update) BEFORE the resumed Accept
	// call ever reaches QuoteService.Create.
	if _, err := h.capabilities.Update(ctx, p.CapabilityID, p.ProviderID, map[string]any{
		"pricing": map[string]any{"model": "fixed", "price_hint": map[string]any{"amount": "9.99", "currency": "USD"}},
	}, "bump-version-drift-test"); err != nil {
		t.Fatalf("Update capability: %v", err)
	}

	_, _, err := openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: task.PrincipalID, TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: idemKey,
	})
	if err == nil {
		t.Fatal("expected accept to refuse a capability version that drifted since winner-claim")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrQuoteMismatch {
		t.Fatalf("expected ErrQuoteMismatch, got %v", err)
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
	if reopened.Status != domain.OpenTaskOpen || reopened.AcceptedProposalID != "" {
		t.Fatalf("expected task reopened after version-drift rejection, got %+v", reopened)
	}
}

// TestOpenTaskPublishReplaySucceedsAfterOriginalExpiresAtElapses is the
// regression test for the P1 finding that Publish's "expires_at must be in
// the future" check ran BEFORE the idempotency replay lookup, using
// time.Now() -- so a legitimate retry of an already-successful Publish call
// (same key, same body, including the same now-stale original expires_at)
// would spuriously fail once enough real time passed, instead of replaying
// the original result.
func TestOpenTaskPublishReplaySucceedsAfterOriginalExpiresAtElapses(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	in := service.PublishOpenTaskInput{
		PrincipalID: "prn_expiry_replay", Title: "task", ExpiresAt: time.Now().UTC().Add(50 * time.Millisecond),
		IdempotencyKey: "publish-expiry-replay",
	}
	first, err := openTasks.Publish(ctx, in)
	if err != nil {
		t.Fatalf("first Publish: %v", err)
	}

	time.Sleep(100 * time.Millisecond) // in.ExpiresAt is now in the past.

	replay, err := openTasks.Publish(ctx, in)
	if err != nil {
		t.Fatalf("replay Publish after expires_at elapsed: %v", err)
	}
	if replay.ID != first.ID {
		t.Fatalf("replay minted a different task: %q vs %q", replay.ID, first.ID)
	}
}

// TestOpenTaskProposeReplaySucceedsAfterTaskAccepted is the regression test
// for the P1 finding that Propose validated live task/capability state
// BEFORE the idempotency replay lookup -- so a legitimate retry of an
// already-successful Propose call (the losing provider's own response was
// lost) would fail with ErrOpenTaskNotOpen once the task was accepted (by
// a DIFFERENT proposal), instead of replaying the original proposal.
func TestOpenTaskProposeReplaySucceedsAfterTaskAccepted(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	capA := registerCapability(t, h, "agt_replay_winner", "1.00")
	capB := registerCapability(t, h, "agt_replay_loser", "2.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_propose_replay", Title: "task", Input: map[string]any{},
		ExpiresAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "publish-propose-replay",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	loserIn := service.ProposeInput{
		ProviderID: "agt_replay_loser", TaskID: task.ID, CapabilityID: capB.ID, IdempotencyKey: "propose-replay-loser",
	}
	firstLoser, err := openTasks.Propose(ctx, loserIn)
	if err != nil {
		t.Fatalf("first Propose (loser): %v", err)
	}
	winner, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_replay_winner", TaskID: task.ID, CapabilityID: capA.ID, IdempotencyKey: "propose-replay-winner",
	})
	if err != nil {
		t.Fatalf("Propose (winner): %v", err)
	}
	if _, _, err := openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_propose_replay", TaskID: task.ID, ProposalID: winner.ID, IdempotencyKey: "accept-propose-replay",
	}); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	// The losing provider's client never saw the original response and
	// retries the EXACT same request now that the task is Fulfilled.
	replayLoser, err := openTasks.Propose(ctx, loserIn)
	if err != nil {
		t.Fatalf("replay Propose after task accepted: %v", err)
	}
	if replayLoser.ID != firstLoser.ID {
		t.Fatalf("replay minted a different proposal: %q vs %q", replayLoser.ID, firstLoser.ID)
	}
}

// TestOpenTaskProposeRejectsSelfDealing and
// TestOpenTaskAcceptRejectsSelfDealing are the regression tests for the
// self-referential-operation guard: a task owner can never propose to, or
// accept a proposal on, their own published task.
func TestOpenTaskProposeRejectsSelfDealing(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_self_deal", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "agt_self_deal", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-self-deal",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_, err = openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_self_deal", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-self-deal",
	})
	if err == nil {
		t.Fatal("expected a task owner proposing on their own task to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrPermissionDenied {
		t.Fatalf("expected ErrPermissionDenied, got %v", err)
	}
}

func TestOpenTaskAcceptRejectsSelfDealing(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_self_deal_2", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_self_deal_2", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-self-deal-2",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_self_deal_2", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-self-deal-2",
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	// Simulate the owner also controlling the proposing identity (Propose
	// already refuses this at submission time; Accept must independently
	// refuse it too, as defense in depth for a self-referential
	// operation, in case a proposal predating that check ever exists).
	_, _, err = openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "agt_self_deal_2", TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: "accept-self-deal-2",
	})
	if err == nil {
		t.Fatal("expected accepting your own proposal to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrPermissionDenied {
		t.Fatalf("expected ErrPermissionDenied, got %v", err)
	}
}

// TestOpenTaskListPublicDefaultLimitParityAgainstPostgres is the regression
// test for the P1 finding that an omitted/zero limit meant opposite things
// to the two store backends (memory: unbounded; Postgres: LIMIT 0, zero
// rows) -- MCP's atos_search_open_tasks forwarded a caller-omitted limit as
// 0 straight through, so the identical call returned every open task under
// the in-memory store and an empty list under Postgres.
// OpenTaskService.ListPublic now clamps centrally; this proves limit=0
// (exactly what an omitted MCP argument produces) returns real results
// against a real Postgres-backed store.
func TestOpenTaskListPublicDefaultLimitParityAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres limit-parity test")
	}
	ctx := context.Background()
	st, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	quotes := service.NewQuoteService(st)
	jobs := service.NewJobService(st, nil, nil, service.NewAccountService(st))
	openTasks := service.NewOpenTaskService(st, quotes, jobs)

	suffix := time.Now().UTC().Format("20060102T150405.000000000")
	for i := 0; i < 3; i++ {
		if _, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
			PrincipalID: "prn_limit_parity_" + suffix, Title: "task",
			ExpiresAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "publish-limit-parity-" + suffix + "-" + string(rune('a'+i)),
		}); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	tasks, err := openTasks.ListPublic(ctx, 0)
	if err != nil {
		t.Fatalf("ListPublic(0): %v", err)
	}
	if len(tasks) == 0 {
		t.Fatal("ListPublic(0) returned zero tasks against Postgres -- the exact P1 bug this test targets")
	}
}

// TestOpenTaskPublishContentValidatedAfterAbandonedReservation is the
// regression test for the finding that Publish's crash-recovery lookup
// (OpenTaskByIdempotencyKey) trusted an already-committed row as a valid
// replay without comparing its content against the current request. The
// gap: if a PRIOR attempt's PutOpenTask succeeded but that attempt's own
// Finish call then failed (for any reason short of the process dying), its
// deferred Release hard-deletes the store.Idempotency record entirely --
// store.Release is exactly the primitive that cleanup path calls, so
// calling it directly here reproduces the identical end state ("the task
// row is committed, but no reservation exists for its key") without
// needing to inject a real Finish failure. A later call reusing the same
// key with genuinely different content must now be rejected; the exact
// same content must still replay successfully.
func TestOpenTaskPublishContentValidatedAfterAbandonedReservation(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	original := service.PublishOpenTaskInput{
		PrincipalID: "prn_abandoned", Title: "original title",
		ExpiresAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "publish-abandoned",
	}
	first, err := openTasks.Publish(ctx, original)
	if err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	if err := h.store().Release(ctx, original.PrincipalID, original.IdempotencyKey); err != nil {
		t.Fatalf("Release (simulating a Finish failure's cleanup): %v", err)
	}

	// Same key, genuinely different content -- must be rejected even
	// though no reservation exists to catch it at the Reserve layer.
	changed := original
	changed.Title = "a completely different title"
	_, err = openTasks.Publish(ctx, changed)
	if err == nil {
		t.Fatal("expected a content mismatch against an abandoned-reservation row to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}

	// Same key, IDENTICAL content -- must still replay successfully.
	replay, err := openTasks.Publish(ctx, original)
	if err != nil {
		t.Fatalf("replay Publish after abandoned reservation: %v", err)
	}
	if replay.ID != first.ID {
		t.Fatalf("replay minted a different task: %q vs %q", replay.ID, first.ID)
	}
}

// TestOpenTaskProposeContentValidatedAfterAbandonedReservation is Propose's
// counterpart to TestOpenTaskPublishContentValidatedAfterAbandonedReservation.
func TestOpenTaskProposeContentValidatedAfterAbandonedReservation(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_propose_abandoned", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_propose_abandoned", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-propose-abandoned",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	original := service.ProposeInput{
		ProviderID: "agt_propose_abandoned", TaskID: task.ID, CapabilityID: cap.ID,
		Message: "original message", IdempotencyKey: "propose-abandoned",
	}
	first, err := openTasks.Propose(ctx, original)
	if err != nil {
		t.Fatalf("first Propose: %v", err)
	}
	if err := h.store().Release(ctx, original.ProviderID, original.IdempotencyKey); err != nil {
		t.Fatalf("Release: %v", err)
	}

	changed := original
	changed.Message = "a completely different message"
	_, err = openTasks.Propose(ctx, changed)
	if err == nil {
		t.Fatal("expected a content mismatch against an abandoned-reservation row to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}

	replay, err := openTasks.Propose(ctx, original)
	if err != nil {
		t.Fatalf("replay Propose after abandoned reservation: %v", err)
	}
	if replay.ID != first.ID {
		t.Fatalf("replay minted a different proposal: %q vs %q", replay.ID, first.ID)
	}
}

// TestQuoteCreateContentValidatedAfterAbandonedReservation is
// QuoteService.Create's counterpart, verifying domain.Quote.IdempotencyRequestHash
// closes the same gap for the idempotent Quote-creation path Phase 3C
// added.
func TestQuoteCreateContentValidatedAfterAbandonedReservation(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	cap := registerCapability(t, h, "agt_quote_abandoned", "1.00")

	original := service.CreateQuoteInput{
		PrincipalID: "prn_quote_abandoned", CapabilityID: cap.ID,
		InputSummary: map[string]any{"x": 1}, IdempotencyKey: "quote-abandoned",
	}
	first, err := h.quotes.Create(ctx, original)
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	if err := h.store().Release(ctx, original.PrincipalID, original.IdempotencyKey); err != nil {
		t.Fatalf("Release: %v", err)
	}

	changed := original
	changed.InputSummary = map[string]any{"x": 2}
	_, err = h.quotes.Create(ctx, changed)
	if err == nil {
		t.Fatal("expected a content mismatch against an abandoned-reservation row to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}

	replay, err := h.quotes.Create(ctx, original)
	if err != nil {
		t.Fatalf("replay Create after abandoned reservation: %v", err)
	}
	if replay.ID != first.ID {
		t.Fatalf("replay minted a different quote: %q vs %q", replay.ID, first.ID)
	}
}
