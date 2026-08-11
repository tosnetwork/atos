package service_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// flakyIdentityCore mirrors flakySignerCore's role exactly (see
// execution_signer_test.go), for CreatePrincipalBinding/
// RevokePrincipalBinding instead.
type flakyIdentityCore struct {
	toscore.Core
	mu                     sync.Mutex
	bindFailuresLeft       int
	bindFailureRetryable   bool
	revokeFailuresLeft     int
	revokeFailureRetryable bool
	bindCalls              int
	revokeCalls            int
}

func (f *flakyIdentityCore) CreatePrincipalBinding(ctx context.Context, callerID, idempotencyKey, principalID, agentID string) (domain.PrincipalIdentityBinding, bool, error) {
	f.mu.Lock()
	f.bindCalls++
	if f.bindFailuresLeft > 0 {
		f.bindFailuresLeft--
		retryable := f.bindFailureRetryable
		f.mu.Unlock()
		return domain.PrincipalIdentityBinding{}, false, domain.NewError(domain.ErrNetworkUnavailable, "simulated network failure", retryable)
	}
	f.mu.Unlock()
	return f.Core.CreatePrincipalBinding(ctx, callerID, idempotencyKey, principalID, agentID)
}

func (f *flakyIdentityCore) RevokePrincipalBinding(ctx context.Context, callerID, idempotencyKey, principalID, reasonCode string) (bool, string, string, error) {
	f.mu.Lock()
	f.revokeCalls++
	if f.revokeFailuresLeft > 0 {
		f.revokeFailuresLeft--
		retryable := f.revokeFailureRetryable
		f.mu.Unlock()
		return false, "", "", domain.NewError(domain.ErrNetworkUnavailable, "simulated network failure", retryable)
	}
	f.mu.Unlock()
	return f.Core.RevokePrincipalBinding(ctx, callerID, idempotencyKey, principalID, reasonCode)
}

func TestIdentityBindingBind_GoldenPath(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	core := toscoremock.New(st)
	core.SeedAgentIdentity("agt_1")
	svc := service.NewIdentityBindingService(st, core)

	op, err := svc.Bind(ctx, service.BindIdentityInput{PrincipalID: "prn_1", AgentID: "agt_1", IdempotencyKey: "key-1"})
	if err != nil {
		t.Fatal(err)
	}
	if op.Checkpoint != domain.IdentityBindingCheckpointCompleted {
		t.Fatalf("checkpoint = %s, want completed", op.Checkpoint)
	}
	binding, found, err := svc.CurrentBinding(ctx, "prn_1")
	if err != nil || !found || binding.AgentID != "agt_1" {
		t.Fatalf("unexpected binding: %+v found=%v err=%v", binding, found, err)
	}
}

func TestIdentityBindingBind_RejectsUnresolvedAgent(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	core := toscoremock.New(st)
	svc := service.NewIdentityBindingService(st, core)

	_, err := svc.Bind(ctx, service.BindIdentityInput{PrincipalID: "prn_1", AgentID: "agt_missing", IdempotencyKey: "key-1"})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestIdentityBindingBind_IdempotentReplayDoesNotCallRPCTwice(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	base := toscoremock.New(st)
	base.SeedAgentIdentity("agt_1")
	flaky := &flakyIdentityCore{Core: base}
	svc := service.NewIdentityBindingService(st, flaky)

	in := service.BindIdentityInput{PrincipalID: "prn_1", AgentID: "agt_1", IdempotencyKey: "key-1"}
	first, err := svc.Bind(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Bind(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned a different operation: %s vs %s", second.ID, first.ID)
	}
	if flaky.bindCalls != 1 {
		t.Fatalf("bindCalls = %d, want 1 (idempotency-key replay must not re-call the RPC)", flaky.bindCalls)
	}
}

func TestIdentityBindingBind_RebindingToDifferentAgentConflicts(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	core := toscoremock.New(st)
	core.SeedAgentIdentity("agt_1")
	core.SeedAgentIdentity("agt_2")
	svc := service.NewIdentityBindingService(st, core)

	if _, err := svc.Bind(ctx, service.BindIdentityInput{PrincipalID: "prn_1", AgentID: "agt_1", IdempotencyKey: "key-1"}); err != nil {
		t.Fatal(err)
	}
	_, err := svc.Bind(ctx, service.BindIdentityInput{PrincipalID: "prn_1", AgentID: "agt_2", IdempotencyKey: "key-2"})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestIdentityBindingRevoke_GoldenPathThenNoOp(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	core := toscoremock.New(st)
	core.SetNetwork("tos-devnet")
	core.SeedAgentIdentity("agt_1")
	svc := service.NewIdentityBindingService(st, core)

	if _, err := svc.Bind(ctx, service.BindIdentityInput{PrincipalID: "prn_1", AgentID: "agt_1", IdempotencyKey: "key-1"}); err != nil {
		t.Fatal(err)
	}
	op, err := svc.Revoke(ctx, service.RevokeIdentityBindingInput{PrincipalID: "prn_1", ReasonCode: "OWNER_REQUEST", IdempotencyKey: "key-revoke"})
	if err != nil {
		t.Fatal(err)
	}
	if op.Checkpoint != domain.IdentityBindingCheckpointCompleted {
		t.Fatalf("checkpoint = %s, want completed", op.Checkpoint)
	}
	if _, found, err := svc.CurrentBinding(ctx, "prn_1"); err != nil || found {
		t.Fatalf("found=%v err=%v after revoke, want not found", found, err)
	}
	// A real revocation must carry its own TOS reference, mirroring bind's
	// binding_ref -- REST/MCP surfaces this as revocation_ref/network so a
	// caller has audit proof of the revocation event itself, not just its
	// local side effect.
	if op.BindingRef == "" || op.RefNetwork == "" {
		t.Fatalf("golden-path revoke must record a revocation ref/network, got BindingRef=%q RefNetwork=%q", op.BindingRef, op.RefNetwork)
	}

	// Revoking a principal with no current binding is not an error.
	noOp, err := svc.Revoke(ctx, service.RevokeIdentityBindingInput{PrincipalID: "prn_never_bound", ReasonCode: "TEST", IdempotencyKey: "key-noop"})
	if err != nil {
		t.Fatal(err)
	}
	if noOp.Checkpoint != domain.IdentityBindingCheckpointCompleted {
		t.Fatalf("no-op revoke checkpoint = %s, want completed", noOp.Checkpoint)
	}
	if noOp.BindingRef != "" || noOp.RefNetwork != "" {
		t.Fatalf("no-op revoke (nothing bound) must not fabricate a ref, got BindingRef=%q RefNetwork=%q", noOp.BindingRef, noOp.RefNetwork)
	}
}

// TestIdentityBindingBind_AmbiguousFailureThenReconcile is the crash/
// lost-response recovery test the Phase 4A brief requires: a transient
// ambiguous failure leaves the operation Reconciling (not Completed, not
// silently discarded); ReconcileStaleOperations resumes it and it
// converges to Completed once the underlying call actually succeeds --
// exactly TestExecutionSignerService_ReconcileStaleOperations_ResumesReconcilingAuthorize's
// shape.
func TestIdentityBindingBind_AmbiguousFailureThenReconcile(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	base := toscoremock.New(st)
	base.SeedAgentIdentity("agt_1")
	flaky := &flakyIdentityCore{Core: base, bindFailuresLeft: 1, bindFailureRetryable: true}
	svc := service.NewIdentityBindingService(st, flaky)

	_, err := svc.Bind(ctx, service.BindIdentityInput{PrincipalID: "prn_1", AgentID: "agt_1", IdempotencyKey: "key-1"})
	if err == nil {
		t.Fatal("expected the ambiguous first attempt to surface a retryable error")
	}
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || !domainErr.Retryable {
		t.Fatalf("err = %v, want a retryable domain error", err)
	}

	op, err := st.IdentityBindingOperationByIdempotencyKey(ctx, "prn_1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if op.Checkpoint != domain.IdentityBindingCheckpointReconciling {
		t.Fatalf("checkpoint = %s, want reconciling after an ambiguous failure", op.Checkpoint)
	}
	if _, found, err := svc.CurrentBinding(ctx, "prn_1"); err != nil || found {
		t.Fatalf("found=%v err=%v, binding must not exist while the operation is still reconciling", found, err)
	}

	// The reconciler resumes it; this time the (now-exhausted) flaky core
	// lets the call through, so it converges to Completed.
	if err := svc.ReconcileStaleOperations(ctx, time.Now().UTC().Add(time.Hour), 10); err != nil {
		t.Fatal(err)
	}
	reconciled, err := st.GetIdentityBindingOperation(ctx, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.Checkpoint != domain.IdentityBindingCheckpointCompleted {
		t.Fatalf("checkpoint after reconcile = %s, want completed", reconciled.Checkpoint)
	}
	if flaky.bindCalls != 2 {
		t.Fatalf("bindCalls = %d, want 2 (one ambiguous failure, one successful reconcile retry)", flaky.bindCalls)
	}
	if _, found, err := svc.CurrentBinding(ctx, "prn_1"); err != nil || !found {
		t.Fatalf("found=%v err=%v after reconcile, want found", found, err)
	}
}

// TestIdentityBindingRevoke_AmbiguousFailureThenReconcile is
// TestIdentityBindingBind_AmbiguousFailureThenReconcile's Revoke
// counterpart -- this cell of the (bind|revoke) x (ambiguous|definitive)
// failure matrix had no test at all despite flakyIdentityCore already
// supporting it.
func TestIdentityBindingRevoke_AmbiguousFailureThenReconcile(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	base := toscoremock.New(st)
	base.SetNetwork("tos-devnet")
	base.SeedAgentIdentity("agt_1")
	flaky := &flakyIdentityCore{Core: base}
	svc := service.NewIdentityBindingService(st, flaky)

	if _, err := svc.Bind(ctx, service.BindIdentityInput{PrincipalID: "prn_1", AgentID: "agt_1", IdempotencyKey: "key-1"}); err != nil {
		t.Fatal(err)
	}
	flaky.revokeFailuresLeft, flaky.revokeFailureRetryable = 1, true

	_, err := svc.Revoke(ctx, service.RevokeIdentityBindingInput{PrincipalID: "prn_1", ReasonCode: "TEST", IdempotencyKey: "key-revoke"})
	if err == nil {
		t.Fatal("expected the ambiguous first attempt to surface a retryable error")
	}
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || !domainErr.Retryable {
		t.Fatalf("err = %v, want a retryable domain error", err)
	}

	op, err := st.IdentityBindingOperationByIdempotencyKey(ctx, "prn_1", "key-revoke")
	if err != nil {
		t.Fatal(err)
	}
	if op.Checkpoint != domain.IdentityBindingCheckpointReconciling {
		t.Fatalf("checkpoint = %s, want reconciling after an ambiguous failure", op.Checkpoint)
	}
	// The binding must still be intact -- an ambiguous revoke failure must
	// not have silently torn it down.
	if _, found, err := svc.CurrentBinding(ctx, "prn_1"); err != nil || !found {
		t.Fatalf("found=%v err=%v, binding must still exist while the revoke operation is still reconciling", found, err)
	}

	if err := svc.ReconcileStaleOperations(ctx, time.Now().UTC().Add(time.Hour), 10); err != nil {
		t.Fatal(err)
	}
	reconciled, err := st.GetIdentityBindingOperation(ctx, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.Checkpoint != domain.IdentityBindingCheckpointCompleted {
		t.Fatalf("checkpoint after reconcile = %s, want completed", reconciled.Checkpoint)
	}
	if !reconciled.Revoked {
		t.Fatal("reconciled revoke must record Revoked=true")
	}
	if flaky.revokeCalls != 2 {
		t.Fatalf("revokeCalls = %d, want 2 (one ambiguous failure, one successful reconcile retry)", flaky.revokeCalls)
	}
	if _, found, err := svc.CurrentBinding(ctx, "prn_1"); err != nil || found {
		t.Fatalf("found=%v err=%v after reconcile, want not found (revoke actually completed)", found, err)
	}
}

// TestIdentityBindingRevoke_DefinitiveRejectionDoesNotReconcile is
// TestIdentityBindingBind_DefinitiveRejectionDoesNotReconcile's Revoke
// counterpart.
func TestIdentityBindingRevoke_DefinitiveRejectionDoesNotReconcile(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	base := toscoremock.New(st)
	base.SetNetwork("tos-devnet")
	base.SeedAgentIdentity("agt_1")
	flaky := &flakyIdentityCore{Core: base}
	svc := service.NewIdentityBindingService(st, flaky)

	if _, err := svc.Bind(ctx, service.BindIdentityInput{PrincipalID: "prn_1", AgentID: "agt_1", IdempotencyKey: "key-1"}); err != nil {
		t.Fatal(err)
	}
	flaky.revokeFailuresLeft, flaky.revokeFailureRetryable = 1, false

	_, err := svc.Revoke(ctx, service.RevokeIdentityBindingInput{PrincipalID: "prn_1", ReasonCode: "TEST", IdempotencyKey: "key-revoke"})
	if err == nil {
		t.Fatal("expected the definitive failure to surface")
	}

	op, err := st.IdentityBindingOperationByIdempotencyKey(ctx, "prn_1", "key-revoke")
	if err != nil {
		t.Fatal(err)
	}
	if op.Checkpoint != domain.IdentityBindingCheckpointIntentPersisted {
		t.Fatalf("checkpoint = %s, want intent_persisted (unchanged by a definitive rejection)", op.Checkpoint)
	}
	if op.FailureReason == "" {
		t.Fatal("expected FailureReason to record the definitive rejection for operator visibility")
	}
	// The binding must still be intact -- a definitively-rejected revoke
	// attempt must not have torn it down either.
	if _, found, err := svc.CurrentBinding(ctx, "prn_1"); err != nil || !found {
		t.Fatalf("found=%v err=%v, binding must still exist after a definitively-rejected revoke", found, err)
	}
}

// TestIdentityBindingBind_DefinitiveRejectionDoesNotReconcile proves a
// non-ambiguous (Retryable=false) failure is recorded as a genuine
// rejection at its current checkpoint (unchanged, with FailureReason set
// for operator visibility), not silently promoted to Reconciling the way
// an ambiguous failure is -- mirrors
// TestExecutionSignerAuthorize_DefinitiveRejectionDoesNotReconcile exactly.
func TestIdentityBindingBind_DefinitiveRejectionDoesNotReconcile(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	base := toscoremock.New(st)
	base.SeedAgentIdentity("agt_1")
	flaky := &flakyIdentityCore{Core: base, bindFailuresLeft: 1, bindFailureRetryable: false}
	svc := service.NewIdentityBindingService(st, flaky)

	_, err := svc.Bind(ctx, service.BindIdentityInput{PrincipalID: "prn_1", AgentID: "agt_1", IdempotencyKey: "key-1"})
	if err == nil {
		t.Fatal("expected the definitive failure to surface")
	}

	op, err := st.IdentityBindingOperationByIdempotencyKey(ctx, "prn_1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if op.Checkpoint != domain.IdentityBindingCheckpointIntentPersisted {
		t.Fatalf("checkpoint = %s, want intent_persisted (unchanged by a definitive rejection)", op.Checkpoint)
	}
	if op.FailureReason == "" {
		t.Fatal("expected FailureReason to record the definitive rejection for operator visibility")
	}
}
