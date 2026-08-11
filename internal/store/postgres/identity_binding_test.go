package postgres_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

func testIdentityBindingOperation(principalID, idempotencyKey, agentID string) domain.IdentityBindingOperation {
	now := time.Now().UTC()
	return domain.IdentityBindingOperation{
		ID: "idop_" + idempotencyKey, PrincipalID: principalID,
		Type: domain.IdentityBindingOperationBind, Checkpoint: domain.IdentityBindingCheckpointIntentPersisted,
		IdempotencyKey: idempotencyKey, AgentID: agentID,
		CreatedAt: now, UpdatedAt: now,
	}
}

func TestOpenIdentityBindingOperation_ThenGet(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	principalID := "prn_idop_" + suffix
	op := testIdentityBindingOperation(principalID, "key_"+suffix, "agt_"+suffix)

	stored, created, err := s.OpenIdentityBindingOperation(ctx, principalID, op)
	if err != nil {
		t.Fatalf("OpenIdentityBindingOperation: %v", err)
	}
	if !created {
		t.Fatal("expected created=true for a new operation")
	}
	if stored.Checkpoint != domain.IdentityBindingCheckpointIntentPersisted {
		t.Fatalf("checkpoint = %s, want intent_persisted", stored.Checkpoint)
	}

	got, err := s.GetIdentityBindingOperation(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetIdentityBindingOperation: %v", err)
	}
	if got.AgentID != op.AgentID || got.PrincipalID != op.PrincipalID {
		t.Fatalf("round-tripped operation mismatch: %+v", got)
	}

	byKey, err := s.IdentityBindingOperationByIdempotencyKey(ctx, principalID, op.IdempotencyKey)
	if err != nil {
		t.Fatalf("IdentityBindingOperationByIdempotencyKey: %v", err)
	}
	if byKey.ID != op.ID {
		t.Fatalf("IdentityBindingOperationByIdempotencyKey returned %s, want %s", byKey.ID, op.ID)
	}
}

func TestGetIdentityBindingOperation_NotFound(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	_, err := s.GetIdentityBindingOperation(ctx, "idop_does-not-exist_"+randSuffix())
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("got %v, want store.ErrNotFound", err)
	}
}

func TestOpenIdentityBindingOperation_ReplaySameContentReturnsExisting(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	principalID := "prn_idop_replay_" + suffix
	op := testIdentityBindingOperation(principalID, "key_"+suffix, "agt_"+suffix)

	first, created1, err := s.OpenIdentityBindingOperation(ctx, principalID, op)
	if err != nil || !created1 {
		t.Fatalf("first open: created=%v err=%v", created1, err)
	}
	second, created2, err := s.OpenIdentityBindingOperation(ctx, principalID, op)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if created2 {
		t.Fatal("expected created=false on a same-content replay")
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned a different operation: %s vs %s", second.ID, first.ID)
	}
}

func TestOpenIdentityBindingOperation_DifferentAgentIDConflicts(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	principalID := "prn_idop_conflict_" + suffix
	op := testIdentityBindingOperation(principalID, "key_"+suffix, "agt_"+suffix)
	if _, _, err := s.OpenIdentityBindingOperation(ctx, principalID, op); err != nil {
		t.Fatal(err)
	}
	changed := op
	changed.AgentID = "agt_different_" + suffix
	_, _, err := s.OpenIdentityBindingOperation(ctx, principalID, changed)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}

func TestUpdateIdentityBindingOperation_RejectsIdentityFieldChange(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	principalID := "prn_idop_reject_" + suffix
	op := testIdentityBindingOperation(principalID, "key_"+suffix, "agt_"+suffix)
	if _, _, err := s.OpenIdentityBindingOperation(ctx, principalID, op); err != nil {
		t.Fatal(err)
	}
	_, err := s.UpdateIdentityBindingOperation(ctx, op.ID, func(current domain.IdentityBindingOperation, exists bool) (domain.IdentityBindingOperation, error) {
		current.AgentID = "agt_smuggled_" + suffix
		return current, nil
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict for a smuggled identity-field change", err)
	}
}

func TestUpdateIdentityBindingOperation_AllowsCheckpointAdvance(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	principalID := "prn_idop_advance_" + suffix
	op := testIdentityBindingOperation(principalID, "key_"+suffix, "agt_"+suffix)
	if _, _, err := s.OpenIdentityBindingOperation(ctx, principalID, op); err != nil {
		t.Fatal(err)
	}
	updated, err := s.UpdateIdentityBindingOperation(ctx, op.ID, func(current domain.IdentityBindingOperation, exists bool) (domain.IdentityBindingOperation, error) {
		if !exists {
			t.Fatal("expected existing operation")
		}
		current.Checkpoint = domain.IdentityBindingCheckpointCompleted
		current.BindingRef = "tos:ref:" + suffix
		current.UpdatedAt = time.Now().UTC()
		return current, nil
	})
	if err != nil {
		t.Fatalf("UpdateIdentityBindingOperation: %v", err)
	}
	if updated.Checkpoint != domain.IdentityBindingCheckpointCompleted || updated.BindingRef != "tos:ref:"+suffix {
		t.Fatalf("unexpected updated operation: %+v", updated)
	}
}

func TestOpenIdentityBindingOperation_TwoIndependentPostgresInstancesConvergeToOne(t *testing.T) {
	url := os.Getenv("ATOS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx := context.Background()
	sA, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sA.Close()
	sB, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sB.Close()

	suffix := randSuffix()
	principalID := "prn_idop_replica_" + suffix
	op := testIdentityBindingOperation(principalID, "key_"+suffix, "agt_"+suffix)

	var wg sync.WaitGroup
	var creators int64
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, created, err := sA.OpenIdentityBindingOperation(ctx, principalID, op)
		if err != nil {
			t.Errorf("replica A OpenIdentityBindingOperation: %v", err)
			return
		}
		if created {
			atomic.AddInt64(&creators, 1)
		}
	}()
	go func() {
		defer wg.Done()
		_, created, err := sB.OpenIdentityBindingOperation(ctx, principalID, op)
		if err != nil {
			t.Errorf("replica B OpenIdentityBindingOperation: %v", err)
			return
		}
		if created {
			atomic.AddInt64(&creators, 1)
		}
	}()
	wg.Wait()
	if creators != 1 {
		t.Fatalf("creators = %d across two replicas, want exactly 1", creators)
	}
}

func TestUpdateIdentityBindingOperation_TwoIndependentPostgresInstancesSerializeAdvance(t *testing.T) {
	url := os.Getenv("ATOS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx := context.Background()
	sA, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sA.Close()
	sB, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sB.Close()

	suffix := randSuffix()
	principalID := "prn_idop_advance_replica_" + suffix
	op := testIdentityBindingOperation(principalID, "key_"+suffix, "agt_"+suffix)
	if _, _, err := sA.OpenIdentityBindingOperation(ctx, principalID, op); err != nil {
		t.Fatal(err)
	}

	advance := func(s interface {
		UpdateIdentityBindingOperation(context.Context, string, func(domain.IdentityBindingOperation, bool) (domain.IdentityBindingOperation, error)) (domain.IdentityBindingOperation, error)
	}, checkpoint domain.IdentityBindingCheckpoint) error {
		_, err := s.UpdateIdentityBindingOperation(ctx, op.ID, func(current domain.IdentityBindingOperation, exists bool) (domain.IdentityBindingOperation, error) {
			current.Checkpoint = checkpoint
			current.UpdatedAt = time.Now().UTC()
			return current, nil
		})
		return err
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = advance(sA, domain.IdentityBindingCheckpointReconciling) }()
	go func() { defer wg.Done(); _ = advance(sB, domain.IdentityBindingCheckpointCompleted) }()
	wg.Wait()

	final, err := sA.GetIdentityBindingOperation(ctx, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Checkpoint != domain.IdentityBindingCheckpointReconciling && final.Checkpoint != domain.IdentityBindingCheckpointCompleted {
		t.Fatalf("final checkpoint = %s, want one of the two racing writers' values, not a torn state", final.Checkpoint)
	}
}

func TestStaleIdentityBindingOperations_ExcludesCompletedAndFreshOperations(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	principalID := "prn_idop_stale_" + suffix

	stuck := testIdentityBindingOperation(principalID, "key_stuck_"+suffix, "agt_stuck_"+suffix)
	stuck.Checkpoint = domain.IdentityBindingCheckpointReconciling
	stuck.UpdatedAt = time.Now().UTC().Add(-time.Hour)
	stuck.CreatedAt = stuck.UpdatedAt
	if _, _, err := s.OpenIdentityBindingOperation(ctx, principalID, stuck); err != nil {
		t.Fatal(err)
	}
	// OpenIdentityBindingOperation always writes CreatedAt/UpdatedAt as
	// provided by the caller at insert time, but the reconciler's staleness
	// window depends on updated_at reflecting the last touch -- re-touch it
	// directly via UpdateIdentityBindingOperation so the row's updated_at is
	// old, mirroring how a real crash would leave it.
	if _, err := s.UpdateIdentityBindingOperation(ctx, stuck.ID, func(current domain.IdentityBindingOperation, exists bool) (domain.IdentityBindingOperation, error) {
		current.UpdatedAt = time.Now().UTC().Add(-time.Hour)
		return current, nil
	}); err != nil {
		t.Fatal(err)
	}

	fresh := testIdentityBindingOperation(principalID+"_fresh", "key_fresh_"+suffix, "agt_fresh_"+suffix)
	if _, _, err := s.OpenIdentityBindingOperation(ctx, principalID+"_fresh", fresh); err != nil {
		t.Fatal(err)
	}

	completed := testIdentityBindingOperation(principalID+"_done", "key_done_"+suffix, "agt_done_"+suffix)
	if _, _, err := s.OpenIdentityBindingOperation(ctx, principalID+"_done", completed); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateIdentityBindingOperation(ctx, completed.ID, func(current domain.IdentityBindingOperation, exists bool) (domain.IdentityBindingOperation, error) {
		current.Checkpoint = domain.IdentityBindingCheckpointCompleted
		current.UpdatedAt = time.Now().UTC().Add(-time.Hour)
		return current, nil
	}); err != nil {
		t.Fatal(err)
	}

	stale, err := s.StaleIdentityBindingOperations(ctx, time.Now().UTC().Add(-30*time.Minute), 100)
	if err != nil {
		t.Fatal(err)
	}
	var sawStuck, sawFresh, sawCompleted bool
	for _, op := range stale {
		switch op.ID {
		case stuck.ID:
			sawStuck = true
		case fresh.ID:
			sawFresh = true
		case completed.ID:
			sawCompleted = true
		}
	}
	if !sawStuck {
		t.Error("expected the stuck reconciling operation in the stale set")
	}
	if sawFresh {
		t.Error("did not expect the fresh operation in the stale set")
	}
	if sawCompleted {
		t.Error("did not expect the completed operation in the stale set")
	}
}

func TestPrincipalBinding_PutGetDelete(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	principalID := "prn_binding_" + suffix
	now := time.Now().UTC().Truncate(time.Microsecond)

	_, found, err := s.CurrentPrincipalBinding(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected no binding before any Put")
	}

	binding := domain.PrincipalIdentityBinding{
		PrincipalID: principalID, AgentID: "agt_" + suffix, Network: "tos-devnet",
		BindingRef: "tos:ref:" + suffix, BoundAt: now, UpdatedAt: now,
	}
	if err := s.PutPrincipalBinding(ctx, binding); err != nil {
		t.Fatalf("PutPrincipalBinding: %v", err)
	}
	got, found, err := s.CurrentPrincipalBinding(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || got.AgentID != binding.AgentID || got.BindingRef != binding.BindingRef {
		t.Fatalf("CurrentPrincipalBinding after Put = %+v, found=%v", got, found)
	}

	// PutPrincipalBinding is an upsert: rebinding the same principal_id to a
	// different agent_id must overwrite, not conflict -- the write-side
	// idempotency/conflict rule lives in IdentityBindingOperation, not here.
	rebinding := binding
	rebinding.AgentID = "agt_rebound_" + suffix
	rebinding.UpdatedAt = now.Add(time.Minute)
	if err := s.PutPrincipalBinding(ctx, rebinding); err != nil {
		t.Fatalf("PutPrincipalBinding rebind: %v", err)
	}
	got, found, err = s.CurrentPrincipalBinding(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || got.AgentID != rebinding.AgentID {
		t.Fatalf("CurrentPrincipalBinding after rebind = %+v, want agent_id=%s", got, rebinding.AgentID)
	}

	if err := s.DeletePrincipalBinding(ctx, principalID); err != nil {
		t.Fatalf("DeletePrincipalBinding: %v", err)
	}
	_, found, err = s.CurrentPrincipalBinding(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected no binding after Delete")
	}

	// Deleting an already-absent binding is a harmless no-op, mirroring
	// tos-protocol's own RevokePrincipalBinding "revoked=false" convention.
	if err := s.DeletePrincipalBinding(ctx, principalID); err != nil {
		t.Fatalf("DeletePrincipalBinding on absent binding: %v", err)
	}
}

func TestCapabilityOwnershipCommitment_PutThenGet(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	capabilityID := "cap_own_" + suffix
	now := time.Now().UTC().Truncate(time.Microsecond)

	_, found, err := s.CapabilityOwnershipCommitmentByVersion(ctx, capabilityID, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected no commitment before any Put")
	}

	commitment := domain.CapabilityOwnershipCommitment{
		CapabilityID: capabilityID, Version: "1.0.0", ProviderID: "agt_" + suffix,
		Network: "tos-devnet", ManifestCommitment: "sha256:" + suffix, OwnershipCommitment: "tos:ref:" + suffix,
		CommittedAt: now,
	}
	if err := s.PutCapabilityOwnershipCommitment(ctx, commitment); err != nil {
		t.Fatalf("PutCapabilityOwnershipCommitment: %v", err)
	}
	got, found, err := s.CapabilityOwnershipCommitmentByVersion(ctx, capabilityID, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if !found || got.ManifestCommitment != commitment.ManifestCommitment || got.ProviderID != commitment.ProviderID {
		t.Fatalf("round-tripped commitment mismatch: %+v", got)
	}
}

func TestCapabilityOwnershipCommitment_ReplaySameContentIsNoOp(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	capabilityID := "cap_own_replay_" + suffix
	now := time.Now().UTC().Truncate(time.Microsecond)
	commitment := domain.CapabilityOwnershipCommitment{
		CapabilityID: capabilityID, Version: "1.0.0", ProviderID: "agt_" + suffix,
		Network: "tos-devnet", ManifestCommitment: "sha256:" + suffix, OwnershipCommitment: "tos:ref:" + suffix,
		CommittedAt: now,
	}
	if err := s.PutCapabilityOwnershipCommitment(ctx, commitment); err != nil {
		t.Fatal(err)
	}
	if err := s.PutCapabilityOwnershipCommitment(ctx, commitment); err != nil {
		t.Fatalf("replay of identical commitment should be a no-op, got: %v", err)
	}
}

func TestCapabilityOwnershipCommitment_DifferentManifestForSameVersionConflicts(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	capabilityID := "cap_own_conflict_" + suffix
	now := time.Now().UTC().Truncate(time.Microsecond)
	commitment := domain.CapabilityOwnershipCommitment{
		CapabilityID: capabilityID, Version: "1.0.0", ProviderID: "agt_" + suffix,
		Network: "tos-devnet", ManifestCommitment: "sha256:" + suffix, OwnershipCommitment: "tos:ref:" + suffix,
		CommittedAt: now,
	}
	if err := s.PutCapabilityOwnershipCommitment(ctx, commitment); err != nil {
		t.Fatal(err)
	}
	changed := commitment
	changed.ManifestCommitment = "sha256:different-" + suffix
	err := s.PutCapabilityOwnershipCommitment(ctx, changed)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}
