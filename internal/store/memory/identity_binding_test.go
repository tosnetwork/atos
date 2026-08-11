package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func testIdentityBindingOp(principalID, idempotencyKey string) domain.IdentityBindingOperation {
	now := time.Now().UTC()
	return domain.IdentityBindingOperation{
		ID: "idop_" + idempotencyKey, PrincipalID: principalID,
		Type: domain.IdentityBindingOperationBind, Checkpoint: domain.IdentityBindingCheckpointIntentPersisted,
		IdempotencyKey: idempotencyKey, AgentID: "agt_1",
		CreatedAt: now, UpdatedAt: now,
	}
}

func TestPrincipalBinding_PutGetDelete(t *testing.T) {
	ctx := context.Background()
	s := New()
	if _, found, err := s.CurrentPrincipalBinding(ctx, "prn_1"); err != nil || found {
		t.Fatalf("found=%v err=%v, want not found", found, err)
	}
	now := time.Now().UTC()
	b := domain.PrincipalIdentityBinding{PrincipalID: "prn_1", AgentID: "agt_1", Network: "tos-test", BindingRef: "tos-test:ref", BoundAt: now, UpdatedAt: now}
	if err := s.PutPrincipalBinding(ctx, b); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.CurrentPrincipalBinding(ctx, "prn_1")
	if err != nil || !found || got.AgentID != "agt_1" {
		t.Fatalf("unexpected binding: %+v found=%v err=%v", got, found, err)
	}
	if err := s.DeletePrincipalBinding(ctx, "prn_1"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.CurrentPrincipalBinding(ctx, "prn_1"); err != nil || found {
		t.Fatalf("found=%v err=%v after delete, want not found", found, err)
	}
}

func TestOpenIdentityBindingOperation_FirstCallCreates(t *testing.T) {
	ctx := context.Background()
	s := New()
	op, created, err := s.OpenIdentityBindingOperation(ctx, "prn_1", testIdentityBindingOp("prn_1", "key-1"))
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if op.Checkpoint != domain.IdentityBindingCheckpointIntentPersisted {
		t.Fatalf("checkpoint = %s", op.Checkpoint)
	}
}

func TestOpenIdentityBindingOperation_ReplaySameContentReturnsExisting(t *testing.T) {
	ctx := context.Background()
	s := New()
	op := testIdentityBindingOp("prn_1", "key-2")
	first, _, err := s.OpenIdentityBindingOperation(ctx, "prn_1", op)
	if err != nil {
		t.Fatal(err)
	}
	second, created, err := s.OpenIdentityBindingOperation(ctx, "prn_1", op)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("replay should report created=false")
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned a different operation: %s vs %s", second.ID, first.ID)
	}
}

func TestOpenIdentityBindingOperation_ReplayDifferentContentConflicts(t *testing.T) {
	ctx := context.Background()
	s := New()
	first := testIdentityBindingOp("prn_1", "key-3")
	if _, _, err := s.OpenIdentityBindingOperation(ctx, "prn_1", first); err != nil {
		t.Fatal(err)
	}
	second := testIdentityBindingOp("prn_1", "key-3")
	second.AgentID = "agt_2" // different content, same idempotency key
	_, _, err := s.OpenIdentityBindingOperation(ctx, "prn_1", second)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestUpdateIdentityBindingOperation_RejectsIdentityFieldChange(t *testing.T) {
	ctx := context.Background()
	s := New()
	op, _, err := s.OpenIdentityBindingOperation(ctx, "prn_1", testIdentityBindingOp("prn_1", "key-4"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UpdateIdentityBindingOperation(ctx, op.ID, func(current domain.IdentityBindingOperation, exists bool) (domain.IdentityBindingOperation, error) {
		current.AgentID = "agt_changed"
		return current, nil
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestUpdateIdentityBindingOperation_AllowsCheckpointAdvance(t *testing.T) {
	ctx := context.Background()
	s := New()
	op, _, err := s.OpenIdentityBindingOperation(ctx, "prn_1", testIdentityBindingOp("prn_1", "key-5"))
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now().UTC()
	updated, err := s.UpdateIdentityBindingOperation(ctx, op.ID, func(current domain.IdentityBindingOperation, exists bool) (domain.IdentityBindingOperation, error) {
		if !exists {
			t.Fatal("expected existing operation")
		}
		current.Checkpoint = domain.IdentityBindingCheckpointCompleted
		current.BindingRef = "tos-test:ref"
		current.CompletedAt = &completed
		current.UpdatedAt = completed
		return current, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Checkpoint != domain.IdentityBindingCheckpointCompleted || updated.BindingRef != "tos-test:ref" {
		t.Fatalf("unexpected updated op: %+v", updated)
	}
}

func TestStaleIdentityBindingOperations_ExcludesCompletedAndRecent(t *testing.T) {
	ctx := context.Background()
	s := New()
	old := testIdentityBindingOp("prn_1", "key-stale")
	old.UpdatedAt = time.Now().UTC().Add(-time.Hour)
	if _, _, err := s.OpenIdentityBindingOperation(ctx, "prn_1", old); err != nil {
		t.Fatal(err)
	}
	recent := testIdentityBindingOp("prn_2", "key-recent")
	if _, _, err := s.OpenIdentityBindingOperation(ctx, "prn_2", recent); err != nil {
		t.Fatal(err)
	}
	completedOp := testIdentityBindingOp("prn_3", "key-completed")
	completedOp.UpdatedAt = time.Now().UTC().Add(-time.Hour)
	opened, _, err := s.OpenIdentityBindingOperation(ctx, "prn_3", completedOp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateIdentityBindingOperation(ctx, opened.ID, func(current domain.IdentityBindingOperation, exists bool) (domain.IdentityBindingOperation, error) {
		current.Checkpoint = domain.IdentityBindingCheckpointCompleted
		current.UpdatedAt = time.Now().UTC().Add(-time.Hour)
		return current, nil
	}); err != nil {
		t.Fatal(err)
	}

	stale, err := s.StaleIdentityBindingOperations(ctx, time.Now().UTC().Add(-30*time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 || stale[0].PrincipalID != "prn_1" {
		t.Fatalf("stale = %+v, want exactly prn_1's operation", stale)
	}
}

func TestLatestCompletedIdentityBindingOperation_NeverBound(t *testing.T) {
	ctx := context.Background()
	s := New()
	_, found, err := s.LatestCompletedIdentityBindingOperation(ctx, "prn_never_seen")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("a principal with no operations at all must report found=false")
	}
}

func TestLatestCompletedIdentityBindingOperation_IgnoresNonCompleted(t *testing.T) {
	ctx := context.Background()
	s := New()
	op := testIdentityBindingOp("prn_1", "key-pending")
	if _, _, err := s.OpenIdentityBindingOperation(ctx, "prn_1", op); err != nil {
		t.Fatal(err)
	}
	// Still intent_persisted -- never completed.
	_, found, err := s.LatestCompletedIdentityBindingOperation(ctx, "prn_1")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("a non-completed operation must not be returned")
	}
}

// TestLatestCompletedIdentityBindingOperation_ReturnsMostRecentByCompletedAt
// proves the LATEST completed operation is selected by completed_at, not by
// insertion/creation order -- this is what
// IdentityBindingService.RevocationHistory relies on to correctly report
// "revoked" (not "bound") after a bind-then-revoke sequence.
func TestLatestCompletedIdentityBindingOperation_ReturnsMostRecentByCompletedAt(t *testing.T) {
	ctx := context.Background()
	s := New()
	bind := testIdentityBindingOp("prn_1", "key-bind")
	if _, _, err := s.OpenIdentityBindingOperation(ctx, "prn_1", bind); err != nil {
		t.Fatal(err)
	}
	earlier := time.Now().UTC().Add(-time.Hour)
	if _, err := s.UpdateIdentityBindingOperation(ctx, bind.ID, func(current domain.IdentityBindingOperation, exists bool) (domain.IdentityBindingOperation, error) {
		current.Checkpoint = domain.IdentityBindingCheckpointCompleted
		current.CompletedAt = &earlier
		return current, nil
	}); err != nil {
		t.Fatal(err)
	}

	revoke := testIdentityBindingOp("prn_1", "key-revoke")
	revoke.Type = domain.IdentityBindingOperationRevoke
	revoke.AgentID = ""
	revoke.ID = "idop_key-revoke"
	if _, _, err := s.OpenIdentityBindingOperation(ctx, "prn_1", revoke); err != nil {
		t.Fatal(err)
	}
	later := time.Now().UTC()
	if _, err := s.UpdateIdentityBindingOperation(ctx, revoke.ID, func(current domain.IdentityBindingOperation, exists bool) (domain.IdentityBindingOperation, error) {
		current.Checkpoint = domain.IdentityBindingCheckpointCompleted
		current.Revoked = true
		current.CompletedAt = &later
		return current, nil
	}); err != nil {
		t.Fatal(err)
	}

	latest, found, err := s.LatestCompletedIdentityBindingOperation(ctx, "prn_1")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected a completed operation to be found")
	}
	if latest.ID != revoke.ID || latest.Type != domain.IdentityBindingOperationRevoke {
		t.Fatalf("latest = %+v, want the later-completed revoke operation", latest)
	}
}

func TestGetIdentityBindingOperation_NotFound(t *testing.T) {
	ctx := context.Background()
	s := New()
	_, err := s.GetIdentityBindingOperation(ctx, "does-not-exist")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestCapabilityOwnershipCommitment_PutAndGet(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now().UTC()
	c := domain.CapabilityOwnershipCommitment{
		CapabilityID: "cap_1", Version: "1.0.0", ProviderID: "prov_1", Network: "tos-test",
		ManifestCommitment: "tos-test:manifest", OwnershipCommitment: "tos-test:ownership", CommittedAt: now,
	}
	if err := s.PutCapabilityOwnershipCommitment(ctx, c); err != nil {
		t.Fatal(err)
	}
	// Idempotent replay with identical content must succeed silently.
	if err := s.PutCapabilityOwnershipCommitment(ctx, c); err != nil {
		t.Fatalf("identical replay should not error: %v", err)
	}
	got, found, err := s.CapabilityOwnershipCommitmentByVersion(ctx, "cap_1", "1.0.0")
	if err != nil || !found || got.ManifestCommitment != "tos-test:manifest" {
		t.Fatalf("unexpected commitment: %+v found=%v err=%v", got, found, err)
	}

	// Different content for the same capability_id+version must conflict --
	// manifest/ownership commitments are immutable once anchored.
	conflicting := c
	conflicting.ManifestCommitment = "tos-test:different-manifest"
	err = s.PutCapabilityOwnershipCommitment(ctx, conflicting)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}
