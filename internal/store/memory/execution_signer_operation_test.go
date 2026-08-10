package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

func testSignerOp(providerID, idempotencyKey string) domain.ExecutionSignerOperation {
	now := time.Now().UTC()
	return domain.ExecutionSignerOperation{
		ID: "sigop_" + idempotencyKey, ProviderID: providerID, CapabilityID: "cap_1", CapabilityVersion: "1.0.0",
		Type: domain.SignerOperationAuthorize, Checkpoint: domain.CheckpointIntentPersisted,
		IdempotencyKey:        idempotencyKey,
		NewAuthorizationID:    "authz_" + idempotencyKey,
		NewExecutionSignerID:  "signer_1",
		NewSignerPublicKey:    []byte{9, 9, 9},
		NewSignatureAlgorithm: "ed25519",
		NewValidFrom:          now,
		NewValidUntil:         now.Add(time.Hour),
		CreatedAt:             now, UpdatedAt: now,
	}
}

func TestOpenSignerOperation_FirstCallCreates(t *testing.T) {
	ctx := context.Background()
	s := New()
	op, created, err := s.OpenSignerOperation(ctx, "prov_1", testSignerOp("prov_1", "key-1"))
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if op.Checkpoint != domain.CheckpointIntentPersisted {
		t.Fatalf("checkpoint = %s", op.Checkpoint)
	}
}

func TestOpenSignerOperation_ReplaySameContentReturnsExisting(t *testing.T) {
	ctx := context.Background()
	s := New()
	// Reuse the exact same value for both calls (not two separate
	// testSignerOp(...) invocations) -- NewValidFrom/NewValidUntil are
	// part of the identity content hash, and two calls to the builder
	// made at genuinely different wall-clock instants would produce
	// different timestamps, which is a real content difference, not the
	// same-request replay this test means to simulate.
	op := testSignerOp("prov_1", "key-2")
	first, _, err := s.OpenSignerOperation(ctx, "prov_1", op)
	if err != nil {
		t.Fatal(err)
	}
	second, created, err := s.OpenSignerOperation(ctx, "prov_1", op)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("replay should report created=false")
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned a different record: %s vs %s", second.ID, first.ID)
	}
}

func TestOpenSignerOperation_ChangedContentConflicts(t *testing.T) {
	ctx := context.Background()
	s := New()
	if _, _, err := s.OpenSignerOperation(ctx, "prov_1", testSignerOp("prov_1", "key-3")); err != nil {
		t.Fatal(err)
	}
	changed := testSignerOp("prov_1", "key-3")
	changed.NewExecutionSignerID = "signer_DIFFERENT"
	_, _, err := s.OpenSignerOperation(ctx, "prov_1", changed)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}

// TestOpenSignerOperation_ChangedRevocationReasonConflicts proves
// RevocationReasonCode is identity content like every other caller-supplied
// field: it is forwarded verbatim into tos-protocol's RevokeExecutionSigner
// request and hashed into ITS commitment digest, so an idempotency-key
// replay that supplies a different reason must conflict rather than
// silently keeping whatever reason the first call happened to persist.
func TestOpenSignerOperation_ChangedRevocationReasonConflicts(t *testing.T) {
	ctx := context.Background()
	s := New()
	// Build the base value once and copy it for the second call (not two
	// separate testSignerOp(...) invocations) -- NewValidFrom/NewValidUntil
	// are themselves part of the identity content hash, and two calls to
	// the builder made at genuinely different wall-clock instants would
	// already differ on those alone, which would make this test pass for
	// the wrong reason regardless of whether RevocationReasonCode is
	// hashed at all.
	op := testSignerOp("prov_1", "key-reason")
	op.RevocationReasonCode = "rotation"
	if _, _, err := s.OpenSignerOperation(ctx, "prov_1", op); err != nil {
		t.Fatal(err)
	}
	changed := op
	changed.RevocationReasonCode = "compromised"
	_, _, err := s.OpenSignerOperation(ctx, "prov_1", changed)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}

func TestUpdateSignerOperation_AllowsCheckpointAdvance(t *testing.T) {
	ctx := context.Background()
	s := New()
	op, _, err := s.OpenSignerOperation(ctx, "prov_1", testSignerOp("prov_1", "key-4"))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := s.UpdateSignerOperation(ctx, op.ID, func(current domain.ExecutionSignerOperation, exists bool) (domain.ExecutionSignerOperation, error) {
		current.Checkpoint = domain.CheckpointNewAuthorizationPending
		return current, nil
	})
	if err != nil {
		t.Fatalf("UpdateSignerOperation: %v", err)
	}
	if updated.Checkpoint != domain.CheckpointNewAuthorizationPending {
		t.Fatalf("checkpoint = %s, want new_authorization_pending", updated.Checkpoint)
	}
}

func TestUpdateSignerOperation_RejectsIdentityFieldChange(t *testing.T) {
	ctx := context.Background()
	s := New()
	op, _, err := s.OpenSignerOperation(ctx, "prov_1", testSignerOp("prov_1", "key-5"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UpdateSignerOperation(ctx, op.ID, func(current domain.ExecutionSignerOperation, exists bool) (domain.ExecutionSignerOperation, error) {
		current.NewExecutionSignerID = "signer_DIFFERENT"
		return current, nil
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}

// TestLatestCompletedSignerOperationByCapability_NotMaskedByOlderVersionsLaterCompletion
// proves the version filter lives in the query itself: a stuck v1
// operation that a reconciler only finishes recovering AFTER a v2 signer
// was already authorized and completed can have a LATER updated_at than
// v2's own completed operation. Before this fix, the query selected "most
// recently updated completed operation for this capability_id, any
// version" and the version check happened afterward in Go, which resolved
// to the v1 row here, got rejected as the wrong version, and reported no
// current signer at all -- even though the real, current v2 signer sits
// right next to it in the same table.
func TestLatestCompletedSignerOperationByCapability_NotMaskedByOlderVersionsLaterCompletion(t *testing.T) {
	ctx := context.Background()
	s := New()

	newerVersionEarlierCompletion := testSignerOp("prov_1", "key-v2-early")
	newerVersionEarlierCompletion.CapabilityVersion = "2.0.0"
	newerVersionEarlierCompletion.Checkpoint = domain.CheckpointCompleted
	newerVersionEarlierCompletion.UpdatedAt = time.Now().UTC().Add(-time.Hour)
	if _, _, err := s.OpenSignerOperation(ctx, "prov_1", newerVersionEarlierCompletion); err != nil {
		t.Fatal(err)
	}

	olderVersionLaterCompletion := testSignerOp("prov_1", "key-v1-late")
	olderVersionLaterCompletion.CapabilityVersion = "1.0.0"
	olderVersionLaterCompletion.Checkpoint = domain.CheckpointCompleted
	olderVersionLaterCompletion.UpdatedAt = time.Now().UTC()
	if _, _, err := s.OpenSignerOperation(ctx, "prov_1", olderVersionLaterCompletion); err != nil {
		t.Fatal(err)
	}

	got, found, err := s.LatestCompletedSignerOperationByCapability(ctx, "cap_1", "2.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected to find the current version's completed operation")
	}
	if got.ID != newerVersionEarlierCompletion.ID {
		t.Fatalf("got operation %s (version %s), want %s (version 2.0.0) -- masked by the older version's later completion",
			got.ID, got.CapabilityVersion, newerVersionEarlierCompletion.ID)
	}
}

func TestLatestSignerOperationByCapability_TracksMostRecentUpdate(t *testing.T) {
	ctx := context.Background()
	s := New()
	older := testSignerOp("prov_1", "key-older")
	older.CreatedAt = time.Now().UTC().Add(-time.Hour)
	older.UpdatedAt = older.CreatedAt
	if _, _, err := s.OpenSignerOperation(ctx, "prov_1", older); err != nil {
		t.Fatal(err)
	}
	newer, _, err := s.OpenSignerOperation(ctx, "prov_1", testSignerOp("prov_1", "key-newer"))
	if err != nil {
		t.Fatal(err)
	}
	latest, found, err := s.LatestSignerOperationByCapability(ctx, "cap_1")
	if err != nil {
		t.Fatal(err)
	}
	if !found || latest.ID != newer.ID {
		t.Fatalf("latest = %+v, want %s", latest, newer.ID)
	}
}

func TestStaleSignerOperations_OnlyNonTerminalBeforeCutoff(t *testing.T) {
	ctx := context.Background()
	s := New()
	stuck := testSignerOp("prov_1", "key-stuck")
	stuck.Checkpoint = domain.CheckpointReconciling
	stuck.UpdatedAt = time.Now().UTC().Add(-time.Hour)
	if _, _, err := s.OpenSignerOperation(ctx, "prov_1", stuck); err != nil {
		t.Fatal(err)
	}
	fresh, _, err := s.OpenSignerOperation(ctx, "prov_1", testSignerOp("prov_1", "key-fresh"))
	if err != nil {
		t.Fatal(err)
	}

	stale, err := s.StaleSignerOperations(ctx, time.Now().UTC().Add(-30*time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 || stale[0].ID != stuck.ID {
		t.Fatalf("stale = %+v, want only %s (fresh op %s must be excluded)", stale, stuck.ID, fresh.ID)
	}
}
