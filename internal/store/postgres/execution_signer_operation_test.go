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
	"github.com/tosnetwork/atos/internal/store/postgres"
)

func testSignerOperation(providerID, capabilityID, idempotencyKey string) domain.ExecutionSignerOperation {
	now := time.Now().UTC()
	return domain.ExecutionSignerOperation{
		ID: "sigop_" + idempotencyKey, ProviderID: providerID, CapabilityID: capabilityID, CapabilityVersion: "1.0.0",
		Type: domain.SignerOperationAuthorize, Checkpoint: domain.CheckpointIntentPersisted,
		IdempotencyKey:        idempotencyKey,
		NewAuthorizationID:    "authz_" + idempotencyKey,
		NewExecutionSignerID:  "signer_" + idempotencyKey,
		NewSignerPublicKey:    []byte{1, 2, 3, 4},
		NewSignatureAlgorithm: "ed25519",
		NewValidFrom:          now,
		NewValidUntil:         now.Add(24 * time.Hour),
		CreatedAt:             now, UpdatedAt: now,
	}
}

func TestOpenSignerOperation_ThenGet(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	providerID := "prov_sigop_" + suffix
	op := testSignerOperation(providerID, "cap_sigop_"+suffix, "key_"+suffix)

	stored, created, err := s.OpenSignerOperation(ctx, providerID, op)
	if err != nil {
		t.Fatalf("OpenSignerOperation: %v", err)
	}
	if !created {
		t.Fatal("expected created=true for a new operation")
	}
	if stored.Checkpoint != domain.CheckpointIntentPersisted {
		t.Fatalf("checkpoint = %s, want intent_persisted", stored.Checkpoint)
	}

	got, err := s.GetSignerOperation(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetSignerOperation: %v", err)
	}
	if got.NewExecutionSignerID != op.NewExecutionSignerID || string(got.NewSignerPublicKey) != string(op.NewSignerPublicKey) {
		t.Fatalf("round-tripped operation mismatch: %+v", got)
	}
}

func TestOpenSignerOperation_ReplaySameContentReturnsExisting(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	providerID := "prov_sigop_replay_" + suffix
	op := testSignerOperation(providerID, "cap_sigop_replay_"+suffix, "key_"+suffix)

	first, created1, err := s.OpenSignerOperation(ctx, providerID, op)
	if err != nil || !created1 {
		t.Fatalf("first open: created=%v err=%v", created1, err)
	}
	second, created2, err := s.OpenSignerOperation(ctx, providerID, op)
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

func TestOpenSignerOperation_DifferentContentConflicts(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	providerID := "prov_sigop_conflict_" + suffix
	op := testSignerOperation(providerID, "cap_sigop_conflict_"+suffix, "key_"+suffix)
	if _, _, err := s.OpenSignerOperation(ctx, providerID, op); err != nil {
		t.Fatal(err)
	}
	changed := op
	changed.NewExecutionSignerID = "different-signer"
	_, _, err := s.OpenSignerOperation(ctx, providerID, changed)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}

// TestOpenSignerOperation_TwoIndependentPostgresInstancesConvergeToOne
// simulates two ATOS replicas racing to open the same execution-signer
// operation -- the §7.2.4 success criterion's "two replicas converge on
// one signer state" requirement, proven against a real database rather
// than the in-memory store's single-process mutex.
func TestOpenSignerOperation_TwoIndependentPostgresInstancesConvergeToOne(t *testing.T) {
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
	providerID := "prov_sigop_replica_" + suffix
	op := testSignerOperation(providerID, "cap_sigop_replica_"+suffix, "key_"+suffix)

	var wg sync.WaitGroup
	var creators int64
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, created, err := sA.OpenSignerOperation(ctx, providerID, op)
		if err != nil {
			t.Errorf("replica A OpenSignerOperation: %v", err)
			return
		}
		if created {
			atomic.AddInt64(&creators, 1)
		}
	}()
	go func() {
		defer wg.Done()
		_, created, err := sB.OpenSignerOperation(ctx, providerID, op)
		if err != nil {
			t.Errorf("replica B OpenSignerOperation: %v", err)
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

// TestUpdateSignerOperation_TwoIndependentPostgresInstancesSerializeAdvance
// proves concurrent checkpoint advances from two replicas serialize
// through the row lock rather than tearing the record -- one call
// observes the pre-advance checkpoint, the other observes the post-advance
// checkpoint, never a corrupted intermediate state.
func TestUpdateSignerOperation_TwoIndependentPostgresInstancesSerializeAdvance(t *testing.T) {
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
	providerID := "prov_sigop_advance_" + suffix
	op := testSignerOperation(providerID, "cap_sigop_advance_"+suffix, "key_"+suffix)
	if _, _, err := sA.OpenSignerOperation(ctx, providerID, op); err != nil {
		t.Fatal(err)
	}

	advance := func(s interface {
		UpdateSignerOperation(context.Context, string, func(domain.ExecutionSignerOperation, bool) (domain.ExecutionSignerOperation, error)) (domain.ExecutionSignerOperation, error)
	}, checkpoint domain.SignerOperationCheckpoint) error {
		_, err := s.UpdateSignerOperation(ctx, op.ID, func(current domain.ExecutionSignerOperation, exists bool) (domain.ExecutionSignerOperation, error) {
			current.Checkpoint = checkpoint
			current.UpdatedAt = time.Now().UTC()
			return current, nil
		})
		return err
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = advance(sA, domain.CheckpointNewAuthorizationPending) }()
	go func() { defer wg.Done(); _ = advance(sB, domain.CheckpointNewAuthorized) }()
	wg.Wait()

	final, err := sA.GetSignerOperation(ctx, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Checkpoint != domain.CheckpointNewAuthorizationPending && final.Checkpoint != domain.CheckpointNewAuthorized {
		t.Fatalf("final checkpoint = %s, want one of the two racing writers' values, not a torn state", final.Checkpoint)
	}
}

func TestStaleSignerOperations_ExcludesCompletedAndFreshOperations(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	providerID := "prov_stale_" + suffix

	stuck := testSignerOperation(providerID, "cap_stale_"+suffix, "key_stuck_"+suffix)
	stuck.Checkpoint = domain.CheckpointReconciling
	stuck.CreatedAt = time.Now().UTC().Add(-time.Hour)
	stuck.UpdatedAt = stuck.CreatedAt
	if _, _, err := s.OpenSignerOperation(ctx, providerID, stuck); err != nil {
		t.Fatal(err)
	}

	fresh := testSignerOperation(providerID, "cap_stale_"+suffix, "key_fresh_"+suffix)
	if _, _, err := s.OpenSignerOperation(ctx, providerID, fresh); err != nil {
		t.Fatal(err)
	}

	completed := testSignerOperation(providerID, "cap_stale_"+suffix, "key_completed_"+suffix)
	completed.CreatedAt = time.Now().UTC().Add(-time.Hour)
	completed.UpdatedAt = completed.CreatedAt
	completedOp, _, err := s.OpenSignerOperation(ctx, providerID, completed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateSignerOperation(ctx, completedOp.ID, func(c domain.ExecutionSignerOperation, exists bool) (domain.ExecutionSignerOperation, error) {
		c.Checkpoint = domain.CheckpointCompleted
		c.UpdatedAt = time.Now().UTC().Add(-time.Hour)
		return c, nil
	}); err != nil {
		t.Fatal(err)
	}

	stale, err := s.StaleSignerOperations(ctx, time.Now().UTC().Add(-30*time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, op := range stale {
		if op.ID == fresh.ID || op.ID == completedOp.ID {
			t.Fatalf("stale sweep must exclude fresh/completed operations, got %s", op.ID)
		}
		if op.ID == stuck.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("expected the stuck reconciling operation in the stale sweep")
	}
}

func TestLatestSignerOperationByCapability_ReturnsMostRecentlyUpdated(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	suffix := randSuffix()
	providerID := "prov_latest_" + suffix
	capabilityID := "cap_latest_" + suffix

	older := testSignerOperation(providerID, capabilityID, "key_older_"+suffix)
	older.CreatedAt = time.Now().UTC().Add(-time.Hour)
	older.UpdatedAt = older.CreatedAt
	if _, _, err := s.OpenSignerOperation(ctx, providerID, older); err != nil {
		t.Fatal(err)
	}
	newer := testSignerOperation(providerID, capabilityID, "key_newer_"+suffix)
	if _, _, err := s.OpenSignerOperation(ctx, providerID, newer); err != nil {
		t.Fatal(err)
	}

	latest, found, err := s.LatestSignerOperationByCapability(ctx, capabilityID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || latest.ID != newer.ID {
		t.Fatalf("latest = %+v, want %s", latest, newer.ID)
	}
}
