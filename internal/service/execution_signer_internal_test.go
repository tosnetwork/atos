package service

import (
	"context"
	"testing"
	"time"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestAdvance_StaleExpectedFromIsANoOp proves advance's compare-and-swap: a
// caller that decides a transition based on an outdated local snapshot of
// an operation's checkpoint must not be able to overwrite a checkpoint
// another driver has already legitimately advanced past. Before this fix,
// advance only refused to write past Terminal() -- it ignored what
// checkpoint the caller thought it was advancing FROM entirely, so any
// slow driver holding a stale snapshot could stomp a faster driver's real
// progress (or even drag a further-along operation backwards), even
// though UpdateSignerOperation's own per-row lock already makes each
// individual advance call internally atomic.
//
// This is a white-box test (package service, not service_test) because
// advance is unexported -- reproducing this race deterministically through
// the public API alone would require winning a real goroutine race.
func TestAdvance_StaleExpectedFromIsANoOp(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := NewExecutionSignerService(st, core, capabilities)

	cap, err := capabilities.Register(ctx, RegisterCapabilityInput{
		ProviderID: "agt_sig_cas", Name: "CAS Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-agt_sig_cas",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	now := time.Now().UTC()
	op := domain.ExecutionSignerOperation{
		ID: "sigop_cas_test", ProviderID: "agt_sig_cas",
		CapabilityID: cap.ID, CapabilityVersion: cap.Version,
		Type: domain.SignerOperationAuthorize, Checkpoint: domain.CheckpointIntentPersisted,
		IdempotencyKey:        "idem-cas-test",
		NewAuthorizationID:    "authz_cas_test",
		NewExecutionSignerID:  "signer-cas-test",
		NewSignerPublicKey:    []byte("0123456789abcdef0123456789abcdef"),
		NewSignatureAlgorithm: "ed25519",
		NewValidFrom:          now.Add(-time.Minute), NewValidUntil: now.Add(24 * time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := st.OpenSignerOperation(ctx, "agt_sig_cas", op); err != nil {
		t.Fatalf("OpenSignerOperation: %v", err)
	}

	// The "real" driver correctly advances from intent_persisted all the
	// way to new_authorized -- simulating a faster driver that has already
	// made real, durable progress (including recording a NewAuthorizationRef
	// no later step should ever discard).
	if _, err := signers.advance(ctx, op.ID, domain.CheckpointIntentPersisted, domain.CheckpointNewAuthorizationPending, "", ""); err != nil {
		t.Fatalf("advance to new_authorization_pending: %v", err)
	}
	if _, err := signers.advance(ctx, op.ID, domain.CheckpointNewAuthorizationPending, domain.CheckpointNewAuthorized, "ref-real", ""); err != nil {
		t.Fatalf("advance to new_authorized: %v", err)
	}

	// A stale driver that only ever observed intent_persisted (e.g. it read
	// the operation before the real driver made any of the progress above)
	// now tries to advance from that stale snapshot straight to completed.
	// Before the fix this would have silently overwritten new_authorized
	// back down to completed, skipping cutover_pending/old_revocation_pending
	// entirely and discarding the real driver's NewAuthorizationRef.
	staleResult, err := signers.advance(ctx, op.ID, domain.CheckpointIntentPersisted, domain.CheckpointCompleted, "", "")
	if err != nil {
		t.Fatalf("stale advance: %v", err)
	}
	if staleResult.Checkpoint != domain.CheckpointNewAuthorized {
		t.Fatalf("stale advance corrupted the checkpoint: got %s, want new_authorized (the real driver's actual progress, untouched)", staleResult.Checkpoint)
	}
	if staleResult.NewAuthorizationRef != "ref-real" {
		t.Fatalf("stale advance discarded the real driver's authorization ref: got %q", staleResult.NewAuthorizationRef)
	}

	stored, err := st.GetSignerOperation(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetSignerOperation: %v", err)
	}
	if stored.Checkpoint != domain.CheckpointNewAuthorized {
		t.Fatalf("persisted checkpoint = %s, want new_authorized -- the stale advance call must not have reached storage", stored.Checkpoint)
	}
}
