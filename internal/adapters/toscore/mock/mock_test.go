package mock

import (
	"context"
	"testing"

	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestCreatePrincipalBinding_RetryWithSameKeyReplaysCreatedTrue proves a
// retry with the SAME idempotency_key against an already-existing binding
// (the crash-recovery scenario: driveBind's CreatePrincipalBinding call
// succeeded, but the caller crashed before advancing its own operation
// checkpoint, so the reconciler retries driveBind with the identical key)
// reports created=true, matching the real server's atomicMutation cache
// (which replays the ORIGINAL response) rather than re-deriving created
// from "does a binding currently exist," which would wrongly report false
// for the very call that created it.
func TestCreatePrincipalBinding_RetryWithSameKeyReplaysCreatedTrue(t *testing.T) {
	ctx := context.Background()
	core := New(memory.New())
	core.SeedAgentIdentity("agt_retry")

	first, created, err := core.CreatePrincipalBinding(ctx, "caller", "idem-1", "prn_retry", "agt_retry")
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first call must report created=true")
	}

	// Simulates the crash-recovery retry: SAME idempotency_key, called
	// again because the caller's own checkpoint update never landed.
	second, created, err := core.CreatePrincipalBinding(ctx, "caller", "idem-1", "prn_retry", "agt_retry")
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("a retry with the SAME idempotency_key must still report created=true, not re-derive false from current state")
	}
	if second.BindingRef != first.BindingRef {
		t.Fatalf("binding_ref changed across retry: %q vs %q", first.BindingRef, second.BindingRef)
	}

	// A DIFFERENT idempotency_key naming the same principal+agent is a
	// genuine no-op rebind, correctly created=false.
	_, created, err = core.CreatePrincipalBinding(ctx, "caller", "idem-2", "prn_retry", "agt_retry")
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("a DIFFERENT idempotency_key rebinding to the same agent must report created=false")
	}
}
