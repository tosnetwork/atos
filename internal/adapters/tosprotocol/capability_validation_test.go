package toprotocol_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/adapters/tosprotocol"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestQuoteExecution_IncompleteCapabilityIdentityReportedBeforeDeadline
// proves that when a QuoteExecution request is invalid in two independent
// ways at once (an incomplete capability identity AND a non-future
// execution_deadline), the capability-identity problem is what's reported --
// matching this method's behavior before commitCapability's identity guard
// was consolidated out of QuoteExecution's own body. Both checks are pure,
// local, and run before any RPC dial, so this needs no live server: New
// never dials synchronously, and QuoteExecution returns before ever
// touching the (never-connected) underlying RPC clients on this invalid
// input.
func TestQuoteExecution_IncompleteCapabilityIdentityReportedBeforeDeadline(t *testing.T) {
	client, err := toprotocol.New(toprotocol.Config{
		BaseURL: "http://127.0.0.1:1", Insecure: true, BearerToken: "test-token", Store: memory.New(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = client.QuoteExecution(context.Background(), tosai.QuoteExecutionRequest{
		Capability:        domain.Capability{},          // ID/ProviderID/Version all empty
		ExecutionDeadline: time.Now().Add(-time.Minute), // already in the past
		TrustMode:         domain.TrustModeManaged,
	})
	if err == nil {
		t.Fatal("expected an error for a request invalid in two independent ways")
	}
	if !strings.Contains(err.Error(), "capability identity is incomplete") {
		t.Fatalf("error = %q, want the capability-identity problem reported first (before the execution_deadline check)", err.Error())
	}
}
