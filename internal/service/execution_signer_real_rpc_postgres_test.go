// Integration test against a real Postgres AND a real in-process
// tos-protocol ConnectRPC server -- skipped unless ATOS_TEST_DATABASE_URL
// is set. Run with:
//
//	ATOS_TEST_DATABASE_URL="postgres://user@localhost:5432/atos_test?sslmode=disable" go test ./internal/service/... -run TestExecutionSignerService_AuthorizeRotateRevoke_RealConnectRPC
package service_test

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/tosprotocol"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
	"github.com/tosnetwork/tos-protocol/pkg/atosrpc"
)

// TestExecutionSignerService_AuthorizeRotateRevoke_RealConnectRPC drives
// ExecutionSignerService's full authorize -> rotate -> revoke durable
// checkpoint sequence (atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.2.2)
// through a REAL tosprotocol.Client talking to a real in-process
// tos-protocol ConnectRPC server, backed by real Postgres -- not the
// toscoremock.Core every other ExecutionSignerService test in this
// package uses. This is what actually closes §7.2's "real ConnectRPC
// tos-protocol, not only a Go-interface mock" gap note for the durable
// checkpoint journal this phase built: internal/adapters/tosprotocol's
// own signer_integration_test.go already proves the raw RPC wire format
// in isolation; this test proves atos's OWN crash-recoverable
// orchestration on top of it behaves identically against a real server as
// it does against the mock in every other test in this package.
func TestExecutionSignerService_AuthorizeRotateRevoke_RealConnectRPC(t *testing.T) {
	url := os.Getenv("ATOS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx := context.Background()
	st, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	protocolServer, err := atosrpc.Open(atosrpc.Config{
		StatePath:   filepath.Join(t.TempDir(), "atos-rpc-signer-service.db"),
		BearerToken: "signer-service-integration-secret",
		Authority:   atosrpc.NewLocalAuthority("tos-local"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer protocolServer.Close()
	httpServer := httptest.NewServer(protocolServer.Handler())
	defer httpServer.Close()

	client, err := toprotocol.New(toprotocol.Config{
		BaseURL: httpServer.URL, BearerToken: "signer-service-integration-secret",
		Insecure: true, Timeout: 20 * time.Second, Store: st,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	providerID := "prov_sig_rpc_svc_" + time.Now().UTC().Format("20060102T150405.000000000")
	capabilities := service.NewCapabilityService(st)
	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Signer Service Real RPC Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeVerified},
		IdempotencyKey:      "register-" + providerID,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := client.RegisterProvider(ctx, providerID, cap); err != nil {
		t.Fatalf("RegisterProvider (commit capability manifest to tos-protocol): %v", err)
	}

	signers := service.NewExecutionSignerService(st, client, capabilities)
	now := time.Now().UTC()

	authOp, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: providerID, CapabilityID: cap.ID,
		ExecutionSignerID: "signer-old-rpc-svc", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(24 * time.Hour),
		IdempotencyKey: "authz-rpc-svc-1",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if authOp.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("authorize checkpoint = %s, want completed: %+v", authOp.Checkpoint, authOp)
	}
	_, signerID, found, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || signerID != "signer-old-rpc-svc" {
		t.Fatalf("CurrentSigner after authorize = (%s, %v), want signer-old-rpc-svc/true", signerID, found)
	}

	rotateOp, err := signers.Rotate(ctx, service.RotateSignerInput{
		ProviderID: providerID, CapabilityID: cap.ID,
		NewExecutionSignerID: "signer-new-rpc-svc", NewSignerPublicKey: testSignerKey(t), NewSignatureAlgorithm: "ed25519",
		NewValidFrom: now.Add(-time.Minute), NewValidUntil: now.Add(24 * time.Hour),
		RevocationReasonCode: "rotation", IdempotencyKey: "rotate-rpc-svc-1",
	})
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if rotateOp.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("rotate checkpoint = %s, want completed: %+v", rotateOp.Checkpoint, rotateOp)
	}
	_, signerID, found, err = signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || signerID != "signer-new-rpc-svc" {
		t.Fatalf("CurrentSigner after rotate = (%s, %v), want signer-new-rpc-svc/true", signerID, found)
	}

	revokeOp, err := signers.Revoke(ctx, service.RevokeSignerInput{
		ProviderID: providerID, CapabilityID: cap.ID,
		ReasonCode: "test-teardown", IdempotencyKey: "revoke-rpc-svc-1",
	})
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if revokeOp.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("revoke checkpoint = %s, want completed: %+v", revokeOp.Checkpoint, revokeOp)
	}
	_, _, found, err = signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected no current signer after revoke")
	}
}
