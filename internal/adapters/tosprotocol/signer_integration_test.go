package toprotocol_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/adapters/tosprotocol"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
	"github.com/tosnetwork/tos-protocol/pkg/atosrpc"
)

// newSignerIntegrationClient stands up a real in-process tos-protocol edge
// server (atosrpc.Open -- the same exported, production constructor
// TestATOSConnectRPCManagedLifecycle uses) and connects a real
// tosprotocol.Client to it. Unlike that test, Worker/Router are
// deliberately omitted: AuthorizeExecutionSigner/RevokeExecutionSigner/
// ResolveExecutionSignerAuthorization never touch them (atosrpc.Config
// documents both as optional), so this is the minimal real-RPC harness
// that actually exercises the previously-untested signer wire path --
// see IMPLEMENTATION_ROADMAP.md §7.2's now-closed "real ConnectRPC
// variant" gap note.
func newSignerIntegrationClient(t *testing.T) *toprotocol.Client {
	t.Helper()
	protocolServer, err := atosrpc.Open(atosrpc.Config{
		StatePath:   filepath.Join(t.TempDir(), "atos-rpc-signer.db"),
		BearerToken: "signer-integration-secret",
		Authority:   atosrpc.NewLocalAuthority("tos-local"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = protocolServer.Close() })
	httpServer := httptest.NewServer(protocolServer.Handler())
	t.Cleanup(httpServer.Close)

	client, err := toprotocol.New(toprotocol.Config{
		BaseURL: httpServer.URL, BearerToken: "signer-integration-secret",
		Insecure: true, Timeout: 20 * time.Second, Store: memory.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// registerSignerTestCapability registers a real Capability through
// CapabilityService (the same path every other Capability in this
// codebase goes through, giving it a real ManifestCommitment) and commits
// it to the real tos-protocol server via RegisterProvider -- required
// because AuthorizeExecutionSigner's server-side handler rejects a signer
// for any (provider, capability, version) it does not already know is
// owned by that provider (CAPABILITY_OWNERSHIP_FAILED).
func registerSignerTestCapability(t *testing.T, ctx context.Context, client *toprotocol.Client, idempotencyKey string) domain.Capability {
	t.Helper()
	capabilities := service.NewCapabilityService(memory.New())
	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_signer_rpc_provider", Name: "Signer RPC Test", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeVerified},
		IdempotencyKey:      idempotencyKey,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := client.RegisterProvider(ctx, cap.ProviderID, cap); err != nil {
		t.Fatalf("RegisterProvider (commit capability manifest to tos-protocol): %v", err)
	}
	return cap
}

func testSignerAuthorizeRequest(cap domain.Capability, authorizationID string, validFrom time.Time) toscore.AuthorizeExecutionSignerRequest {
	return toscore.AuthorizeExecutionSignerRequest{
		AuthorizationID: authorizationID, ProviderID: cap.ProviderID,
		CapabilityID: cap.ID, CapabilityVersion: cap.Version,
		ExecutionSignerID: "sig_rpc_1", SignerPublicKey: []byte("0123456789abcdef0123456789abcdef"),
		SignatureAlgorithm: "ed25519",
		ValidFrom:          validFrom, ValidUntil: validFrom.Add(24 * time.Hour),
	}
}

// TestATOSConnectRPCAuthorizeExecutionSigner_CreatesAndReplaysIdempotently
// proves the real wire format for AuthorizeExecutionSigner against a real
// tos-protocol server: first call creates, and a retry with the exact same
// AuthorizationID converges on the identical durable authorization (same
// AuthorizationRef) rather than minting a second, divergent one. The
// retry's created flag is verified true here too -- a literal retry with
// the same caller_id+idempotency_key replays the ORIGINAL response
// verbatim at the RPC transport layer, so created keeps its original
// value; see toscore.Core.AuthorizeExecutionSigner's doc comment for why
// this codebase never branches on it.
func TestATOSConnectRPCAuthorizeExecutionSigner_CreatesAndReplaysIdempotently(t *testing.T) {
	ctx := context.Background()
	client := newSignerIntegrationClient(t)
	cap := registerSignerTestCapability(t, ctx, client, "register-signer-rpc-1")
	validFrom := time.Now().UTC()
	req := testSignerAuthorizeRequest(cap, "authz_rpc_1", validFrom)

	first, created, err := client.AuthorizeExecutionSigner(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first AuthorizeExecutionSigner call reported created=false")
	}
	if first.ExecutionSignerID != req.ExecutionSignerID || first.Revoked {
		t.Fatalf("unexpected authorization: %+v", first)
	}

	replay, created, err := client.AuthorizeExecutionSigner(ctx, req)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if !created {
		t.Fatal("retry with the same AuthorizationID did not replay the original created=true response")
	}
	if replay.AuthorizationRef != first.AuthorizationRef || replay.ExecutionSignerID != first.ExecutionSignerID {
		t.Fatalf("replay diverged from original -- a second, divergent authorization was minted: %+v vs %+v", replay, first)
	}
}

// TestATOSConnectRPCRevokeExecutionSigner_RevokesAndReplaysIdempotently
// proves the real wire format for RevokeExecutionSigner: revoking an
// authorized signer succeeds (revoked=true), and replaying the revoke
// against the same AuthorizationID converges on the same revoked
// authorization (same RevocationRef) without error, rather than failing on
// an already-revoked authorization. revoked stays true on the replay too
// -- tos-protocol's RevokeExecutionSigner has no revoked=false path once a
// signer has ever been revoked; see toscore.Core's doc comment.
func TestATOSConnectRPCRevokeExecutionSigner_RevokesAndReplaysIdempotently(t *testing.T) {
	ctx := context.Background()
	client := newSignerIntegrationClient(t)
	cap := registerSignerTestCapability(t, ctx, client, "register-signer-rpc-2")
	validFrom := time.Now().UTC()
	req := testSignerAuthorizeRequest(cap, "authz_rpc_2", validFrom)
	if _, _, err := client.AuthorizeExecutionSigner(ctx, req); err != nil {
		t.Fatal(err)
	}

	revoked, revokedFlag, err := client.RevokeExecutionSigner(ctx, toscore.RevokeExecutionSignerRequest{
		AuthorizationID: req.AuthorizationID, ProviderID: req.ProviderID, ReasonCode: "rotation",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !revokedFlag {
		t.Fatal("first RevokeExecutionSigner call reported revoked=false")
	}
	if !revoked.Revoked {
		t.Fatalf("returned authorization not marked revoked: %+v", revoked)
	}

	replayRevoked, replayFlag, err := client.RevokeExecutionSigner(ctx, toscore.RevokeExecutionSignerRequest{
		AuthorizationID: req.AuthorizationID, ProviderID: req.ProviderID, ReasonCode: "rotation",
	})
	if err != nil {
		t.Fatalf("idempotent revoke replay: %v", err)
	}
	if !replayFlag {
		t.Fatal("replay of an already-revoked authorization reported revoked=false")
	}
	if !replayRevoked.Revoked {
		t.Fatalf("replay result not marked revoked: %+v", replayRevoked)
	}
}

// TestATOSConnectRPCResolveExecutionSignerAuthorization proves the real
// wire format across the signer's full lifecycle: not found before
// authorization, authorized once granted, and no longer authorized
// (found=false, matching the client's "currently authorized" contract --
// see tosprotocol.Client.ResolveExecutionSignerAuthorization's doc
// comment) after revocation.
func TestATOSConnectRPCResolveExecutionSignerAuthorization(t *testing.T) {
	ctx := context.Background()
	client := newSignerIntegrationClient(t)
	cap := registerSignerTestCapability(t, ctx, client, "register-signer-rpc-3")
	validFrom := time.Now().UTC()
	req := testSignerAuthorizeRequest(cap, "authz_rpc_3", validFrom)

	_, found, err := client.ResolveExecutionSignerAuthorization(ctx, req.ProviderID, req.CapabilityID, req.CapabilityVersion, req.ExecutionSignerID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("resolved an authorization before one was ever authorized")
	}

	if _, _, err := client.AuthorizeExecutionSigner(ctx, req); err != nil {
		t.Fatal(err)
	}
	resolved, found, err := client.ResolveExecutionSignerAuthorization(ctx, req.ProviderID, req.CapabilityID, req.CapabilityVersion, req.ExecutionSignerID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected the just-authorized signer to resolve as currently authorized")
	}
	if resolved.ExecutionSignerID != req.ExecutionSignerID {
		t.Fatalf("resolved wrong signer: %+v", resolved)
	}

	if _, _, err := client.RevokeExecutionSigner(ctx, toscore.RevokeExecutionSignerRequest{
		AuthorizationID: req.AuthorizationID, ProviderID: req.ProviderID, ReasonCode: "rotation",
	}); err != nil {
		t.Fatal(err)
	}
	_, found, err = client.ResolveExecutionSignerAuthorization(ctx, req.ProviderID, req.CapabilityID, req.CapabilityVersion, req.ExecutionSignerID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("resolved a revoked authorization as currently authorized")
	}
}
