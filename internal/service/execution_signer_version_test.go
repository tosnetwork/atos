package service_test

import (
	"context"
	"testing"
	"time"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestCurrentSigner_DoesNotCarryAcrossCapabilityVersionBump is atos-spec's
// §31 capability-version-change scenario applied to execution-signer
// currency: a signer authorized for Capability version N must NOT be
// treated as the current signer once the Capability moves to version
// N+1, mirroring the same version-scoping rule already proven for
// certification (TestHealthService_Availability_CertificationDoesNotCarryAcrossCapabilityVersionBump)
// and stated explicitly in docs/IMPLEMENTATION_ROADMAP.md §7.2.0: "since
// it is itself version-scoped ... signer authorization currency [is reset
// by a version bump]". The old version's completed operation remains
// part of that operation's own auditable history (never mutated) --
// only "is this the CURRENT signer" changes.
func TestCurrentSigner_DoesNotCarryAcrossCapabilityVersionBump(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	signers := service.NewExecutionSignerService(st, core, capabilities)

	cap := registerSignerTestCapability(t, capabilities, "agt_sig_version", domain.TrustModeManaged)
	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_version", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-v1", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-version-1",
	}); err != nil {
		t.Fatal(err)
	}

	_, signerID, found, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || signerID != "signer-v1" {
		t.Fatalf("test setup invalid: current signer before the version bump = %q found=%v, want signer-v1", signerID, found)
	}

	// Bump the Capability's version via an unrelated field (pricing) --
	// deliberately not touching signer authorization at all.
	updated, err := capabilities.Update(ctx, cap.ID, "agt_sig_version", map[string]any{
		"pricing": map[string]any{
			"model":      "fixed",
			"price_hint": map[string]any{"amount": "2.00", "currency": "USD"},
		},
	}, "update-sig-version-price")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Version == cap.Version {
		t.Fatal("test setup invalid: capability version must change after the price update")
	}

	_, _, foundAfterBump, err := signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if foundAfterBump {
		t.Fatal("signer-v1's authorization must not be treated as current for the new capability version -- it was authorized for the superseded version only")
	}

	// The old operation's own history must remain intact (auditable),
	// even though it's no longer "current".
	oldOp, found, err := signers.Status(ctx, cap.ID)
	if err != nil || !found {
		t.Fatalf("Status: found=%v err=%v", found, err)
	}
	if oldOp.NewExecutionSignerID != "signer-v1" || oldOp.Checkpoint != domain.CheckpointCompleted {
		t.Fatalf("historical operation must remain unmutated: %+v", oldOp)
	}

	// Authorizing a signer for the NEW version must succeed independently
	// (this is the corrected, expected path forward -- not an error).
	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "agt_sig_version", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-v2", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
		IdempotencyKey: "authz-version-2",
	}); err != nil {
		t.Fatalf("Authorize for the new version: %v", err)
	}
	_, signerID, found, err = signers.CurrentSigner(ctx, cap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || signerID != "signer-v2" {
		t.Fatalf("current signer after re-authorizing for the new version = %q found=%v, want signer-v2", signerID, found)
	}
}
