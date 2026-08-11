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

// tosBackedAuthorityFixture wires the full dependency chain
// TOSBackedActivationAuthority.Evaluate needs: a capability requesting
// verified, a bound-and-network-matched provider identity, and a current,
// live-verifiable execution signer -- the exact positive path the Phase 4A
// brief's acceptance test requires (create/resolve identity -> bind
// provider -> register/resolve ownership -> commit manifest -> authorize
// signer -> evaluate -> active).
func tosBackedAuthorityFixture(t *testing.T, providerID string) (*service.TOSBackedActivationAuthority, domain.Capability, *toscoremock.Core, *service.ExecutionSignerService) {
	t.Helper()
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	capabilities.WithManifestAnchor(core)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	identities := service.NewIdentityBindingService(st, core)

	cap := registerSignerTestCapability(t, capabilities, providerID, domain.TrustModeManaged, domain.TrustModeVerified)

	core.SeedAgentIdentity("agt_" + providerID)
	if _, err := identities.Bind(ctx, service.BindIdentityInput{
		PrincipalID: providerID, AgentID: "agt_" + providerID, IdempotencyKey: "bind-" + providerID,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	now := time.Now().UTC()
	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: providerID, CapabilityID: cap.ID,
		ExecutionSignerID: "signer-" + providerID, SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(24 * time.Hour),
		IdempotencyKey: "authz-" + providerID,
	}); err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	authority := service.NewTOSBackedActivationAuthority(core, st, signers)
	return authority, cap, core, signers
}

func TestTOSBackedActivationAuthority_GoldenPathGrantsVerified(t *testing.T) {
	authority, cap, _, _ := tosBackedAuthorityFixture(t, "prn_authority_golden")
	granted, reasonCode, err := authority.Evaluate(context.Background(), "prn_authority_golden", cap.ID, cap.Version, domain.TrustModeVerified)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if !granted {
		t.Fatalf("granted = false, reasonCode = %q, want granted=true", reasonCode)
	}
	if reasonCode != "" {
		t.Fatalf("granted=true carried a non-empty reasonCode: %q", reasonCode)
	}
}

func TestTOSBackedActivationAuthority_NativeAlwaysDenied(t *testing.T) {
	authority, cap, _, _ := tosBackedAuthorityFixture(t, "prn_authority_native")
	granted, reasonCode, err := authority.Evaluate(context.Background(), "prn_authority_native", cap.ID, cap.Version, domain.TrustModeNative)
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != service.ReasonNativeNotSupported {
		t.Fatalf("got granted=%v reasonCode=%q, want denied with %q", granted, reasonCode, service.ReasonNativeNotSupported)
	}
}

func TestTOSBackedActivationAuthority_UnconfiguredNetworkDenies(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st) // network never set -- Network() returns ""
	capabilities.WithManifestAnchor(core)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "prn_authority_nonet", domain.TrustModeManaged, domain.TrustModeVerified)
	authority := service.NewTOSBackedActivationAuthority(core, st, signers)

	granted, reasonCode, err := authority.Evaluate(ctx, "prn_authority_nonet", cap.ID, cap.Version, domain.TrustModeVerified)
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != service.ReasonNetworkMismatch {
		t.Fatalf("got granted=%v reasonCode=%q, want denied with %q for an unconfigured network", granted, reasonCode, service.ReasonNetworkMismatch)
	}
}

func TestTOSBackedActivationAuthority_UnboundProviderDenies(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	capabilities.WithManifestAnchor(core)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	cap := registerSignerTestCapability(t, capabilities, "prn_authority_unbound", domain.TrustModeManaged, domain.TrustModeVerified)
	authority := service.NewTOSBackedActivationAuthority(core, st, signers)

	granted, reasonCode, err := authority.Evaluate(ctx, "prn_authority_unbound", cap.ID, cap.Version, domain.TrustModeVerified)
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != service.ReasonProviderIdentityNotBound {
		t.Fatalf("got granted=%v reasonCode=%q, want denied with %q", granted, reasonCode, service.ReasonProviderIdentityNotBound)
	}
}

func TestTOSBackedActivationAuthority_RevokedProviderIdentityDenies(t *testing.T) {
	authority, cap, core, _ := tosBackedAuthorityFixture(t, "prn_authority_revoked")
	ctx := context.Background()
	if _, _, _, err := core.RevokePrincipalBinding(ctx, "atos-gateway", "revoke-"+cap.ProviderID, "prn_authority_revoked", "test-revocation"); err != nil {
		t.Fatal(err)
	}

	granted, reasonCode, err := authority.Evaluate(ctx, "prn_authority_revoked", cap.ID, cap.Version, domain.TrustModeVerified)
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != service.ReasonProviderIdentityRevoked {
		t.Fatalf("got granted=%v reasonCode=%q, want denied with %q", granted, reasonCode, service.ReasonProviderIdentityRevoked)
	}
}

func TestTOSBackedActivationAuthority_NetworkMismatchDenies(t *testing.T) {
	authority, cap, core, _ := tosBackedAuthorityFixture(t, "prn_authority_wrongnet")
	// Simulate the underlying tos-protocol connection failing over to (or
	// being reconfigured for) a different network than the one this
	// binding was anchored on.
	core.SetNetwork("tos-other-network")

	granted, reasonCode, err := authority.Evaluate(context.Background(), "prn_authority_wrongnet", cap.ID, cap.Version, domain.TrustModeVerified)
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != service.ReasonNetworkMismatch {
		t.Fatalf("got granted=%v reasonCode=%q, want denied with %q", granted, reasonCode, service.ReasonNetworkMismatch)
	}
}

func TestTOSBackedActivationAuthority_WrongOwnerDenies(t *testing.T) {
	authority, cap, core, _ := tosBackedAuthorityFixture(t, "prn_authority_owner")
	ctx := context.Background()
	// A second, independently bound principal impersonating ownership of
	// someone else's capability.
	core.SeedAgentIdentity("agt_impostor")
	if _, _, err := core.CreatePrincipalBinding(ctx, "atos-gateway", "bind-impostor", "prn_authority_impostor", "agt_impostor"); err != nil {
		t.Fatal(err)
	}

	granted, reasonCode, err := authority.Evaluate(ctx, "prn_authority_impostor", cap.ID, cap.Version, domain.TrustModeVerified)
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != service.ReasonCapabilityOwnershipInvalid {
		t.Fatalf("got granted=%v reasonCode=%q, want denied with %q", granted, reasonCode, service.ReasonCapabilityOwnershipInvalid)
	}
}

func TestTOSBackedActivationAuthority_WrongVersionDenies(t *testing.T) {
	authority, cap, _, _ := tosBackedAuthorityFixture(t, "prn_authority_version")
	granted, reasonCode, err := authority.Evaluate(context.Background(), "prn_authority_version", cap.ID, "9.9.9-does-not-exist", domain.TrustModeVerified)
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != service.ReasonManifestNotCommitted {
		t.Fatalf("got granted=%v reasonCode=%q, want denied with %q for a version mismatch", granted, reasonCode, service.ReasonManifestNotCommitted)
	}
}

func TestTOSBackedActivationAuthority_UnanchoredManifestDenies(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st) // deliberately NOT .WithManifestAnchor -- manifest never anchored
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	signers := service.NewExecutionSignerService(st, core, capabilities)
	identities := service.NewIdentityBindingService(st, core)
	cap := registerSignerTestCapability(t, capabilities, "prn_authority_unanchored", domain.TrustModeManaged, domain.TrustModeVerified)

	core.SeedAgentIdentity("agt_prn_authority_unanchored")
	if _, err := identities.Bind(ctx, service.BindIdentityInput{
		PrincipalID: "prn_authority_unanchored", AgentID: "agt_prn_authority_unanchored", IdempotencyKey: "bind-unanchored",
	}); err != nil {
		t.Fatal(err)
	}
	authority := service.NewTOSBackedActivationAuthority(core, st, signers)

	granted, reasonCode, err := authority.Evaluate(ctx, "prn_authority_unanchored", cap.ID, cap.Version, domain.TrustModeVerified)
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != service.ReasonManifestNotCommitted {
		t.Fatalf("got granted=%v reasonCode=%q, want denied with %q for an unanchored capability", granted, reasonCode, service.ReasonManifestNotCommitted)
	}
}

func TestTOSBackedActivationAuthority_NoSignerDenies(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	capabilities.WithManifestAnchor(core)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	identities := service.NewIdentityBindingService(st, core)
	cap := registerSignerTestCapability(t, capabilities, "prn_authority_nosigner", domain.TrustModeManaged, domain.TrustModeVerified)

	core.SeedAgentIdentity("agt_prn_authority_nosigner")
	if _, err := identities.Bind(ctx, service.BindIdentityInput{
		PrincipalID: "prn_authority_nosigner", AgentID: "agt_prn_authority_nosigner", IdempotencyKey: "bind-nosigner",
	}); err != nil {
		t.Fatal(err)
	}
	authority := service.NewTOSBackedActivationAuthority(core, st, signers)

	granted, reasonCode, err := authority.Evaluate(ctx, "prn_authority_nosigner", cap.ID, cap.Version, domain.TrustModeVerified)
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != service.ReasonSignerNotAuthorized {
		t.Fatalf("got granted=%v reasonCode=%q, want denied with %q for a capability with no current signer", granted, reasonCode, service.ReasonSignerNotAuthorized)
	}
}

// boundAndRevokedCore wraps a working *toscoremock.Core and forces
// ResolvePrincipalBindingStatus to report bound=true together with
// revoked=true -- a combination the mock's own bookkeeping (a binding is
// either in principalBindings XOR revokedBindings) can never produce, but
// that a real tos-protocol server response is not wire-guaranteed to
// exclude (Bound and Status are independent response fields). This proves
// TOSBackedActivationAuthority.Evaluate checks revoked unconditionally
// rather than only inside `if !bound`.
type boundAndRevokedCore struct {
	*toscoremock.Core
	binding domain.PrincipalIdentityBinding
}

func (c *boundAndRevokedCore) ResolvePrincipalBindingStatus(ctx context.Context, principalID string) (domain.PrincipalIdentityBinding, bool, bool, string, error) {
	return c.binding, true, true, "test-forced-revocation", nil
}

func TestTOSBackedActivationAuthority_BoundAndRevokedSimultaneouslyDenies(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capabilities := service.NewCapabilityService(st)
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-devnet")
	capabilities.WithManifestAnchor(core)
	signers := service.NewExecutionSignerService(st, core, capabilities)
	identities := service.NewIdentityBindingService(st, core)
	cap := registerSignerTestCapability(t, capabilities, "prn_authority_boundrevoked", domain.TrustModeManaged, domain.TrustModeVerified)

	core.SeedAgentIdentity("agt_prn_authority_boundrevoked")
	if _, err := identities.Bind(ctx, service.BindIdentityInput{
		PrincipalID: "prn_authority_boundrevoked", AgentID: "agt_prn_authority_boundrevoked", IdempotencyKey: "bind-boundrevoked",
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := signers.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: "prn_authority_boundrevoked", CapabilityID: cap.ID,
		ExecutionSignerID: "signer-boundrevoked", SignerPublicKey: testSignerKey(t), SignatureAlgorithm: "ed25519",
		ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(24 * time.Hour),
		IdempotencyKey: "authz-boundrevoked",
	}); err != nil {
		t.Fatal(err)
	}

	wrapped := &boundAndRevokedCore{
		Core: core,
		binding: domain.PrincipalIdentityBinding{
			PrincipalID: "prn_authority_boundrevoked", AgentID: "agt_prn_authority_boundrevoked",
			Network: "tos-devnet", BindingRef: "ref",
		},
	}
	authority := service.NewTOSBackedActivationAuthority(wrapped, st, signers)

	granted, reasonCode, err := authority.Evaluate(ctx, "prn_authority_boundrevoked", cap.ID, cap.Version, domain.TrustModeVerified)
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != service.ReasonProviderIdentityRevoked {
		t.Fatalf("got granted=%v reasonCode=%q, want denied with %q when bound=true and revoked=true are both reported", granted, reasonCode, service.ReasonProviderIdentityRevoked)
	}
}

func TestTOSBackedActivationAuthority_RevokedSignerDenies(t *testing.T) {
	authority, cap, _, signers := tosBackedAuthorityFixture(t, "prn_authority_sigrevoked")
	ctx := context.Background()
	if _, err := signers.Revoke(ctx, service.RevokeSignerInput{
		ProviderID: "prn_authority_sigrevoked", CapabilityID: cap.ID,
		ReasonCode: "test-revocation", IdempotencyKey: "revoke-sigrevoked",
	}); err != nil {
		t.Fatal(err)
	}

	granted, reasonCode, err := authority.Evaluate(ctx, "prn_authority_sigrevoked", cap.ID, cap.Version, domain.TrustModeVerified)
	if err != nil {
		t.Fatal(err)
	}
	if granted || reasonCode != service.ReasonSignerNotAuthorized {
		t.Fatalf("got granted=%v reasonCode=%q, want denied with %q once the signer is revoked", granted, reasonCode, service.ReasonSignerNotAuthorized)
	}
}
