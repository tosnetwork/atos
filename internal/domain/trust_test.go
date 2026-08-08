package domain

import "testing"

func activeSupport(modes ...TrustMode) ModeSupport {
	support := ModeSupport{
		TrustModeManaged:  {Status: ModeSupportUnsupported},
		TrustModeVerified: {Status: ModeSupportUnsupported, ProofProfile: ProofProfileTOSVerifiedV1},
		TrustModeNative:   {Status: ModeSupportUnsupported, ProofProfile: ProofProfileTOSNativeV1},
	}
	for _, mode := range modes {
		entry := support[mode]
		entry.Status = ModeSupportActive
		entry.ProofProfile = StandardProofProfile(mode)
		support[mode] = entry
	}
	return support
}

func TestResolveTrustModeAutoPrefersManagedWithoutProofRequirements(t *testing.T) {
	mode, profile, err := ResolveTrustMode(RequestedTrustAuto, ProofRequirements{}, activeSupport(TrustModeManaged, TrustModeVerified))
	if err != nil {
		t.Fatal(err)
	}
	if mode != TrustModeManaged || profile != ProofProfileNone {
		t.Fatalf("got mode=%q profile=%q, want managed with no network profile", mode, profile)
	}
}

func TestResolveTrustModeAutoRequiresStrongerModeForTOSProof(t *testing.T) {
	mode, profile, err := ResolveTrustMode(
		RequestedTrustAuto,
		ProofRequirements{NetworkVerifiableReceipt: true},
		activeSupport(TrustModeManaged, TrustModeVerified, TrustModeNative),
	)
	if err != nil {
		t.Fatal(err)
	}
	if mode != TrustModeVerified || profile != ProofProfileTOSVerifiedV1 {
		t.Fatalf("got mode=%q profile=%q, want verified/tos_verified_v1", mode, profile)
	}
}

func TestResolveTrustModeNeverSilentlyDowngradesConcreteRequest(t *testing.T) {
	if _, _, err := ResolveTrustMode(RequestedTrustNative, ProofRequirements{}, activeSupport(TrustModeManaged)); err == nil {
		t.Fatal("expected explicit native request to fail when native is not active")
	}
	if _, _, err := ResolveTrustMode(
		RequestedTrustManaged,
		ProofRequirements{TOSSettlement: true},
		activeSupport(TrustModeManaged, TrustModeVerified),
	); err == nil {
		t.Fatal("expected explicit managed request to fail when TOS settlement is required")
	}
}

func TestActiveModesNeverContainsAutoAndIsStable(t *testing.T) {
	modes := activeSupport(TrustModeNative, TrustModeManaged, TrustModeVerified).ActiveModes()
	want := []TrustMode{TrustModeManaged, TrustModeVerified, TrustModeNative}
	if len(modes) != len(want) {
		t.Fatalf("got %v, want %v", modes, want)
	}
	for i := range want {
		if modes[i] != want[i] {
			t.Fatalf("got %v, want stable order %v", modes, want)
		}
	}
}
