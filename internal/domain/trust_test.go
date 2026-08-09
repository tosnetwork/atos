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

func TestValidateCommittedTrustAcceptsOnlyNormativePairs(t *testing.T) {
	tests := []struct {
		name    string
		mode    TrustMode
		profile ProofProfile
		valid   bool
	}{
		{name: "managed", mode: TrustModeManaged, profile: ProofProfileNone, valid: true},
		{name: "verified", mode: TrustModeVerified, profile: ProofProfileTOSVerifiedV1, valid: true},
		{name: "native", mode: TrustModeNative, profile: ProofProfileTOSNativeV1, valid: true},
		{name: "empty mode", mode: TrustMode(""), profile: ProofProfileNone},
		{name: "auto cannot be committed", mode: TrustMode(RequestedTrustAuto), profile: ProofProfileNone},
		{name: "managed with verified profile", mode: TrustModeManaged, profile: ProofProfileTOSVerifiedV1},
		{name: "verified without profile", mode: TrustModeVerified, profile: ProofProfileNone},
		{name: "native with verified profile", mode: TrustModeNative, profile: ProofProfileTOSVerifiedV1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCommittedTrust(tc.mode, tc.profile)
			if tc.valid && err != nil {
				t.Fatalf("valid pair rejected: %v", err)
			}
			if !tc.valid && err == nil {
				t.Fatalf("invalid pair accepted: mode=%q profile=%q", tc.mode, tc.profile)
			}
		})
	}
}
