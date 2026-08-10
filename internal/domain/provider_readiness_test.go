package domain

import "testing"

func TestReadinessReasonCode(t *testing.T) {
	tests := []struct {
		name                                                                                     string
		status                                                                                   ModeSupportStatus
		transportHealthy, healthFresh, certificationCurrent, signerAuthorized, activationGranted bool
		want                                                                                     string
	}{
		{
			name:   "active has no blocker regardless of stale evidence fields",
			status: ModeSupportActive, transportHealthy: false, healthFresh: false,
			certificationCurrent: false, signerAuthorized: false, activationGranted: false,
			want: "",
		},
		{
			name:   "requested is always no-evidence-yet even if a field is coincidentally true",
			status: ModeSupportRequested, transportHealthy: true, healthFresh: true,
			certificationCurrent: true, signerAuthorized: true, activationGranted: true,
			want: "NO_READINESS_EVIDENCE_YET",
		},
		{
			name:   "pending with unhealthy transport reports the transport blocker first",
			status: ModeSupportPending, transportHealthy: false, healthFresh: false,
			certificationCurrent: false, signerAuthorized: false, activationGranted: false,
			want: "TRANSPORT_UNHEALTHY",
		},
		{
			name:   "pending with healthy-but-stale transport reports staleness, not certification",
			status: ModeSupportPending, transportHealthy: true, healthFresh: false,
			certificationCurrent: false, signerAuthorized: false, activationGranted: false,
			want: "HEALTH_STALE",
		},
		{
			name:   "pending with fresh healthy transport but no current certification",
			status: ModeSupportPending, transportHealthy: true, healthFresh: true,
			certificationCurrent: false, signerAuthorized: false, activationGranted: false,
			want: "CERTIFICATION_NOT_CURRENT",
		},
		{
			name:   "pending with certification current but no signer authorized",
			status: ModeSupportPending, transportHealthy: true, healthFresh: true,
			certificationCurrent: true, signerAuthorized: false, activationGranted: false,
			want: "SIGNER_NOT_AUTHORIZED",
		},
		{
			name:   "pending with every evidence dimension satisfied but not yet activated",
			status: ModeSupportPending, transportHealthy: true, healthFresh: true,
			certificationCurrent: true, signerAuthorized: true, activationGranted: false,
			want: "ACTIVATION_AUTHORITY_UNAVAILABLE",
		},
		{
			name:   "suspended walks the same priority order as pending",
			status: ModeSupportSuspended, transportHealthy: false, healthFresh: true,
			certificationCurrent: true, signerAuthorized: true, activationGranted: false,
			want: "TRANSPORT_UNHEALTHY",
		},
		{
			name:   "unsupported with no evidence at all still reports the transport blocker (not requested-specific)",
			status: ModeSupportUnsupported, transportHealthy: false, healthFresh: false,
			certificationCurrent: false, signerAuthorized: false, activationGranted: false,
			want: "TRANSPORT_UNHEALTHY",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ReadinessReasonCode(tc.status, tc.transportHealthy, tc.healthFresh, tc.certificationCurrent, tc.signerAuthorized, tc.activationGranted)
			if got != tc.want {
				t.Fatalf("ReadinessReasonCode(%s, ...) = %q, want %q", tc.status, got, tc.want)
			}
		})
	}
}
