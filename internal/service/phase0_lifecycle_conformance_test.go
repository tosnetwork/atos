package service_test

import (
	"context"
	"strings"
	"testing"
	"time"

	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestPhase0OneAPIRunsEveryConcreteMode is the roadmap's Phase 0 success
// criterion. The same Capability, Quote and Invoke shapes run through a
// deliberately simulated contract fixture for Managed, Verified and Native.
// The fixture uses visibly non-TOS simulated references and is never selected
// by runtime composition; production mocks remain Managed-only and fail closed.
func TestPhase0OneAPIRunsEveryConcreteMode(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	capability := domain.Capability{
		ID: "cap_phase0_lifecycle", CanonicalURI: "atos://capability/phase0-lifecycle",
		ProviderID: "agt_phase0", Name: "Phase 0 Lifecycle", Description: "contract conformance fixture",
		Version: "1.0.0", Status: domain.CapabilityActive, DeliveryMode: domain.DeliveryInstant,
		InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		ModeSupport: domain.ModeSupport{
			domain.TrustModeManaged:  {Status: domain.ModeSupportActive},
			domain.TrustModeVerified: {Status: domain.ModeSupportActive, ProofProfile: domain.ProofProfileTOSVerifiedV1},
			domain.TrustModeNative:   {Status: domain.ModeSupportActive, ProofProfile: domain.ProofProfileTOSNativeV1},
		},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified, domain.TrustModeNative},
		SupportedTrustModes: []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified, domain.TrustModeNative},
	}
	if err := st.Put(ctx, capability); err != nil {
		t.Fatal(err)
	}

	accounts := service.NewAccountService(st)
	quotes := service.NewQuoteService(st).WithAccountService(accounts)
	core := toscoremock.NewContractFixture(st)
	// Verified/Native execution receipts require a genuinely resolvable
	// execution-signer authorization (Managed does not -- see
	// toscore/mock.Core.VerifyExecutionReceipt's doc comment); pre-authorize
	// the fixed signer identity tosaimock.NewContractFixture's synthesized
	// receipts always use ("sig_mock_tos_ai"), mirroring what a real
	// Phase 3B signer-authorize call would have durably recorded before
	// this Job ever ran.
	if _, _, err := core.AuthorizeExecutionSigner(ctx, toscore.AuthorizeExecutionSignerRequest{
		AuthorizationID: "auth_mock_" + capability.ID, ProviderID: capability.ProviderID,
		CapabilityID: capability.ID, CapabilityVersion: capability.Version,
		ExecutionSignerID: "sig_mock_tos_ai",
		ValidFrom:         time.Now().UTC().Add(-24 * time.Hour), ValidUntil: time.Now().UTC().Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("pre-authorize execution signer: %v", err)
	}
	jobs := service.NewJobService(st, tosaimock.NewContractFixture(), core, accounts)
	tests := []struct {
		requested domain.RequestedTrustMode
		mode      domain.TrustMode
		profile   domain.ProofProfile
	}{
		{domain.RequestedTrustManaged, domain.TrustModeManaged, domain.ProofProfileNone},
		{domain.RequestedTrustVerified, domain.TrustModeVerified, domain.ProofProfileTOSVerifiedV1},
		{domain.RequestedTrustNative, domain.TrustModeNative, domain.ProofProfileTOSNativeV1},
	}

	for _, tc := range tests {
		t.Run(string(tc.requested), func(t *testing.T) {
			principalID := "prn_phase0_" + string(tc.mode)
			quote, err := quotes.Create(ctx, service.CreateQuoteInput{
				PrincipalID: principalID, CapabilityID: capability.ID,
				RequestedTrustMode: tc.requested, InputSummary: map[string]any{"mode": tc.mode},
			})
			if err != nil {
				t.Fatal(err)
			}
			if quote.TrustMode != tc.mode || quote.ProofProfile != tc.profile {
				t.Fatalf("quote mode/profile = %q/%q, want %q/%q", quote.TrustMode, quote.ProofProfile, tc.mode, tc.profile)
			}
			result, err := jobs.Invoke(ctx, service.SubmitInput{
				PrincipalID: principalID, CapabilityID: capability.ID, QuoteID: quote.ID,
				Input: map[string]any{"mode": tc.mode}, IdempotencyKey: "phase0-lifecycle-" + string(tc.mode),
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Type != service.ResultCompleted || result.Job.State != domain.JobCompleted {
				t.Fatalf("result/job = %q/%q, want completed", result.Type, result.Job.State)
			}
			if result.Job.TrustMode != quote.TrustMode || result.Job.ProofProfile != quote.ProofProfile {
				t.Fatalf("job mode/profile = %q/%q, quote = %q/%q", result.Job.TrustMode, result.Job.ProofProfile, quote.TrustMode, quote.ProofProfile)
			}
			if result.Job.ProofStatus.Receipt != domain.ProofVerified || result.Job.ProofStatus.Settlement != domain.ProofSettled {
				t.Fatalf("job proof status = %+v", result.Job.ProofStatus)
			}
			escrow, err := st.GetEscrow(ctx, result.Job.EscrowID)
			if err != nil {
				t.Fatal(err)
			}
			if escrow.TrustMode != quote.TrustMode || escrow.ProofProfile != quote.ProofProfile || escrow.Status != domain.EscrowSettled {
				t.Fatalf("escrow contract = %+v", escrow)
			}
			receipt, err := st.ReceiptByJob(ctx, result.Job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.TrustMode != quote.TrustMode || receipt.ProofProfile != quote.ProofProfile || receipt.Status != domain.ReceiptSettled {
				t.Fatalf("settlement receipt = %+v", receipt)
			}
			if tc.mode == domain.TrustModeManaged {
				if receipt.NetworkProofRef != "" || receipt.ProofStatus != domain.ProofNotRequired {
					t.Fatalf("Managed fixture fabricated network proof: %+v", receipt)
				}
			} else {
				if receipt.ProofStatus != domain.ProofVerified || !strings.HasPrefix(receipt.NetworkProofRef, "simulated:atos-v0.2:") {
					t.Fatalf("stronger-mode contract fixture lacks simulated proof: %+v", receipt)
				}
			}
			if string(result.Job.TrustMode) == string(domain.RequestedTrustAuto) {
				t.Fatal("auto survived into committed Job state")
			}
		})
	}
}
