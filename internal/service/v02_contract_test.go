package service_test

import (
	"context"
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

func TestCapabilityModeActivationSeparatesRequestedFromSupported(t *testing.T) {
	h := newHarness()
	cap, err := h.capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID:   "agt_modes",
		Name:         "Mode Test",
		Description:  "tests requested versus active modes",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{
			Model:     domain.PricingFixed,
			PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"},
		},
		RequestedTrustModes: []domain.TrustMode{
			domain.TrustModeManaged,
			domain.TrustModeVerified,
			domain.TrustModeNative,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cap.SupportedTrustModes) != 1 || cap.SupportedTrustModes[0] != domain.TrustModeManaged {
		t.Fatalf("supported modes = %v, want managed only", cap.SupportedTrustModes)
	}
	if cap.ModeSupport.Entry(domain.TrustModeVerified).Status != domain.ModeSupportPending {
		t.Fatalf("verified status = %q, want pending", cap.ModeSupport.Entry(domain.TrustModeVerified).Status)
	}
	if cap.ModeSupport.Entry(domain.TrustModeNative).Status != domain.ModeSupportPending {
		t.Fatalf("native status = %q, want pending", cap.ModeSupport.Entry(domain.TrustModeNative).Status)
	}
}

func TestExplicitUnavailableModeFailsWithoutManagedFallback(t *testing.T) {
	h := newHarness()
	cap := registerCapability(t, h, "agt_provider", "1.00")
	_, err := h.quotes.Create(context.Background(), service.CreateQuoteInput{
		CapabilityID:       cap.ID,
		RequestedTrustMode: domain.RequestedTrustVerified,
	})
	if err == nil {
		t.Fatal("expected explicit verified request to fail while Verified is unavailable")
	}
	de, ok := err.(*domain.Error)
	if !ok || de.Code != domain.ErrTrustModeUnavailable {
		t.Fatalf("got %v, want trust_mode_unavailable", err)
	}
}

func TestManagedQuoteContractPropagatesThroughReceipt(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	cap := registerCapability(t, h, "agt_provider", "1.00")
	quote, err := h.quotes.Create(ctx, service.CreateQuoteInput{
		CapabilityID:       cap.ID,
		RequestedTrustMode: domain.RequestedTrustAuto,
	})
	if err != nil {
		t.Fatal(err)
	}
	if quote.TrustMode != domain.TrustModeManaged || quote.ProofProfile != domain.ProofProfileNone {
		t.Fatalf("quote mode/profile = %q/%q", quote.TrustMode, quote.ProofProfile)
	}
	result, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID:    "prn_contract",
		CapabilityID:   cap.ID,
		QuoteID:        quote.ID,
		Input:          map[string]any{"hello": "world"},
		IdempotencyKey: "v02-contract-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Job.TrustMode != quote.TrustMode || result.Job.ProofProfile != quote.ProofProfile {
		t.Fatalf("job mode/profile = %q/%q, quote = %q/%q", result.Job.TrustMode, result.Job.ProofProfile, quote.TrustMode, quote.ProofProfile)
	}
	receipt, err := h.store().ReceiptByJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TrustMode != quote.TrustMode || receipt.ProofProfile != quote.ProofProfile {
		t.Fatalf("receipt mode/profile = %q/%q, quote = %q/%q", receipt.TrustMode, receipt.ProofProfile, quote.TrustMode, quote.ProofProfile)
	}
	if receipt.ExecutionSignerID == "" || receipt.InputCommitment == "" || receipt.OutputCommitment == "" {
		t.Fatalf("receipt is missing signer or content commitments: %+v", receipt)
	}
	if receipt.NetworkProofRef != "" {
		t.Fatalf("Managed mock must not fabricate a TOS proof: %q", receipt.NetworkProofRef)
	}
}

func TestCapabilityArtifactMetadataDerivedFromSchema(t *testing.T) {
	h := newHarness()
	cap, err := h.capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID:  "agt_artifact",
		Name:        "PDF Analyzer",
		Description: "analyzes a PDF",
		DeliveryMode: domain.DeliveryAsync,
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"document": map[string]any{
					"type": "object",
					"properties": map[string]any{"artifact_id": map[string]any{"type": "string"}},
				},
			},
		},
		OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "2.00", Currency: "USD"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cap.RequiresArtifactTransfer || len(cap.ArtifactInputFields) != 1 || cap.ArtifactInputFields[0] != "document" {
		t.Fatalf("artifact metadata = required:%v inputs:%v", cap.RequiresArtifactTransfer, cap.ArtifactInputFields)
	}
}
