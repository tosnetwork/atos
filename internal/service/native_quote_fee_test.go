package service_test

import (
	"context"
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func TestNativeQuotePreservesExistingGatewayFee(t *testing.T) {
	store := memory.New()
	capability := domain.Capability{
		ID: "cap-native-fee", ProviderID: "provider-native", Name: "Native", Description: "native fee regression",
		Version: "1.0.0", Status: domain.CapabilityActive, DeliveryMode: domain.DeliveryInstant,
		InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		ModeSupport:         domain.ModeSupport{domain.TrustModeNative: {Status: domain.ModeSupportActive, ProofProfile: domain.ProofProfileTOSNativeV1}},
		SupportedTrustModes: []domain.TrustMode{domain.TrustModeNative},
	}
	if err := store.Put(context.Background(), capability); err != nil {
		t.Fatal(err)
	}
	quote, err := service.NewQuoteService(store).Create(context.Background(), service.CreateQuoteInput{
		PrincipalID: "principal-native", CapabilityID: capability.ID,
		RequestedTrustMode: domain.RequestedTrustNative, IdempotencyKey: "native-fee",
	})
	if err != nil {
		t.Fatal(err)
	}
	if quote.TrustMode != domain.TrustModeNative || quote.Price.Subtotal != "1.00" ||
		quote.Price.Fees != "0.05" || quote.Price.TotalMax != "1.05" {
		t.Fatalf("Native fee semantics changed: mode=%s price=%+v", quote.TrustMode, quote.Price)
	}
}
