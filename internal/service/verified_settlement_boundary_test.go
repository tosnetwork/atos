package service

import (
	"context"
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/financial"
)

type settlementBoundaryFinancialStub struct {
	financial.ATOSFinancialAdapter
	settleCalls int
}

func (s *settlementBoundaryFinancialStub) Settle(context.Context, financial.TransferRequest) (financial.Event, error) {
	s.settleCalls++
	return financial.Event{}, nil
}

func TestVerifiedSettlementNeverEntersManagedFinancialLedger(t *testing.T) {
	stub := new(settlementBoundaryFinancialStub)
	service := &JobService{financial: stub}
	job := domain.Job{ID: "job-verified", PrincipalID: "principal", ProviderID: "provider"}
	quote := domain.Quote{ID: "quote-verified", TrustMode: domain.TrustModeVerified}
	snapshot := domain.BillingSnapshot{
		JobID: job.ID, ProviderID: job.ProviderID, ReceiptID: "receipt-verified",
		ProviderGross:   domain.Money{Amount: "0.700000000", Currency: "TOS"},
		GatewayFee:      domain.Money{Amount: "0.100000000", Currency: "TOS"},
		PrincipalRefund: domain.Money{Amount: "0.200000000", Currency: "TOS"},
	}
	if err := service.settleFinancialLegs(context.Background(), job, quote, snapshot, "settlement-verified"); err != nil {
		t.Fatal(err)
	}
	if stub.settleCalls != 0 {
		t.Fatalf("Verified settlement emitted %d Managed/Blnk transfers", stub.settleCalls)
	}
}
