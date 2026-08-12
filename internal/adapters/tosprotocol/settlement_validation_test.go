package toprotocol

import (
	"testing"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	atostosv1 "github.com/tosnetwork/tos-protocol/gen/atos/tos/v1"
	"google.golang.org/protobuf/proto"
)

func TestVerifiedSettlementResponseRequiresExactTupleConservationAndFinality(t *testing.T) {
	client := &Client{network: "tos-test"}
	local := domain.Escrow{
		ID: "escrow-1", QuoteID: "quote-1", JobID: "job-1", PrincipalID: "principal-1",
		ProviderID: "provider-1", CapabilityID: "capability-1", CapabilityVersion: "1.0.0",
		TrustMode: domain.TrustModeVerified, ProofProfile: domain.ProofProfileTOSVerifiedV1,
		Reserved:        domain.Money{Amount: "1.000000000", Currency: "TOS"},
		NetworkProofRef: "tos:task-escrow:v1:contract", ReservationDigest: "sha256:reservation",
		QuoteCommitmentDigest: "sha256:quote", QuoteCommitmentRef: "tos:tx:v1:quote",
		ContractCodeHash: "tvm-cell-sha256:code", Finalized: true, FinalizedCheckpoint: 100,
	}
	request := toscore.SettleJobRequest{
		EscrowID: local.ID, JobID: local.JobID, ReceiptID: "receipt-1",
		ActualCost: domain.Money{Amount: "0.700000000", Currency: "TOS"},
	}
	charged := &atostosv1.NetworkAmount{Asset: "TOS", AtomicAmount: "700000000"}
	settlement := &atostosv1.Settlement{
		SettlementId: "settlement-1", EscrowId: local.ID, QuoteId: local.QuoteID,
		JobId: local.JobID, ReceiptId: request.ReceiptID, Charged: proto.Clone(charged).(*atostosv1.NetworkAmount),
		Refunded: &atostosv1.NetworkAmount{Asset: "TOS", AtomicAmount: "300000000"},
		State:    atostosv1.SettlementState_SETTLEMENT_STATE_SETTLED, SettledUnixMillis: 1,
		SettlementRef: &atostosv1.NetworkReference{Network: "tos-test", Reference: "tos:tx:v1:settle", Finalized: true, FinalizedCheckpoint: 101},
	}
	escrow := &atostosv1.Escrow{
		EscrowId: local.ID, QuoteId: local.QuoteID, JobId: local.JobID,
		PrincipalId: local.PrincipalID, ProviderId: local.ProviderID,
		CapabilityId: local.CapabilityID, CapabilityVersion: local.CapabilityVersion,
		TrustMode:    atostosv1.TrustMode_TRUST_MODE_VERIFIED,
		ProofProfile: atostosv1.ProofProfile_PROOF_PROFILE_TOS_VERIFIED_V1,
		Reserved:     &atostosv1.NetworkAmount{Asset: "TOS", AtomicAmount: "1000000000"},
		State:        atostosv1.EscrowState_ESCROW_STATE_SETTLED, Finalized: true, FinalizedCheckpoint: 100,
		EscrowRef:         &atostosv1.NetworkReference{Network: "tos-test", Reference: local.NetworkProofRef, Finalized: true, FinalizedCheckpoint: 100},
		ReservationDigest: local.ReservationDigest, QuoteCommitmentDigest: local.QuoteCommitmentDigest,
		QuoteCommitmentRef: &atostosv1.NetworkReference{Network: "tos-test", Reference: local.QuoteCommitmentRef, Finalized: true, FinalizedCheckpoint: 90},
		ContractCodeHash:   local.ContractCodeHash,
	}
	if err := client.validateSettlementResponse(request, local, charged, settlement, escrow); err != nil {
		t.Fatalf("valid settlement rejected: %v", err)
	}
	tests := map[string]func(*atostosv1.Settlement, *atostosv1.Escrow){
		"zero checkpoint": func(s *atostosv1.Settlement, _ *atostosv1.Escrow) { s.SettlementRef.FinalizedCheckpoint = 0 },
		"wrong receipt":   func(s *atostosv1.Settlement, _ *atostosv1.Escrow) { s.ReceiptId = "receipt-other" },
		"wrong job":       func(_ *atostosv1.Settlement, e *atostosv1.Escrow) { e.JobId = "job-other" },
		"wrong escrow reference": func(_ *atostosv1.Settlement, e *atostosv1.Escrow) {
			e.EscrowRef.Reference = "tos:task-escrow:v1:other"
		},
		"wrong reservation digest": func(_ *atostosv1.Settlement, e *atostosv1.Escrow) {
			e.ReservationDigest = "sha256:other"
		},
		"amount inflation":      func(s *atostosv1.Settlement, _ *atostosv1.Escrow) { s.Refunded.AtomicAmount = "400000000" },
		"checkpoint regression": func(s *atostosv1.Settlement, _ *atostosv1.Escrow) { s.SettlementRef.FinalizedCheckpoint = 99 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			s := proto.Clone(settlement).(*atostosv1.Settlement)
			e := proto.Clone(escrow).(*atostosv1.Escrow)
			mutate(s, e)
			if err := client.validateSettlementResponse(request, local, charged, s, e); err == nil {
				t.Fatal("malformed settlement was accepted")
			}
		})
	}
}
