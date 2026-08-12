package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func TestPostgresVerifiedEscrowProjectionIsMonotonic(t *testing.T) {
	s := openTestStore(t)
	id := "esc-monotonic-" + time.Now().UTC().Format("150405.000000000")
	e := domain.Escrow{ID: id, QuoteID: "q-" + id, JobID: "j-" + id, PrincipalID: "p", ProviderID: "provider", CapabilityID: "cap", ReservationDigest: "sha256:reserve", NetworkProofRef: "contract", Status: domain.EscrowReserved, FinalizedCheckpoint: 10, CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour)}
	if err := s.PutEscrow(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	d := e
	d.Status, d.FinalizedCheckpoint = domain.EscrowDisputed, 20
	if err := s.PutEscrow(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if err := s.PutEscrow(context.Background(), e); err != store.ErrConflict {
		t.Fatalf("stale projection err=%v", err)
	}
	terminal := d
	terminal.Status, terminal.FinalizedCheckpoint = domain.EscrowSettled, 30
	if err := s.PutEscrow(context.Background(), terminal); err != nil {
		t.Fatal(err)
	}
	if err := s.PutEscrow(context.Background(), d); err != store.ErrConflict {
		t.Fatalf("terminal regression err=%v", err)
	}
}

func TestPostgresVerifiedResolutionIntentSurvivesIndependentStoreRead(t *testing.T) {
	ctx := context.Background()
	writer := openTestStore(t)
	reader := openTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	id := "dispute-intent-" + now.Format("150405.000000")
	requestedAt := now.Add(time.Second)
	d := domain.Dispute{
		ID:                    id,
		PrincipalID:           "principal-" + id,
		ProviderID:            "provider-" + id,
		JobID:                 "job-" + id,
		QuoteID:               "quote-" + id,
		CapabilityID:          "cap-" + id,
		ReceiptID:             "receipt-" + id,
		SettlementID:          "settlement-" + id,
		EarningID:             "earning-" + id,
		ChargedAmount:         domain.Money{Amount: "1", Currency: "TOS"},
		OriginalRefundAmount:  domain.Money{Amount: "0", Currency: "TOS"},
		Reason:                "result_mismatch",
		ReviewStatus:          domain.DisputeUnderReview,
		EconomicState:         domain.DisputeEconomicVerifiedResolutionPending,
		PendingOutcome:        domain.DisputeOutcomePrincipal,
		PendingReviewerID:     "reviewer-" + id,
		ResolutionRequestedAt: &requestedAt,
		TrustMode:             domain.TrustModeVerified,
		EscrowID:              "escrow-" + id,
		DisputeDigest:         "sha256:dispute",
		DisputeRef:            "tos:dispute",
		DisputeCheckpoint:     20,
		OpenedAt:              now,
		UpdatedAt:             now,
	}
	if _, err := writer.UpdateDispute(ctx, id, func(domain.Dispute, bool) (domain.Dispute, error) { return d, nil }); err != nil {
		t.Fatal(err)
	}

	got, err := reader.GetDispute(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.PendingOutcome != d.PendingOutcome || got.PendingReviewerID != d.PendingReviewerID || got.ResolutionRequestedAt == nil || !got.ResolutionRequestedAt.Equal(requestedAt) {
		t.Fatalf("private resolution intent was not durable: %+v", got)
	}
}

func TestPostgresTerminalReceiptCheckpointCanAdvanceButNotRegress(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	r := domain.Receipt{ID: "receipt-checkpoint-" + now.Format("150405.000000"), QuoteID: "q", EscrowID: "e", JobID: "j-" + now.Format("150405.000000"), PrincipalID: "p", TrustMode: domain.TrustModeVerified, ProofProfile: domain.ProofProfileTOSVerifiedV1, Charged: domain.Money{Amount: "0.000000000", Currency: "TOS"}, Refunded: domain.Money{Amount: "1.000000000", Currency: "TOS"}, Status: domain.ReceiptReleasedAfterDispute, NetworkProofRef: "tos:terminal", Finalized: true, FinalizedCheckpoint: 10, NetworkProofCheckpoint: 10, CreatedAt: now}
	if err := s.PutReceipt(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.FinalizedCheckpoint, r.NetworkProofCheckpoint = 20, 20
	if err := s.PutReceipt(ctx, r); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetReceipt(ctx, r.ID)
	if err != nil || got.FinalizedCheckpoint != 20 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	r.FinalizedCheckpoint, r.NetworkProofCheckpoint = 15, 15
	if err := s.PutReceipt(ctx, r); err != store.ErrConflict {
		t.Fatalf("regression err=%v", err)
	}
}
