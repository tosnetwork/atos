package memory

import (
	"context"
	"testing"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

func TestVerifiedEscrowAndDisputeTerminalProjectionCannotRegress(t *testing.T) {
	s := New()
	escrow := domain.Escrow{ID: "esc-1", QuoteID: "q-1", JobID: "j-1", PrincipalID: "p-1", ProviderID: "provider-1", CapabilityID: "cap-1", ReservationDigest: "sha256:reserve", NetworkProofRef: "contract", Status: domain.EscrowReserved, FinalizedCheckpoint: 10}
	if err := s.PutEscrow(context.Background(), escrow); err != nil {
		t.Fatal(err)
	}
	disputed := escrow
	disputed.Status, disputed.DisputeDigest, disputed.FinalizedCheckpoint = domain.EscrowDisputed, "sha256:dispute", 20
	if err := s.PutEscrow(context.Background(), disputed); err != nil {
		t.Fatal(err)
	}
	if err := s.PutEscrow(context.Background(), escrow); err != store.ErrConflict {
		t.Fatalf("stale escrow regression err=%v", err)
	}
	resolved := disputed
	resolved.Status, resolved.FinalizedCheckpoint = domain.EscrowSettled, 30
	if err := s.PutEscrow(context.Background(), resolved); err != nil {
		t.Fatal(err)
	}
	if err := s.PutEscrow(context.Background(), disputed); err != store.ErrConflict {
		t.Fatalf("terminal escrow regression err=%v", err)
	}

	d := domain.Dispute{ID: "d-1", JobID: "j-1", TrustMode: domain.TrustModeVerified, EconomicState: domain.DisputeEconomicVerifiedResolutionPending, ReviewStatus: domain.DisputeUnderReview, DisputeDigest: "sha256:dispute", DisputeRef: "open-ref", DisputeCheckpoint: 20, PendingOutcome: domain.DisputeOutcomePrincipal, PendingReviewerID: "reviewer"}
	s.disputes[d.ID], s.disputesByJob[d.JobID] = d, d.ID
	terminal := d
	terminal.EconomicState, terminal.ReviewStatus, terminal.Outcome, terminal.ResolutionDigest, terminal.ResolutionRef, terminal.ResolutionCheckpoint = domain.DisputeEconomicVerifiedResolved, domain.DisputeResolvedForPrincipal, domain.DisputeOutcomePrincipal, "sha256:resolution", "resolution-ref", 30
	if _, err := s.UpdateDispute(context.Background(), d.ID, func(domain.Dispute, bool) (domain.Dispute, error) { return terminal, nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateDispute(context.Background(), d.ID, func(domain.Dispute, bool) (domain.Dispute, error) { return d, nil }); err != store.ErrConflict {
		t.Fatalf("terminal dispute regression err=%v", err)
	}
}
