package service

import (
	"context"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/memory"
	"github.com/tosnetwork/tos-protocol/pkg/verifiedproof"
	"testing"
	"time"
)

func TestPortableProofRejectsManagedAndCrossPrincipal(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	core := toscoremock.New(s)
	svc := NewPortableProofService(s, core)
	r := domain.Receipt{ID: "r1", QuoteID: "q1", EscrowID: "e1", JobID: "j1", PrincipalID: "p1", TrustMode: domain.TrustModeManaged, CreatedAt: time.Now()}
	if e := s.PutReceipt(ctx, r); e != nil {
		t.Fatal(e)
	}
	if _, e := svc.Create(ctx, r.ID, "other"); !domainErrorIs(e, domain.ErrPermissionDenied) {
		t.Fatalf("cross-principal err=%v", e)
	}
	if _, e := svc.Create(ctx, r.ID, r.PrincipalID); !domainErrorIs(e, domain.ErrProofProfileUnavailable) {
		t.Fatalf("managed err=%v", e)
	}
}

func TestProofServiceQuoteRefUsesCommittedBusinessIdentity(t *testing.T) {
	q := domain.Quote{
		ServiceQuoteID:            "provider-service-quote",
		UnderlyingServiceQuoteRef: "tos:tx:v1:chain-observation",
	}
	if got := proofServiceQuoteRef(q); got != q.ServiceQuoteID {
		t.Fatalf("proof service quote ref = %q, want committed %q", got, q.ServiceQuoteID)
	}
}

func TestSameProofSemanticsAllowsOnlyCheckpointAdvancement(t *testing.T) {
	stored := proofSemanticsFixture(101)
	live := proofSemanticsFixture(202)

	if !sameProofSemantics(stored, live) {
		t.Fatal("equal proof semantics with advanced finality checkpoint were rejected")
	}
	if got := stored.Quote.CommitmentRef.FinalizedCheckpoint; got != 101 {
		t.Fatalf("stored package was mutated: checkpoint=%d, want 101", got)
	}
	if got := live.Quote.CommitmentRef.FinalizedCheckpoint; got != 202 {
		t.Fatalf("live package was mutated: checkpoint=%d, want 202", got)
	}

	regressed := proofSemanticsFixture(100)
	if sameProofSemantics(stored, regressed) {
		t.Fatal("checkpoint regression was accepted")
	}

	changed := proofSemanticsFixture(202)
	changed.Escrow.ReservationDigest = "sha256:changed"
	if sameProofSemantics(stored, changed) {
		t.Fatal("changed immutable tuple was accepted")
	}
}

func proofSemanticsFixture(checkpoint uint64) verifiedproof.Package {
	ref := func(value string) verifiedproof.Reference {
		return verifiedproof.Reference{Network: "tos-localnet", Reference: value, FinalizedCheckpoint: checkpoint}
	}
	return verifiedproof.Package{
		Version:              verifiedproof.Version,
		Canonicalization:     verifiedproof.Canonicalization,
		NetworkID:            "tos-localnet",
		GatewayDomain:        "atos.im",
		PrincipalID:          "principal",
		RequesterAgentID:     "requester-agent",
		RequesterIdentityRef: ref("requester-ref"),
		ProviderID:           "provider",
		ProviderIdentityRef:  ref("provider-ref"),
		Capability: verifiedproof.Capability{
			CapabilityID:      "capability",
			CapabilityVersion: "1.0.0",
			ManifestDigest:    "sha256:manifest",
			OwnershipRef:      ref("ownership-ref"),
		},
		Quote: verifiedproof.Quote{
			QuoteID:          "quote",
			CommitmentDigest: "sha256:quote",
			CommitmentRef:    ref("quote-ref"),
			TermsDigest:      "sha256:terms",
		},
		Escrow: verifiedproof.Escrow{
			EscrowID:          "escrow",
			JobID:             "job",
			ContractRef:       ref("contract-ref"),
			ReservationDigest: "sha256:reservation",
			ReservationRef:    ref("reservation-ref"),
		},
		SignerAuthorization: &verifiedproof.SignerAuthorization{AuthorizationID: "authorization", AuthorizationRef: ref("authorization-ref")},
		Receipt:             &verifiedproof.Receipt{ReceiptID: "receipt", ReceiptRef: ref("receipt-ref")},
		Outcome:             verifiedproof.Outcome{Kind: "provider_settlement", OutcomeRef: ref("outcome-ref")},
		ProofOfService:      &verifiedproof.ProofOfService{EvidenceID: "evidence", EvidenceRef: ref("evidence-ref")},
	}
}
