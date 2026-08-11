package service_test

import (
	"context"
	"sync"
	"testing"
	"time"

	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func verifiedQuoteHarness(t *testing.T) (*service.QuoteService, *service.ExecutionSignerService, *toscoremock.Core, *memory.Store, domain.Capability) {
	t.Helper()
	ctx := context.Background()
	st := memory.New()
	core := toscoremock.NewContractFixture(st)
	core.SetNetwork("tos-test")
	for _, id := range []string{"requester-agent", "provider-agent"} {
		core.SeedAgentIdentity(id)
	}
	if _, _, err := core.CreatePrincipalBinding(ctx, "gateway", "bind-requester", "requester", "requester-agent"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.CreatePrincipalBinding(ctx, "gateway", "bind-provider", "provider", "provider-agent"); err != nil {
		t.Fatal(err)
	}
	cap := domain.Capability{ID: "cap-verified", ProviderID: "provider", Name: "Verified", Description: "test", Version: "1.0.0", ManifestCommitment: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Status: domain.CapabilityActive, DeliveryMode: domain.DeliveryInstant, InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"}, Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}}, ModeSupport: domain.ModeSupport{domain.TrustModeVerified: {Status: domain.ModeSupportActive, ProofProfile: domain.ProofProfileTOSVerifiedV1}}, SupportedTrustModes: []domain.TrustMode{domain.TrustModeVerified}}
	ref, err := core.CommitCapabilityManifest(ctx, cap)
	if err != nil {
		t.Fatal(err)
	}
	cap.Ownership = domain.CapabilityOwnership{Status: domain.OwnershipAnchored, Network: "tos-test", Commitment: ref}
	if err := st.Put(ctx, cap); err != nil {
		t.Fatal(err)
	}
	signers := service.NewExecutionSignerService(st, core, service.NewCapabilityService(st))
	_, err = signers.Authorize(ctx, service.AuthorizeSignerInput{ProviderID: "provider", CapabilityID: cap.ID, ExecutionSignerID: "signer-1", SignerPublicKey: []byte("01234567890123456789012345678901"), SignatureAlgorithm: "ed25519", ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(time.Hour), ValidFromExplicit: true, ValidUntilExplicit: true, IdempotencyKey: "signer-idem"})
	if err != nil {
		t.Fatal(err)
	}
	quotes := service.NewQuoteService(st).WithVerifiedCommitmentAuthority(core, signers, "atos.im")
	return quotes, signers, core, st, cap
}

func TestVerifiedQuoteIsCommittedBeforeUsableAndAutoIsConcrete(t *testing.T) {
	quotes, _, _, st, cap := verifiedQuoteHarness(t)
	q, err := quotes.Create(context.Background(), service.CreateQuoteInput{PrincipalID: "requester", CapabilityID: cap.ID, RequestedTrustMode: domain.RequestedTrustAuto, ProofRequirements: domain.ProofRequirements{NetworkVerifiableReceipt: true}, IdempotencyKey: "verified-auto"})
	if err != nil {
		t.Fatal(err)
	}
	if q.TrustMode != domain.TrustModeVerified || q.Commitment == nil || !q.Commitment.Finalized || q.Commitment.State != "committed" {
		t.Fatalf("quote not canonically committed: %+v", q)
	}
	stored, err := st.GetQuote(context.Background(), q.ID)
	if err != nil || stored.Commitment.Reference != q.Commitment.Reference {
		t.Fatalf("stored=%+v err=%v", stored, err)
	}
	op, err := st.GetQuoteCommitmentOperation(context.Background(), q.ID)
	if err != nil || op.Checkpoint != domain.QuoteCommitmentCompleted {
		t.Fatalf("op=%+v err=%v", op, err)
	}
	op, err = st.UpdateQuoteCommitmentOperation(context.Background(), q.ID, func(current domain.QuoteCommitmentOperation) (domain.QuoteCommitmentOperation, error) {
		current.Checkpoint = domain.QuoteCommitmentReconciling
		current.FailureReason = "stale replica"
		return current, nil
	})
	if err != nil || op.Checkpoint != domain.QuoteCommitmentCompleted || op.FailureReason != "" {
		t.Fatalf("terminal operation regressed: %+v err=%v", op, err)
	}
}

func TestVerifiedQuoteChangedSemanticsConflictAndJobGate(t *testing.T) {
	quotes, _, core, st, cap := verifiedQuoteHarness(t)
	q, err := quotes.Create(context.Background(), service.CreateQuoteInput{PrincipalID: "requester", CapabilityID: cap.ID, RequestedTrustMode: domain.RequestedTrustVerified, IdempotencyKey: "verified-gate"})
	if err != nil {
		t.Fatal(err)
	}
	mutated := q
	mutated.Price.TotalMax = "9.99"
	if _, err := core.CommitQuote(context.Background(), mutated); err == nil {
		t.Fatal("changed committed Quote semantics succeeded")
	}
	q.Commitment = nil
	if err := st.PutQuote(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	jobs := service.NewJobService(st, nil, core, service.NewAccountService(st))
	if _, err := jobs.CreateJob(context.Background(), service.SubmitInput{PrincipalID: "requester", CapabilityID: cap.ID, QuoteID: q.ID, IdempotencyKey: "job-gate"}); err == nil {
		t.Fatal("job started from uncommitted Verified Quote")
	}
}

func TestVerifiedQuoteLostResponseResumesOriginalQuote(t *testing.T) {
	quotes, _, core, st, cap := verifiedQuoteHarness(t)
	in := service.CreateQuoteInput{PrincipalID: "requester", CapabilityID: cap.ID, RequestedTrustMode: domain.RequestedTrustVerified, IdempotencyKey: "lost-response"}
	core.LoseNextCommitQuoteResponse()
	if _, err := quotes.Create(context.Background(), in); err == nil {
		t.Fatal("expected injected lost response")
	}
	op, err := st.QuoteCommitmentOperationByIdempotencyKey(context.Background(), in.PrincipalID, in.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := quotes.Create(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ID != op.QuoteID {
		t.Fatalf("recovery minted %q, want original %q", recovered.ID, op.QuoteID)
	}
	if recovered.Commitment == nil || !recovered.Commitment.Finalized {
		t.Fatalf("recovered Quote is not usable: %+v", recovered)
	}
}

func TestVerifiedQuoteTwoServicesConvergeOnOneOperation(t *testing.T) {
	quotesA, signers, core, st, cap := verifiedQuoteHarness(t)
	quotesB := service.NewQuoteService(st).WithVerifiedCommitmentAuthority(core, signers, "atos.im")
	in := service.CreateQuoteInput{PrincipalID: "requester", CapabilityID: cap.ID, RequestedTrustMode: domain.RequestedTrustVerified, IdempotencyKey: "replica-race"}
	start := make(chan struct{})
	results := make(chan domain.Quote, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, quotes := range []*service.QuoteService{quotesA, quotesB} {
		wg.Add(1)
		go func(qs *service.QuoteService) {
			defer wg.Done()
			<-start
			q, err := qs.Create(context.Background(), in)
			results <- q
			errs <- err
		}(quotes)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	var ids []string
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for q := range results {
		ids = append(ids, q.ID)
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("replicas did not converge: %v", ids)
	}
}

func TestVerifiedQuoteReconcilerProjectsAuthorityCommittedOperation(t *testing.T) {
	quotes, _, core, st, cap := verifiedQuoteHarness(t)
	seed, err := quotes.Create(context.Background(), service.CreateQuoteInput{PrincipalID: "requester", CapabilityID: cap.ID, RequestedTrustMode: domain.RequestedTrustVerified, IdempotencyKey: "projection-seed"})
	if err != nil {
		t.Fatal(err)
	}
	q := seed
	q.ID = "q_projection_crash"
	q.IdempotencyKey = "projection-crash"
	q.IdempotencyRequestHash = "projection-request-hash"
	q.Commitment = nil
	commitment, err := core.CommitQuote(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	q.Commitment = &domain.QuoteCommitmentProjection{State: "committed", Network: commitment.Network, Reference: commitment.Reference, Digest: commitment.Digest, Finalized: commitment.Finalized, FinalizedCheckpoint: commitment.FinalizedCheckpoint}
	if _, reserved, err := st.Reserve(context.Background(), q.PrincipalID, q.IdempotencyKey, q.IdempotencyRequestHash, time.Now().Add(time.Hour)); err != nil || !reserved {
		t.Fatalf("reserve=%v err=%v", reserved, err)
	}
	old := time.Now().UTC().Add(-time.Minute)
	if _, created, err := st.OpenQuoteCommitment(context.Background(), domain.QuoteCommitmentOperation{QuoteID: q.ID, Quote: q, ContentHash: "projection-content", Checkpoint: domain.QuoteCommitmentAuthorityCommitted, CreatedAt: old, UpdatedAt: old}); err != nil || !created {
		t.Fatalf("open created=%v err=%v", created, err)
	}
	if _, err := st.GetQuote(context.Background(), q.ID); err == nil {
		t.Fatal("test setup unexpectedly has a public Quote projection")
	}
	if err := quotes.ReconcileStaleOperations(context.Background(), time.Now().UTC(), 100); err != nil {
		t.Fatal(err)
	}
	projected, err := st.GetQuote(context.Background(), q.ID)
	if err != nil || projected.Commitment == nil {
		t.Fatalf("Quote projection was not recovered: %+v err=%v", projected, err)
	}
	op, err := st.GetQuoteCommitmentOperation(context.Background(), q.ID)
	if err != nil || op.Checkpoint != domain.QuoteCommitmentCompleted {
		t.Fatalf("operation did not become terminal after projection: %+v err=%v", op, err)
	}
}
