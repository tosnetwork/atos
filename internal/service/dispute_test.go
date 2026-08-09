package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/storage/local"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// disputeHarness extends earningsHarness with an ArtifactService and
// DisputeService, exactly as cmd/api/main.go wires them.
func disputeHarness(t *testing.T) (harness, *service.EarningsService, *service.DisputeService) {
	t.Helper()
	h, earnings := earningsHarness(t)
	blobStorage, err := local.New(t.TempDir(), "http://localhost", h.store())
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	artifacts := service.NewArtifactService(h.store(), blobStorage)
	disputes := service.NewDisputeService(h.store(), h.jobs, earnings, h.accounts, artifacts)
	return h, earnings, disputes
}

func putTestArtifact(t *testing.T, h harness, principalID, artifactID string) domain.DisputeEvidence {
	t.Helper()
	now := time.Now().UTC()
	err := h.store().PutArtifact(context.Background(), domain.StoredArtifact{
		ID: artifactID, OwnerPrincipalID: principalID, ContentType: "text/plain",
		SizeBytes: 10, Status: domain.ArtifactAvailable, CreatedAt: now, ExpiresAt: now.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}
	return domain.DisputeEvidence{ArtifactID: artifactID, Description: "screenshot"}
}

func completedJob(t *testing.T, h harness, providerID, principalID, price string) domain.Job {
	t.Helper()
	ctx := context.Background()
	cap := registerCapability(t, h, providerID, price)
	quote := createQuote(t, h, cap.ID)
	result, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: principalID, CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "job-" + principalID + "-" + cap.ID,
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Job.State != domain.JobCompleted {
		t.Fatalf("job state = %s, want completed", result.Job.State)
	}
	return result.Job
}

func TestDisputeOpen_HappyPath_FreezesAvailableEarning(t *testing.T) {
	ctx := context.Background()
	h, earnings, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_avail", "prn_dispute_avail", "1.00")

	if _, err := earnings.MaturationSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	evidence := putTestArtifact(t, h, "prn_dispute_avail", "art_evidence_avail")

	d, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: "prn_dispute_avail", JobID: job.ID, Reason: "not delivered",
		Description: "did not receive expected output", Evidence: []domain.DisputeEvidence{evidence},
		IdempotencyKey: "dispute-open-avail",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if d.ReviewStatus != domain.DisputeOpened {
		t.Fatalf("review status = %s, want opened", d.ReviewStatus)
	}
	if d.EconomicState != domain.DisputeEconomicFrozen {
		t.Fatalf("economic state = %s, want frozen", d.EconomicState)
	}
	if d.ProviderID != job.ProviderID || d.JobID != job.ID || d.QuoteID != job.QuoteID {
		t.Fatalf("binding mismatch: %+v", d)
	}
	if d.ChargedAmount.Amount != "1.05" {
		t.Fatalf("charged amount = %s, want 1.05", d.ChargedAmount.Amount)
	}
	if len(d.Evidence) != 1 || d.Evidence[0].ArtifactID != "art_evidence_avail" {
		t.Fatalf("evidence not bound: %+v", d.Evidence)
	}

	earning, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if earning.Status != domain.EarningFrozen {
		t.Fatalf("earning status = %s, want frozen", earning.Status)
	}

	// A frozen earning must never mature into payout via the normal sweeps.
	if _, err := earnings.PayoutSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	after, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != domain.EarningFrozen {
		t.Fatalf("earning status after PayoutSweep = %s, want still frozen", after.Status)
	}
}

func TestDisputeOpen_HappyPath_FreezesMaturingEarning(t *testing.T) {
	ctx := context.Background()
	h, earnings, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_maturing", "prn_dispute_maturing", "1.00")
	// No MaturationSweep -- the earning is still Maturing.

	d, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: "prn_dispute_maturing", JobID: job.ID, Reason: "quality issue",
		IdempotencyKey: "dispute-open-maturing",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if d.EconomicState != domain.DisputeEconomicFrozen {
		t.Fatalf("economic state = %s, want frozen", d.EconomicState)
	}
	earning, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if earning.Status != domain.EarningFrozen {
		t.Fatalf("earning status = %s, want frozen", earning.Status)
	}
	// A frozen earning must never mature via the normal sweep either.
	if _, err := earnings.MaturationSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	after, err := earnings.Get(ctx, d.EarningID, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != domain.EarningFrozen {
		t.Fatalf("earning status after MaturationSweep = %s, want still frozen", after.Status)
	}
}

func TestDisputeOpen_IdempotentReplaySameKeyReturnsOriginal(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_idem", "prn_dispute_idem", "1.00")

	in := service.OpenDisputeInput{
		PrincipalID: "prn_dispute_idem", JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-idem",
	}
	first, err := disputes.Open(ctx, in)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	second, err := disputes.Open(ctx, in)
	if err != nil {
		t.Fatalf("replay Open: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned a different dispute: %s vs %s", second.ID, first.ID)
	}
}

func TestDisputeOpen_ChangedSemanticsSameKeyConflict(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_conflict", "prn_dispute_conflict", "1.00")

	_, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: "prn_dispute_conflict", JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-conflict",
	})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	_, err = disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: "prn_dispute_conflict", JobID: job.ID, Reason: "completely different reason",
		IdempotencyKey: "dispute-open-conflict",
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}
}

func TestDisputeOpen_ConcurrentOpenersConvergeToOneDispute(t *testing.T) {
	ctx := context.Background()
	h, earnings, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_concurrent", "prn_dispute_concurrent", "1.00")

	const attempts = 8
	results := make(chan domain.Dispute, attempts)
	errs := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		go func(i int) {
			d, err := disputes.Open(ctx, service.OpenDisputeInput{
				PrincipalID: "prn_dispute_concurrent", JobID: job.ID, Reason: "not delivered",
				IdempotencyKey: "dispute-open-concurrent-" + string(rune('a'+i)),
			})
			results <- d
			errs <- err
		}(i)
	}
	ids := make(map[string]bool)
	for i := 0; i < attempts; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Open: %v", err)
		}
		ids[(<-results).ID] = true
	}
	if len(ids) != 1 {
		t.Fatalf("observed %d distinct dispute ids, want exactly 1: %v", len(ids), ids)
	}

	all, err := earnings.ListByProvider(ctx, job.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	frozen := 0
	for _, e := range all {
		if e.Status == domain.EarningFrozen {
			frozen++
		}
	}
	if frozen != 1 {
		t.Fatalf("frozen earnings = %d, want exactly 1", frozen)
	}
}

func TestDisputeOpen_WrongPrincipalRejected(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_wrongprincipal", "prn_dispute_owner", "1.00")

	_, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: "prn_someone_else", JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-wrong",
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrPermissionDenied {
		t.Fatalf("got %v, want domain.ErrPermissionDenied", err)
	}
}

// TestDisputeOpen_ProviderCannotOpenAsPrincipal proves the job's own
// provider cannot open a dispute against itself by presenting its own
// identity as the principal.
func TestDisputeOpen_ProviderCannotOpenAsPrincipal(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_selfprovider", "prn_dispute_selfprovider", "1.00")

	_, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: job.ProviderID, JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-selfprovider",
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrPermissionDenied {
		t.Fatalf("got %v, want domain.ErrPermissionDenied", err)
	}
}

func TestDisputeOpen_NonManagedTrustModeRejected(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_verified", "prn_dispute_verified", "1.00")

	if _, err := h.store().UpdateJob(ctx, job.ID, func(j domain.Job, exists bool) (domain.Job, error) {
		j.TrustMode = domain.TrustModeVerified
		return j, nil
	}); err != nil {
		t.Fatal(err)
	}

	_, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: "prn_dispute_verified", JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-verified",
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrDisputeNotEligible {
		t.Fatalf("got %v, want domain.ErrDisputeNotEligible", err)
	}
}

func TestDisputeOpen_ExpiredWindowRejected(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_expired", "prn_dispute_expired", "1.00")

	longAgo := time.Now().UTC().Add(-73 * time.Hour)
	if _, err := h.store().UpdateJob(ctx, job.ID, func(j domain.Job, exists bool) (domain.Job, error) {
		j.CompletedAt = &longAgo
		return j, nil
	}); err != nil {
		t.Fatal(err)
	}

	_, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: "prn_dispute_expired", JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-expired",
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrDisputeWindowExpired {
		t.Fatalf("got %v, want domain.ErrDisputeWindowExpired", err)
	}
}

// TestDisputeOpen_QuoteBindingSubstitutionRejected proves a Job whose
// quote_id was tampered to point at a Quote belonging to a different
// capability/provider is rejected rather than silently disputed against
// the wrong contract.
func TestDisputeOpen_QuoteBindingSubstitutionRejected(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_substitution", "prn_dispute_substitution", "1.00")

	otherCap := registerCapability(t, h, "agt_dispute_other_provider", "5.00")
	otherQuote := createQuote(t, h, otherCap.ID)

	if _, err := h.store().UpdateJob(ctx, job.ID, func(j domain.Job, exists bool) (domain.Job, error) {
		j.QuoteID = otherQuote.ID
		return j, nil
	}); err != nil {
		t.Fatal(err)
	}

	_, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: "prn_dispute_substitution", JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-substitution",
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrDisputeNotEligible {
		t.Fatalf("got %v, want domain.ErrDisputeNotEligible", err)
	}
}

// TestDisputeOpen_CapabilityVersionSubstitutionRejected proves the
// earning/job binding check catches a mismatch that quote-binding
// validation alone would not: a Job whose capability_version was tampered
// after settlement so it no longer matches the ProviderEarning's own
// capability_version (frozen from the BillingSnapshot at settlement time).
func TestDisputeOpen_CapabilityVersionSubstitutionRejected(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_capver", "prn_dispute_capver", "1.00")

	if _, err := h.store().UpdateJob(ctx, job.ID, func(j domain.Job, exists bool) (domain.Job, error) {
		j.CapabilityVersion = "9.9.9-tampered"
		return j, nil
	}); err != nil {
		t.Fatal(err)
	}

	_, err := disputes.Open(ctx, service.OpenDisputeInput{
		PrincipalID: "prn_dispute_capver", JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-capver",
	})
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrDisputeNotEligible {
		t.Fatalf("got %v, want domain.ErrDisputeNotEligible", err)
	}
}

// TestDisputeOpen_IdempotentReplayBypassesWindowExpiry proves a replay of
// an already-successful Open call returns the original dispute even if
// dynamic eligibility state (here, the dispute window) would now reject a
// fresh request -- idempotency is reserved before any dynamic check runs,
// so a genuinely successful prior result is never re-litigated against
// state that has since changed.
func TestDisputeOpen_IdempotentReplayBypassesWindowExpiry(t *testing.T) {
	ctx := context.Background()
	h, _, disputes := disputeHarness(t)
	job := completedJob(t, h, "agt_dispute_replaywindow", "prn_dispute_replaywindow", "1.00")

	in := service.OpenDisputeInput{
		PrincipalID: "prn_dispute_replaywindow", JobID: job.ID, Reason: "not delivered",
		IdempotencyKey: "dispute-open-replaywindow",
	}
	first, err := disputes.Open(ctx, in)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}

	// Simulate the dispute window having since expired -- a fresh request
	// against this Job would now be rejected.
	longAgo := time.Now().UTC().Add(-73 * time.Hour)
	if _, err := h.store().UpdateJob(ctx, job.ID, func(j domain.Job, exists bool) (domain.Job, error) {
		j.CompletedAt = &longAgo
		return j, nil
	}); err != nil {
		t.Fatal(err)
	}

	replay, err := disputes.Open(ctx, in)
	if err != nil {
		t.Fatalf("replay Open (should bypass the now-expired window and return the cached result): %v", err)
	}
	if replay.ID != first.ID {
		t.Fatalf("replay returned a different dispute: %s vs %s", replay.ID, first.ID)
	}
}
