package service_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	payoutmock "github.com/tosnetwork/atos/internal/adapters/payout/mock"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// earningsHarness extends newHarness with an EarningsService wired into the
// JobService, exactly as cmd/api/main.go would, so this test exercises the
// real settlement path end to end -- not a stubbed shortcut.
func earningsHarness(t *testing.T) (harness, *service.EarningsService) {
	t.Helper()
	h := newHarness()
	earnings := service.NewEarningsService(h.store(), payoutmock.New()).WithMaturationPeriod(time.Nanosecond)
	h.jobs.WithEarnings(earnings)
	return h, earnings
}

// TestJobSettlement_CreatesProviderEarning proves the full Quote -> Invoke
// -> settle pipeline creates exactly one ProviderEarning for the completed
// Job's settlement, with the amount correctly matching what was actually
// charged (a Fixed-price capability charges its full subtotal).
func TestJobSettlement_CreatesProviderEarning(t *testing.T) {
	ctx := context.Background()
	h, earnings := earningsHarness(t)

	cap := registerCapability(t, h, "agt_earning_provider", "1.00")
	quote := createQuote(t, h, cap.ID)

	result, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_earning_client", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "earning-happy-path",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Job.State != domain.JobCompleted {
		t.Fatalf("job state = %s, want completed", result.Job.State)
	}

	list, err := earnings.ListByProvider(ctx, "agt_earning_provider")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("found %d earnings for the provider, want exactly 1", len(list))
	}
	e := list[0]
	if e.JobID != result.Job.ID {
		t.Fatalf("earning job_id = %s, want %s", e.JobID, result.Job.ID)
	}
	if e.Status != domain.EarningMaturing {
		t.Fatalf("earning status = %s, want maturing", e.Status)
	}
	// Fixed pricing (no MeteredRates): full subtotal (1.00), 5% gateway fee
	// (0.05) kept by ATOS out of the 1.05 total charged.
	if e.NetAmount.Amount != "1.00" {
		t.Fatalf("net amount = %s, want 1.00", e.NetAmount.Amount)
	}
	if e.GatewayFee.Amount != "0.05" {
		t.Fatalf("gateway fee = %s, want 0.05", e.GatewayFee.Amount)
	}
	if e.GrossAmount.Amount != "1.05" {
		t.Fatalf("gross amount = %s, want 1.05", e.GrossAmount.Amount)
	}

	snap, err := earnings.BillingSnapshotForJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatalf("BillingSnapshotForJob: %v", err)
	}
	if snap.GrossCharge.Amount != "1.05" {
		t.Fatalf("billing snapshot gross charge = %s, want 1.05", snap.GrossCharge.Amount)
	}

	// The earning must reach Available and then Paid through the normal
	// reconciler sweeps, proving the full lifecycle wires together.
	if _, err := earnings.MaturationSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := earnings.PayoutSweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	final, err := earnings.Get(ctx, e.ID, "agt_earning_provider")
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != domain.EarningPaid {
		t.Fatalf("final earning status = %s, want paid", final.Status)
	}
}

// TestJobSettlement_MeteredCapabilityChargesUsage proves a Metered
// capability's Quote freezes MeteredRates, and settlement bills the
// verified usage rather than the full subtotal, end to end.
func TestJobSettlement_MeteredCapabilityChargesUsage(t *testing.T) {
	ctx := context.Background()
	h, earnings := earningsHarness(t)

	cap, err := h.capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_metered_provider", Name: "Metered Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{
			Model:     domain.PricingMetered,
			PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"},
			MeteredRates: &domain.MeteredRates{
				// Metered on input size, not output: the mock provider's
				// response always embeds a fixed descriptive note plus an
				// echo of the input, so its OutputBytes is large enough to
				// clamp against the subtotal at any 2-decimal rate: it isn't
				// a useful dimension for demonstrating a genuinely
				// below-maximum metered charge. The request Input here
				// (`{"x":1}`, 7 bytes) is small and fully controlled by this
				// test, so metering on it deterministically charges well
				// under the 1.00 quoted subtotal.
				PerInputByte: "0.01",
			},
		},
		IdempotencyKey: "register-metered-provider",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	quote := createQuote(t, h, cap.ID)
	if quote.MeteredRates == nil {
		t.Fatal("quote did not freeze the capability's metered rates")
	}

	result, err := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_metered_client", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "metered-happy-path",
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.Job.State != domain.JobCompleted {
		t.Fatalf("job state = %s, want completed", result.Job.State)
	}

	snap, err := earnings.BillingSnapshotForJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatalf("BillingSnapshotForJob: %v", err)
	}
	if snap.GrossCharge.Amount == "1.05" {
		t.Fatalf("metered job charged the full quoted subtotal+fee (1.05); expected usage-based billing to charge less")
	}
	total, err := parseCents(t, snap.GrossCharge.Amount)
	if err != nil {
		t.Fatal(err)
	}
	if total > 105 || total < 0 {
		t.Fatalf("gross charge %s out of the [0, 1.05] bound implied by quote.total_max", snap.GrossCharge.Amount)
	}
}

// rejectingVerifyCore forces every execution receipt verification to fail,
// simulating a tampered or otherwise unverifiable receipt -- exactly the
// scenario metered billing's usage source must never be exposed to: a
// caller/provider-supplied Usage that never passed verification.
type rejectingVerifyCore struct {
	toscore.Core
}

func (c *rejectingVerifyCore) VerifyExecutionReceipt(ctx context.Context, escrowID string, receipt domain.ExecutionReceipt) (toscore.VerifyExecutionReceiptResult, error) {
	return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "injected verification failure (simulated tampered/unverifiable usage)"}, nil
}

// TestJobSettlement_FailedVerificationNeverChargesOrEarns proves that when
// an execution receipt fails verification, metered billing's computed
// charge is never actually applied: the job fails, the principal is
// refunded in full, and no ProviderEarning is created. This is what makes
// "usage for billing only from a verified Execution Receipt" true in
// practice -- billing may compute a value from an as-yet-unverified
// receipt, but that value only ever reaches SettleJob (and therefore
// RecordSettlement) once verification has actually succeeded.
func TestJobSettlement_FailedVerificationNeverChargesOrEarns(t *testing.T) {
	ctx := context.Background()
	h := crashHarness(func(core toscore.Core) toscore.Core { return &rejectingVerifyCore{Core: core} })
	earnings := service.NewEarningsService(h.store(), payoutmock.New())
	h.jobs.WithEarnings(earnings)

	cap := registerCapability(t, h, "agt_reject_provider", "1.00")
	quote := createQuote(t, h, cap.ID)
	before, err := h.accounts.Get(ctx, "prn_reject_client")
	if err != nil {
		t.Fatal(err)
	}

	result, _ := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_reject_client", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "reject-verify-1",
	})
	if result.Job.State != domain.JobFailed {
		t.Fatalf("job state = %s, want failed", result.Job.State)
	}

	after, err := h.accounts.Get(ctx, "prn_reject_client")
	if err != nil {
		t.Fatal(err)
	}
	if after.Balance != before.Balance {
		t.Fatalf("balance after rejected verification = %s, want unchanged from %s (full refund)", after.Balance.Amount, before.Balance.Amount)
	}

	list, err := earnings.ListByProvider(ctx, "agt_reject_provider")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("earnings for a failed settlement = %+v, want none", list)
	}
}

// TestJobSettlement_LegacyBadFrozenPricingReleasesAndRefunds proves that a
// Quote whose frozen MeteredRates are invalid -- e.g. persisted before
// validatePricing existed, or otherwise corrupted -- cannot leave a Job (and
// the principal's escrowed funds) stuck retrying settlement forever.
// computeBillingSnapshot's failure for such a Quote is deterministic (same
// frozen Quote + same verified Receipt every time), so settlement must fail
// the Job outright, release the escrow, and refund the principal in full --
// not loop the Job through JobReconciling/EconomicEscrowReserved.
func TestJobSettlement_LegacyBadFrozenPricingReleasesAndRefunds(t *testing.T) {
	ctx := context.Background()
	h, earnings := earningsHarness(t)

	cap, err := h.capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: "agt_badrate_provider", Name: "Metered Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{
			Model:        domain.PricingMetered,
			PriceHint:    domain.PriceHint{Amount: "1.00", Currency: "USD"},
			MeteredRates: &domain.MeteredRates{PerOutputToken: "0.01"},
		},
		IdempotencyKey: "register-badrate-provider",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	quote := createQuote(t, h, cap.ID)

	// Simulate a Quote whose frozen pricing predates validatePricing (or was
	// otherwise corrupted in storage): overwrite the already-created Quote
	// directly in the store with an invalid rate, bypassing
	// QuoteService.Create's defense-in-depth check entirely.
	quote.MeteredRates = &domain.MeteredRates{PerOutputToken: "not-a-number"}
	if err := h.store().PutQuote(ctx, quote); err != nil {
		t.Fatalf("PutQuote (corrupting frozen rate): %v", err)
	}

	before, err := h.accounts.Get(ctx, "prn_badrate_client")
	if err != nil {
		t.Fatal(err)
	}

	result, invokeErr := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_badrate_client", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "badrate-1",
	})
	if result.Job.State != domain.JobFailed {
		t.Fatalf("job state = %s (err=%v), want failed", result.Job.State, invokeErr)
	}
	if result.Job.EconomicState != domain.EconomicReleased {
		t.Fatalf("economic state = %s, want released", result.Job.EconomicState)
	}
	if result.Job.ReconciliationRequired {
		t.Fatal("a terminal Job must not be left with reconciliation still required")
	}

	after, err := h.accounts.Get(ctx, "prn_badrate_client")
	if err != nil {
		t.Fatal(err)
	}
	if after.Balance != before.Balance {
		t.Fatalf("balance after bad-pricing settlement failure = %s, want unchanged from %s (full refund)", after.Balance.Amount, before.Balance.Amount)
	}

	list, err := earnings.ListByProvider(ctx, "agt_badrate_provider")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("earnings for a bad-pricing settlement failure = %+v, want none", list)
	}

	// A Job already terminal (Failed) must not be reconciled again --
	// proving there is no infinite reconciliation loop left behind.
	reconciled, err := h.jobs.ReconcileJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatalf("ReconcileJob on a terminal job: %v", err)
	}
	if reconciled.State != domain.JobFailed || reconciled.ReconciliationRequired {
		t.Fatalf("reconciling a terminal job changed its state: %+v", reconciled)
	}
}

// TestJobSettlement_CorruptFixedModelWithFrozenRatesReleasesAndRefunds
// proves the PricingModel/MeteredRates compatibility check
// computeBillingSnapshot now enforces also fails closed end to end through
// real settlement: a Quote whose PricingModel is Fixed but which somehow
// still carries a frozen MeteredRates (corrupted, or predating that
// validation) must never be silently billed by usage -- it must fail the
// Job, release the escrow, and refund the principal in full, exactly like
// an outright malformed rate does.
func TestJobSettlement_CorruptFixedModelWithFrozenRatesReleasesAndRefunds(t *testing.T) {
	ctx := context.Background()
	h, earnings := earningsHarness(t)

	cap := registerCapability(t, h, "agt_modelmismatch_provider", "1.00")
	quote := createQuote(t, h, cap.ID)
	if quote.PricingModel != domain.PricingFixed || quote.MeteredRates != nil {
		t.Fatalf("sanity check failed: quote = %+v", quote)
	}

	// Simulate a Quote that somehow froze a MeteredRates despite a Fixed
	// pricing_model (corrupted, or predating the Model/MeteredRates
	// compatibility check): overwrite the already-created Quote directly in
	// the store, bypassing QuoteService.Create's defense-in-depth entirely.
	quote.MeteredRates = &domain.MeteredRates{PerOutputToken: "0.01"}
	if err := h.store().PutQuote(ctx, quote); err != nil {
		t.Fatalf("PutQuote (corrupting frozen pricing_model/rates combination): %v", err)
	}

	before, err := h.accounts.Get(ctx, "prn_modelmismatch_client")
	if err != nil {
		t.Fatal(err)
	}

	result, invokeErr := h.jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: "prn_modelmismatch_client", CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "modelmismatch-1",
	})
	if result.Job.State != domain.JobFailed {
		t.Fatalf("job state = %s (err=%v), want failed", result.Job.State, invokeErr)
	}
	if result.Job.EconomicState != domain.EconomicReleased {
		t.Fatalf("economic state = %s, want released", result.Job.EconomicState)
	}

	after, err := h.accounts.Get(ctx, "prn_modelmismatch_client")
	if err != nil {
		t.Fatal(err)
	}
	if after.Balance != before.Balance {
		t.Fatalf("balance after pricing_model/metered_rates mismatch = %s, want unchanged from %s (full refund)", after.Balance.Amount, before.Balance.Amount)
	}

	list, err := earnings.ListByProvider(ctx, "agt_modelmismatch_provider")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("earnings for a pricing_model/metered_rates mismatch = %+v, want none", list)
	}
}

func parseCents(t *testing.T, amount string) (int, error) {
	t.Helper()
	parts := strings.SplitN(amount, ".", 2)
	whole, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}
	frac := 0
	if len(parts) == 2 {
		frac, err = strconv.Atoi(parts[1])
		if err != nil {
			return 0, err
		}
	}
	return whole*100 + frac, nil
}
