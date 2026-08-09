package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

func testBillingSnapshot(jobID string) domain.BillingSnapshot {
	return domain.BillingSnapshot{
		JobID: jobID, QuoteID: "q_1", ReceiptID: "rcpt_1", ProviderID: "prov_1",
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0", TrustMode: domain.TrustModeManaged,
		GrossCharge: domain.Money{Amount: "0.52", Currency: "USD"}, ProviderGross: domain.Money{Amount: "0.50", Currency: "USD"},
		GatewayFee: domain.Money{Amount: "0.02", Currency: "USD"}, PrincipalRefund: domain.Money{Amount: "0.53", Currency: "USD"},
		CalculatedAt: time.Now().UTC(),
	}
}

func TestPutBillingSnapshotIdempotentRetry(t *testing.T) {
	ctx := context.Background()
	s := New()
	snap := testBillingSnapshot("job_1")

	first, created, err := s.PutBillingSnapshot(ctx, snap)
	if err != nil || !created {
		t.Fatalf("first put: created=%v err=%v", created, err)
	}
	// A later recomputation may have a different CalculatedAt but must
	// still count as identical economic content.
	retry := snap
	retry.CalculatedAt = snap.CalculatedAt.Add(time.Hour)
	second, created, err := s.PutBillingSnapshot(ctx, retry)
	if err != nil {
		t.Fatalf("retry put: %v", err)
	}
	if created {
		t.Fatal("retry with identical economic content should report created=false")
	}
	if second.GrossCharge != first.GrossCharge {
		t.Fatalf("retry returned different content: %+v vs %+v", second, first)
	}
}

func TestPutBillingSnapshotConflictingRecomputeIsRejected(t *testing.T) {
	ctx := context.Background()
	s := New()
	snap := testBillingSnapshot("job_conflict")
	if _, _, err := s.PutBillingSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}

	conflicting := snap
	conflicting.GrossCharge = domain.Money{Amount: "0.99", Currency: "USD"}
	_, _, err := s.PutBillingSnapshot(ctx, conflicting)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}

	got, err := s.BillingSnapshotByJob(ctx, "job_conflict")
	if err != nil {
		t.Fatal(err)
	}
	if got.GrossCharge.Amount != "0.52" {
		t.Fatalf("stored snapshot mutated by rejected conflict: %s", got.GrossCharge.Amount)
	}
}

func testMemEarning(providerID, jobID, settlementID string) domain.ProviderEarning {
	now := time.Now().UTC()
	return domain.ProviderEarning{
		ID: "earn_" + settlementID, ProviderID: providerID, JobID: jobID,
		QuoteID: "q_" + jobID, ReceiptID: "rcpt_" + jobID, SettlementID: settlementID,
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0",
		GrossAmount: domain.Money{Amount: "1.05", Currency: "USD"},
		GatewayFee:  domain.Money{Amount: "0.05", Currency: "USD"},
		NetAmount:   domain.Money{Amount: "1.00", Currency: "USD"},
		Status:      domain.EarningMaturing, CreatedAt: now, MaturesAt: now,
	}
}

func TestCreateEarningIdempotentRetryIgnoresLifecycleFields(t *testing.T) {
	ctx := context.Background()
	s := New()
	e := testMemEarning("prov_1", "job_1", "settle_1")
	first, created, err := s.CreateEarning(ctx, e)
	if err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}
	// A retry with a different Status/CreatedAt/MaturesAt (as a freshly
	// constructed earning object would have) must still be recognized as
	// the same economic earning.
	retry := e
	retry.Status = domain.EarningAvailable
	retry.CreatedAt = e.CreatedAt.Add(time.Hour)
	retry.MaturesAt = e.MaturesAt.Add(2 * time.Hour)
	second, created, err := s.CreateEarning(ctx, retry)
	if err != nil {
		t.Fatalf("retry create: %v", err)
	}
	if created {
		t.Fatal("retry with identical identity/economic fields should report created=false")
	}
	if second.ID != first.ID || second.Status != first.Status {
		t.Fatalf("retry should return the ORIGINAL stored earning unchanged: %+v", second)
	}
}

func TestCreateEarningConflictingContentIsRejected(t *testing.T) {
	ctx := context.Background()
	s := New()
	e := testMemEarning("prov_1", "job_1", "settle_conflict")
	if _, _, err := s.CreateEarning(ctx, e); err != nil {
		t.Fatal(err)
	}

	conflicting := e
	conflicting.NetAmount = domain.Money{Amount: "999.99", Currency: "USD"}
	_, _, err := s.CreateEarning(ctx, conflicting)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrIdempotencyConflict {
		t.Fatalf("got %v, want domain.ErrIdempotencyConflict", err)
	}

	got, err := s.EarningBySettlement(ctx, "settle_conflict")
	if err != nil {
		t.Fatal(err)
	}
	if got.NetAmount.Amount != "1.00" {
		t.Fatalf("stored earning mutated by rejected conflict: %s", got.NetAmount.Amount)
	}
}
