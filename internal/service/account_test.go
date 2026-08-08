package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func TestManagedDailySpendWindowResetsAtUTCMidnight(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 8, 23, 59, 0, 0, time.UTC)
	accounts := service.NewAccountService(memory.New()).WithClock(func() time.Time { return now })
	if err := accounts.Debit(ctx, "prn_daily", "3.00", "USD"); err != nil {
		t.Fatal(err)
	}
	before, err := accounts.Get(ctx, "prn_daily")
	if err != nil {
		t.Fatal(err)
	}
	if before.SpendPolicy.RemainingToday.Amount != "17.00" {
		t.Fatalf("remaining before reset = %s", before.SpendPolicy.RemainingToday.Amount)
	}
	if !before.SpendPolicy.ResetAt.Equal(time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("reset_at = %s", before.SpendPolicy.ResetAt)
	}

	now = time.Date(2026, 8, 9, 0, 0, 1, 0, time.UTC)
	after, err := accounts.Get(ctx, "prn_daily")
	if err != nil {
		t.Fatal(err)
	}
	if after.SpendPolicy.RemainingToday.Amount != "20.00" {
		t.Fatalf("remaining after reset = %s, want 20.00", after.SpendPolicy.RemainingToday.Amount)
	}
	if after.Balance.Amount != "22.00" {
		t.Fatalf("balance after reset = %s, daily reset must not mint credits", after.Balance.Amount)
	}
	if !after.SpendPolicy.ResetAt.Equal(time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("next reset_at = %s", after.SpendPolicy.ResetAt)
	}
}
