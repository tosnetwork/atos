package money

import "testing"

func TestParse(t *testing.T) {
	tests := []struct {
		input    string
		decimals int
		want     string
	}{
		{"5.25", 2, "5.25"},
		{"5", 2, "5.00"},
		{"0.01", 2, "0.01"},
		{"0", 2, "0.00"},
		{"", 2, "0.00"},
		{"100", 0, "100"},
	}
	for _, tt := range tests {
		got, err := Parse(tt.input, "USD", tt.decimals)
		if tt.input == "" {
			if err == nil {
				t.Errorf("Parse(%q) expected error, got %v", tt.input, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Parse(%q): %v", tt.input, err)
		}
		if got.String() != tt.want {
			t.Errorf("Parse(%q).String() = %q, want %q", tt.input, got.String(), tt.want)
		}
	}
}

func TestParseRejectsInvalid(t *testing.T) {
	invalid := []string{"-1", "-0.01", "1.234", "abc", "1.2.3", "1e5"}
	for _, in := range invalid {
		if _, err := Parse(in, "USD", 2); err == nil {
			t.Errorf("Parse(%q) should have failed", in)
		}
	}
}

func TestAdd(t *testing.T) {
	a, _ := Parse("5.25", "USD", 2)
	b, _ := Parse("2.75", "USD", 2)
	sum, err := a.Add(b)
	if err != nil {
		t.Fatal(err)
	}
	if sum.String() != "8.00" {
		t.Errorf("got %q, want 8.00", sum.String())
	}
}

func TestAddCurrencyMismatch(t *testing.T) {
	a, _ := Parse("5.25", "USD", 2)
	b, _ := Parse("2.75", "EUR", 2)
	if _, err := a.Add(b); err != ErrCurrencyMismatch {
		t.Errorf("got %v, want ErrCurrencyMismatch", err)
	}
}

func TestSub(t *testing.T) {
	a, _ := Parse("5.25", "USD", 2)
	b, _ := Parse("2.75", "USD", 2)
	diff, err := a.Sub(b)
	if err != nil {
		t.Fatal(err)
	}
	if diff.String() != "2.50" {
		t.Errorf("got %q, want 2.50", diff.String())
	}
}

func TestSubGoingNegativeFails(t *testing.T) {
	a, _ := Parse("2.00", "USD", 2)
	b, _ := Parse("5.00", "USD", 2)
	if _, err := a.Sub(b); err != ErrInsufficientFunds {
		t.Errorf("got %v, want ErrInsufficientFunds", err)
	}
}

func TestCmp(t *testing.T) {
	a, _ := Parse("5.00", "USD", 2)
	b, _ := Parse("5.00", "USD", 2)
	c, _ := Parse("4.99", "USD", 2)
	if a.Cmp(b) != 0 {
		t.Errorf("equal amounts should compare 0")
	}
	if a.Cmp(c) <= 0 {
		t.Errorf("5.00 should be > 4.99")
	}
	if c.Cmp(a) >= 0 {
		t.Errorf("4.99 should be < 5.00")
	}
}

func TestIsZeroIsPositive(t *testing.T) {
	zero := Zero("USD", 2)
	if !zero.IsZero() {
		t.Error("Zero() should be zero")
	}
	if zero.IsPositive() {
		t.Error("Zero() should not be positive")
	}
	one, _ := Parse("0.01", "USD", 2)
	if one.IsZero() {
		t.Error("0.01 should not be zero")
	}
	if !one.IsPositive() {
		t.Error("0.01 should be positive")
	}
}

func TestMulUint64(t *testing.T) {
	rate, _ := Parse("0.001", "USD", 3)
	got := rate.MulUint64(2500)
	if got.String() != "2.500" {
		t.Fatalf("MulUint64 = %q, want 2.500", got.String())
	}
	zeroRate, _ := Parse("0", "USD", 2)
	if got := zeroRate.MulUint64(1_000_000); !got.IsZero() {
		t.Fatalf("zero rate * n should stay zero, got %q", got.String())
	}
}

func TestMulDivProportionalSplit(t *testing.T) {
	fees, _ := Parse("0.05", "USD", 2)
	subtotal, _ := Parse("1.00", "USD", 2)
	half, _ := Parse("0.50", "USD", 2)

	got, err := fees.MulDiv(half, subtotal)
	if err != nil {
		t.Fatal(err)
	}
	// 0.05 * 0.50 / 1.00 = 0.025 -> truncates to 0.02 at 2 decimals.
	if got.String() != "0.02" {
		t.Fatalf("MulDiv = %q, want 0.02 (truncated)", got.String())
	}

	full, err := fees.MulDiv(subtotal, subtotal)
	if err != nil {
		t.Fatal(err)
	}
	if full.String() != fees.String() {
		t.Fatalf("scaling by numerator==denominator should be a no-op, got %q want %q", full.String(), fees.String())
	}
}

func TestMulDivRejectsZeroDenominator(t *testing.T) {
	fees, _ := Parse("0.05", "USD", 2)
	zero, _ := Parse("0", "USD", 2)
	if _, err := fees.MulDiv(fees, zero); err == nil {
		t.Fatal("expected an error for a zero denominator")
	}
}

func TestMulDivRejectsCurrencyMismatch(t *testing.T) {
	usd, _ := Parse("1.00", "USD", 2)
	eur, _ := Parse("1.00", "EUR", 2)
	if _, err := usd.MulDiv(eur, usd); err == nil {
		t.Fatal("expected a currency mismatch error")
	}
	if _, err := usd.MulDiv(usd, eur); err == nil {
		t.Fatal("expected a currency mismatch error")
	}
}

func TestMin(t *testing.T) {
	small, _ := Parse("1.00", "USD", 2)
	big, _ := Parse("2.00", "USD", 2)
	if got := small.Min(big); got.String() != "1.00" {
		t.Fatalf("Min = %q, want 1.00", got.String())
	}
	if got := big.Min(small); got.String() != "1.00" {
		t.Fatalf("Min = %q, want 1.00", got.String())
	}
}

func TestParseRejectsTooManyDecimals(t *testing.T) {
	if _, err := Parse("1.999", "USD", 2); err == nil {
		t.Error("expected error for excess decimal precision")
	}
}
