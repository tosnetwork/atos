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

func TestParseRejectsTooManyDecimals(t *testing.T) {
	if _, err := Parse("1.999", "USD", 2); err == nil {
		t.Error("expected error for excess decimal precision")
	}
}
