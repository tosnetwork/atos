// Package money represents financial amounts as fixed-point minor units
// (e.g. cents) backed by big.Int, never float64, and every arithmetic
// operation is checked so a bug fails loudly instead of silently drifting
// an escrow balance.
package money

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
)

var (
	ErrNegativeAmount    = errors.New("money: amount must not be negative")
	ErrCurrencyMismatch  = errors.New("money: currency mismatch")
	ErrInsufficientFunds = errors.New("money: insufficient funds")
)

// Amount is a non-negative fixed-point value in a single currency's minor
// units (e.g. USD cents, decimals=2).
type Amount struct {
	Minor    *big.Int
	Currency string
	Decimals int
}

// Zero returns a zero-value Amount in the given currency.
func Zero(currency string, decimals int) Amount {
	return Amount{Minor: big.NewInt(0), Currency: currency, Decimals: decimals}
}

// Parse converts a decimal string (e.g. "5.25") into minor units. It rejects
// negative amounts, malformed input, and precision beyond decimals.
func Parse(value, currency string, decimals int) (Amount, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.HasPrefix(value, "-") {
		return Amount{}, fmt.Errorf("money: invalid amount %q", value)
	}
	parts := strings.SplitN(value, ".", 2)
	whole := parts[0]
	frac := ""
	if len(parts) == 2 {
		frac = parts[1]
	}
	if whole == "" {
		whole = "0"
	}
	if len(frac) > decimals {
		return Amount{}, fmt.Errorf("money: %q has more than %d decimal places", value, decimals)
	}
	for _, r := range whole + frac {
		if r < '0' || r > '9' {
			return Amount{}, fmt.Errorf("money: invalid amount %q", value)
		}
	}
	digits := whole + frac + strings.Repeat("0", decimals-len(frac))
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		digits = "0"
	}
	n, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return Amount{}, fmt.Errorf("money: invalid amount %q", value)
	}
	return Amount{Minor: n, Currency: currency, Decimals: decimals}, nil
}

// String renders the amount back to a decimal string.
func (a Amount) String() string {
	s := a.Minor.String()
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	if a.Decimals == 0 {
		if neg {
			return "-" + s
		}
		return s
	}
	if len(s) <= a.Decimals {
		s = strings.Repeat("0", a.Decimals-len(s)+1) + s
	}
	cut := len(s) - a.Decimals
	out := s[:cut] + "." + s[cut:]
	if neg {
		return "-" + out
	}
	return out
}

func (a Amount) sameUnit(b Amount) error {
	if a.Currency != b.Currency {
		return ErrCurrencyMismatch
	}
	if a.Decimals != b.Decimals {
		return fmt.Errorf("money: decimals mismatch (%d vs %d)", a.Decimals, b.Decimals)
	}
	return nil
}

// Add returns a+b, checked for currency/decimals agreement.
func (a Amount) Add(b Amount) (Amount, error) {
	if err := a.sameUnit(b); err != nil {
		return Amount{}, err
	}
	return Amount{Minor: new(big.Int).Add(a.Minor, b.Minor), Currency: a.Currency, Decimals: a.Decimals}, nil
}

// Sub returns a-b, checked for currency/decimals agreement and for a
// negative result — money amounts are never allowed to go negative.
func (a Amount) Sub(b Amount) (Amount, error) {
	if err := a.sameUnit(b); err != nil {
		return Amount{}, err
	}
	result := new(big.Int).Sub(a.Minor, b.Minor)
	if result.Sign() < 0 {
		return Amount{}, ErrInsufficientFunds
	}
	return Amount{Minor: result, Currency: a.Currency, Decimals: a.Decimals}, nil
}

// Cmp compares a and b, panicking on currency/decimals mismatch — callers
// that cannot guarantee matching units must call sameUnit-checked methods
// instead (Add/Sub) rather than Cmp.
func (a Amount) Cmp(b Amount) int {
	if err := a.sameUnit(b); err != nil {
		panic(err)
	}
	return a.Minor.Cmp(b.Minor)
}

// IsZero reports whether the amount is exactly zero.
func (a Amount) IsZero() bool {
	return a.Minor.Sign() == 0
}

// IsPositive reports whether the amount is strictly greater than zero.
func (a Amount) IsPositive() bool {
	return a.Minor.Sign() > 0
}

// MulUint64 returns a*n. big.Int has no fixed-width overflow, so this is
// exact regardless of magnitude. Used to price a metered usage count
// (e.g. output tokens) against a per-unit rate.
func (a Amount) MulUint64(n uint64) Amount {
	return Amount{Minor: new(big.Int).Mul(a.Minor, new(big.Int).SetUint64(n)), Currency: a.Currency, Decimals: a.Decimals}
}

// MulDiv returns a*numerator/denominator, truncating toward zero (Go's
// big.Int.Div semantics), checked for currency/decimals agreement between
// a, numerator and denominator. Used to scale one amount in proportion to
// the ratio of two others -- e.g. splitting a quoted gateway fee in
// proportion to how much of the quoted provider subtotal was actually
// metered, so the split amounts are always guaranteed to sum to at most
// the original quoted total. denominator must not be zero.
func (a Amount) MulDiv(numerator, denominator Amount) (Amount, error) {
	if err := a.sameUnit(numerator); err != nil {
		return Amount{}, err
	}
	if err := a.sameUnit(denominator); err != nil {
		return Amount{}, err
	}
	if denominator.Minor.Sign() == 0 {
		return Amount{}, errors.New("money: division by zero")
	}
	product := new(big.Int).Mul(a.Minor, numerator.Minor)
	result := new(big.Int).Div(product, denominator.Minor)
	return Amount{Minor: result, Currency: a.Currency, Decimals: a.Decimals}, nil
}

// Min returns the smaller of a and b, checked for currency/decimals
// agreement.
func (a Amount) Min(b Amount) Amount {
	if a.Cmp(b) <= 0 {
		return a
	}
	return b
}
