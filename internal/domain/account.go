package domain

import "time"

// SpendPolicy bounds autonomous spending. ResetAt is the UTC boundary after
// which RemainingToday is replenished to DailyLimit by the AccountService.
type SpendPolicy struct {
	PerCallAutonomousLimit Money     `json:"per_call_autonomous_limit"`
	DailyLimit             Money     `json:"daily_limit"`
	RemainingToday         Money     `json:"remaining_today"`
	ResetAt                time.Time `json:"reset_at"`
}

type Account struct {
	PrincipalID string      `json:"-"`
	Balance     Money       `json:"balance"`
	SpendPolicy SpendPolicy `json:"spend_policy"`
	TrustPolicy TrustPolicy `json:"trust_policy"`
}
