package domain

// SpendPolicy bounds autonomous spending.
type SpendPolicy struct {
	PerCallAutonomousLimit Money `json:"per_call_autonomous_limit"`
	DailyLimit             Money `json:"daily_limit"`
	RemainingToday         Money `json:"remaining_today"`
}

type Account struct {
	PrincipalID string      `json:"-"`
	Balance     Money       `json:"balance"`
	SpendPolicy SpendPolicy `json:"spend_policy"`
	TrustPolicy TrustPolicy `json:"trust_policy"`
}
