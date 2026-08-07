package domain

// SpendPolicy bounds autonomous spending per docs/MCP.md's atos_account and
// the Spending Policy flow in README.md.
type SpendPolicy struct {
	PerCallAutonomousLimit Money `json:"per_call_autonomous_limit"`
	DailyLimit             Money `json:"daily_limit"`
	RemainingToday         Money `json:"remaining_today"`
}

type Account struct {
	PrincipalID string      `json:"-"`
	Balance     Money       `json:"balance"`
	SpendPolicy SpendPolicy `json:"spend_policy"`
}
