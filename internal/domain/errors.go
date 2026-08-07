package domain

// ErrorCode enumerates the stable machine codes from docs/API.md's error
// envelope so REST and MCP surfaces can share one vocabulary.
type ErrorCode string

const (
	ErrAuthenticationRequired ErrorCode = "authentication_required"
	ErrPermissionDenied       ErrorCode = "permission_denied"
	ErrRateLimited            ErrorCode = "rate_limited"
	ErrValidationFailed       ErrorCode = "validation_failed"
	ErrCapabilityUnavailable  ErrorCode = "capability_unavailable"
	ErrQuoteExpired           ErrorCode = "quote_expired"
	ErrQuoteMismatch          ErrorCode = "quote_mismatch"
	ErrSpendLimitExceeded     ErrorCode = "spend_limit_exceeded"
	ErrInsufficientBalance    ErrorCode = "insufficient_balance"
	ErrIdempotencyConflict    ErrorCode = "idempotency_conflict"
	ErrJobNotCancelable       ErrorCode = "job_not_cancelable"
	ErrProviderFailed         ErrorCode = "provider_failed"
	ErrSettlementFailed       ErrorCode = "settlement_failed"
	ErrNotFound               ErrorCode = "not_found"
)

// Error is a machine-coded application error. It carries whether the caller
// may safely retry, matching docs/API.md's error envelope.
type Error struct {
	Code      ErrorCode
	Message   string
	Retryable bool
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

func NewError(code ErrorCode, message string, retryable bool) *Error {
	return &Error{Code: code, Message: message, Retryable: retryable}
}
