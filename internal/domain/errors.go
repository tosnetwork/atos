package domain

type ErrorCode string

const (
	ErrAuthenticationRequired       ErrorCode = "authentication_required"
	ErrPermissionDenied             ErrorCode = "permission_denied"
	ErrRateLimited                  ErrorCode = "rate_limited"
	ErrValidationFailed             ErrorCode = "validation_failed"
	ErrCapabilityUnavailable        ErrorCode = "capability_unavailable"
	ErrTrustModeUnavailable         ErrorCode = "trust_mode_unavailable"
	ErrProofRequirementsUnsatisfied ErrorCode = "proof_requirements_unsatisfied"
	ErrProofProfileUnavailable      ErrorCode = "proof_profile_unavailable"
	ErrNetworkUnavailable           ErrorCode = "network_unavailable"
	ErrQuoteExpired                 ErrorCode = "quote_expired"
	ErrQuoteMismatch                ErrorCode = "quote_mismatch"
	ErrQuoteModeMismatch            ErrorCode = "quote_mode_mismatch"
	ErrRequoteRequired              ErrorCode = "requote_required"
	ErrSpendLimitExceeded           ErrorCode = "spend_limit_exceeded"
	ErrSpendConfirmationRequired    ErrorCode = "spend_confirmation_required"
	ErrSpendConfirmationDenied      ErrorCode = "spend_confirmation_denied"
	ErrSpendConfirmationExpired     ErrorCode = "spend_confirmation_expired"
	ErrInsufficientBalance          ErrorCode = "insufficient_balance"
	ErrIdempotencyConflict          ErrorCode = "idempotency_conflict"
	ErrJobNotCancelable             ErrorCode = "job_not_cancelable"
	ErrArtifactNotFound             ErrorCode = "artifact_not_found"
	ErrArtifactAccessDenied         ErrorCode = "artifact_access_denied"
	ErrUploadExpired                ErrorCode = "upload_expired"
	ErrUploadMismatch               ErrorCode = "upload_mismatch"
	ErrProviderFailed               ErrorCode = "provider_failed"
	ErrSettlementFailed             ErrorCode = "settlement_failed"
	ErrNotFound                     ErrorCode = "not_found"
	ErrStreamSequenceConflict       ErrorCode = "stream_sequence_conflict"
	ErrStreamOffsetInvalid          ErrorCode = "stream_offset_invalid"
	ErrStreamDigestInvalid          ErrorCode = "stream_digest_invalid"
	ErrStreamTerminal               ErrorCode = "stream_terminal"
	ErrStreamChunkTooLarge          ErrorCode = "stream_chunk_too_large"
	ErrStreamCursorMismatch         ErrorCode = "stream_cursor_mismatch"
	ErrStreamJobBindingMismatch     ErrorCode = "stream_job_binding_mismatch"
	ErrStreamEventTypeUnsupported   ErrorCode = "stream_event_type_unsupported"
	ErrDisputeWindowExpired         ErrorCode = "dispute_window_expired"
	ErrDisputeNotEligible           ErrorCode = "dispute_not_eligible"
	ErrDisputeInvalidTransition     ErrorCode = "dispute_invalid_transition"
)

type Error struct {
	Code      ErrorCode
	Message   string
	Retryable bool
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

func NewError(code ErrorCode, message string, retryable bool) *Error {
	return &Error{Code: code, Message: message, Retryable: retryable}
}
