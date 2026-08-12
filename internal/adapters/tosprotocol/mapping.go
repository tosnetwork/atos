package toprotocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/domain"
	atostosv1 "github.com/tosnetwork/tos-protocol/gen/atos/tos/v1"
)

const errorCodeHeader = "Atos-Error-Code"

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return domain.NewError(domain.ErrNetworkUnavailable, err.Error(), true)
	}
	stable := strings.TrimSpace(connectErr.Meta().Get(errorCodeHeader))
	code := domain.ErrProviderFailed
	retryable := false
	switch stable {
	case "TRUST_MODE_UNAVAILABLE":
		code = domain.ErrTrustModeUnavailable
		retryable = true
	case "PROOF_PROFILE_UNAVAILABLE":
		code = domain.ErrProofProfileUnavailable
		retryable = true
	case "CAPABILITY_UNAVAILABLE", "CAPABILITY_OWNERSHIP_FAILED":
		code = domain.ErrCapabilityUnavailable
	case "QUOTE_EXPIRED", "SERVICE_QUOTE_EXPIRED":
		code = domain.ErrQuoteExpired
	case "QUOTE_MISMATCH", "SERVICE_QUOTE_MISMATCH", "ESCROW_MISMATCH", "JOB_MISMATCH", "RECEIPT_MISMATCH":
		code = domain.ErrQuoteMismatch
	case "REQUOTE_REQUIRED":
		code = domain.ErrRequoteRequired
	case "IDEMPOTENCY_CONFLICT":
		code = domain.ErrIdempotencyConflict
	case "PERMISSION_DENIED", "SIGNER_UNAUTHORIZED":
		code = domain.ErrPermissionDenied
	case "SETTLEMENT_FAILED", "RECEIPT_INVALID":
		code = domain.ErrSettlementFailed
	case "NETWORK_UNAVAILABLE", "PROVIDER_UNAVAILABLE", "ESCROW_UNAVAILABLE", "EXECUTION_UNCERTAIN":
		code = domain.ErrNetworkUnavailable
		retryable = true
	case "INVALID_ARGUMENT", "INPUT_COMMITMENT_MISMATCH", "DEADLINE_EXCEEDED":
		code = domain.ErrValidationFailed
	case "NOT_FOUND":
		code = domain.ErrNotFound
	default:
		switch connectErr.Code() {
		case connect.CodeUnauthenticated:
			code = domain.ErrAuthenticationRequired
		case connect.CodePermissionDenied:
			code = domain.ErrPermissionDenied
		case connect.CodeInvalidArgument:
			code = domain.ErrValidationFailed
		case connect.CodeNotFound:
			code = domain.ErrNotFound
		case connect.CodeAlreadyExists, connect.CodeAborted:
			code = domain.ErrIdempotencyConflict
		case connect.CodeResourceExhausted:
			code = domain.ErrRateLimited
			retryable = true
		case connect.CodeUnavailable, connect.CodeDeadlineExceeded:
			code = domain.ErrNetworkUnavailable
			retryable = true
		case connect.CodeFailedPrecondition:
			code = domain.ErrQuoteMismatch
		default:
			code = domain.ErrProviderFailed
		}
	}
	return domain.NewError(code, connectErr.Message(), retryable)
}

func trustMode(mode domain.TrustMode) atostosv1.TrustMode {
	switch mode {
	case domain.TrustModeManaged:
		return atostosv1.TrustMode_TRUST_MODE_MANAGED
	case domain.TrustModeVerified:
		return atostosv1.TrustMode_TRUST_MODE_VERIFIED
	case domain.TrustModeNative:
		return atostosv1.TrustMode_TRUST_MODE_NATIVE
	default:
		return atostosv1.TrustMode_TRUST_MODE_UNSPECIFIED
	}
}

func domainTrustMode(mode atostosv1.TrustMode) domain.TrustMode {
	switch mode {
	case atostosv1.TrustMode_TRUST_MODE_MANAGED:
		return domain.TrustModeManaged
	case atostosv1.TrustMode_TRUST_MODE_VERIFIED:
		return domain.TrustModeVerified
	case atostosv1.TrustMode_TRUST_MODE_NATIVE:
		return domain.TrustModeNative
	default:
		return ""
	}
}

func proofProfile(profile domain.ProofProfile) atostosv1.ProofProfile {
	switch profile {
	case domain.ProofProfileNone:
		return atostosv1.ProofProfile_PROOF_PROFILE_NONE
	case domain.ProofProfileTOSVerifiedV1:
		return atostosv1.ProofProfile_PROOF_PROFILE_TOS_VERIFIED_V1
	case domain.ProofProfileTOSNativeV1:
		return atostosv1.ProofProfile_PROOF_PROFILE_TOS_NATIVE_V1
	default:
		return atostosv1.ProofProfile_PROOF_PROFILE_UNSPECIFIED
	}
}

func domainProofProfile(profile atostosv1.ProofProfile) domain.ProofProfile {
	switch profile {
	case atostosv1.ProofProfile_PROOF_PROFILE_TOS_VERIFIED_V1:
		return domain.ProofProfileTOSVerifiedV1
	case atostosv1.ProofProfile_PROOF_PROFILE_TOS_NATIVE_V1:
		return domain.ProofProfileTOSNativeV1
	default:
		return domain.ProofProfileNone
	}
}

// thirdPartyTransport maps domain.EndpointAdapterType to the tos-protocol
// wire enum. ok is false for a native/human binding (or an empty one),
// which is not a third-party transport at all -- see
// thirdPartyBindingProto's doc comment.
func thirdPartyTransport(t domain.EndpointAdapterType) (atostosv1.EndpointAdapterType, bool) {
	switch t {
	case domain.AdapterHTTP:
		return atostosv1.EndpointAdapterType_ENDPOINT_ADAPTER_TYPE_HTTP, true
	case domain.AdapterMCP:
		return atostosv1.EndpointAdapterType_ENDPOINT_ADAPTER_TYPE_MCP, true
	case domain.AdapterA2A:
		return atostosv1.EndpointAdapterType_ENDPOINT_ADAPTER_TYPE_A2A, true
	default:
		return atostosv1.EndpointAdapterType_ENDPOINT_ADAPTER_TYPE_UNSPECIFIED, false
	}
}

// thirdPartyBindingProto maps a domain.CapabilityBinding onto
// atos-spec's ThirdPartyBinding wire message (see atos-spec
// docs/THIRD_PARTY_EXECUTION_PLANE.md), or returns (nil, nil) for a
// tos-native/human binding -- an ordinary Job, not a third-party one.
// binding_commitment is a content digest over the binding itself (not a
// pre-existing string commitment the way InputCommitment is), so it is
// computed here rather than parsed.
func thirdPartyBindingProto(binding *domain.CapabilityBinding) (*atostosv1.ThirdPartyBinding, error) {
	if binding == nil {
		return nil, nil
	}
	transport, ok := thirdPartyTransport(binding.Transport)
	if !ok {
		return nil, nil
	}
	encoded, err := json.Marshal(binding)
	if err != nil {
		return nil, domain.NewError(domain.ErrValidationFailed, "capability binding is not serializable", false)
	}
	sum := sha256.Sum256(encoded)
	return &atostosv1.ThirdPartyBinding{
		Transport: transport, EndpointRef: binding.EndpointRef,
		BindingCommitment: &atostosv1.Digest{Algorithm: "sha256", Value: sum[:]},
	}, nil
}

func digest(value string) (*atostosv1.Digest, error) {
	algorithm, encoded, found := strings.Cut(strings.TrimSpace(value), ":")
	if !found || algorithm != "sha256" {
		return nil, domain.NewError(domain.ErrValidationFailed, "expected sha256 commitment", false)
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != 32 {
		return nil, domain.NewError(domain.ErrValidationFailed, "invalid sha256 commitment", false)
	}
	return &atostosv1.Digest{Algorithm: algorithm, Value: decoded}, nil
}

func digestString(value *atostosv1.Digest) string {
	if value == nil || value.Algorithm == "" || len(value.Value) == 0 {
		return ""
	}
	return value.Algorithm + ":" + hex.EncodeToString(value.Value)
}

func reference(value *atostosv1.NetworkReference) string {
	if value == nil {
		return ""
	}
	return value.Reference
}

func money(value *atostosv1.Money) domain.Money {
	if value == nil {
		return domain.Money{}
	}
	return domain.Money{Amount: value.Amount, Currency: value.Currency}
}

func networkAmount(value domain.Money) (*atostosv1.NetworkAmount, error) {
	decimals := 2
	if value.Currency == "TOS" {
		decimals = 9
	}
	minor, err := amountToAtomic(value.Amount, decimals)
	if err != nil {
		return nil, domain.NewError(domain.ErrValidationFailed, "invalid monetary amount: "+err.Error(), false)
	}
	return &atostosv1.NetworkAmount{Asset: value.Currency, AtomicAmount: minor}, nil
}

func amountToAtomic(value string, decimals int) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.HasPrefix(value, "-") {
		return "", errors.New("amount is empty or negative")
	}
	parts := strings.SplitN(value, ".", 2)
	whole := parts[0]
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if whole == "" {
		whole = "0"
	}
	if len(fraction) > decimals {
		return "", errors.New("amount has too many decimal places")
	}
	for _, r := range whole + fraction {
		if r < '0' || r > '9' {
			return "", errors.New("amount is not decimal")
		}
	}
	fraction += strings.Repeat("0", decimals-len(fraction))
	atomic := strings.TrimLeft(whole+fraction, "0")
	if atomic == "" {
		atomic = "0"
	}
	return atomic, nil
}

func atomicToAmount(value string, decimals int) string {
	value = strings.TrimLeft(strings.TrimSpace(value), "0")
	if value == "" {
		value = "0"
	}
	if len(value) <= decimals {
		value = strings.Repeat("0", decimals+1-len(value)) + value
	}
	return value[:len(value)-decimals] + "." + value[len(value)-decimals:]
}

func proofCheckpoint(value atostosv1.VerificationStatus) domain.ProofCheckpoint {
	switch value {
	case atostosv1.VerificationStatus_VERIFICATION_STATUS_NOT_REQUIRED:
		return domain.ProofNotRequired
	case atostosv1.VerificationStatus_VERIFICATION_STATUS_PENDING:
		return domain.ProofPending
	case atostosv1.VerificationStatus_VERIFICATION_STATUS_VERIFIED:
		return domain.ProofVerified
	case atostosv1.VerificationStatus_VERIFICATION_STATUS_FAILED:
		return domain.ProofFailed
	default:
		return domain.ProofPending
	}
}

func domainProofStatus(value *atostosv1.ProofStatus) domain.ProofStatus {
	if value == nil {
		return domain.ProofStatus{}
	}
	return domain.ProofStatus{
		Quote: proofCheckpoint(value.Quote), Escrow: proofCheckpoint(value.Escrow),
		Receipt: proofCheckpoint(value.Receipt), Settlement: proofCheckpoint(value.Settlement),
	}
}

func domainJobState(value atostosv1.JobState) domain.JobState {
	switch value {
	case atostosv1.JobState_JOB_STATE_SUBMITTED:
		return domain.JobSubmitted
	case atostosv1.JobState_JOB_STATE_WORKING:
		return domain.JobWorking
	case atostosv1.JobState_JOB_STATE_INPUT_REQUIRED:
		return domain.JobInputRequired
	case atostosv1.JobState_JOB_STATE_COMPLETED:
		return domain.JobCompleted
	case atostosv1.JobState_JOB_STATE_FAILED, atostosv1.JobState_JOB_STATE_UNCERTAIN:
		return domain.JobFailed
	case atostosv1.JobState_JOB_STATE_CANCELED:
		return domain.JobCanceled
	case atostosv1.JobState_JOB_STATE_REJECTED:
		return domain.JobRejected
	default:
		return domain.JobFailed
	}
}

func executionResult(value atostosv1.ExecutionResult) domain.ExecutionResult {
	switch value {
	case atostosv1.ExecutionResult_EXECUTION_RESULT_SUCCESS:
		return domain.ExecutionSuccess
	case atostosv1.ExecutionResult_EXECUTION_RESULT_FAILED:
		return domain.ExecutionFailed
	case atostosv1.ExecutionResult_EXECUTION_RESULT_CANCELED:
		return domain.ExecutionCanceled
	case atostosv1.ExecutionResult_EXECUTION_RESULT_TIMED_OUT:
		return domain.ExecutionTimedOut
	case atostosv1.ExecutionResult_EXECUTION_RESULT_REJECTED:
		return domain.ExecutionRejected
	default:
		return ""
	}
}

func protoExecutionResult(value domain.ExecutionResult) atostosv1.ExecutionResult {
	switch value {
	case domain.ExecutionSuccess:
		return atostosv1.ExecutionResult_EXECUTION_RESULT_SUCCESS
	case domain.ExecutionFailed:
		return atostosv1.ExecutionResult_EXECUTION_RESULT_FAILED
	case domain.ExecutionCanceled:
		return atostosv1.ExecutionResult_EXECUTION_RESULT_CANCELED
	case domain.ExecutionTimedOut:
		return atostosv1.ExecutionResult_EXECUTION_RESULT_TIMED_OUT
	case domain.ExecutionRejected:
		return atostosv1.ExecutionResult_EXECUTION_RESULT_REJECTED
	default:
		return atostosv1.ExecutionResult_EXECUTION_RESULT_UNSPECIFIED
	}
}

func requireFuture(value time.Time, field string) error {
	if value.IsZero() || !value.After(time.Now()) {
		return domain.NewError(domain.ErrValidationFailed, fmt.Sprintf("%s must be in the future", field), false)
	}
	return nil
}
