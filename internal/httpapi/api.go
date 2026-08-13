// Package httpapi implements the REST surface from atos-spec/docs/API.md.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

const maxRequestJSONBytes = 1 << 20

// ReadinessChecker represents a required downstream production boundary.
// A nil checker is intentionally not considered ready: liveness must never be
// mistaken for authorization to receive new economic work.
type ReadinessChecker interface {
	CheckReady(context.Context) error
}

// decodeRequestJSON enforces one bounded JSON value with no unknown struct
// fields or trailing data. Keeping this rule shared prevents REST adapters from
// accepting a wider contract than MCP/A2A or the published OpenAPI model.
func decodeRequestJSON(r *http.Request, dst any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestJSONBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxRequestJSONBytes {
		return fmt.Errorf("JSON body exceeds %d bytes", maxRequestJSONBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON body contains trailing data")
		}
		return err
	}
	return nil
}

type Server struct {
	Readiness    ReadinessChecker
	readinessMu  sync.Mutex
	readinessAt  time.Time
	readinessErr error
	Auth         *auth.Service
	Capabilities *service.CapabilityService
	// Health is optional: nil omits the readiness projection from
	// GET /capabilities/{id} entirely rather than erroring (see
	// service.CapabilityReadiness's doc comment).
	Health           *service.HealthService
	ExecutionSigners *service.ExecutionSignerService
	Certifications   *service.CertificationService
	// Passkeys backs POST /v1/auth/passkey/... -- production wiring MUST
	// always set it (service.NewPasskeyService is nil-webAuthn-safe: every
	// method fails closed with ErrPasskeyNotConfigured when
	// ATOS_WEBAUTHN_RP_ID is unset, rather than the Server itself needing a
	// nil check per handler).
	Passkeys *service.PasskeyService
	// ActivationAuthority backs POST /capabilities/{id}/activation/evaluate
	// (atos-spec docs/API.md §2.2) -- unlike Health, this is not optional:
	// production wiring MUST always set it (service.FailClosedActivationAuthority
	// today), since domain.ActivationAuthority has no nil-safe default.
	ActivationAuthority domain.ActivationAuthority
	// IdentityBindings backs Phase 4A's identity-binding operator surface
	// (atos-spec docs/API.md §9A) -- nil is safe: handlers would panic on
	// first use, but production wiring (cmd/api/main.go) always sets it,
	// same "no nil-safe default, wiring is the guard" contract as
	// ActivationAuthority above.
	IdentityBindings *service.IdentityBindingService
	OpenTasks        *service.OpenTaskService
	Quotes           *service.QuoteService
	Jobs             *service.JobService
	Streams          *service.StreamService
	Accounts         *service.AccountService
	Receipts         *service.ReceiptService
	ProofPackages    *service.PortableProofService
	Earnings         *service.EarningsService
	Disputes         *service.DisputeService
	Artifacts        *service.ArtifactService
	Logger           *slog.Logger
	PublicBaseURL    string
	ApprovalToken    string
	// AdminApprovalToken additionally gates approval of a Device
	// Authorization grant requesting an admin scope (see
	// auth.RequiresAdminApproval) -- required on top of ApprovalToken, not
	// instead of it. Empty means admin-scoped grants can never be
	// approved, matching secureEqual's own empty-vs-nonempty rejection.
	AdminApprovalToken string
	// TrustedProxyCIDRs mirrors config.Config.TrustedProxyCIDRs, parsed
	// once at startup -- see clientIP's doc comment (internal/httpapi/
	// passkey.go) for why an unconfigured (nil) value means forwarded-IP
	// headers are never trusted.
	TrustedProxyCIDRs []netip.Prefix
}

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", s.handleLivez)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /.well-known/agent-card.json", s.handleAgentCard)
	mux.HandleFunc("GET /.well-known/agent.json", s.handleAgentCard)
	mux.HandleFunc("GET /skills/atos/SKILL.md", s.handleSkill)
	mux.HandleFunc("GET /activate", s.handleActivationPage)
	mux.HandleFunc("POST /activate", s.handleActivationDecisionPage)
	mux.HandleFunc("GET /confirm", s.handleConfirmationPage)

	mux.HandleFunc("POST /v1/auth/device", s.handleAuthDeviceStart)
	mux.HandleFunc("POST /v1/auth/device/token", s.handleAuthDeviceToken)
	mux.HandleFunc("POST /v1/auth/device/decision", s.handleAuthDeviceDecision)
	mux.HandleFunc("POST /v1/auth/token/refresh", s.handleAuthTokenRefresh)
	mux.HandleFunc("POST /v1/auth/revoke", s.withScopes(s.handleAuthRevoke))
	mux.HandleFunc("GET /v1/auth/devices", s.withScopes(s.handleListDevices))
	mux.HandleFunc("DELETE /v1/auth/devices/{id}", s.withScopes(s.handleRevokeDevice))

	mux.HandleFunc("POST /v1/auth/passkey/register/begin", s.handleBeginPasskeyRegistration)
	mux.HandleFunc("POST /v1/auth/passkey/register/finish/{ceremony_id}", s.handleFinishPasskeyRegistration)
	mux.HandleFunc("POST /v1/auth/passkey/login/begin", s.handleBeginPasskeyLogin)
	mux.HandleFunc("POST /v1/auth/passkey/login/finish/{ceremony_id}", s.handleFinishPasskeyLogin)

	mux.HandleFunc("GET /v1/capabilities", s.withScopes(s.handleSearchCapabilities, auth.ScopeCapabilitiesRead))
	mux.HandleFunc("GET /v1/capabilities/mine", s.withScopes(s.handleListMyCapabilities, auth.ScopeCapabilitiesWrite))
	mux.HandleFunc("GET /v1/capabilities/{id}", s.withScopes(s.handleGetCapability, auth.ScopeCapabilitiesRead))
	mux.HandleFunc("POST /v1/capabilities", s.withScopes(s.handleRegisterCapability, auth.ScopeCapabilitiesWrite))
	mux.HandleFunc("PATCH /v1/capabilities/{id}", s.withScopes(s.handleUpdateCapability, auth.ScopeCapabilitiesWrite))
	mux.HandleFunc("POST /v1/capabilities/{id}/pause", s.withScopes(s.handlePauseCapability, auth.ScopeCapabilitiesWrite))
	mux.HandleFunc("POST /v1/capabilities/{id}/resume", s.withScopes(s.handleResumeCapability, auth.ScopeCapabilitiesWrite))

	mux.HandleFunc("POST /v1/capabilities/{id}/execution-signer/authorize", s.withScopes(s.handleAuthorizeExecutionSigner, auth.ScopeExecutionSignersWrite))
	mux.HandleFunc("POST /v1/capabilities/{id}/execution-signer/rotate", s.withScopes(s.handleRotateExecutionSigner, auth.ScopeExecutionSignersWrite))
	mux.HandleFunc("POST /v1/capabilities/{id}/execution-signer/revoke", s.withScopes(s.handleRevokeExecutionSigner, auth.ScopeExecutionSignersWrite))
	mux.HandleFunc("GET /v1/capabilities/{id}/execution-signer", s.withScopes(s.handleGetExecutionSignerStatus, auth.ScopeExecutionSignersRead))

	mux.HandleFunc("POST /v1/capabilities/{id}/activation/evaluate", s.withScopes(s.handleEvaluateActivation, auth.ScopeActivationEvaluate))

	mux.HandleFunc("POST /v1/identity-bindings/{principal_id}/bind", s.withScopes(s.handleBindIdentity, auth.ScopeIdentityBindingsWrite))
	mux.HandleFunc("POST /v1/identity-bindings/{principal_id}/revoke", s.withScopes(s.handleRevokeIdentity, auth.ScopeIdentityBindingsWrite))
	mux.HandleFunc("GET /v1/identity-bindings/{principal_id}", s.withScopes(s.handleIdentityBindingStatus, auth.ScopeIdentityBindingsRead))

	mux.HandleFunc("POST /v1/capabilities/{id}/certification", s.withScopes(s.handleOpenCertification, auth.ScopeCertificationsWrite))
	mux.HandleFunc("GET /v1/capabilities/{id}/certification", s.withScopes(s.handleGetCertificationStatus, auth.ScopeCertificationsRead))

	mux.HandleFunc("POST /v1/quotes", s.withScopes(s.handleCreateQuote, auth.ScopeQuotesRead))
	mux.HandleFunc("GET /v1/quotes/{id}", s.withScopes(s.handleGetQuote, auth.ScopeQuotesRead))

	// Phase 3C: Open Task Marketplace (atos-spec docs/IMPLEMENTATION_ROADMAP.md
	// §7.3). GET /v1/open-tasks and GET .../proposals are readable with
	// ScopeOpenTasksRead alone -- sensitive-field redaction happens inside
	// OpenTaskService, not by gating the route itself, since "public
	// browse" and "owner's own detail" are the SAME endpoint
	// distinguished only by response shape (see handleListOpenTasks).
	// Submitting/withdrawing a proposal is the provider-role action and
	// requires the separate, explicit-grant-only ScopeOpenTaskProposalsWrite.
	mux.HandleFunc("POST /v1/open-tasks", s.withScopes(s.handlePublishOpenTask, auth.ScopeOpenTasksWrite))
	mux.HandleFunc("GET /v1/open-tasks", s.withScopes(s.handleListOpenTasks, auth.ScopeOpenTasksRead))
	mux.HandleFunc("GET /v1/open-tasks/{task_id}", s.withScopes(s.handleGetOpenTask, auth.ScopeOpenTasksRead))
	mux.HandleFunc("POST /v1/open-tasks/{task_id}/cancel", s.withScopes(s.handleCancelOpenTask, auth.ScopeOpenTasksWrite))
	mux.HandleFunc("POST /v1/open-tasks/{task_id}/proposals", s.withScopes(s.handleProposeOpenTask, auth.ScopeOpenTaskProposalsWrite))
	mux.HandleFunc("GET /v1/open-tasks/{task_id}/proposals", s.withScopes(s.handleListOpenTaskProposals, auth.ScopeOpenTasksRead))
	mux.HandleFunc("POST /v1/open-tasks/{task_id}/proposals/{proposal_id}/withdraw", s.withScopes(s.handleWithdrawOpenTaskProposal, auth.ScopeOpenTaskProposalsWrite))
	mux.HandleFunc("POST /v1/open-tasks/{task_id}/proposals/{proposal_id}/accept", s.withScopes(s.handleAcceptOpenTaskProposal, auth.ScopeOpenTasksWrite))

	mux.HandleFunc("POST /v1/invocations", s.withScopes(s.handleInvoke, auth.ScopeInvocationsCreate))
	mux.HandleFunc("GET /v1/invocations/{id}", s.withScopes(s.handleGetJob, auth.ScopeJobsRead))
	mux.HandleFunc("POST /v1/jobs", s.withScopes(s.handleCreateJob, auth.ScopeJobsCreate))
	mux.HandleFunc("GET /v1/jobs/{id}", s.withScopes(s.handleGetJob, auth.ScopeJobsRead))
	mux.HandleFunc("GET /v1/jobs/{id}/stream", s.withScopes(s.handleStreamJob, auth.ScopeJobsRead))
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.withScopes(s.handleCancelJob, auth.ScopeJobsCancel))
	mux.HandleFunc("GET /v1/confirmations/{code}", s.withScopes(s.handleGetConfirmation, auth.ScopeAccountRead))
	mux.HandleFunc("POST /v1/confirmations/{code}/decision", s.withScopes(s.handleConfirmationDecision, auth.ScopeAccountRead))

	mux.HandleFunc("GET /v1/account", s.withScopes(s.handleGetAccount, auth.ScopeAccountRead))
	mux.HandleFunc("GET /v1/account/usage", s.withScopes(s.handleGetAccountUsage, auth.ScopeAccountRead))
	mux.HandleFunc("GET /v1/account/receipts", s.withScopes(s.handleListReceipts, auth.ScopeAccountRead))
	mux.HandleFunc("GET /v1/receipts/{id}", s.withScopes(s.handleGetReceipt, auth.ScopeAccountRead))
	mux.HandleFunc("GET /v1/receipts/{id}/settlement-proof", s.withScopes(s.handleSettlementProof, auth.ScopeProofsRead))
	mux.HandleFunc("POST /v1/receipts/{id}/proof-package", s.withScopes(s.handleCreateProofPackage, auth.ScopeProofsRead))
	mux.HandleFunc("GET /v1/receipts/{id}/proof-package", s.withScopes(s.handleGetProofPackage, auth.ScopeProofsRead))
	mux.HandleFunc("GET /v1/jobs/{id}/billing", s.withScopes(s.handleGetJobBilling, auth.ScopeJobsRead))

	mux.HandleFunc("GET /v1/provider/earnings", s.withScopes(s.handleListEarnings, auth.ScopeEarningsRead))
	mux.HandleFunc("GET /v1/provider/earnings/{id}", s.withScopes(s.handleGetEarning, auth.ScopeEarningsRead))

	mux.HandleFunc("POST /v1/jobs/{id}/disputes", s.withScopes(s.handleOpenDispute, auth.ScopeDisputesOpen))
	mux.HandleFunc("GET /v1/disputes/{id}", s.withScopes(s.handleGetDispute, auth.ScopeDisputesRead))
	mux.HandleFunc("GET /v1/disputes", s.withScopes(s.handleListDisputes, auth.ScopeDisputesRead))
	mux.HandleFunc("POST /v1/disputes/{id}/review", s.withScopes(s.handleReviewDispute, auth.ScopeDisputesReview))
	mux.HandleFunc("POST /v1/disputes/{id}/resolve", s.withScopes(s.handleResolveDispute, auth.ScopeDisputesReview))

	mux.HandleFunc("GET /v1/taxonomy", s.withScopes(s.handleTaxonomy, auth.ScopeCapabilitiesRead))
	mux.HandleFunc("GET /v1/network/status", s.withScopes(s.handleNetworkStatus, auth.ScopeCapabilitiesRead))
	mux.HandleFunc("GET /v1/providers/{id}/agent-card", s.withScopes(s.handleProviderAgentCard, auth.ScopeCapabilitiesRead))

	// Artifact authorization is operation/resource-specific and therefore
	// enforced inside the handler/service after basic authentication.
	mux.HandleFunc("POST /v1/uploads", s.withScopes(s.handleCreateUpload))
	mux.HandleFunc("POST /v1/uploads/{id}/complete", s.withScopes(s.handleCompleteUpload))
	mux.HandleFunc("GET /v1/artifacts/{id}", s.withScopes(s.handleGetArtifact))
	mux.HandleFunc("GET /v1/artifacts/{id}/download-url", s.withScopes(s.handleGetDownloadURL))
	return mux
}

type authContext struct {
	Principal auth.Principal
	Token     string
}

type authContextKeyType struct{}

var authContextKey = authContextKeyType{}

func (s *Server) withScopes(next http.HandlerFunc, required ...auth.Scope) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Auth == nil {
			writeError(w, http.StatusServiceUnavailable, domain.ErrAuthenticationRequired, "authorization service unavailable", true)
			return
		}
		authz := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(authz, "Bearer ")
		token = strings.TrimSpace(token)
		if !ok || token == "" {
			writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, "missing or malformed Authorization header", false)
			return
		}
		principal, err := s.Auth.Authenticate(token)
		if err != nil {
			writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, err.Error(), false)
			return
		}
		if !principal.HasAll(required...) {
			writeError(w, http.StatusForbidden, domain.ErrPermissionDenied, "token does not grant the required scope", false)
			return
		}
		ctx := context.WithValue(r.Context(), authContextKey, authContext{Principal: principal, Token: token})
		next(w, r.WithContext(ctx))
	}
}

func authFrom(r *http.Request) authContext {
	value, _ := r.Context().Value(authContextKey).(authContext)
	return value
}

func principalFrom(r *http.Request) string      { return authFrom(r).Principal.ID }
func scopesFrom(r *http.Request) auth.Principal { return authFrom(r).Principal }

func (s *Server) handleLivez(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.Readiness == nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer cancel()
	if err := s.checkReady(ctx); err != nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) checkReady(ctx context.Context) error {
	s.readinessMu.Lock()
	defer s.readinessMu.Unlock()
	now := time.Now()
	if !s.readinessAt.IsZero() && now.Sub(s.readinessAt) < 2*time.Second {
		return s.readinessErr
	}
	err := s.Readiness.CheckReady(ctx)
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		s.readinessAt, s.readinessErr = now, err
	}
	return err
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errorEnvelope struct {
	Error struct {
		Code      domain.ErrorCode `json:"code"`
		Message   string           `json:"message"`
		Retryable bool             `json:"retryable"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code domain.ErrorCode, message string, retryable bool) {
	var env errorEnvelope
	env.Error.Code = code
	env.Error.Message = message
	env.Error.Retryable = retryable
	writeJSON(w, status, env)
}

func writeDomainErr(w http.ResponseWriter, err error) {
	de, ok := err.(*domain.Error)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), true)
		return
	}
	writeError(w, statusForCode(de.Code), de.Code, de.Message, de.Retryable)
}

func statusForCode(code domain.ErrorCode) int {
	switch code {
	case domain.ErrAuthenticationRequired:
		return http.StatusUnauthorized
	case domain.ErrPermissionDenied, domain.ErrArtifactAccessDenied:
		return http.StatusForbidden
	case domain.ErrRateLimited:
		return http.StatusTooManyRequests
	case domain.ErrValidationFailed, domain.ErrUploadMismatch, domain.ErrStreamOffsetInvalid,
		domain.ErrStreamDigestInvalid, domain.ErrStreamChunkTooLarge, domain.ErrStreamCursorMismatch,
		domain.ErrDisputeNotEligible:
		return http.StatusBadRequest
	case domain.ErrNotFound, domain.ErrCapabilityUnavailable, domain.ErrArtifactNotFound:
		return http.StatusNotFound
	case domain.ErrUploadExpired:
		return http.StatusGone
	case domain.ErrQuoteExpired, domain.ErrQuoteMismatch, domain.ErrQuoteModeMismatch,
		domain.ErrTrustModeUnavailable, domain.ErrProofRequirementsUnsatisfied,
		domain.ErrProofProfileUnavailable, domain.ErrRequoteRequired,
		domain.ErrIdempotencyConflict, domain.ErrJobNotCancelable,
		domain.ErrSpendConfirmationRequired, domain.ErrSpendConfirmationDenied,
		domain.ErrSpendConfirmationExpired, domain.ErrStreamSequenceConflict,
		domain.ErrStreamTerminal, domain.ErrDisputeWindowExpired, domain.ErrDisputeInvalidTransition,
		domain.ErrOpenTaskNotOpen, domain.ErrOpenTaskAcceptanceInProgress,
		domain.ErrOpenTaskProposalStale, domain.ErrOpenTaskProposalWithdrawn:
		return http.StatusConflict
	case domain.ErrSpendLimitExceeded, domain.ErrInsufficientBalance:
		return http.StatusPaymentRequired
	case domain.ErrProviderFailed, domain.ErrSettlementFailed, domain.ErrNetworkUnavailable:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}
