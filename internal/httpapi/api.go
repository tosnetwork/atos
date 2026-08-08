// Package httpapi implements the REST surface from atos-spec/docs/API.md.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type Server struct {
	Auth         *auth.Service
	Capabilities *service.CapabilityService
	Quotes       *service.QuoteService
	Jobs         *service.JobService
	Accounts     *service.AccountService
	Receipts     *service.ReceiptService
	Artifacts    *service.ArtifactService
	Logger       *slog.Logger
}

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", s.handleLivez)
	mux.HandleFunc("GET /.well-known/agent-card.json", s.handleAgentCard)
	mux.HandleFunc("GET /.well-known/agent.json", s.handleAgentCard)

	mux.HandleFunc("POST /v1/auth/device", s.handleAuthDeviceStart)
	mux.HandleFunc("POST /v1/auth/device/token", s.handleAuthDeviceToken)
	mux.HandleFunc("POST /v1/auth/token/refresh", s.handleAuthTokenRefresh)
	mux.HandleFunc("POST /v1/auth/revoke", s.withScopes(s.handleAuthRevoke))

	mux.HandleFunc("GET /v1/capabilities", s.withScopes(s.handleSearchCapabilities, auth.ScopeCapabilitiesRead))
	mux.HandleFunc("GET /v1/capabilities/{id}", s.withScopes(s.handleGetCapability, auth.ScopeCapabilitiesRead))
	mux.HandleFunc("POST /v1/capabilities", s.withScopes(s.handleRegisterCapability, auth.ScopeCapabilitiesWrite))
	mux.HandleFunc("PATCH /v1/capabilities/{id}", s.withScopes(s.handleUpdateCapability, auth.ScopeCapabilitiesWrite))
	mux.HandleFunc("POST /v1/capabilities/{id}/pause", s.withScopes(s.handlePauseCapability, auth.ScopeCapabilitiesWrite))
	mux.HandleFunc("POST /v1/capabilities/{id}/resume", s.withScopes(s.handleResumeCapability, auth.ScopeCapabilitiesWrite))

	mux.HandleFunc("POST /v1/quotes", s.withScopes(s.handleCreateQuote, auth.ScopeQuotesRead))
	mux.HandleFunc("GET /v1/quotes/{id}", s.withScopes(s.handleGetQuote, auth.ScopeQuotesRead))

	mux.HandleFunc("POST /v1/invocations", s.withScopes(s.handleInvoke, auth.ScopeInvocationsCreate))
	mux.HandleFunc("GET /v1/invocations/{id}", s.withScopes(s.handleGetJob, auth.ScopeJobsRead))
	mux.HandleFunc("POST /v1/jobs", s.withScopes(s.handleCreateJob, auth.ScopeJobsCreate))
	mux.HandleFunc("GET /v1/jobs/{id}", s.withScopes(s.handleGetJob, auth.ScopeJobsRead))
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.withScopes(s.handleCancelJob, auth.ScopeJobsCancel))

	mux.HandleFunc("GET /v1/account", s.withScopes(s.handleGetAccount, auth.ScopeAccountRead))
	mux.HandleFunc("GET /v1/account/usage", s.withScopes(s.handleGetAccountUsage, auth.ScopeAccountRead))
	mux.HandleFunc("GET /v1/account/receipts", s.withScopes(s.handleListReceipts, auth.ScopeAccountRead))
	mux.HandleFunc("GET /v1/receipts/{id}", s.withScopes(s.handleGetReceipt, auth.ScopeAccountRead))
	mux.HandleFunc("GET /v1/receipts/{id}/settlement-proof", s.withScopes(s.handleSettlementProof, auth.ScopeProofsRead))

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
	case domain.ErrValidationFailed, domain.ErrUploadMismatch:
		return http.StatusBadRequest
	case domain.ErrNotFound, domain.ErrCapabilityUnavailable, domain.ErrArtifactNotFound:
		return http.StatusNotFound
	case domain.ErrUploadExpired:
		return http.StatusGone
	case domain.ErrQuoteExpired, domain.ErrQuoteMismatch, domain.ErrQuoteModeMismatch,
		domain.ErrTrustModeUnavailable, domain.ErrProofRequirementsUnsatisfied,
		domain.ErrProofProfileUnavailable, domain.ErrRequoteRequired,
		domain.ErrIdempotencyConflict, domain.ErrJobNotCancelable:
		return http.StatusConflict
	case domain.ErrSpendLimitExceeded, domain.ErrInsufficientBalance:
		return http.StatusPaymentRequired
	case domain.ErrProviderFailed, domain.ErrSettlementFailed, domain.ErrNetworkUnavailable:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}
