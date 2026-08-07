// Package httpapi implements the REST surface from ~/atos-spec/docs/API.md.
// It is a thin adapter: every handler validates the transport-level shape
// of a request and then delegates to internal/service, which owns all
// business rules.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type Server struct {
	Capabilities *service.CapabilityService
	Quotes       *service.QuoteService
	Jobs         *service.JobService
	Accounts     *service.AccountService
	Logger       *slog.Logger
}

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /livez", s.handleLivez)

	mux.HandleFunc("GET /.well-known/agent-card.json", s.handleAgentCard)
	mux.HandleFunc("GET /.well-known/agent.json", s.handleAgentCard)

	mux.HandleFunc("POST /v1/auth/device", s.handleAuthDeviceStart)
	mux.HandleFunc("POST /v1/auth/device/token", s.handleAuthDeviceToken)

	mux.HandleFunc("GET /v1/capabilities", s.withAuth(s.handleSearchCapabilities))
	mux.HandleFunc("GET /v1/capabilities/{id}", s.withAuth(s.handleGetCapability))
	mux.HandleFunc("POST /v1/capabilities", s.withAuth(s.handleRegisterCapability))
	mux.HandleFunc("PATCH /v1/capabilities/{id}", s.withAuth(s.handleUpdateCapability))

	mux.HandleFunc("POST /v1/quotes", s.withAuth(s.handleCreateQuote))
	mux.HandleFunc("GET /v1/quotes/{id}", s.withAuth(s.handleGetQuote))

	mux.HandleFunc("POST /v1/invocations", s.withAuth(s.handleInvoke))
	mux.HandleFunc("GET /v1/invocations/{id}", s.withAuth(s.handleGetJob))

	mux.HandleFunc("POST /v1/jobs", s.withAuth(s.handleCreateJob))
	mux.HandleFunc("GET /v1/jobs/{id}", s.withAuth(s.handleGetJob))
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.withAuth(s.handleCancelJob))

	mux.HandleFunc("GET /v1/account", s.withAuth(s.handleGetAccount))

	return mux
}

// principalContextKey stores the authenticated principal_id resolved by
// withAuth so downstream handlers never re-parse the Authorization header.
type principalContextKeyType struct{}

var principalContextKey = principalContextKeyType{}

// withAuth implements the Bearer-token half of docs/AUTH.md. Phase 0/1
// simplification: the token *is* the principal_id (no signature, no
// expiry) — swap this for real Device Auth token verification before this
// ships anywhere but a local sandbox.
func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(authz, "Bearer ")
		token = strings.TrimSpace(token)
		if !ok || token == "" {
			writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, "missing or malformed Authorization header", false)
			return
		}
		ctx := context.WithValue(r.Context(), principalContextKey, token)
		next(w, r.WithContext(ctx))
	}
}

func principalFrom(r *http.Request) string {
	v, _ := r.Context().Value(principalContextKey).(string)
	return v
}

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

// writeDomainError maps a domain.Error to the HTTP status docs/API.md
// implies for its code, falling back to 500 for anything unrecognized —
// which should only happen for genuine bugs, not expected business
// outcomes (those all have a domain.ErrorCode).
func writeDomainErr(w http.ResponseWriter, err error) {
	de, ok := err.(*domain.Error)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), true)
		return
	}
	status := statusForCode(de.Code)
	writeError(w, status, de.Code, de.Message, de.Retryable)
}

func statusForCode(code domain.ErrorCode) int {
	switch code {
	case domain.ErrAuthenticationRequired:
		return http.StatusUnauthorized
	case domain.ErrPermissionDenied:
		return http.StatusForbidden
	case domain.ErrRateLimited:
		return http.StatusTooManyRequests
	case domain.ErrValidationFailed:
		return http.StatusBadRequest
	case domain.ErrNotFound, domain.ErrCapabilityUnavailable:
		return http.StatusNotFound
	case domain.ErrQuoteExpired, domain.ErrQuoteMismatch:
		return http.StatusConflict
	case domain.ErrSpendLimitExceeded, domain.ErrInsufficientBalance:
		return http.StatusPaymentRequired
	case domain.ErrIdempotencyConflict:
		return http.StatusConflict
	case domain.ErrJobNotCancelable:
		return http.StatusConflict
	case domain.ErrProviderFailed, domain.ErrSettlementFailed:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}
