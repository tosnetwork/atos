package httpapi

import (
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/go-webauthn/webauthn/protocol"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store"
)

// Human account authentication (passkey/WebAuthn) -- atos-spec
// docs/AUTH.md's "Human Account Authentication (Passkey/WebAuthn)" section.
// finish handlers deliberately do not use decodeRequestJSON: the request
// body is the raw WebAuthn attestation/assertion response, which the
// go-webauthn library parses directly from r.Body via
// FinishRegistration/FinishDiscoverableLogin -- both are still wrapped in
// http.MaxBytesReader plus a short per-request read deadline, matching
// decodeRequestJSON's own size bound and adding a time bound MaxBytesReader
// alone doesn't provide, since every passkey route here is reachable by a
// fully anonymous, unauthenticated caller.

// passkeyFinishReadDeadline bounds how long reading a (size-bounded, ~few
// KB) WebAuthn attestation/assertion body may take -- generous for any
// real client, but short enough that a deliberately slow-drip body cannot
// tie up a connection/goroutine indefinitely. Set per-request via
// http.ResponseController rather than the shared http.Server's
// ReadTimeout, which would also bound GET /v1/jobs/{id}/stream's 5-minute
// SSE response and blob uploads' 15-minute upload TTL -- see
// cmd/api/main.go's httpServer construction for that reasoning.
const passkeyFinishReadDeadline = 10 * time.Second

// clientIP identifies the caller for rate-limiting purposes only -- never
// as an identity or authorization signal. Deliberately does NOT trust any
// proxy header (X-Real-IP, X-Forwarded-For, ...): atos has no configured,
// verified trusted-reverse-proxy boundary (the same gap docs/AUTH.md's
// "Human Account Authentication" section exists to start closing), so
// trusting a client-suppliable header here would let every anonymous
// caller pick their own rate-limit bucket per request and bypass the
// limiter entirely. RemoteAddr is the TCP connection's actual source
// address, which an HTTP client cannot spoof on its own.
func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// writePasskeyErr never forwards a raw internal error string to an
// anonymous caller. A *protocol.Error (go-webauthn's own typed error for
// every challenge/origin/signature/CBOR validation failure -- an ordinary,
// expected outcome when a caller submits an invalid response, not a server
// malfunction) is answered as a generic authentication failure without a
// server-side error log, so a flood of deliberately-invalid attempts
// cannot flood logs or masquerade as infrastructure failures. Every other
// domain-level condition this package's own code can produce is
// classified explicitly below; anything left over (a Postgres driver
// error, an auth persistence failure) is logged in full server-side and
// answered with a stable, generic message -- a genuine infrastructure
// failure must read as 500, never disguised as a 401 authentication
// failure the way a blanket "default: 401" fallback would.
func (s *Server) writePasskeyErr(w http.ResponseWriter, r *http.Request, err error) {
	var webauthnErr *protocol.Error
	switch {
	case errors.Is(err, service.ErrPasskeyNotConfigured):
		writeError(w, http.StatusServiceUnavailable, domain.ErrValidationFailed, err.Error(), false)
	case errors.Is(err, service.ErrPasskeyRateLimited):
		writeError(w, http.StatusTooManyRequests, domain.ErrRateLimited, err.Error(), true)
	case errors.Is(err, service.ErrNoPasskeyCredentials), errors.As(err, &webauthnErr):
		writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, "authentication failed", false)
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, "ceremony not found or expired", false)
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, domain.ErrValidationFailed, "account already exists", false)
	default:
		if s.Logger != nil {
			s.Logger.Error("passkey ceremony failed", "error", err, "path", r.URL.Path)
		}
		writeError(w, http.StatusInternalServerError, domain.ErrValidationFailed, "passkey ceremony failed", true)
	}
}

func (s *Server) handleBeginPasskeyRegistration(w http.ResponseWriter, r *http.Request) {
	ceremonyID, options, err := s.Passkeys.BeginRegistration(r.Context(), clientIP(r))
	if err != nil {
		s.writePasskeyErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ceremony_id": ceremonyID, "options": options})
}

func (s *Server) handleFinishPasskeyRegistration(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestJSONBytes)
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(passkeyFinishReadDeadline))
	pair, err := s.Passkeys.FinishRegistration(r.Context(), r.PathValue("ceremony_id"), r)
	if err != nil {
		s.writePasskeyErr(w, r, err)
		return
	}
	writeTokenPair(w, http.StatusCreated, pair)
}

func (s *Server) handleBeginPasskeyLogin(w http.ResponseWriter, r *http.Request) {
	ceremonyID, options, err := s.Passkeys.BeginLogin(r.Context(), clientIP(r))
	if err != nil {
		s.writePasskeyErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ceremony_id": ceremonyID, "options": options})
}

func (s *Server) handleFinishPasskeyLogin(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestJSONBytes)
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(passkeyFinishReadDeadline))
	pair, err := s.Passkeys.FinishLogin(r.Context(), r.PathValue("ceremony_id"), r)
	if err != nil {
		s.writePasskeyErr(w, r, err)
		return
	}
	writeTokenPair(w, http.StatusOK, pair)
}
