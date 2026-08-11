package httpapi

import (
	"errors"
	"net"
	"net/http"
	"strings"

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
// http.MaxBytesReader, matching decodeRequestJSON's own bound, since every
// passkey route here is reachable by a fully anonymous, unauthenticated
// caller.

// clientIP mirrors tosnetwork/atos-aidrop's own helper: trust X-Real-IP
// when a reverse proxy sets it, otherwise fall back to the raw connection's
// address. Used only to key the passkey rate limiter, never as an identity
// or authorization signal.
func clientIP(r *http.Request) string {
	if ip := strings.TrimSpace(r.Header.Get("X-Real-IP")); ip != "" {
		return ip
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// writePasskeyErr never forwards a raw internal error string to an
// anonymous caller. Every domain-level condition this package's own code
// can produce is classified explicitly below; anything else (a Postgres
// driver error, a JSON/CBOR parse failure inside go-webauthn, an auth
// persistence failure) is logged in full server-side and answered with a
// stable, generic message -- an infrastructure failure must read as
// 500/503, never disguised as a 401 authentication failure the way a
// blanket "default: 401" fallback would.
func (s *Server) writePasskeyErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, service.ErrPasskeyNotConfigured):
		writeError(w, http.StatusServiceUnavailable, domain.ErrValidationFailed, err.Error(), false)
	case errors.Is(err, service.ErrPasskeyRateLimited):
		writeError(w, http.StatusTooManyRequests, domain.ErrRateLimited, err.Error(), true)
	case errors.Is(err, service.ErrNoPasskeyCredentials):
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
	pair, err := s.Passkeys.FinishLogin(r.Context(), r.PathValue("ceremony_id"), r)
	if err != nil {
		s.writePasskeyErr(w, r, err)
		return
	}
	writeTokenPair(w, http.StatusOK, pair)
}
