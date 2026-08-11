package httpapi

import (
	"errors"
	"net"
	"net/http"
	"strings"
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
// var, not const, so tests can shrink it and prove the deadline actually
// fires within a bounded, fast test run rather than waiting out the real
// production value.
var passkeyFinishReadDeadline = 10 * time.Second

// clientIP identifies the caller for rate-limiting purposes only -- never
// as an identity or authorization signal. The forwarded-IP headers
// (X-Real-IP, X-Forwarded-For) are trusted ONLY when the request's actual
// TCP peer address falls inside s.TrustedProxyCIDRs (config
// ATOS_TRUSTED_PROXY_CIDRS, empty/unset by default) -- an HTTP client
// cannot spoof its own TCP source address, but it CAN set any header value
// it likes, so trusting a header from an untrusted peer would let every
// anonymous caller pick its own rate-limit bucket per request. The
// opposite failure mode matters too: if ATOS actually runs behind a load
// balancer/ingress/CDN and this is left unconfigured, every request
// resolves to that proxy's own single address and shares one rate-limit
// bucket -- set TrustedProxyCIDRs to the proxy's real address range to
// avoid that. Single-hop only (the first/only forwarded value is used, no
// X-Forwarded-For chain-walking) -- sufficient for one reverse proxy
// directly in front of ATOS, not a multi-hop proxy chain.
func (s *Server) clientIP(r *http.Request) string {
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		peer = r.RemoteAddr
	}
	if !s.peerIsTrustedProxy(peer) {
		return peer
	}
	if ip := strings.TrimSpace(r.Header.Get("X-Real-IP")); ip != "" {
		return ip
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if first, _, ok := strings.Cut(xff, ","); ok {
			return strings.TrimSpace(first)
		}
		return strings.TrimSpace(xff)
	}
	return peer
}

func (s *Server) peerIsTrustedProxy(peer string) bool {
	if len(s.TrustedProxyCIDRs) == 0 {
		return false
	}
	parsed := net.ParseIP(peer)
	if parsed == nil {
		return false
	}
	for _, cidr := range s.TrustedProxyCIDRs {
		if cidr.Contains(parsed) {
			return true
		}
	}
	return false
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
	ceremonyID, options, err := s.Passkeys.BeginRegistration(r.Context(), s.clientIP(r))
	if err != nil {
		s.writePasskeyErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ceremony_id": ceremonyID, "options": options})
}

// applyFinishReadDeadline bounds how long reading the (already
// size-bounded) request body may take, and returns a func the caller MUST
// defer to clear the deadline again before returning -- SetReadDeadline
// sets an ABSOLUTE deadline on the underlying connection, and HTTP/1.1
// keep-alive connections are reused across requests, so leaving a short
// deadline in place after this handler finishes would corrupt reads for
// whatever unrelated request lands on the same connection next. A failure
// to set the deadline (e.g. a ResponseWriter that genuinely does not
// support it, unlike the real server after statusRecorder's Unwrap fix) is
// logged, not silently discarded -- MaxBytesReader still bounds total
// damage either way, but a silent failure here is exactly the kind of bug
// that let the read-deadline defense go unnoticed as ineffective before.
func (s *Server) applyFinishReadDeadline(w http.ResponseWriter, r *http.Request) func() {
	rc := http.NewResponseController(w)
	if err := rc.SetReadDeadline(time.Now().Add(passkeyFinishReadDeadline)); err != nil && s.Logger != nil {
		s.Logger.Warn("passkey finish route could not set a read deadline", "error", err, "path", r.URL.Path)
	}
	return func() {
		_ = rc.SetReadDeadline(time.Time{})
	}
}

func (s *Server) handleFinishPasskeyRegistration(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestJSONBytes)
	defer s.applyFinishReadDeadline(w, r)()
	pair, err := s.Passkeys.FinishRegistration(r.Context(), r.PathValue("ceremony_id"), r)
	if err != nil {
		s.writePasskeyErr(w, r, err)
		return
	}
	writeTokenPair(w, http.StatusCreated, pair)
}

func (s *Server) handleBeginPasskeyLogin(w http.ResponseWriter, r *http.Request) {
	ceremonyID, options, err := s.Passkeys.BeginLogin(r.Context(), s.clientIP(r))
	if err != nil {
		s.writePasskeyErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ceremony_id": ceremonyID, "options": options})
}

func (s *Server) handleFinishPasskeyLogin(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestJSONBytes)
	defer s.applyFinishReadDeadline(w, r)()
	pair, err := s.Passkeys.FinishLogin(r.Context(), r.PathValue("ceremony_id"), r)
	if err != nil {
		s.writePasskeyErr(w, r, err)
		return
	}
	writeTokenPair(w, http.StatusOK, pair)
}
