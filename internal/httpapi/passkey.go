package httpapi

import (
	"errors"
	"net"
	"net/http"
	"net/netip"
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
// as an identity or authorization signal. Forwarded-IP headers (X-Real-IP,
// X-Forwarded-For) are consulted ONLY when the request's actual TCP peer
// address falls inside s.TrustedProxyCIDRs (config ATOS_TRUSTED_PROXY_CIDRS,
// empty/unset by default): an HTTP client cannot spoof its own TCP source
// address, but it CAN set any header value it likes, so trusting a header
// from an untrusted peer would let every anonymous caller pick its own
// rate-limit bucket per request. The opposite failure mode matters too: if
// ATOS actually runs behind a load balancer/ingress/CDN and this is left
// unconfigured, every request resolves to that proxy's own single address
// and shares one rate-limit bucket -- set TrustedProxyCIDRs to the proxy's
// real address range to avoid that.
//
// A trusted TCP peer does not make every element of a forwarded header
// trustworthy: common reverse-proxy configurations (nginx's default
// `proxy_add_x_forwarded_for`, for example) APPEND to any X-Forwarded-For
// the client already sent rather than overwriting it, so a client can
// pre-seed `X-Forwarded-For: <anything>` and have the trusted proxy append
// the real address after it -- naively taking the leftmost/first value
// would return the client's own forged entry. X-Real-IP is different: a
// correctly configured proxy SETS (overwrites) it to a single value, not a
// chain, so it's trusted directly once the peer is trusted. X-Forwarded-For
// is walked right-to-left, skipping any entry that is itself inside a
// trusted CIDR (another hop this deployment also trusts), returning the
// first entry that isn't -- the address the trust chain actually
// terminates at. Every candidate is strictly parsed via net/netip; anything
// that doesn't parse as an IP is skipped rather than trusted verbatim.
func (s *Server) clientIP(r *http.Request) string {
	peer := peerAddr(r.RemoteAddr)
	if !peer.IsValid() || !s.addrIsTrustedProxy(peer) {
		return peerString(peer, r.RemoteAddr)
	}
	if raw := strings.TrimSpace(r.Header.Get("X-Real-IP")); raw != "" {
		if addr, err := netip.ParseAddr(raw); err == nil {
			return addr.String()
		}
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		for i := len(parts) - 1; i >= 0; i-- {
			addr, err := netip.ParseAddr(strings.TrimSpace(parts[i]))
			if err != nil {
				continue
			}
			if !s.addrIsTrustedProxy(addr) {
				return addr.String()
			}
		}
	}
	return peerString(peer, r.RemoteAddr)
}

// peerAddr parses an http.Request.RemoteAddr's host portion as a
// netip.Addr, returning the zero (invalid) Addr if it cannot be parsed --
// callers must check IsValid() before use.
func peerAddr(remoteAddr string) netip.Addr {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}
	return addr
}

// peerString renders a parsed peer address canonically, or falls back to
// the raw RemoteAddr verbatim if it never parsed at all (better than an
// empty rate-limit key).
func peerString(peer netip.Addr, rawRemoteAddr string) string {
	if peer.IsValid() {
		return peer.String()
	}
	return rawRemoteAddr
}

func (s *Server) addrIsTrustedProxy(addr netip.Addr) bool {
	if len(s.TrustedProxyCIDRs) == 0 {
		return false
	}
	for _, prefix := range s.TrustedProxyCIDRs {
		if prefix.Contains(addr) {
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
