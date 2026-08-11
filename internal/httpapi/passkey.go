package httpapi

import (
	"errors"
	"net/http"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store"
)

// Human account authentication (passkey/WebAuthn) -- atos-spec
// docs/AUTH.md's "Human Account Authentication (Passkey/WebAuthn)" section.
// finish handlers deliberately do not use decodeRequestJSON: the request
// body is the raw WebAuthn attestation/assertion response, which the
// go-webauthn library parses directly from r.Body via
// FinishRegistration/FinishDiscoverableLogin.

func writePasskeyErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrPasskeyNotConfigured):
		writeError(w, http.StatusServiceUnavailable, domain.ErrValidationFailed, err.Error(), false)
	case errors.Is(err, service.ErrNoPasskeyCredentials):
		writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, err.Error(), false)
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, "ceremony not found or expired", false)
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, domain.ErrValidationFailed, "account already exists", false)
	default:
		writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, err.Error(), false)
	}
}

func (s *Server) handleBeginPasskeyRegistration(w http.ResponseWriter, r *http.Request) {
	ceremonyID, options, err := s.Passkeys.BeginRegistration(r.Context())
	if err != nil {
		writePasskeyErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ceremony_id": ceremonyID, "options": options})
}

func (s *Server) handleFinishPasskeyRegistration(w http.ResponseWriter, r *http.Request) {
	pair, err := s.Passkeys.FinishRegistration(r.Context(), r.PathValue("ceremony_id"), r)
	if err != nil {
		writePasskeyErr(w, err)
		return
	}
	writeTokenPair(w, http.StatusCreated, pair)
}

func (s *Server) handleBeginPasskeyLogin(w http.ResponseWriter, r *http.Request) {
	ceremonyID, options, err := s.Passkeys.BeginLogin(r.Context())
	if err != nil {
		writePasskeyErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ceremony_id": ceremonyID, "options": options})
}

func (s *Server) handleFinishPasskeyLogin(w http.ResponseWriter, r *http.Request) {
	pair, err := s.Passkeys.FinishLogin(r.Context(), r.PathValue("ceremony_id"), r)
	if err != nil {
		writePasskeyErr(w, err)
		return
	}
	writeTokenPair(w, http.StatusOK, pair)
}
