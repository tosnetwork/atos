package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

func (s *Server) handleAuthDeviceStart(w http.ResponseWriter, r *http.Request) {
	if s.Auth == nil {
		writeError(w, http.StatusServiceUnavailable, domain.ErrAuthenticationRequired, "authorization service unavailable", true)
		return
	}
	var req struct {
		ClientType      string   `json:"client_type"`
		ClientName      string   `json:"client_name"`
		RequestedScopes []string `json:"requested_scopes"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed device authorization request: "+err.Error(), false)
		return
	}
	grant, err := s.Auth.StartDevice(req.RequestedScopes)
	if err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, err.Error(), false)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"device_code":               grant.DeviceCode,
		"user_code":                 grant.UserCode,
		"verification_uri":          "https://atos.im/activate",
		"verification_uri_complete": "https://atos.im/activate?code=" + grant.UserCode,
		"expires_in":                int64(time.Until(grant.ExpiresAt).Seconds()),
		"interval":                  1,
	})
}

func (s *Server) handleAuthDeviceToken(w http.ResponseWriter, r *http.Request) {
	if s.Auth == nil {
		writeError(w, http.StatusServiceUnavailable, domain.ErrAuthenticationRequired, "authorization service unavailable", true)
		return
	}
	var req struct {
		DeviceCode string `json:"device_code"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.DeviceCode == "" {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "device_code is required", false)
		return
	}
	pair, err := s.Auth.ExchangeDevice(req.DeviceCode)
	if err != nil {
		code := domain.ErrValidationFailed
		status := http.StatusBadRequest
		retryable := false
		if err.Error() == "authorization_pending" {
			code = domain.ErrAuthenticationRequired
			status = http.StatusAccepted
			retryable = true
		}
		writeError(w, status, code, err.Error(), retryable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":  pair.AccessToken,
		"token_type":    "Bearer",
		"expires_in":    pair.ExpiresIn,
		"refresh_token": pair.RefreshToken,
		"principal_id":  pair.Principal.ID,
		"scopes":        pair.Principal.ScopeStrings(),
	})
}

func (s *Server) handleAuthTokenRefresh(w http.ResponseWriter, r *http.Request) {
	if s.Auth == nil {
		writeError(w, http.StatusServiceUnavailable, domain.ErrAuthenticationRequired, "authorization service unavailable", true)
		return
	}
	var req struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RefreshToken == "" {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "refresh_token is required", false)
		return
	}
	pair, err := s.Auth.Refresh(req.RefreshToken)
	if err != nil {
		writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, err.Error(), false)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":  pair.AccessToken,
		"token_type":    "Bearer",
		"expires_in":    pair.ExpiresIn,
		"refresh_token": pair.RefreshToken,
		"principal_id":  pair.Principal.ID,
		"scopes":        pair.Principal.ScopeStrings(),
	})
}

func (s *Server) handleAuthRevoke(w http.ResponseWriter, r *http.Request) {
	if s.Auth != nil {
		s.Auth.Revoke(authFrom(r).Token)
	}
	w.WriteHeader(http.StatusNoContent)
}
