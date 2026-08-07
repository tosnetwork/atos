package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/domain"
)

// handleAuthDeviceStart implements POST /v1/auth/device from docs/AUTH.md.
// Phase 0 stub: it always immediately "authorizes" the device instead of
// waiting for a human to visit verification_uri — good enough to exercise
// the rest of the API locally, not a real auth flow. Wire in a real
// verification step (and a real signed access_token, not principal_id
// echoed back as the bearer value) before this leaves a sandbox.
func (s *Server) handleAuthDeviceStart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientType      string   `json:"client_type"`
		ClientName      string   `json:"client_name"`
		RequestedScopes []string `json:"requested_scopes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed JSON body", false)
		return
	}
	deviceCode := "dc_" + uuid.NewString()
	writeJSON(w, http.StatusOK, map[string]any{
		"device_code":               deviceCode,
		"user_code":                 "DEMO-" + deviceCode[3:7],
		"verification_uri":          "https://atos.im/activate",
		"verification_uri_complete": "https://atos.im/activate?code=" + deviceCode,
		"expires_in":                900,
		"interval":                  1,
	})
}

// handleAuthDeviceToken always succeeds immediately (see handleAuthDeviceStart)
// and mints principal_id == the device_code the caller polled with, so the
// same client always gets the same principal across a session.
func (s *Server) handleAuthDeviceToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DeviceCode string `json:"device_code"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.DeviceCode == "" {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "device_code is required", false)
		return
	}
	principalID := "prn_" + req.DeviceCode
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":  principalID,
		"token_type":    "Bearer",
		"expires_in":    3600,
		"refresh_token": "rt_" + uuid.NewString(),
		"principal_id":  principalID,
		"scopes":        []string{"capabilities:read", "quotes:read", "invocations:create", "jobs:read", "account:read"},
	})
}
