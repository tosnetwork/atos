package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"html"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tosnetwork/atos/internal/auth"
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
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed device authorization request: "+err.Error(), false)
		return
	}
	grant, err := s.Auth.StartDevice(req.ClientType, req.ClientName, req.RequestedScopes)
	if err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, err.Error(), false)
		return
	}
	base := strings.TrimRight(s.PublicBaseURL, "/")
	if base == "" {
		base = "http://localhost:8080"
	}
	verificationURI := base + "/activate"
	writeJSON(w, http.StatusOK, map[string]any{
		"device_code":               grant.DeviceCode,
		"user_code":                 grant.UserCode,
		"verification_uri":          verificationURI,
		"verification_uri_complete": verificationURI + "?code=" + url.QueryEscape(grant.UserCode),
		"expires_in":                maxInt64(1, int64(time.Until(grant.ExpiresAt).Seconds())),
		"interval":                  maxInt64(1, int64(s.Auth.PollInterval().Seconds())),
	})
}

func (s *Server) handleAuthDeviceToken(w http.ResponseWriter, r *http.Request) {
	if s.Auth == nil {
		writeOAuthError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "authorization service unavailable")
		return
	}
	var req struct {
		DeviceCode string `json:"device_code"`
	}
	if err := decodeRequestJSON(r, &req); err != nil || strings.TrimSpace(req.DeviceCode) == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "device_code is required")
		return
	}
	pair, err := s.Auth.ExchangeDevice(req.DeviceCode)
	if err != nil {
		if oauthErr, ok := err.(*auth.OAuthError); ok {
			writeOAuthError(w, http.StatusBadRequest, oauthErr.Code, oauthErr.Description)
			return
		}
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":  pair.AccessToken,
		"token_type":    "Bearer",
		"expires_in":    pair.ExpiresIn,
		"refresh_token": pair.RefreshToken,
		"principal_id":  pair.Principal.ID,
		"device_id":     pair.Principal.DeviceID,
		"scopes":        pair.Principal.ScopeStrings(),
	})
}

func (s *Server) handleAuthDeviceDecision(w http.ResponseWriter, r *http.Request) {
	if s.Auth == nil {
		writeError(w, http.StatusServiceUnavailable, domain.ErrAuthenticationRequired, "authorization service unavailable", true)
		return
	}
	if !secureEqual(r.Header.Get("X-ATOS-Approval-Token"), s.ApprovalToken) {
		writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, "trusted consent authorization is required", false)
		return
	}
	principalID := strings.TrimSpace(r.Header.Get("X-ATOS-Principal-ID"))
	if principalID == "" {
		writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, "authenticated consent principal is required", false)
		return
	}
	var req struct {
		UserCode string `json:"user_code"`
		Decision string `json:"decision"`
	}
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed device decision", false)
		return
	}
	approve := strings.EqualFold(req.Decision, "approve")
	if !approve && !strings.EqualFold(req.Decision, "deny") {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "decision must be approve or deny", false)
		return
	}
	grant, err := s.Auth.DecideDevice(req.UserCode, principalID, approve)
	if err != nil {
		writeError(w, http.StatusConflict, domain.ErrValidationFailed, err.Error(), false)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"user_code":   grant.UserCode,
		"status":      grant.Status,
		"client_type": grant.ClientType,
		"client_name": grant.ClientName,
		"scopes":      grant.Scopes,
		"expires_at":  grant.ExpiresAt,
	})
}

func (s *Server) handleActivationPage(w http.ResponseWriter, r *http.Request) {
	code := strings.TrimSpace(r.URL.Query().Get("code"))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
	if code == "" {
		_, _ = w.Write([]byte(activationHTML("Enter the user code shown by your ATOS client.", "", "", "", nil, false, "")))
		return
	}
	grant, err := s.Auth.GrantByUserCode(code)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(activationHTML("This authorization code is invalid or expired.", code, "not_found", "", nil, false, "")))
		return
	}
	message := "Authorization is waiting for a decision in the authenticated ATOS consent surface."
	canDecide := false
	principal := strings.TrimSpace(r.Header.Get("X-ATOS-Principal-ID"))
	if grant.Status == auth.DeviceGrantApproved {
		message = "Authorization approved. Return to your client; it will receive the token on its next poll."
	} else if grant.Status == auth.DeviceGrantDenied {
		message = "Authorization denied."
	} else if grant.Status == auth.DeviceGrantPending && principal != "" && secureEqual(r.Header.Get("X-ATOS-Approval-Token"), s.ApprovalToken) {
		message = "Review the client and requested scopes, then approve or deny access."
		canDecide = true
	}
	csrf := ""
	if canDecide {
		csrf = s.consentCSRF(principal, grant.UserCode)
	}
	_, _ = w.Write([]byte(activationHTML(message, grant.UserCode, string(grant.Status), grant.ClientName, grant.Scopes, canDecide, csrf)))
}

// handleActivationDecisionPage is the browser-facing half of Device
// Authorization. A trusted login/reverse-proxy boundary authenticates the user
// and injects the private approval token plus principal header. External
// clients cannot authorize themselves merely by knowing the user code.
func (s *Server) handleActivationDecisionPage(w http.ResponseWriter, r *http.Request) {
	if !secureEqual(r.Header.Get("X-ATOS-Approval-Token"), s.ApprovalToken) {
		writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, "trusted consent authorization is required", false)
		return
	}
	principalID := strings.TrimSpace(r.Header.Get("X-ATOS-Principal-ID"))
	if principalID == "" {
		writeError(w, http.StatusUnauthorized, domain.ErrAuthenticationRequired, "authenticated consent principal is required", false)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := r.ParseForm(); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "invalid activation form", false)
		return
	}
	decision := strings.TrimSpace(r.Form.Get("decision"))
	approve := strings.EqualFold(decision, "approve")
	if !approve && !strings.EqualFold(decision, "deny") {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "decision must be approve or deny", false)
		return
	}
	code := normalizePageCode(r.Form.Get("user_code"))
	if code == "" {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "user_code is required", false)
		return
	}
	if !secureEqual(r.Form.Get("csrf_token"), s.consentCSRF(principalID, code)) {
		writeError(w, http.StatusForbidden, domain.ErrPermissionDenied, "invalid consent form token", false)
		return
	}
	if _, err := s.Auth.DecideDevice(code, principalID, approve); err != nil {
		writeError(w, http.StatusConflict, domain.ErrValidationFailed, err.Error(), false)
		return
	}
	http.Redirect(w, r, "/activate?code="+url.QueryEscape(code), http.StatusSeeOther)
}

func activationHTML(message, code, status, clientName string, scopes []auth.Scope, canDecide bool, csrf string) string {
	scopeHTML := ""
	for _, scope := range scopes {
		scopeHTML += "<li><code>" + html.EscapeString(string(scope)) + "</code></li>"
	}
	details := ""
	if clientName != "" {
		details = "<p>Client: <strong>" + html.EscapeString(clientName) + "</strong></p><ul>" + scopeHTML + "</ul>"
	}
	forms := ""
	if canDecide {
		escaped := html.EscapeString(code)
		forms = "<form method=\"post\" action=\"/activate\"><input type=\"hidden\" name=\"user_code\" value=\"" + escaped + "\">" +
			"<input type=\"hidden\" name=\"csrf_token\" value=\"" + html.EscapeString(csrf) + "\">" +
			"<button name=\"decision\" value=\"approve\" type=\"submit\">Approve</button> " +
			"<button name=\"decision\" value=\"deny\" type=\"submit\">Deny</button></form>"
	}
	return "<!doctype html><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">" +
		"<title>Authorize ATOS</title><style>body{font-family:system-ui;max-width:720px;margin:64px auto;padding:0 24px;line-height:1.5}" +
		"code{background:#f2f2f2;padding:.25rem .45rem;border-radius:.35rem}button{font:inherit;padding:.55rem 1rem;margin:.5rem .25rem .5rem 0}</style>" +
		"<h1>Authorize ATOS</h1><p>" + html.EscapeString(message) + "</p>" +
		"<p>User code: <code>" + html.EscapeString(code) + "</code></p>" + details +
		"<p>Status: " + html.EscapeString(status) + "</p>" + forms
}

func (s *Server) consentCSRF(principalID, userCode string) string {
	mac := hmac.New(sha256.New, []byte(s.ApprovalToken))
	_, _ = mac.Write([]byte("atos-device-consent-v1\x00" + principalID + "\x00" + normalizePageCode(userCode)))
	return hex.EncodeToString(mac.Sum(nil))
}

func normalizePageCode(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, " ", "")
	if len(value) == 8 && !strings.Contains(value, "-") {
		value = value[:4] + "-" + value[4:]
	}
	return value
}

func (s *Server) handleAuthTokenRefresh(w http.ResponseWriter, r *http.Request) {
	if s.Auth == nil {
		writeOAuthError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "authorization service unavailable")
		return
	}
	var req struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := decodeRequestJSON(r, &req); err != nil || strings.TrimSpace(req.RefreshToken) == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "refresh_token is required")
		return
	}
	pair, err := s.Auth.Refresh(req.RefreshToken)
	if err != nil {
		if oauthErr, ok := err.(*auth.OAuthError); ok {
			writeOAuthError(w, http.StatusUnauthorized, oauthErr.Code, oauthErr.Description)
			return
		}
		writeOAuthError(w, http.StatusUnauthorized, "invalid_grant", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":  pair.AccessToken,
		"token_type":    "Bearer",
		"expires_in":    pair.ExpiresIn,
		"refresh_token": pair.RefreshToken,
		"principal_id":  pair.Principal.ID,
		"device_id":     pair.Principal.DeviceID,
		"scopes":        pair.Principal.ScopeStrings(),
	})
}

func (s *Server) handleAuthRevoke(w http.ResponseWriter, r *http.Request) {
	if s.Auth != nil {
		s.Auth.Revoke(authFrom(r).Token)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListDevices(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"devices": s.Auth.Devices(principalFrom(r))})
}

func (s *Server) handleRevokeDevice(w http.ResponseWriter, r *http.Request) {
	if err := s.Auth.RevokeDevice(principalFrom(r), r.PathValue("id")); err != nil {
		writeError(w, http.StatusNotFound, domain.ErrNotFound, "device not found", false)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeOAuthError(w http.ResponseWriter, status int, code, description string) {
	writeJSON(w, status, map[string]any{"error": code, "error_description": description})
}

func secureEqual(a, b string) bool {
	if a == "" || b == "" || len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
