package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/tosnetwork/atos/internal/auth"
)

func newAdminScopeTestServer(t *testing.T) (*Server, string, string) {
	t.Helper()
	authorization := auth.NewService()
	approvalToken := strings.Repeat("a", 32)
	adminApprovalToken := strings.Repeat("b", 32)
	server := &Server{
		Auth: authorization, ApprovalToken: approvalToken, AdminApprovalToken: adminApprovalToken,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return server, approvalToken, adminApprovalToken
}

func TestAdminScopeGrant_RejectedWithoutAdminApprovalToken(t *testing.T) {
	server, approvalToken, _ := newAdminScopeTestServer(t)
	grant, err := server.Auth.StartDevice("test", "admin-scope-test", []string{string(auth.ScopeActivationEvaluate)})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Mux())
	defer httpServer.Close()

	body := map[string]any{"user_code": grant.UserCode, "decision": "approve"}
	resp := phase01Request(t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/auth/device/decision", "", body,
		map[string]string{"X-ATOS-Approval-Token": approvalToken, "X-ATOS-Principal-ID": "prn_admin_test"})
	if resp.Status != http.StatusUnauthorized {
		t.Fatalf("approval without admin token = %d %s, want 401", resp.Status, resp.Body)
	}

	// The rejected attempt must not have consumed or corrupted the pending
	// grant -- it should still be approvable with the correct token.
	resolved, err := server.Auth.GrantByUserCode(grant.UserCode)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Status != auth.DeviceGrantPending {
		t.Fatalf("grant status = %s, want still pending after a rejected approval attempt", resolved.Status)
	}
}

func TestAdminScopeGrant_ApprovedWithAdminApprovalToken(t *testing.T) {
	server, approvalToken, adminApprovalToken := newAdminScopeTestServer(t)
	grant, err := server.Auth.StartDevice("test", "admin-scope-test", []string{string(auth.ScopeActivationEvaluate)})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Mux())
	defer httpServer.Close()

	body := map[string]any{"user_code": grant.UserCode, "decision": "approve"}
	resp := phase01Request(t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/auth/device/decision", "", body,
		map[string]string{
			"X-ATOS-Approval-Token":       approvalToken,
			"X-ATOS-Admin-Approval-Token": adminApprovalToken,
			"X-ATOS-Principal-ID":         "prn_admin_test",
		})
	if resp.Status != http.StatusOK {
		t.Fatalf("approval with admin token = %d %s, want 200", resp.Status, resp.Body)
	}
	resolved, err := server.Auth.GrantByUserCode(grant.UserCode)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Status != auth.DeviceGrantApproved {
		t.Fatalf("grant status = %s, want approved", resolved.Status)
	}
}

func TestOrdinaryScopeGrant_UnaffectedByMissingAdminApprovalToken(t *testing.T) {
	server, approvalToken, _ := newAdminScopeTestServer(t)
	grant, err := server.Auth.StartDevice("test", "ordinary-scope-test", []string{string(auth.ScopeCapabilitiesRead)})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Mux())
	defer httpServer.Close()

	body := map[string]any{"user_code": grant.UserCode, "decision": "approve"}
	resp := phase01Request(t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/auth/device/decision", "", body,
		map[string]string{"X-ATOS-Approval-Token": approvalToken, "X-ATOS-Principal-ID": "prn_ordinary_test"})
	if resp.Status != http.StatusOK {
		t.Fatalf("approval of an ordinary-scope grant without admin token = %d %s, want 200", resp.Status, resp.Body)
	}
}

// TestAdminScopeGrant_WebFormPathAlsoGated proves handleActivationDecisionPage
// (the browser-facing form path, not just the JSON API path) enforces the
// same admin-approval gate.
func TestAdminScopeGrant_WebFormPathAlsoGated(t *testing.T) {
	server, approvalToken, adminApprovalToken := newAdminScopeTestServer(t)
	grant, err := server.Auth.StartDevice("test", "admin-scope-form-test", []string{string(auth.ScopeActivationEvaluate)})
	if err != nil {
		t.Fatal(err)
	}
	principalID := "prn_admin_form_test"
	csrf := server.consentCSRF(principalID, grant.UserCode)
	httpServer := httptest.NewServer(server.Mux())
	defer httpServer.Close()

	postForm := func(headers map[string]string) *http.Response {
		form := url.Values{"user_code": {grant.UserCode}, "decision": {"approve"}, "csrf_token": {csrf}}
		req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/activate", strings.NewReader(form.Encode()))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		client := *httpServer.Client()
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp
	}

	withoutAdmin := postForm(map[string]string{"X-ATOS-Approval-Token": approvalToken, "X-ATOS-Principal-ID": principalID})
	if withoutAdmin.StatusCode != http.StatusUnauthorized {
		t.Fatalf("form approval without admin token = %d, want 401", withoutAdmin.StatusCode)
	}
	resolved, err := server.Auth.GrantByUserCode(grant.UserCode)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Status != auth.DeviceGrantPending {
		t.Fatalf("grant status = %s, want still pending", resolved.Status)
	}

	withAdmin := postForm(map[string]string{
		"X-ATOS-Approval-Token": approvalToken, "X-ATOS-Admin-Approval-Token": adminApprovalToken,
		"X-ATOS-Principal-ID": principalID,
	})
	if withAdmin.StatusCode != http.StatusSeeOther {
		t.Fatalf("form approval with admin token = %d, want 303 redirect", withAdmin.StatusCode)
	}
	resolved, err = server.Auth.GrantByUserCode(grant.UserCode)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Status != auth.DeviceGrantApproved {
		t.Fatalf("grant status = %s, want approved", resolved.Status)
	}
}
