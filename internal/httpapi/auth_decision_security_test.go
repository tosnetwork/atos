package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tosnetwork/atos/internal/auth"
)

func TestDeviceDecisionRequiresTrustedPrincipalHeader(t *testing.T) {
	authorization := auth.NewService()
	grant, err := authorization.StartDevice("codex", "security-test", nil)
	if err != nil {
		t.Fatal(err)
	}
	approvalToken := strings.Repeat("s", 32)
	server := &Server{Auth: authorization, ApprovalToken: approvalToken, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	httpServer := httptest.NewServer(server.Mux())
	defer httpServer.Close()

	body := map[string]any{"user_code": grant.UserCode, "decision": "approve"}
	missing := phase01Request(t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/auth/device/decision", "", body,
		map[string]string{"X-ATOS-Approval-Token": approvalToken})
	if missing.Status != http.StatusUnauthorized {
		t.Fatalf("missing trusted principal header = %d %s", missing.Status, missing.Body)
	}

	approved := phase01Request(t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/auth/device/decision", "", body,
		map[string]string{"X-ATOS-Approval-Token": approvalToken, "X-ATOS-Principal-ID": "prn_security"})
	if approved.Status != http.StatusOK {
		t.Fatalf("trusted principal decision = %d %s", approved.Status, approved.Body)
	}
	resolved, err := authorization.GrantByUserCode(grant.UserCode)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.PrincipalID != "prn_security" {
		t.Fatalf("approved principal = %q", resolved.PrincipalID)
	}
	_ = context.Background()
}
