package mcp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	payoutmock "github.com/tosnetwork/atos/internal/adapters/payout/mock"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// TestProviderEarningsToolHiddenWithoutScope proves atos_provider_earnings
// is unreachable to a caller without earnings:read, mirroring
// TestScopeGatedToolIsUnreachableToConsumer for the capability-management
// tools.
func TestProviderEarningsToolHiddenWithoutScope(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	token := accessToken(t, authorization, auth.ScopeCapabilitiesRead)
	body := bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"atos_provider_earnings","arguments":{}}}`)
	req := httptest.NewRequest(http.MethodPost, "/mcp", body)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	(&Server{Auth: authorization}).Handler()(recorder, req)
	var response rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != codeMethodNotFound {
		t.Fatalf("got response %s, want method-not-found for hidden tool", recorder.Body.String())
	}
}

// TestProviderEarningsToolListsOnlyOwnEarnings proves a provider calling
// atos_provider_earnings with the required scope sees only its own
// earnings, never another provider's.
func TestProviderEarningsToolListsOnlyOwnEarnings(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	earnings := service.NewEarningsService(st, payoutmock.New())

	tokenA := accessToken(t, authorization, auth.ScopeEarningsRead)
	principalA := mustPrincipal(t, authorization, tokenA)
	tokenB := accessToken(t, authorization, auth.ScopeEarningsRead)
	principalB := mustPrincipal(t, authorization, tokenB)

	snapA := domain.BillingSnapshot{
		JobID: "job_a", QuoteID: "q_a", ReceiptID: "rcpt_a", ProviderID: principalA,
		CapabilityID: "cap_a", CapabilityVersion: "1.0.0", TrustMode: domain.TrustModeManaged,
		GrossCharge: domain.Money{Amount: "1.05", Currency: "USD"}, ProviderGross: domain.Money{Amount: "1.00", Currency: "USD"},
		GatewayFee: domain.Money{Amount: "0.05", Currency: "USD"}, PrincipalRefund: domain.Money{Amount: "0.00", Currency: "USD"},
	}
	if _, err := earnings.RecordSettlement(t.Context(), snapA, "settle_a"); err != nil {
		t.Fatal(err)
	}
	snapB := snapA
	snapB.JobID, snapB.ReceiptID, snapB.ProviderID = "job_b", "rcpt_b", principalB
	if _, err := earnings.RecordSettlement(t.Context(), snapB, "settle_b"); err != nil {
		t.Fatal(err)
	}

	server := &Server{Auth: authorization, Earnings: earnings}
	body := bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"atos_provider_earnings","arguments":{}}}`)
	req := httptest.NewRequest(http.MethodPost, "/mcp", body)
	req.Header.Set("Authorization", "Bearer "+tokenA)
	recorder := httptest.NewRecorder()
	server.Handler()(recorder, req)

	var response rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil {
		t.Fatalf("unexpected tool error: %+v", response.Error)
	}
	var result struct {
		Result struct {
			StructuredContent struct {
				Earnings []domain.ProviderEarning `json:"earnings"`
			} `json:"structuredContent"`
		} `json:"result"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	got := result.Result.StructuredContent.Earnings
	if len(got) != 1 || got[0].ProviderID != principalA {
		t.Fatalf("provider A's earnings list = %+v, want exactly its own earning", got)
	}
}

func mustPrincipal(t *testing.T, authorization *auth.Service, token string) string {
	t.Helper()
	principal, err := authorization.Authenticate(token)
	if err != nil {
		t.Fatal(err)
	}
	return principal.ID
}
