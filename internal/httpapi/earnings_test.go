package httpapi

import (
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

func earningsAccessToken(t *testing.T, svc *auth.Service, scopes ...auth.Scope) (token, principalID string) {
	t.Helper()
	raw := make([]string, len(scopes))
	for i, scope := range scopes {
		raw[i] = string(scope)
	}
	grant, err := svc.StartDevice("test", "REST Test", raw)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := svc.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	return pair.AccessToken, pair.Principal.ID
}

// TestListEarnings_ScopesToOwnProvider proves GET /v1/provider/earnings
// only ever returns the authenticated caller's own earnings, never another
// provider's, regardless of what's in the store.
func TestListEarnings_ScopesToOwnProvider(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	earnings := service.NewEarningsService(st, payoutmock.New())
	tokenA, principalA := earningsAccessToken(t, authorization, auth.ScopeEarningsRead)
	_, principalB := earningsAccessToken(t, authorization, auth.ScopeEarningsRead)

	snap := domain.BillingSnapshot{
		JobID: "job_a", QuoteID: "q_a", ReceiptID: "rcpt_a", ProviderID: principalA,
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0", TrustMode: domain.TrustModeManaged,
		GrossCharge: domain.Money{Amount: "1.05", Currency: "USD"}, ProviderGross: domain.Money{Amount: "1.00", Currency: "USD"},
		GatewayFee: domain.Money{Amount: "0.05", Currency: "USD"}, PrincipalRefund: domain.Money{Amount: "0.00", Currency: "USD"},
	}
	if _, err := earnings.RecordSettlement(t.Context(), snap, "settle_a"); err != nil {
		t.Fatal(err)
	}
	snapB := snap
	snapB.JobID, snapB.ReceiptID, snapB.ProviderID = "job_b", "rcpt_b", principalB
	if _, err := earnings.RecordSettlement(t.Context(), snapB, "settle_b"); err != nil {
		t.Fatal(err)
	}

	server := &Server{Auth: authorization, Earnings: earnings}
	req := httptest.NewRequest(http.MethodGet, "/v1/provider/earnings", nil)
	req.Header.Set("Authorization", "Bearer "+tokenA)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Earnings []domain.ProviderEarning `json:"earnings"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Earnings) != 1 || payload.Earnings[0].ProviderID != principalA {
		t.Fatalf("earnings = %+v, want exactly provider A's own earning", payload.Earnings)
	}
}

// TestGetEarning_RejectsNonOwningProvider proves GET
// /v1/provider/earnings/{id} returns 403 for a provider that does not own
// the earning, never leaking its contents.
func TestGetEarning_RejectsNonOwningProvider(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	earnings := service.NewEarningsService(st, payoutmock.New())
	_, principalA := earningsAccessToken(t, authorization, auth.ScopeEarningsRead)
	tokenB, _ := earningsAccessToken(t, authorization, auth.ScopeEarningsRead)

	snap := domain.BillingSnapshot{
		JobID: "job_owned", QuoteID: "q_owned", ReceiptID: "rcpt_owned", ProviderID: principalA,
		CapabilityID: "cap_1", CapabilityVersion: "1.0.0", TrustMode: domain.TrustModeManaged,
		GrossCharge: domain.Money{Amount: "1.05", Currency: "USD"}, ProviderGross: domain.Money{Amount: "1.00", Currency: "USD"},
		GatewayFee: domain.Money{Amount: "0.05", Currency: "USD"}, PrincipalRefund: domain.Money{Amount: "0.00", Currency: "USD"},
	}
	e, err := earnings.RecordSettlement(t.Context(), snap, "settle_owned")
	if err != nil {
		t.Fatal(err)
	}

	server := &Server{Auth: authorization, Earnings: earnings}
	req := httptest.NewRequest(http.MethodGet, "/v1/provider/earnings/"+e.ID, nil)
	req.Header.Set("Authorization", "Bearer "+tokenB)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body = %s", recorder.Code, recorder.Body.String())
	}
}

// TestGetJobBilling_RejectsNonPartyPrincipal proves GET
// /v1/jobs/{id}/billing rejects a principal who is neither the job's payer
// nor its provider.
func TestGetJobBilling_RejectsNonPartyPrincipal(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	earnings := service.NewEarningsService(st, payoutmock.New())
	tokenOutsider, _ := earningsAccessToken(t, authorization, auth.ScopeJobsRead)

	job := domain.Job{
		ID: "job_billing_1", CapabilityID: "cap_1", QuoteID: "q_1",
		PrincipalID: "prn_owner", ProviderID: "prov_owner", State: domain.JobCompleted,
		EconomicState: domain.EconomicSettled,
	}
	if err := st.PutJob(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	snap := domain.BillingSnapshot{
		JobID: job.ID, QuoteID: job.QuoteID, ReceiptID: "rcpt_1", ProviderID: job.ProviderID,
		CapabilityID: job.CapabilityID, CapabilityVersion: "1.0.0", TrustMode: domain.TrustModeManaged,
		GrossCharge: domain.Money{Amount: "1.05", Currency: "USD"},
	}
	if _, _, err := st.PutBillingSnapshot(t.Context(), snap); err != nil {
		t.Fatal(err)
	}

	jobs := service.NewJobService(st, nil, nil, nil)
	server := &Server{Auth: authorization, Jobs: jobs, Earnings: earnings}
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/billing", nil)
	req.Header.Set("Authorization", "Bearer "+tokenOutsider)
	recorder := httptest.NewRecorder()
	server.Mux().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body = %s", recorder.Code, recorder.Body.String())
	}
}
