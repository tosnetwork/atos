package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	payoutmock "github.com/tosnetwork/atos/internal/adapters/payout/mock"
	"github.com/tosnetwork/atos/internal/adapters/storage/local"
	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/atos/internal/store/memory"
)

// providerToolsHarness wires the same real Quote -> Invoke -> execute ->
// settle pipeline internal/service's own tests use, so these MCP-level
// tests exercise real authorization plus a real underlying job, not a
// stubbed shortcut.
type providerToolsHarness struct {
	auth         *auth.Service
	capabilities *service.CapabilityService
	quotes       *service.QuoteService
	accounts     *service.AccountService
	jobs         *service.JobService
	disputes     *service.DisputeService
	st           store.Store
}

func newProviderToolsHarness(t *testing.T) providerToolsHarness {
	t.Helper()
	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	provider := tosaimock.New()
	core := toscoremock.New(st)
	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, provider, core, accounts)
	earnings := service.NewEarningsService(st, payoutmock.New()).WithMaturationPeriod(time.Nanosecond)
	jobs.WithEarnings(earnings)
	blobStorage, err := local.New(t.TempDir(), "http://localhost", st)
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	artifacts := service.NewArtifactService(st, blobStorage)
	disputes := service.NewDisputeService(st, jobs, earnings, accounts, artifacts)
	return providerToolsHarness{auth: authorization, capabilities: capabilities, quotes: quotes, accounts: accounts, jobs: jobs, disputes: disputes, st: st}
}

func (h providerToolsHarness) server() *Server {
	return &Server{Auth: h.auth, Capabilities: h.capabilities, Quotes: h.quotes, Jobs: h.jobs, Accounts: h.accounts, Disputes: h.disputes}
}

func (h providerToolsHarness) registerCapability(t *testing.T, providerID string) domain.Capability {
	t.Helper()
	cap, err := h.capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: providerID, Name: "Test Capability", Description: "for tests",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:        domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		IdempotencyKey: "register-" + providerID,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return cap
}

func (h providerToolsHarness) completedJob(t *testing.T, providerID, principalID string) domain.Job {
	t.Helper()
	cap := h.registerCapability(t, providerID)
	quote, err := h.quotes.Create(context.Background(), service.CreateQuoteInput{CapabilityID: cap.ID})
	if err != nil {
		t.Fatalf("Create quote: %v", err)
	}
	result, err := h.jobs.Invoke(context.Background(), service.SubmitInput{
		PrincipalID: principalID, CapabilityID: cap.ID, QuoteID: quote.ID,
		Input: map[string]any{"x": 1}, IdempotencyKey: "invoke-" + providerID + "-" + principalID,
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	return result.Job
}

func callTool(t *testing.T, server *Server, token, toolName string, args map[string]any) rpcResponse {
	t.Helper()
	argsJSON, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	bodyStr := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + toolName + `","arguments":` + string(argsJSON) + `}}`
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(bodyStr))
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	server.Handler()(recorder, req)
	var response rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response %s: %v", recorder.Body.String(), err)
	}
	return response
}

// toolCallFailed reports whether resp represents a failed tool call --
// either a JSON-RPC protocol-level error (resp.Error, used for hidden/
// unknown tools and malformed params) or a domain error surfaced through
// the tool RESULT itself (isError:true), which is how handleToolCall
// reports every domain.Error a handler returns (see server.go's
// handleToolCall: it always calls writeRPCResult, never writeRPCError,
// for a handler's own returned error).
func toolCallFailed(t *testing.T, resp rpcResponse) bool {
	t.Helper()
	if resp.Error != nil {
		return true
	}
	raw, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result.IsError
}

func TestProviderJobsTool_HiddenWithoutScope(t *testing.T) {
	h := newProviderToolsHarness(t)
	token := accessToken(t, h.auth, auth.ScopeCapabilitiesRead)
	resp := callTool(t, h.server(), token, "atos_provider_jobs", map[string]any{})
	if resp.Error == nil || resp.Error.Code != codeMethodNotFound {
		t.Fatalf("got %+v, want method-not-found for hidden tool", resp.Error)
	}
}

func TestProviderJobsTool_ListsOnlyOwnJobs(t *testing.T) {
	h := newProviderToolsHarness(t)
	tokenA := accessToken(t, h.auth, auth.ScopeProviderJobsRead)
	providerA := mustPrincipal(t, h.auth, tokenA)
	h.completedJob(t, providerA, "prn_a")
	h.completedJob(t, "agt_other_provider", "prn_b")

	resp := callTool(t, h.server(), tokenA, "atos_provider_jobs", map[string]any{})
	if toolCallFailed(t, resp) {
		t.Fatalf("unexpected error: %+v", resp)
	}
	var result struct {
		Result struct {
			StructuredContent struct {
				Jobs []domain.Job `json:"jobs"`
			} `json:"structuredContent"`
		} `json:"result"`
	}
	raw, _ := json.Marshal(resp)
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Result.StructuredContent.Jobs) != 1 || result.Result.StructuredContent.Jobs[0].ProviderID != providerA {
		t.Fatalf("jobs = %+v, want exactly provider A's own job", result.Result.StructuredContent.Jobs)
	}
}

func TestProviderJobsTool_GetSingleJob_WrongProviderDenied(t *testing.T) {
	h := newProviderToolsHarness(t)
	tokenA := accessToken(t, h.auth, auth.ScopeProviderJobsRead)
	job := h.completedJob(t, "agt_owner", "prn_x")

	resp := callTool(t, h.server(), tokenA, "atos_provider_jobs", map[string]any{"job_id": job.ID})
	if !toolCallFailed(t, resp) {
		t.Fatal("expected an error reading another provider's job by id")
	}
}

func TestDeliverJobTool_HiddenWithoutScope(t *testing.T) {
	h := newProviderToolsHarness(t)
	token := accessToken(t, h.auth, auth.ScopeCapabilitiesRead)
	resp := callTool(t, h.server(), token, "atos_deliver_job", map[string]any{"job_id": "job_x", "output": map[string]any{}, "idempotency_key": "k1"})
	if resp.Error == nil || resp.Error.Code != codeMethodNotFound {
		t.Fatalf("got %+v, want method-not-found for hidden tool", resp.Error)
	}
}

func TestDeliverJobTool_CannotOverrideAuthoritativeFields(t *testing.T) {
	// The tool's input schema only accepts job_id/output/idempotency_key
	// -- no provider_id, trust_mode, proof_profile, or price field exists
	// for a caller to even attempt to set. Prove that stuffing one into
	// arguments anyway has no effect (the schema/handler both ignore
	// unknown fields; provider identity always comes from principal.ID).
	h := newProviderToolsHarness(t)
	tokenA := accessToken(t, h.auth, auth.ScopeProviderJobsDeliver)
	providerA := mustPrincipal(t, h.auth, tokenA)
	job := h.completedJob(t, providerA, "prn_deliver")

	resp := callTool(t, h.server(), tokenA, "atos_deliver_job", map[string]any{
		"job_id": job.ID, "output": map[string]any{"ok": true},
		"provider_id": "agt_someone_else", "trust_mode": "native", "idempotency_key": "deliver-k1",
	})
	// The job already auto-completed via the mock provider (DeliveryMode
	// instant), so DeliverResult's idempotent-replay path returns it
	// unchanged rather than erroring -- proving delivery never re-settles
	// nor lets the extraneous fields redirect it to another provider.
	if toolCallFailed(t, resp) {
		t.Fatalf("unexpected error: %+v", resp)
	}
	current, err := h.jobs.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ProviderID != providerA {
		t.Fatalf("provider_id changed to %q, want unchanged %q", current.ProviderID, providerA)
	}
	if current.TrustMode != domain.TrustModeManaged {
		t.Fatalf("trust_mode changed to %q, want unchanged managed", current.TrustMode)
	}
}

func TestRequestSettlementTool_HiddenWithoutScope(t *testing.T) {
	h := newProviderToolsHarness(t)
	token := accessToken(t, h.auth, auth.ScopeCapabilitiesRead)
	resp := callTool(t, h.server(), token, "atos_request_settlement", map[string]any{"job_id": "job_x", "idempotency_key": "k1"})
	if resp.Error == nil || resp.Error.Code != codeMethodNotFound {
		t.Fatalf("got %+v, want method-not-found for hidden tool", resp.Error)
	}
}

// TestRequestSettlementTool_ReadOnlyScopeInsufficientForWrite proves the
// newly-defined settlement:write scope is enforced as a genuinely separate
// mutation scope: holding only settlement:read (an existing, pre-Phase-3A
// read-only scope) must not authorize atos_request_settlement -- the
// specific risk the roadmap flagged by name ("do not overload a read-only
// scope to authorize a money-changing operation").
func TestRequestSettlementTool_ReadOnlyScopeInsufficientForWrite(t *testing.T) {
	h := newProviderToolsHarness(t)
	token := accessToken(t, h.auth, auth.ScopeSettlementRead)
	resp := callTool(t, h.server(), token, "atos_request_settlement", map[string]any{"job_id": "job_x", "idempotency_key": "k1"})
	if resp.Error == nil || resp.Error.Code != codeMethodNotFound {
		t.Fatalf("got %+v, want method-not-found: settlement:read alone must not authorize atos_request_settlement", resp.Error)
	}
}

func TestDeliverJobTool_WrongProviderDenied(t *testing.T) {
	h := newProviderToolsHarness(t)
	tokenA := accessToken(t, h.auth, auth.ScopeProviderJobsDeliver)
	job := h.completedJob(t, "agt_deliver_owner", "prn_deliver_wrong")

	resp := callTool(t, h.server(), tokenA, "atos_deliver_job", map[string]any{
		"job_id": job.ID, "output": map[string]any{"ok": true}, "idempotency_key": "deliver-wrong-1",
	})
	if !toolCallFailed(t, resp) {
		t.Fatal("expected delivery from a non-owning provider to be rejected")
	}
}

func TestRequestSettlementTool_AlreadySettledJobIsIdempotent(t *testing.T) {
	h := newProviderToolsHarness(t)
	tokenA := accessToken(t, h.auth, auth.ScopeSettlementWrite)
	providerA := mustPrincipal(t, h.auth, tokenA)
	job := h.completedJob(t, providerA, "prn_settle")

	resp := callTool(t, h.server(), tokenA, "atos_request_settlement", map[string]any{"job_id": job.ID, "idempotency_key": "settle-k1"})
	if toolCallFailed(t, resp) {
		t.Fatalf("unexpected error: %+v", resp)
	}
}

func TestRequestSettlementTool_WrongProviderDenied(t *testing.T) {
	h := newProviderToolsHarness(t)
	tokenA := accessToken(t, h.auth, auth.ScopeSettlementWrite)
	job := h.completedJob(t, "agt_owner", "prn_settle2")

	resp := callTool(t, h.server(), tokenA, "atos_request_settlement", map[string]any{"job_id": job.ID, "idempotency_key": "settle-k2"})
	if !toolCallFailed(t, resp) {
		t.Fatal("expected an error requesting settlement for another provider's job")
	}
}

func TestDisputeJobTool_HiddenWithoutScope(t *testing.T) {
	h := newProviderToolsHarness(t)
	token := accessToken(t, h.auth, auth.ScopeDisputesOpen)
	resp := callTool(t, h.server(), token, "atos_dispute_job", map[string]any{"operation": "review", "dispute_id": "dispute_x"})
	if resp.Error == nil || resp.Error.Code != codeMethodNotFound {
		t.Fatalf("got %+v, want method-not-found for hidden tool", resp.Error)
	}
}

func TestDisputeJobTool_ReviewAndResolveHappyPath(t *testing.T) {
	h := newProviderToolsHarness(t)
	job := h.completedJob(t, "agt_dispute_provider", "prn_dispute_1")
	d, err := h.disputes.Open(context.Background(), service.OpenDisputeInput{
		PrincipalID: "prn_dispute_1", JobID: job.ID, Reason: "not delivered", IdempotencyKey: "dispute-open-mcp-1",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	reviewerToken := accessToken(t, h.auth, auth.ScopeDisputesReview)
	reviewResp := callTool(t, h.server(), reviewerToken, "atos_dispute_job", map[string]any{"operation": "review", "dispute_id": d.ID})
	if toolCallFailed(t, reviewResp) {
		t.Fatalf("review: %+v", reviewResp)
	}

	resolveResp := callTool(t, h.server(), reviewerToken, "atos_dispute_job", map[string]any{
		"operation": "resolve", "dispute_id": d.ID, "outcome": "provider",
	})
	if toolCallFailed(t, resolveResp) {
		t.Fatalf("resolve: %+v", resolveResp)
	}

	final, err := h.disputes.Get(context.Background(), d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.ReviewStatus != domain.DisputeResolvedForProvider {
		t.Fatalf("review_status = %s, want resolved_for_provider", final.ReviewStatus)
	}
}

// TestDisputeJobTool_ReviewerIDAlwaysFromAuthenticatedPrincipal proves
// toolDisputeJob never reads a caller-supplied reviewer/party identity out
// of arguments -- only principal.ID flows into DisputeService.Review/
// Resolve as the reviewer, so a caller stuffing an unrelated reviewer_id
// into the request has no effect. (Party-cannot-review-their-own-dispute
// itself is DisputeService.Review's own invariant, already covered
// end-to-end in internal/service/dispute_resolve_test.go; this test's
// job is only to prove the MCP facade cannot be used to bypass it by
// spoofing identity.)
func TestDisputeJobTool_ReviewerIDAlwaysFromAuthenticatedPrincipal(t *testing.T) {
	h := newProviderToolsHarness(t)
	job := h.completedJob(t, "agt_dispute_provider3", "prn_dispute_3")
	d, err := h.disputes.Open(context.Background(), service.OpenDisputeInput{
		PrincipalID: "prn_dispute_3", JobID: job.ID, Reason: "not delivered", IdempotencyKey: "dispute-open-mcp-3",
	})
	if err != nil {
		t.Fatal(err)
	}

	reviewerToken := accessToken(t, h.auth, auth.ScopeDisputesReview)
	reviewerID := mustPrincipal(t, h.auth, reviewerToken)
	resp := callTool(t, h.server(), reviewerToken, "atos_dispute_job", map[string]any{
		"operation": "review", "dispute_id": d.ID,
		"reviewer_id": "agt_dispute_provider3", // spoofed party identity -- must be ignored
	})
	if toolCallFailed(t, resp) {
		t.Fatalf("review: %+v", resp)
	}
	after, err := h.disputes.Get(context.Background(), d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.ReviewerID != reviewerID {
		t.Fatalf("reviewer_id = %q, want the authenticated caller %q (spoofed value must be ignored)", after.ReviewerID, reviewerID)
	}
}

func TestDisputeJobTool_InvalidOperationRejected(t *testing.T) {
	h := newProviderToolsHarness(t)
	token := accessToken(t, h.auth, auth.ScopeDisputesReview)
	resp := callTool(t, h.server(), token, "atos_dispute_job", map[string]any{"operation": "delete_everything", "dispute_id": "dispute_x"})
	if !toolCallFailed(t, resp) {
		t.Fatal("expected an error for an invalid operation")
	}
}

// revokedDeviceToken issues a real device access token carrying scopes,
// then immediately revokes the issuing device -- modeling a principal
// whose authorization was granted and has since been withdrawn (as
// opposed to one who never had it, which the *_HiddenWithoutScope tests
// above already cover). server.go's handleRequest calls s.authenticate
// uniformly for every method before any tool-specific dispatch (see
// server.go's "principal, err := s.authenticate(r)"), so a correctly
// wired tool must be denied at that same protocol layer, never reached.
func revokedDeviceToken(t *testing.T, authorization *auth.Service, scopes ...auth.Scope) (token, principalID string) {
	t.Helper()
	raw := make([]string, len(scopes))
	for i, scope := range scopes {
		raw[i] = string(scope)
	}
	grant, err := authorization.StartDevice("test", "MCP Test", raw)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := authorization.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	if err := authorization.RevokeDevice(pair.Principal.ID, pair.Principal.DeviceID); err != nil {
		t.Fatal(err)
	}
	return pair.AccessToken, pair.Principal.ID
}

// assertDeniedByRevocation asserts resp was rejected at the protocol
// (authentication) layer -- codeUnauthorized in resp.Error -- not by a
// domain-level tool error (isError in the result), proving the request
// never reached the tool handler at all once the issuing device was
// revoked.
func assertDeniedByRevocation(t *testing.T, toolName string, resp rpcResponse) {
	t.Helper()
	if resp.Error == nil {
		t.Fatalf("%s: expected a protocol-level auth error after device revocation, got %+v", toolName, resp)
	}
	if resp.Error.Code != codeUnauthorized {
		t.Fatalf("%s: error code = %d, want codeUnauthorized (%d): %+v", toolName, resp.Error.Code, codeUnauthorized, resp.Error)
	}
}

func TestProviderJobsTool_DeniedAfterDeviceRevoked(t *testing.T) {
	h := newProviderToolsHarness(t)
	token, providerID := revokedDeviceToken(t, h.auth, auth.ScopeProviderJobsRead)
	h.completedJob(t, providerID, "prn_revoked_1")

	resp := callTool(t, h.server(), token, "atos_provider_jobs", map[string]any{})
	assertDeniedByRevocation(t, "atos_provider_jobs", resp)
}

func TestDeliverJobTool_DeniedAfterDeviceRevoked(t *testing.T) {
	h := newProviderToolsHarness(t)
	token, providerID := revokedDeviceToken(t, h.auth, auth.ScopeProviderJobsDeliver)
	job := h.completedJob(t, providerID, "prn_revoked_2")

	resp := callTool(t, h.server(), token, "atos_deliver_job", map[string]any{
		"job_id": job.ID, "output": map[string]any{}, "idempotency_key": "deliver-revoked-2",
	})
	assertDeniedByRevocation(t, "atos_deliver_job", resp)
}

func TestRequestSettlementTool_DeniedAfterDeviceRevoked(t *testing.T) {
	h := newProviderToolsHarness(t)
	token, providerID := revokedDeviceToken(t, h.auth, auth.ScopeSettlementWrite)
	job := h.completedJob(t, providerID, "prn_revoked_3")

	resp := callTool(t, h.server(), token, "atos_request_settlement", map[string]any{"job_id": job.ID})
	assertDeniedByRevocation(t, "atos_request_settlement", resp)
}

func TestDisputeJobTool_DeniedAfterDeviceRevoked(t *testing.T) {
	h := newProviderToolsHarness(t)
	job := h.completedJob(t, "agt_dispute_revoked", "prn_revoked_4")
	d, err := h.disputes.Open(context.Background(), service.OpenDisputeInput{
		PrincipalID: "prn_revoked_4", JobID: job.ID, Reason: "not delivered", IdempotencyKey: "dispute-open-revoked-4",
	})
	if err != nil {
		t.Fatal(err)
	}
	token, _ := revokedDeviceToken(t, h.auth, auth.ScopeDisputesReview)

	resp := callTool(t, h.server(), token, "atos_dispute_job", map[string]any{"operation": "review", "dispute_id": d.ID})
	assertDeniedByRevocation(t, "atos_dispute_job", resp)
}
