package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	"github.com/tosnetwork/atos/internal/adapters/tosprotocol"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/postgres"
	atostosv1 "github.com/tosnetwork/tos-protocol/gen/atos/tos/v1"
	"github.com/tosnetwork/tos-protocol/pkg/atosrpc"
	"github.com/tosnetwork/tos-protocol/pkg/chain"
	"github.com/tosnetwork/tos-protocol/pkg/economic"
)

type quoteAcceptanceAuthority struct {
	mu   sync.Mutex
	refs map[string]string
}

func (a *quoteAcceptanceAuthority) Network() string { return "tos-test" }
func (a *quoteAcceptanceAuthority) Supports(mode atosrpc.TrustMode) bool {
	return mode == atosrpc.TrustModeManaged || mode == atosrpc.TrustModeVerified
}
func (a *quoteAcceptanceAuthority) CheckReady(context.Context) error { return nil }
func (a *quoteAcceptanceAuthority) Close() error                     { return nil }
func (a *quoteAcceptanceAuthority) Commit(_ context.Context, kind, id, digest string) (atosrpc.NetworkReference, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := kind + "\x00" + id + "\x00" + digest
	if reference, ok := a.refs[key]; ok {
		return atosrpc.NetworkReference{Network: a.Network(), Reference: reference, Finalized: true, FinalizedCheckpoint: 42}, nil
	}
	reference := "tos:test:" + kind + ":" + id + ":" + digest
	a.refs[key] = reference
	return atosrpc.NetworkReference{Network: a.Network(), Reference: reference, Finalized: true, FinalizedCheckpoint: 42}, nil
}
func (a *quoteAcceptanceAuthority) ResolveCommitment(_ context.Context, kind, id, digest string, expected *atosrpc.NetworkReference) (*atosrpc.NetworkReference, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	reference, ok := a.refs[kind+"\x00"+id+"\x00"+digest]
	if !ok {
		return nil, atosrpc.ErrCommitmentNotFound
	}
	if expected != nil && (expected.Network != a.Network() || expected.Reference != reference) {
		return nil, errors.New("canonical commitment mismatch")
	}
	return &atosrpc.NetworkReference{Network: a.Network(), Reference: reference, Finalized: true, FinalizedCheckpoint: 42}, nil
}

type quoteAcceptanceEconomy struct {
	mu     sync.Mutex
	result economic.Result
	found  bool
}

func (e *quoteAcceptanceEconomy) ResolveEscrow(context.Context, economic.ReserveEscrowRequest) (economic.Result, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.result, e.found, nil
}

type quoteAcceptanceQuoter struct{}

func (quoteAcceptanceQuoter) QuoteExecution(_ context.Context, req tosai.QuoteExecutionRequest) (tosai.ServiceExecutionQuote, error) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return tosai.ServiceExecutionQuote{ID: "service-quote-phase4b1", Reference: "service:quote:phase4b1", ExpiresAt: now.Add(5 * time.Minute), ExecutionDeadline: req.ExecutionDeadline.UTC().Truncate(time.Millisecond)}, nil
}

func (*quoteAcceptanceEconomy) Network() string { return "tos-test" }
func (*quoteAcceptanceEconomy) Supports(m economic.TrustMode) bool {
	return m == economic.TrustModeVerified
}
func (*quoteAcceptanceEconomy) CheckReady(context.Context) error { return nil }
func (*quoteAcceptanceEconomy) Close() error                     { return nil }
func (e *quoteAcceptanceEconomy) ReserveEscrow(_ context.Context, req economic.ReserveEscrowRequest) (economic.Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	state := chain.TaskEscrowState{Network: "tos-test", ContractAddress: "0:" + strings.Repeat("44", 32), Creator: req.Creator, Agent: req.Agent, HasAgent: true, BudgetNanoTOS: req.BudgetNanoTOS, BalanceNanoTOS: req.BudgetNanoTOS, DeadlineUnix: req.DeadlineUnix, Status: chain.TaskEscrowStatusOpen, PolicyHash: req.PolicyHash, PermissionHash: req.PermissionHash, CodeHash: "tvm-cell-sha256:" + strings.Repeat("cc", 32), ObservedMasterSeqno: 77, ObservedAt: time.Now().UTC()}
	e.result = economic.Result{State: state, ContractReference: state.ContractAddress, TransitionReference: "tos:tx:reserve", ActionID: "task-action-reserve"}
	e.found = true
	return e.result, nil
}
func (*quoteAcceptanceEconomy) AcceptEscrow(context.Context, economic.AcceptEscrowRequest) (economic.Result, error) {
	return economic.Result{}, nil
}
func (*quoteAcceptanceEconomy) CommitResult(context.Context, economic.CommitResultRequest) (economic.Result, error) {
	return economic.Result{}, nil
}
func (e *quoteAcceptanceEconomy) ReleaseEscrow(_ context.Context, req economic.ReleaseEscrowRequest) (economic.Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.result.State.Status = chain.TaskEscrowStatusCancelled
	e.result.State.BudgetNanoTOS = 0
	e.result.State.BalanceNanoTOS = 0
	e.result.State.ObservedMasterSeqno = 78
	e.result.TransitionReference = "tos:tx:release"
	e.result.ActionID = "task-action-release"
	e.result.CreatorPaidNanoTOS = req.BudgetNanoTOS
	return e.result, nil
}
func (*quoteAcceptanceEconomy) SettleProvider(context.Context, economic.SettleProviderRequest) (economic.Result, error) {
	return economic.Result{}, nil
}
func (*quoteAcceptanceEconomy) RefundPrincipal(context.Context, economic.RefundPrincipalRequest) (economic.Result, error) {
	return economic.Result{}, nil
}
func (*quoteAcceptanceEconomy) OpenDispute(context.Context, economic.OpenDisputeRequest) (economic.Result, error) {
	return economic.Result{}, nil
}
func (*quoteAcceptanceEconomy) ResolveDispute(context.Context, economic.ResolveDisputeRequest) (economic.Result, error) {
	return economic.Result{}, nil
}
func (e *quoteAcceptanceEconomy) ReadEconomicState(context.Context, string) (chain.TaskEscrowState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.result.State, nil
}

type failAfterRealEscrowProvider struct {
	tosai.Provider
	release <-chan struct{}
}

func (f failAfterRealEscrowProvider) SubmitJob(context.Context, tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	<-f.release
	return tosai.SubmitJobResult{State: domain.JobFailed}, nil
}

// TestVerifiedQuoteRealHTTPPostgresProtocol is the Phase 4B-1 composition
// acceptance path: real public HTTP, real PostgreSQL operation/projection,
// real ConnectRPC transport and a real tos-protocol Server/TrustService.
func TestVerifiedQuoteRealHTTPPostgresProtocol(t *testing.T) {
	databaseURL := os.Getenv("ATOS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL not set; skipping real Phase 4B-1 integration test")
	}
	ctx := context.Background()
	st, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	authority := &quoteAcceptanceAuthority{refs: make(map[string]string)}
	economy := new(quoteAcceptanceEconomy)
	protocolServer, err := atosrpc.Open(atosrpc.Config{StatePath: filepath.Join(t.TempDir(), "tos-protocol.db"), BearerToken: "phase4b1-secret", Authority: authority, EconomicDriver: economy, TrustDomain: "atos.im"})
	if err != nil {
		t.Fatal(err)
	}
	defer protocolServer.Close()
	protocolHTTP := httptest.NewServer(protocolServer.Handler())
	defer protocolHTTP.Close()
	client, err := toprotocol.New(toprotocol.Config{BaseURL: protocolHTTP.URL, BearerToken: "phase4b1-secret", Insecure: true, Timeout: 10 * time.Second, Store: st, Network: "tos-test", TrustDomain: "atos.im"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	authorization, err := auth.Open(auth.Config{AutoApprove: true})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := authorization.StartDevice("test", "Phase 4B-2", []string{string(auth.ScopeQuotesRead), string(auth.ScopeJobsCreate), string(auth.ScopeJobsRead)})
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := authorization.ExchangeDevice(grant.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	providerID := "provider_phase4b1_" + strings.ReplaceAll(time.Now().UTC().Format("150405.000000000"), ".", "")
	requesterAgent := "agent_" + grant.PrincipalID
	// Capability activation in tos-protocol resolves the provider's TOS Agent
	// Identity by provider_id, so the provider binding deliberately uses that
	// same canonical Agent ID.
	providerAgent := providerID
	for i, id := range []string{requesterAgent, providerAgent} {
		if err := protocolServer.SeedIdentity(&atostosv1.AgentIdentity{AgentId: id, CanonicalUri: "atos://" + id, Assurance: "tos_verified", Controllers: []string{"0:" + strings.Repeat(string(rune('a'+i)), 64)}, IdentityRef: &atosrpc.NetworkReference{Network: "tos-test", Reference: "identity:" + id, Finalized: true, FinalizedCheckpoint: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := client.CreatePrincipalBinding(ctx, "gateway", "bind-requester-"+grant.PrincipalID, grant.PrincipalID, requesterAgent); err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.CreatePrincipalBinding(ctx, "gateway", "bind-provider-"+providerID, providerID, providerAgent); err != nil {
		t.Fatal(err)
	}

	capabilities := service.NewCapabilityService(st).WithManifestAnchor(client)
	cap, err := capabilities.Register(ctx, service.RegisterCapabilityInput{ProviderID: providerID, Name: "Verified Quote Real Acceptance", Description: "phase4b1", DeliveryMode: domain.DeliveryInstant, InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"}, Pricing: domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.000000000", Currency: "TOS"}}, RequestedTrustModes: []domain.TrustMode{domain.TrustModeVerified}, IdempotencyKey: "register-" + providerID})
	if err != nil {
		t.Fatal(err)
	}
	cap, err = st.UpdateCapability(ctx, cap.ID, func(current domain.Capability, _ bool) (domain.Capability, error) {
		current.Status = domain.CapabilityActive
		current.ModeSupport[domain.TrustModeVerified] = domain.ModeSupportEntry{Status: domain.ModeSupportActive, ProofProfile: domain.ProofProfileTOSVerifiedV1}
		current.SupportedTrustModes = []domain.TrustMode{domain.TrustModeVerified}
		return current, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	signers := service.NewExecutionSignerService(st, client, capabilities)
	_, err = signers.Authorize(ctx, service.AuthorizeSignerInput{ProviderID: providerID, CapabilityID: cap.ID, ExecutionSignerID: "signer-phase4b1", SignerPublicKey: []byte("01234567890123456789012345678901"), SignatureAlgorithm: "ed25519", ValidFrom: time.Now().UTC().Add(-time.Minute), ValidUntil: time.Now().UTC().Add(time.Hour), ValidFromExplicit: true, ValidUntilExplicit: true, IdempotencyKey: "authorize-" + providerID})
	if err != nil {
		t.Fatal(err)
	}
	quotes := service.NewQuoteService(st, quoteAcceptanceQuoter{}).WithVerifiedCommitmentAuthority(client, signers, "atos.im")
	providerRelease := make(chan struct{})
	defer close(providerRelease)
	jobs := service.NewJobService(st, failAfterRealEscrowProvider{Provider: tosaimock.NewContractFixture(), release: providerRelease}, client, service.NewAccountService(st))
	api := httptest.NewServer((&Server{Auth: authorization, Capabilities: capabilities, Quotes: quotes, Jobs: jobs}).Mux())
	defer api.Close()
	body, _ := json.Marshal(map[string]any{"capability_id": cap.ID, "requested_trust_mode": "verified"})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, api.URL+"/v1/quotes", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "real-verified-quote-"+providerID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		failure, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /v1/quotes status=%d body=%s", resp.StatusCode, failure)
	}
	var public domain.PublicQuote
	if err := json.NewDecoder(resp.Body).Decode(&public); err != nil {
		t.Fatal(err)
	}
	if public.TrustMode != domain.TrustModeVerified || public.Commitment == nil || !public.Commitment.Finalized || public.Commitment.FinalizedCheckpoint == 0 {
		t.Fatalf("unusable Verified Quote returned: %+v", public)
	}
	stored, err := quotes.Get(ctx, public.ID)
	if err != nil {
		t.Fatal(err)
	}
	commitment, found, err := client.GetQuoteCommitment(ctx, stored)
	if err != nil || !found || commitment.Digest != public.Commitment.Digest {
		t.Fatalf("live canonical recheck found=%v commitment=%+v err=%v", found, commitment, err)
	}
	jobBody, _ := json.Marshal(map[string]any{"capability_id": cap.ID, "quote_id": public.ID, "input": map[string]any{"task": "reserve"}, "idempotency_key": "real-phase4b2-job-" + providerID})
	jobReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, api.URL+"/v1/jobs", bytes.NewReader(jobBody))
	jobReq.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	jobReq.Header.Set("Content-Type", "application/json")
	jobResp, err := http.DefaultClient.Do(jobReq)
	if err != nil {
		t.Fatal(err)
	}
	defer jobResp.Body.Close()
	if jobResp.StatusCode != http.StatusAccepted && jobResp.StatusCode != http.StatusOK {
		failure, _ := io.ReadAll(jobResp.Body)
		t.Fatalf("POST /v1/jobs status=%d body=%s", jobResp.StatusCode, failure)
	}
	var submitted submitResponse
	if err = json.NewDecoder(jobResp.Body).Decode(&submitted); err != nil {
		t.Fatal(err)
	}
	if submitted.Job.EscrowID == "" {
		t.Fatalf("verified Job executed without reserved escrow: %+v", submitted.Job)
	}
	durableJob, err := st.GetJob(ctx, submitted.Job.ID)
	if err != nil || durableJob.EconomicState != domain.EconomicEscrowReserved {
		t.Fatalf("durable verified Job checkpoint=%+v err=%v", durableJob, err)
	}
	operation, err := st.GetEscrowOperation(ctx, submitted.Job.ID, domain.EscrowOperationReserve)
	if err != nil || operation.Checkpoint != domain.EscrowOperationCompleted || !operation.Escrow.Finalized {
		t.Fatalf("durable escrow operation=%+v err=%v", operation, err)
	}
}
