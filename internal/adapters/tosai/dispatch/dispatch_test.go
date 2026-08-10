package dispatch

import (
	"context"
	"errors"
	"testing"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/domain"
)

// stubNative is a minimal tosai.Provider test double that records calls.
type stubNative struct {
	submitted []tosai.SubmitJobRequest
	submitRes tosai.SubmitJobResult
	submitErr error
	getRes    tosai.SubmitJobResult
	getErr    error
	cancelErr error
	canceled  []string
}

func (s *stubNative) RegisterProvider(ctx context.Context, providerID string, capability domain.Capability) error {
	return nil
}
func (s *stubNative) GetProviderStatus(ctx context.Context, providerID string) (bool, error) {
	return true, nil
}
func (s *stubNative) SubmitJob(ctx context.Context, req tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	s.submitted = append(s.submitted, req)
	return s.submitRes, s.submitErr
}
func (s *stubNative) GetJob(ctx context.Context, jobID string) (tosai.SubmitJobResult, error) {
	return s.getRes, s.getErr
}
func (s *stubNative) CancelJob(ctx context.Context, jobID, reason string) error {
	s.canceled = append(s.canceled, jobID)
	return s.cancelErr
}
func (s *stubNative) FetchResult(ctx context.Context, jobID string) (map[string]any, error) {
	return s.getRes.Output, s.getErr
}
func (s *stubNative) FetchReceipt(ctx context.Context, jobID string) (*domain.ExecutionReceipt, error) {
	return s.getRes.Receipt, s.getErr
}

// stubAdapter is a provideradapter.ProviderAdapter test double.
type stubAdapter struct {
	transport   domain.EndpointAdapterType
	invokeCalls int
	queryCalls  int
	invokeRes   provideradapter.InvokeResult
	invokeErr   error
	queryRes    provideradapter.InvokeResult
	queryFound  bool
	queryErr    error
	cancelErr   error
}

func (a *stubAdapter) Transport() domain.EndpointAdapterType { return a.transport }
func (a *stubAdapter) Invoke(ctx context.Context, req provideradapter.InvokeRequest) (provideradapter.InvokeResult, error) {
	a.invokeCalls++
	return a.invokeRes, a.invokeErr
}
func (a *stubAdapter) Query(ctx context.Context, endpointRef, idempotencyKey string) (provideradapter.InvokeResult, bool, error) {
	a.queryCalls++
	return a.queryRes, a.queryFound, a.queryErr
}
func (a *stubAdapter) Cancel(ctx context.Context, endpointRef, idempotencyKey, reason string) error {
	return a.cancelErr
}
func (a *stubAdapter) Health(ctx context.Context, endpointRef string) domain.AdapterHealthCheck {
	return domain.AdapterHealthCheck{Transport: a.transport, Status: domain.AdapterHealthHealthy}
}

func nativeBindingReq() tosai.SubmitJobRequest {
	return tosai.SubmitJobRequest{JobID: "job_1", CapabilityID: "cap_1", TrustMode: domain.TrustModeManaged}
}

func httpBindingReq() tosai.SubmitJobRequest {
	return tosai.SubmitJobRequest{
		JobID: "job_1", CapabilityID: "cap_1", CapabilityVersion: "1.0.0", ProviderID: "prov_1",
		QuoteID: "q_1", EscrowID: "esc_1", PrincipalID: "prn_1", TrustMode: domain.TrustModeManaged,
		Input:   map[string]any{"x": 1},
		Binding: &domain.CapabilityBinding{Transport: domain.AdapterHTTP, EndpointRef: "https://provider.example.com/invoke", EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged}},
	}
}

func TestSubmitJob_NoBindingsDelegatesToNative(t *testing.T) {
	native := &stubNative{submitRes: tosai.SubmitJobResult{State: domain.JobCompleted}}
	p := New(native, provideradapter.NewResolver())

	_, err := p.SubmitJob(context.Background(), nativeBindingReq())
	if err != nil {
		t.Fatal(err)
	}
	if len(native.submitted) != 1 {
		t.Fatalf("native.SubmitJob called %d times, want 1", len(native.submitted))
	}
}

func TestSubmitJob_TOSNativeBindingDelegatesToNative(t *testing.T) {
	native := &stubNative{submitRes: tosai.SubmitJobResult{State: domain.JobCompleted}}
	p := New(native, provideradapter.NewResolver())
	req := nativeBindingReq()
	req.Binding = &domain.CapabilityBinding{Transport: domain.AdapterTOSNative, EndpointRef: "internal:mock"}

	if _, err := p.SubmitJob(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(native.submitted) != 1 {
		t.Fatalf("native.SubmitJob called %d times, want 1", len(native.submitted))
	}
}

func TestSubmitJob_HTTPBindingRoutesToAdapterAndSynthesizesReceipt(t *testing.T) {
	adapter := &stubAdapter{
		transport: domain.AdapterHTTP,
		invokeRes: provideradapter.InvokeResult{Status: provideradapter.InvokeCompleted, Output: map[string]any{"ok": true}},
	}
	native := &stubNative{}
	p := New(native, provideradapter.NewResolver(adapter))

	result, err := p.SubmitJob(context.Background(), httpBindingReq())
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if len(native.submitted) != 0 {
		t.Fatal("native.SubmitJob must not be called for an http-bound capability")
	}
	if adapter.invokeCalls != 1 {
		t.Fatalf("adapter.Invoke called %d times, want 1", adapter.invokeCalls)
	}
	if result.State != domain.JobCompleted {
		t.Fatalf("state = %s, want completed", result.State)
	}
	if result.Receipt == nil {
		t.Fatal("expected a synthesized ExecutionReceipt -- settlement requires one (economic_recovery.go rejects a nil Receipt)")
	}
	if result.Receipt.JobID != "job_1" || result.Receipt.ProviderID != "prov_1" || result.Receipt.QuoteID != "q_1" || result.Receipt.EscrowID != "esc_1" {
		t.Fatalf("receipt identity fields wrong: %+v", result.Receipt)
	}
	if result.Receipt.Result != domain.ExecutionSuccess {
		t.Fatalf("receipt result = %s, want success", result.Receipt.Result)
	}
	if result.Receipt.InputHash == "" || result.Receipt.OutputHash == "" || result.Receipt.Signature == "" {
		t.Fatalf("receipt missing commitment/signature fields: %+v", result.Receipt)
	}
}

// TestSubmitJob_RemoteThirdPartyExecutionRoutesThroughNative proves
// WithRemoteThirdPartyExecution's contract: an http/mcp/a2a-bound Job goes
// straight to native.SubmitJob (carrying req.Binding unchanged for
// tosprotocol.Client to thread onto the wire), and the local
// provideradapter is never touched at all -- not even Queried.
func TestSubmitJob_RemoteThirdPartyExecutionRoutesThroughNative(t *testing.T) {
	adapter := &stubAdapter{transport: domain.AdapterHTTP}
	native := &stubNative{submitRes: tosai.SubmitJobResult{State: domain.JobCompleted, Output: map[string]any{"ok": true}}}
	p := New(native, provideradapter.NewResolver(adapter), WithRemoteThirdPartyExecution(true))

	req := httpBindingReq()
	result, err := p.SubmitJob(context.Background(), req)
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if len(native.submitted) != 1 {
		t.Fatalf("native.SubmitJob called %d times, want exactly 1", len(native.submitted))
	}
	if native.submitted[0].Binding != req.Binding {
		t.Fatal("native.SubmitJob must receive the exact same Binding, never a re-resolved one")
	}
	if adapter.invokeCalls != 0 || adapter.queryCalls != 0 {
		t.Fatalf("local adapter was touched (invoke=%d query=%d) despite remote third-party execution being enabled",
			adapter.invokeCalls, adapter.queryCalls)
	}
	if result.State != domain.JobCompleted {
		t.Fatalf("state = %s, want completed", result.State)
	}

	// GetJob/CancelJob/FetchResult/FetchReceipt must also fall through to
	// native -- SubmitJob never populated a local route to recall, exactly
	// as when native handles a tos-native Job.
	native.getRes = tosai.SubmitJobResult{State: domain.JobCompleted, Output: map[string]any{"ok": true}}
	if _, err := p.GetJob(context.Background(), req.JobID); err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if err := p.CancelJob(context.Background(), req.JobID, "test"); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if len(native.canceled) != 1 || native.canceled[0] != req.JobID {
		t.Fatalf("native.CancelJob was not called for a remotely-dispatched job: %+v", native.canceled)
	}
}

func TestSubmitJob_QueriesBeforeInvoking(t *testing.T) {
	adapter := &stubAdapter{
		transport:  domain.AdapterHTTP,
		queryFound: true,
		queryRes:   provideradapter.InvokeResult{Status: provideradapter.InvokeCompleted, Output: map[string]any{"recovered": true}},
	}
	native := &stubNative{}
	p := New(native, provideradapter.NewResolver(adapter))

	result, err := p.SubmitJob(context.Background(), httpBindingReq())
	if err != nil {
		t.Fatal(err)
	}
	if adapter.invokeCalls != 0 {
		t.Fatalf("Invoke called %d times, want 0 -- Query found a prior result, must not re-invoke", adapter.invokeCalls)
	}
	if result.Output["recovered"] != true {
		t.Fatalf("output = %+v, want the Query-recovered result", result.Output)
	}
}

func TestSubmitJob_NoAdapterForTransportErrors(t *testing.T) {
	native := &stubNative{}
	p := New(native, provideradapter.NewResolver()) // no adapters registered
	_, err := p.SubmitJob(context.Background(), httpBindingReq())
	if err == nil {
		t.Fatal("expected an error when no adapter is registered for the binding's transport")
	}
}

func TestSubmitJob_PendingStatusReturnsWorking(t *testing.T) {
	adapter := &stubAdapter{transport: domain.AdapterHTTP, invokeRes: provideradapter.InvokeResult{Status: provideradapter.InvokePending}}
	p := New(&stubNative{}, provideradapter.NewResolver(adapter))

	result, err := p.SubmitJob(context.Background(), httpBindingReq())
	if err != nil {
		t.Fatal(err)
	}
	if result.State != domain.JobWorking {
		t.Fatalf("state = %s, want working", result.State)
	}
	if result.Receipt != nil {
		t.Fatal("a pending result must not carry a receipt")
	}
}

func TestSubmitJob_FailedStatusReturnsFailed(t *testing.T) {
	adapter := &stubAdapter{transport: domain.AdapterHTTP, invokeRes: provideradapter.InvokeResult{Status: provideradapter.InvokeFailed, FailureReason: "bad input"}}
	p := New(&stubNative{}, provideradapter.NewResolver(adapter))

	result, err := p.SubmitJob(context.Background(), httpBindingReq())
	if err != nil {
		t.Fatal(err)
	}
	if result.State != domain.JobFailed {
		t.Fatalf("state = %s, want failed", result.State)
	}
}

func TestGetJob_UnknownRouteDelegatesToNative(t *testing.T) {
	native := &stubNative{getErr: domain.NewError(domain.ErrNotFound, "unknown", false)}
	p := New(native, provideradapter.NewResolver())
	_, err := p.GetJob(context.Background(), "job_never_submitted")
	if err == nil {
		t.Fatal("expected native's not-found error to surface")
	}
}

func TestGetJob_RemembersRouteFromSubmitJob(t *testing.T) {
	adapter := &stubAdapter{
		transport: domain.AdapterHTTP,
		invokeRes: provideradapter.InvokeResult{Status: provideradapter.InvokePending},
	}
	p := New(&stubNative{}, provideradapter.NewResolver(adapter))
	if _, err := p.SubmitJob(context.Background(), httpBindingReq()); err != nil {
		t.Fatal(err)
	}

	adapter.queryFound = true
	adapter.queryRes = provideradapter.InvokeResult{Status: provideradapter.InvokeCompleted, Output: map[string]any{"done": true}}
	result, err := p.GetJob(context.Background(), "job_1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if result.State != domain.JobCompleted {
		t.Fatalf("state = %s, want completed", result.State)
	}
	if adapter.queryCalls == 0 {
		t.Fatal("expected GetJob to Query the remembered route")
	}
}

func TestGetJob_NotFoundAtProviderReturnsDomainNotFound(t *testing.T) {
	adapter := &stubAdapter{transport: domain.AdapterHTTP, invokeRes: provideradapter.InvokeResult{Status: provideradapter.InvokePending}}
	p := New(&stubNative{}, provideradapter.NewResolver(adapter))
	if _, err := p.SubmitJob(context.Background(), httpBindingReq()); err != nil {
		t.Fatal(err)
	}
	adapter.queryFound = false

	_, err := p.GetJob(context.Background(), "job_1")
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.ErrNotFound {
		t.Fatalf("got %v, want domain.ErrNotFound", err)
	}
}

func TestCancelJob_UnsupportedIsBestEffortNoop(t *testing.T) {
	adapter := &stubAdapter{transport: domain.AdapterHTTP, invokeRes: provideradapter.InvokeResult{Status: provideradapter.InvokePending}, cancelErr: provideradapter.ErrCancelUnsupported}
	p := New(&stubNative{}, provideradapter.NewResolver(adapter))
	if _, err := p.SubmitJob(context.Background(), httpBindingReq()); err != nil {
		t.Fatal(err)
	}
	if err := p.CancelJob(context.Background(), "job_1", "no longer needed"); err != nil {
		t.Fatalf("CancelJob: %v, want nil (best-effort no-op)", err)
	}
}

func TestCancelJob_RealErrorSurfaces(t *testing.T) {
	adapter := &stubAdapter{transport: domain.AdapterHTTP, invokeRes: provideradapter.InvokeResult{Status: provideradapter.InvokePending}, cancelErr: errors.New("boom")}
	p := New(&stubNative{}, provideradapter.NewResolver(adapter))
	if _, err := p.SubmitJob(context.Background(), httpBindingReq()); err != nil {
		t.Fatal(err)
	}
	if err := p.CancelJob(context.Background(), "job_1", "no longer needed"); err == nil {
		t.Fatal("expected the real cancel error to surface")
	}
}

func TestFetchResult_And_FetchReceipt_RouteThroughGetJob(t *testing.T) {
	adapter := &stubAdapter{
		transport: domain.AdapterHTTP,
		invokeRes: provideradapter.InvokeResult{Status: provideradapter.InvokeCompleted, Output: map[string]any{"ok": true}},
	}
	p := New(&stubNative{}, provideradapter.NewResolver(adapter))
	if _, err := p.SubmitJob(context.Background(), httpBindingReq()); err != nil {
		t.Fatal(err)
	}
	adapter.queryFound = true
	adapter.queryRes = provideradapter.InvokeResult{Status: provideradapter.InvokeCompleted, Output: map[string]any{"ok": true}}

	output, err := p.FetchResult(context.Background(), "job_1")
	if err != nil || output["ok"] != true {
		t.Fatalf("FetchResult: output=%+v err=%v", output, err)
	}
	receipt, err := p.FetchReceipt(context.Background(), "job_1")
	if err != nil || receipt == nil {
		t.Fatalf("FetchReceipt: receipt=%v err=%v", receipt, err)
	}
}

// Binding selection itself (domain.SelectBinding) is now tested directly
// in internal/domain, since it moved there to be shared with
// JobService.submit's Job-creation-time freeze -- see
// domain.TestSelectBinding_* for that coverage. Nothing in this package
// selects a binding anymore; SubmitJob only ever dispatches the Job's own
// already-frozen req.Binding.
func TestSubmitJob_NilBindingDelegatesToNative(t *testing.T) {
	native := &stubNative{submitRes: tosai.SubmitJobResult{State: domain.JobCompleted}}
	p := New(native, provideradapter.NewResolver())
	req := nativeBindingReq()
	req.Binding = nil

	if _, err := p.SubmitJob(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(native.submitted) != 1 {
		t.Fatalf("native.SubmitJob called %d times, want 1", len(native.submitted))
	}
}

// stubStreamer is a stubNative that also implements tosai.Streamer, so
// dispatch.Provider's own StreamJobEvents type assertion succeeds.
type stubStreamer struct {
	stubNative
	streamCalls []string
	streamErr   error
}

func (s *stubStreamer) StreamJobEvents(ctx context.Context, req tosai.StreamJobEventsRequest, onEvent func(domain.JobEvent) error) error {
	s.streamCalls = append(s.streamCalls, req.JobID)
	return s.streamErr
}

func TestProvider_ImplementsStreamerWhenNativeDoes(t *testing.T) {
	var _ tosai.Streamer = (*Provider)(nil)
}

func TestStreamJobEvents_DelegatesToNativeForNativeJob(t *testing.T) {
	native := &stubStreamer{}
	p := New(native, provideradapter.NewResolver())
	if err := p.StreamJobEvents(context.Background(), tosai.StreamJobEventsRequest{JobID: "job_native"}, nil); err != nil {
		t.Fatalf("StreamJobEvents: %v", err)
	}
	if len(native.streamCalls) != 1 || native.streamCalls[0] != "job_native" {
		t.Fatalf("native stream calls = %v", native.streamCalls)
	}
}

func TestStreamJobEvents_ErrorsForThirdPartyJob(t *testing.T) {
	adapter := &stubAdapter{transport: domain.AdapterHTTP, invokeRes: provideradapter.InvokeResult{Status: provideradapter.InvokeCompleted}}
	native := &stubStreamer{}
	p := New(native, provideradapter.NewResolver(adapter))
	if _, err := p.SubmitJob(context.Background(), httpBindingReq()); err != nil {
		t.Fatal(err)
	}

	err := p.StreamJobEvents(context.Background(), tosai.StreamJobEventsRequest{JobID: "job_1"}, nil)
	if err == nil {
		t.Fatal("expected an explicit error for streaming a third-party-routed job, not silent success")
	}
	if len(native.streamCalls) != 0 {
		t.Fatal("native.StreamJobEvents must not be called for a third-party-routed job")
	}
}
