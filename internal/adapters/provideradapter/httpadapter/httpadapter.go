// Package httpadapter implements provideradapter.ProviderAdapter over a
// production-safe outbound HTTP client for third-party Capability bindings.
//
// Wire contract (ATOS-defined, since no external convention exists yet):
// Invoke issues `POST {endpoint_ref}` with header `Idempotency-Key` and a
// JSON body `{job_id, capability_id, capability_version, idempotency_key,
// input, deadline}`; a 2xx response body is decoded as
// `{status: "completed"|"failed"|"pending", output, usage, failure_reason}`.
// Query issues `GET {endpoint_ref}?idempotency_key=...` with the same
// header, expecting the identical response shape; a 404 is treated as "no
// record", not an error. This contract is intentionally minimal and is the
// first concrete implementation of the `http` CapabilityBinding transport --
// see the Capability manifest-commitment tests for why a provider cannot
// silently change this contract without bumping its Capability version.
package httpadapter

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/domain"
)

const (
	defaultTimeout          = 30 * time.Second
	defaultMaxResponseBytes = 1 << 20 // 1 MiB
	defaultHealthTimeout    = 5 * time.Second
)

// Config configures an Adapter. Client, if set, overrides the constructed
// *http.Client entirely -- tests use this to point at an httptest server
// and to disable the outbound network policy where a test deliberately
// needs to reach a loopback address without setting the escape-hatch env
// var globally for the whole process.
type Config struct {
	Timeout          time.Duration
	MaxResponseBytes int64
	Client           *http.Client
}

// Adapter is provideradapter.ProviderAdapter's HTTP implementation.
type Adapter struct {
	client           *http.Client
	maxResponseBytes int64
}

// New builds an Adapter with the production-safe default outbound policy
// (see policy.go): SSRF-resistant DialContext, no automatic redirect
// following (a 3xx is treated as the final response, never silently
// chased), and bounded response bodies.
func New(cfg Config) *Adapter {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	maxBytes := cfg.MaxResponseBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxResponseBytes
	}
	client := cfg.Client
	if client == nil {
		baseDialer := &net.Dialer{Timeout: 10 * time.Second}
		transport := &http.Transport{
			DialContext:           policyDialContext(baseDialer.DialContext),
			TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
			MaxIdleConns:          64,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			// ForceAttemptHTTP2 is Go's default (true); left implicit.
		}
		client = &http.Client{Timeout: timeout, Transport: transport}
	}
	// Do not silently follow redirects -- a 3xx becomes the final observed
	// response instead of being chased to a caller-uncontrolled
	// destination. This is a security policy, not a transport detail, so
	// it is enforced unconditionally -- even when a caller supplies their
	// own *http.Client (e.g. tests pointing Transport at an httptest
	// server) -- rather than being silently bypassable via that injection
	// point.
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Adapter{client: client, maxResponseBytes: maxBytes}
}

func (a *Adapter) Transport() domain.EndpointAdapterType { return domain.AdapterHTTP }

type wireRequest struct {
	JobID             string         `json:"job_id"`
	CapabilityID      string         `json:"capability_id"`
	CapabilityVersion string         `json:"capability_version"`
	IdempotencyKey    string         `json:"idempotency_key"`
	Input             map[string]any `json:"input"`
	Deadline          *time.Time     `json:"deadline,omitempty"`
}

type wireResponse struct {
	Status        string         `json:"status"`
	Output        map[string]any `json:"output"`
	Usage         domain.Usage   `json:"usage"`
	FailureReason string         `json:"failure_reason"`
}

func (a *Adapter) Invoke(ctx context.Context, req provideradapter.InvokeRequest) (provideradapter.InvokeResult, error) {
	if req.EndpointRef == "" {
		return provideradapter.InvokeResult{}, errors.New("httpadapter: endpoint_ref is required")
	}
	if req.IdempotencyKey == "" {
		return provideradapter.InvokeResult{}, errors.New("httpadapter: idempotency_key is required")
	}
	body := wireRequest{
		JobID: req.JobID, CapabilityID: req.CapabilityID, CapabilityVersion: req.CapabilityVersion,
		IdempotencyKey: req.IdempotencyKey, Input: req.Input,
	}
	if !req.Deadline.IsZero() {
		d := req.Deadline
		body.Deadline = &d
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return provideradapter.InvokeResult{}, fmt.Errorf("httpadapter: encode request: %w", err)
	}

	callCtx := ctx
	var cancel context.CancelFunc
	if !req.Deadline.IsZero() {
		callCtx, cancel = context.WithDeadline(ctx, req.Deadline)
		defer cancel()
	}

	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, req.EndpointRef, bytes.NewReader(encoded))
	if err != nil {
		return provideradapter.InvokeResult{}, fmt.Errorf("httpadapter: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", req.IdempotencyKey)

	resp, err := a.client.Do(httpReq)
	if err != nil {
		// Connection failure, timeout, or the response arrived after the
		// bound deadline was already exceeded -- nothing here proves the
		// provider ever received or acted on the request, so this is a
		// safe-to-retry failed attempt, never a fabricated Completed or
		// Failed outcome.
		return provideradapter.InvokeResult{}, fmt.Errorf("httpadapter: request failed: %w", err)
	}
	return a.decodeResult(resp)
}

func (a *Adapter) decodeResult(resp *http.Response) (provideradapter.InvokeResult, error) {
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, a.maxResponseBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return provideradapter.InvokeResult{}, fmt.Errorf("httpadapter: read response: %w", err)
	}
	if int64(len(raw)) > a.maxResponseBytes {
		return provideradapter.InvokeResult{}, fmt.Errorf("httpadapter: response exceeded %d byte limit", a.maxResponseBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return provideradapter.InvokeResult{}, fmt.Errorf("httpadapter: non-2xx response: %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "" && !isJSONContentType(ct) {
		return provideradapter.InvokeResult{}, fmt.Errorf("httpadapter: unexpected content-type %q", ct)
	}
	var wire wireResponse
	if err := json.Unmarshal(raw, &wire); err != nil {
		return provideradapter.InvokeResult{}, fmt.Errorf("httpadapter: malformed JSON response: %w", err)
	}
	status, err := decodeStatus(wire.Status)
	if err != nil {
		return provideradapter.InvokeResult{}, err
	}
	return provideradapter.InvokeResult{
		Status: status, Output: wire.Output, Usage: wire.Usage, FailureReason: wire.FailureReason,
	}, nil
}

func decodeStatus(raw string) (provideradapter.InvokeStatus, error) {
	switch provideradapter.InvokeStatus(raw) {
	case provideradapter.InvokeCompleted, provideradapter.InvokeFailed, provideradapter.InvokePending:
		return provideradapter.InvokeStatus(raw), nil
	default:
		return "", fmt.Errorf("httpadapter: unknown status %q in provider response", raw)
	}
}

func isJSONContentType(ct string) bool {
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	return strings.EqualFold(mediaType, "application/json")
}

func (a *Adapter) Query(ctx context.Context, idempotencyKey string) (provideradapter.InvokeResult, bool, error) {
	return provideradapter.InvokeResult{}, false, errQueryRequiresEndpoint
}

// QueryAt looks up a previously attempted invocation's outcome at
// endpointRef by idempotencyKey. The plain provideradapter.ProviderAdapter
// interface's Query(ctx, key) has no endpoint parameter (mirroring
// payout.Adapter's single-rail assumption), but this adapter serves many
// distinct third-party endpoints, so internal/adapters/tosai/dispatch calls
// this method directly (it holds the concrete *Adapter type, not just the
// interface) rather than the interface method, which exists only to
// satisfy provideradapter.ProviderAdapter and always returns
// errQueryRequiresEndpoint.
func (a *Adapter) QueryAt(ctx context.Context, endpointRef, idempotencyKey string) (provideradapter.InvokeResult, bool, error) {
	if endpointRef == "" || idempotencyKey == "" {
		return provideradapter.InvokeResult{}, false, errors.New("httpadapter: endpoint_ref and idempotency_key are required")
	}
	u, err := url.Parse(endpointRef)
	if err != nil {
		return provideradapter.InvokeResult{}, false, fmt.Errorf("httpadapter: invalid endpoint_ref: %w", err)
	}
	q := u.Query()
	q.Set("idempotency_key", idempotencyKey)
	u.RawQuery = q.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return provideradapter.InvokeResult{}, false, fmt.Errorf("httpadapter: build query request: %w", err)
	}
	httpReq.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return provideradapter.InvokeResult{}, false, fmt.Errorf("httpadapter: query request failed: %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return provideradapter.InvokeResult{}, false, nil
	}
	result, err := a.decodeResult(resp)
	if err != nil {
		return provideradapter.InvokeResult{}, false, err
	}
	return result, true, nil
}

var errQueryRequiresEndpoint = errors.New("httpadapter: Query requires an endpoint; call QueryAt directly")

func (a *Adapter) Cancel(ctx context.Context, idempotencyKey, reason string) error {
	return provideradapter.ErrCancelUnsupported
}

// Health performs a bounded, best-effort GET against endpointRef and
// reports pure reachability -- any HTTP response at all (including a 4xx a
// provider might return for a bare GET at an invoke-only endpoint) counts
// as healthy, since this is a transport-reachability probe, not a
// functional check; that deeper check is sandbox certification's job.
func (a *Adapter) Health(ctx context.Context, endpointRef string) domain.AdapterHealthCheck {
	now := time.Now().UTC()
	check := domain.AdapterHealthCheck{Transport: domain.AdapterHTTP, EndpointRef: endpointRef, CheckedAt: now}
	if endpointRef == "" {
		check.Status = domain.AdapterHealthUnhealthy
		check.FailureReason = "endpoint_ref is empty"
		return check
	}
	healthCtx, cancel := context.WithTimeout(ctx, defaultHealthTimeout)
	defer cancel()
	start := time.Now()
	req, err := http.NewRequestWithContext(healthCtx, http.MethodGet, endpointRef, nil)
	if err != nil {
		check.Status = domain.AdapterHealthUnhealthy
		check.FailureReason = err.Error()
		return check
	}
	resp, err := a.client.Do(req)
	check.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		check.Status = domain.AdapterHealthUnhealthy
		check.FailureReason = err.Error()
		return check
	}
	_ = resp.Body.Close()
	check.Status = domain.AdapterHealthHealthy
	return check
}

// AllowPrivateNetworksEnv exposes the outbound-policy escape-hatch env var
// name for tests and operators; the policy defaults to rejecting
// private/loopback/link-local destinations and this must be set
// deliberately, never implied.
const AllowPrivateNetworksEnv = allowPrivateNetworksEnv
