// Package toprotocol implements ATOS's real ConnectRPC client for the
// tos-protocol ATOS/TOS v0.2 boundary. It intentionally implements both the
// execution and trust/economic adapter interfaces with one connection so the
// gateway cannot accidentally split one Job across different backends.
package toprotocol

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/observability"
	"github.com/tosnetwork/atos/internal/store"
	atostosv1 "github.com/tosnetwork/tos-protocol/gen/atos/tos/v1"
	"github.com/tosnetwork/tos-protocol/gen/atos/tos/v1/atostosv1connect"
)

const (
	defaultTimeout                 = 30 * time.Second
	defaultMaxMessageBytes         = 16 << 20
	defaultRetention               = 48 * time.Hour
	defaultExecutionMaxOutputBytes = 1 << 20
)

// Config describes the privileged ATOS -> tos-protocol transport. Plaintext
// HTTP is accepted only when Insecure is explicitly true.
type Config struct {
	BaseURL         string
	BearerToken     string
	Timeout         time.Duration
	MaxMessageBytes int
	Insecure        bool
	ServerName      string
	CAFile          string
	ClientCertFile  string
	ClientKeyFile   string
	Store           store.Store
	// Network is this deployment's configured TOS network identity (see
	// config.TOSRPCConfig.Network's doc comment). Empty means unconfigured
	// -- Client.Network() then returns "", which TOSBackedActivationAuthority
	// treats as a hard fail-closed condition, never a wildcard.
	Network string
}

type Client struct {
	baseURL    string
	token      string
	timeout    time.Duration
	retention  time.Duration
	network    string
	httpClient *http.Client
	store      store.Store

	identity           atostosv1connect.IdentityServiceClient
	capability         atostosv1connect.CapabilityServiceClient
	trust              atostosv1connect.TrustServiceClient
	settlement         atostosv1connect.SettlementServiceClient
	proof              atostosv1connect.ProofServiceClient
	execution          atostosv1connect.ExecutionGatewayServiceClient
	financialIntegrity atostosv1connect.FinancialIntegrityServiceClient

	receipts  sync.Map // receipt_id -> *ExecutionReceiptEnvelope
	proofRefs sync.Map // client-facing receipt/proof id -> protocol proof reference
}

func New(config Config) (*Client, error) {
	config.BaseURL = strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	config.BearerToken = strings.TrimSpace(config.BearerToken)
	if config.BaseURL == "" {
		return nil, errors.New("tos-protocol RPC base URL is required")
	}
	if config.BearerToken == "" {
		return nil, errors.New("tos-protocol RPC bearer token is required")
	}
	if config.Store == nil {
		return nil, errors.New("ATOS store is required for the tos-protocol RPC backend")
	}
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return nil, errors.New("tos-protocol RPC base URL must be an absolute http(s) URL")
	}
	if parsed.Scheme == "http" && !config.Insecure {
		return nil, errors.New("plaintext tos-protocol RPC requires explicit insecure mode")
	}
	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}
	if config.Timeout <= 0 || config.Timeout > 15*time.Minute {
		return nil, errors.New("invalid tos-protocol RPC timeout")
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaultMaxMessageBytes
	}
	if config.MaxMessageBytes <= 0 || config.MaxMessageBytes > 64<<20 {
		return nil, errors.New("invalid tos-protocol RPC message limit")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if parsed.Scheme == "https" {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: config.ServerName}
		if config.CAFile != "" {
			pem, err := os.ReadFile(config.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read tos-protocol CA: %w", err)
			}
			pool, err := x509.SystemCertPool()
			if err != nil || pool == nil {
				pool = x509.NewCertPool()
			}
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("tos-protocol CA file contains no valid certificate")
			}
			tlsConfig.RootCAs = pool
		}
		if (config.ClientCertFile == "") != (config.ClientKeyFile == "") {
			return nil, errors.New("tos-protocol client certificate and key must be configured together")
		}
		if config.ClientCertFile != "" {
			certificate, err := tls.LoadX509KeyPair(config.ClientCertFile, config.ClientKeyFile)
			if err != nil {
				return nil, fmt.Errorf("load tos-protocol client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{certificate}
		}
		transport.TLSClientConfig = tlsConfig
	}
	httpClient := &http.Client{Transport: transport}
	options := []connect.ClientOption{
		connect.WithReadMaxBytes(config.MaxMessageBytes),
		connect.WithSendMaxBytes(config.MaxMessageBytes),
	}
	client := &Client{
		baseURL: config.BaseURL, token: config.BearerToken,
		timeout: config.Timeout, retention: defaultRetention,
		httpClient: httpClient, store: config.Store, network: config.Network,
	}
	client.identity = atostosv1connect.NewIdentityServiceClient(httpClient, config.BaseURL, options...)
	client.capability = atostosv1connect.NewCapabilityServiceClient(httpClient, config.BaseURL, options...)
	client.trust = atostosv1connect.NewTrustServiceClient(httpClient, config.BaseURL, options...)
	client.settlement = atostosv1connect.NewSettlementServiceClient(httpClient, config.BaseURL, options...)
	client.proof = atostosv1connect.NewProofServiceClient(httpClient, config.BaseURL, options...)
	client.execution = atostosv1connect.NewExecutionGatewayServiceClient(httpClient, config.BaseURL, options...)
	client.financialIntegrity = atostosv1connect.NewFinancialIntegrityServiceClient(httpClient, config.BaseURL, options...)
	return client, nil
}

// CheckReady verifies that the configured endpoint is reachable. It does not
// switch to a mock backend on failure.
func (c *Client) CheckReady(ctx context.Context) error {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request, err := http.NewRequestWithContext(callCtx, http.MethodGet, c.baseURL+"/healthz", nil)
	if err != nil {
		return err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol readiness check failed: "+err.Error(), true)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return domain.NewError(domain.ErrNetworkUnavailable, fmt.Sprintf("tos-protocol readiness returned HTTP %d", response.StatusCode), true)
	}
	return nil
}

func (c *Client) callContext(ctx context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	bounded := time.Now().Add(c.timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(bounded) {
		bounded = contextDeadline
	}
	if !deadline.IsZero() && deadline.Before(bounded) {
		bounded = deadline
	}
	return context.WithDeadline(ctx, bounded)
}

func (c *Client) requestContext(ctx context.Context, callerID, idempotencyKey string, deadline time.Time) *atostosv1.RequestContext {
	requestID := observability.RequestID(ctx)
	if requestID == "" {
		requestID = uuid.NewString()
	}
	traceID := observability.TraceID(ctx)
	if !validTraceID(traceID) {
		traceID = randomHex(16)
	}
	deadlineMS := time.Now().Add(c.timeout).UnixMilli()
	if !deadline.IsZero() && deadline.UnixMilli() < deadlineMS {
		deadlineMS = deadline.UnixMilli()
	}
	return &atostosv1.RequestContext{
		RequestId: requestID, TraceId: traceID, IdempotencyKey: idempotencyKey,
		CallerId: callerID, DeadlineUnixMillis: deadlineMS,
	}
}

func decorateRequest[T any](c *Client, ctx context.Context, request *connect.Request[T]) {
	request.Header().Set("Authorization", "Bearer "+c.token)
	if requestID := observability.RequestID(ctx); requestID != "" {
		request.Header().Set("X-Request-Id", requestID)
	}
	traceID := observability.TraceID(ctx)
	if validTraceID(traceID) {
		request.Header().Set("Traceparent", "00-"+traceID+"-"+randomHex(8)+"-01")
	}
}

func validTraceID(value string) bool {
	if len(value) != 32 || strings.Trim(value, "0") == "" {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func randomHex(size int) string {
	buffer := make([]byte, size)
	if _, err := rand.Read(buffer); err != nil {
		return strings.ReplaceAll(uuid.NewString(), "-", "")[:size*2]
	}
	return hex.EncodeToString(buffer)
}

// Close releases idle transport resources. It never closes the ATOS store,
// which remains owned by the gateway process.
func (c *Client) Close() error {
	if c == nil || c.httpClient == nil {
		return nil
	}
	if transport, ok := c.httpClient.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	return nil
}
