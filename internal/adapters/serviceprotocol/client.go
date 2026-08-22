// Package toprotocol is the privileged Native-only client for tos-service-protocol.
package toprotocol

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	nativev1 "github.com/tosnetwork/tos-service-protocol/gen/tos/service/v1"
	"github.com/tosnetwork/tos-service-protocol/gen/tos/service/v1/tosservicev1connect"
)

const (
	defaultTimeout         = 30 * time.Second
	defaultMaxMessageBytes = 16 << 20
)

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
}

type Client struct {
	baseURL    string
	token      string
	timeout    time.Duration
	httpClient *http.Client
	native     tosservicev1connect.NativeServiceClient
	dns        tosservicev1connect.DNSAliasServiceClient
}

func New(config Config) (*Client, error) {
	config.BaseURL = strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	config.BearerToken = strings.TrimSpace(config.BearerToken)
	if config.BaseURL == "" || config.BearerToken == "" {
		return nil, errors.New("tos-service-protocol RPC base URL and bearer token are required")
	}
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return nil, errors.New("tos-service-protocol RPC base URL must be an absolute http(s) URL")
	}
	if parsed.Scheme == "http" && !config.Insecure {
		return nil, errors.New("plaintext tos-service-protocol RPC requires explicit insecure mode")
	}
	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}
	if config.Timeout <= 0 || config.Timeout > 15*time.Minute {
		return nil, errors.New("invalid tos-service-protocol RPC timeout")
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaultMaxMessageBytes
	}
	if config.MaxMessageBytes <= 0 || config.MaxMessageBytes > 64<<20 {
		return nil, errors.New("invalid tos-service-protocol RPC message limit")
	}
	if (config.ClientCertFile == "") != (config.ClientKeyFile == "") {
		return nil, errors.New("tos-service-protocol client certificate and key must be configured together")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if parsed.Scheme == "https" {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: config.ServerName}
		if config.CAFile != "" {
			pem, err := os.ReadFile(config.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read tos-service-protocol CA: %w", err)
			}
			pool, err := x509.SystemCertPool()
			if err != nil || pool == nil {
				pool = x509.NewCertPool()
			}
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("tos-service-protocol CA file contains no valid certificate")
			}
			tlsConfig.RootCAs = pool
		}
		if config.ClientCertFile != "" {
			certificate, err := tls.LoadX509KeyPair(config.ClientCertFile, config.ClientKeyFile)
			if err != nil {
				return nil, fmt.Errorf("load tos-service-protocol client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{certificate}
		}
		transport.TLSClientConfig = tlsConfig
	}
	httpClient := &http.Client{Transport: transport}
	options := []connect.ClientOption{connect.WithReadMaxBytes(config.MaxMessageBytes), connect.WithSendMaxBytes(config.MaxMessageBytes)}
	return &Client{baseURL: config.BaseURL, token: config.BearerToken, timeout: config.Timeout, httpClient: httpClient,
		native: tosservicev1connect.NewNativeServiceClient(httpClient, config.BaseURL, options...),
		dns:    tosservicev1connect.NewDNSAliasServiceClient(httpClient, config.BaseURL, options...)}, nil
}

func (c *Client) ResolveDNSAlias(ctx context.Context, request *connect.Request[nativev1.ResolveDNSAliasRequest]) (*connect.Response[nativev1.ResolveDNSAliasResponse], error) {
	if c == nil || c.dns == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("DNS alias backend is unavailable"))
	}
	if request == nil || request.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("DNS alias request is required"))
	}
	callCtx, cancel := c.callContext(ctx, nativeDeadline(request.Msg.GetContext()))
	defer cancel()
	request.Header().Set("Authorization", "Bearer "+c.token)
	return c.dns.ResolveDNSAlias(callCtx, request)
}

func (c *Client) SubmitNativeAction(ctx context.Context, request *connect.Request[nativev1.SubmitNativeActionRequest]) (*connect.Response[nativev1.SubmitNativeActionResponse], error) {
	if c == nil || c.native == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("Native backend is unavailable"))
	}
	if request == nil || request.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("Native submission request is required"))
	}
	callCtx, cancel := c.callContext(ctx, nativeDeadline(request.Msg.GetContext()))
	defer cancel()
	request.Header().Set("Authorization", "Bearer "+c.token)
	return c.native.SubmitNativeAction(callCtx, request)
}

func (c *Client) ResolveNativeState(ctx context.Context, request *connect.Request[nativev1.ResolveNativeStateRequest]) (*connect.Response[nativev1.ResolveNativeStateResponse], error) {
	if c == nil || c.native == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("Native backend is unavailable"))
	}
	if request == nil || request.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("Native resolution request is required"))
	}
	callCtx, cancel := c.callContext(ctx, nativeDeadline(request.Msg.GetContext()))
	defer cancel()
	request.Header().Set("Authorization", "Bearer "+c.token)
	return c.native.ResolveNativeState(callCtx, request)
}

func nativeDeadline(value *nativev1.RequestContext) time.Time {
	if value == nil || value.DeadlineUnixMillis <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(value.DeadlineUnixMillis)
}

func (c *Client) CheckReady(ctx context.Context) error {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request, err := http.NewRequestWithContext(callCtx, http.MethodGet, c.baseURL+"/healthz", nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("tos-service-protocol readiness check failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("tos-service-protocol readiness returned HTTP %d", response.StatusCode)
	}
	return nil
}

func (c *Client) callContext(ctx context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	bounded := time.Now().Add(c.timeout)
	if current, ok := ctx.Deadline(); ok && current.Before(bounded) {
		bounded = current
	}
	if !deadline.IsZero() && deadline.Before(bounded) {
		bounded = deadline
	}
	return context.WithDeadline(ctx, bounded)
}

func (c *Client) Close() error {
	if c != nil && c.httpClient != nil {
		if transport, ok := c.httpClient.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
	return nil
}
