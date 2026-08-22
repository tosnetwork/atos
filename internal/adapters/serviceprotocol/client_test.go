package toprotocol

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	nativev1 "github.com/tosnetwork/tos-service-protocol/gen/tos/service/v1"
	"github.com/tosnetwork/tos-service-protocol/gen/tos/service/v1/tosservicev1connect"
)

type dnsBackendStub struct {
	authorization string
}

func (s *dnsBackendStub) ResolveDNSAlias(_ context.Context, request *connect.Request[nativev1.ResolveDNSAliasRequest]) (*connect.Response[nativev1.ResolveDNSAliasResponse], error) {
	s.authorization = request.Header().Get("Authorization")
	return connect.NewResponse(&nativev1.ResolveDNSAliasResponse{CanonicalName: request.Msg.Name}), nil
}

func TestNewRejectsImplicitPlaintextRPC(t *testing.T) {
	_, err := New(Config{BaseURL: "http://127.0.0.1:9000", BearerToken: "token"})
	if err == nil || !strings.Contains(err.Error(), "requires explicit insecure mode") {
		t.Fatalf("New() error = %v", err)
	}
}

func TestNewRequiresBearerToken(t *testing.T) {
	_, err := New(Config{BaseURL: "https://tos-service-protocol.internal"})
	if err == nil || !strings.Contains(err.Error(), "bearer token") {
		t.Fatalf("New() error = %v", err)
	}
}

func TestNewAcceptsExplicitLocalPlaintext(t *testing.T) {
	client, err := New(Config{BaseURL: "http://127.0.0.1:9000", BearerToken: "token", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDNSAliasUsesPrivateBackendCredential(t *testing.T) {
	backend := &dnsBackendStub{}
	path, handler := tosservicev1connect.NewDNSAliasServiceHandler(backend)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, BearerToken: "private-token", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := connect.NewRequest(&nativev1.ResolveDNSAliasRequest{Name: "alice.tos", Context: &nativev1.RequestContext{
		RequestId: "dns-one", CallerId: "test", DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli()}})
	request.Header().Set("Authorization", "Bearer public-token")
	response, err := client.ResolveDNSAlias(context.Background(), request)
	if err != nil || response.Msg.CanonicalName != "alice.tos" {
		t.Fatalf("DNS response = %#v, %v", response, err)
	}
	if backend.authorization != "Bearer private-token" {
		t.Fatalf("backend Authorization = %q", backend.authorization)
	}
}
