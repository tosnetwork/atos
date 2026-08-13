package nativegateway

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/auth"
	nativev1 "github.com/tosnetwork/tos-protocol/gen/atos/native/v1"
)

type backendStub struct{ submissions, resolutions int }

func (b *backendStub) SubmitNativeAction(context.Context, *connect.Request[nativev1.SubmitNativeActionRequest]) (*connect.Response[nativev1.SubmitNativeActionResponse], error) {
	b.submissions++
	return connect.NewResponse(&nativev1.SubmitNativeActionResponse{ActionHash: "sha256:test"}), nil
}
func (b *backendStub) ResolveNativeState(context.Context, *connect.Request[nativev1.ResolveNativeStateRequest]) (*connect.Response[nativev1.ResolveNativeStateResponse], error) {
	b.resolutions++
	return connect.NewResponse(&nativev1.ResolveNativeStateResponse{}), nil
}

func TestNativeGatewayAuthIsTransportOnlyAndScopeSeparated(t *testing.T) {
	authorization, err := auth.Open(auth.Config{AutoApprove: true, PollInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer authorization.Close()
	readToken, err := authorization.IssueForPrincipal("reader", []auth.Scope{auth.ScopeNativeRead}, "test", "reader")
	if err != nil {
		t.Fatal(err)
	}
	relayToken, err := authorization.IssueForPrincipal("relayer", []auth.Scope{auth.ScopeNativeRelay}, "test", "relayer")
	if err != nil {
		t.Fatal(err)
	}
	backend := &backendStub{}
	server := &Server{Auth: authorization, Backend: backend}
	resolve := connect.NewRequest(&nativev1.ResolveNativeStateRequest{})
	resolve.Header().Set("Authorization", "Bearer "+readToken.AccessToken)
	if _, err := server.ResolveNativeState(context.Background(), resolve); err != nil {
		t.Fatal(err)
	}
	submit := connect.NewRequest(&nativev1.SubmitNativeActionRequest{})
	submit.Header().Set("Authorization", "Bearer "+readToken.AccessToken)
	if _, err := server.SubmitNativeAction(context.Background(), submit); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("read token relay error = %v", err)
	}
	submit.Header().Set("Authorization", "Bearer "+relayToken.AccessToken)
	if _, err := server.SubmitNativeAction(context.Background(), submit); err != nil {
		t.Fatal(err)
	}
	if backend.resolutions != 1 || backend.submissions != 1 {
		t.Fatal("Native gateway changed backend call counts")
	}
}
