package nativegateway

import (
	"context"
	"testing"

	"connectrpc.com/connect"
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

func TestTransportPermissionsAreSeparated(t *testing.T) {
	backend := &backendStub{}
	server := &Server{Authorizer: NewTokenAuthorizer("read-secret", "relay-secret"), Backend: backend}
	resolve := connect.NewRequest(&nativev1.ResolveNativeStateRequest{})
	resolve.Header().Set("Authorization", "Bearer read-secret")
	if _, err := server.ResolveNativeState(context.Background(), resolve); err != nil {
		t.Fatal(err)
	}
	submit := connect.NewRequest(&nativev1.SubmitNativeActionRequest{})
	submit.Header().Set("Authorization", "Bearer read-secret")
	if _, err := server.SubmitNativeAction(context.Background(), submit); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("read token relay error = %v", err)
	}
	submit.Header().Set("Authorization", "Bearer relay-secret")
	if _, err := server.SubmitNativeAction(context.Background(), submit); err != nil {
		t.Fatal(err)
	}
	if backend.resolutions != 1 || backend.submissions != 1 {
		t.Fatal("gateway changed backend call counts")
	}
}

func TestUnknownTokenFailsBeforeBackend(t *testing.T) {
	backend := &backendStub{}
	server := &Server{Authorizer: NewTokenAuthorizer("read-secret", "relay-secret"), Backend: backend}
	request := connect.NewRequest(&nativev1.ResolveNativeStateRequest{})
	request.Header().Set("Authorization", "Bearer wrong")
	if _, err := server.ResolveNativeState(context.Background(), request); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("error = %v", err)
	}
	if backend.resolutions != 0 {
		t.Fatal("unauthenticated request reached backend")
	}
}
