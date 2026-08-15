package nativegateway

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	nativev1 "github.com/tosnetwork/tos-protocol/gen/atos/native/v1"
	"github.com/tosnetwork/tos-protocol/pkg/capabilitycatalog"
)

type backendStub struct{ submissions, resolutions int }

func (b *backendStub) SubmitNativeAction(context.Context, *connect.Request[nativev1.SubmitNativeActionRequest]) (*connect.Response[nativev1.SubmitNativeActionResponse], error) {
	b.submissions++
	return connect.NewResponse(&nativev1.SubmitNativeActionResponse{ActionHash: "sha256:test"}), nil
}

type discoveryStub struct{ published, listed, loaded int }

func (d *discoveryStub) List(context.Context, uint32, string) (*capabilitycatalog.Page, error) {
	d.listed++
	return &capabilitycatalog.Page{}, nil
}
func (d *discoveryStub) PublishManifest(context.Context, string, []byte) (*nativev1.NativeStateV1, string, error) {
	d.published++
	return &nativev1.NativeStateV1{}, "sha256:test", nil
}
func (d *discoveryStub) Manifest(string) ([]byte, error) {
	d.loaded++
	return []byte{1}, nil
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

func TestDiscoveryPermissionsAndDispatch(t *testing.T) {
	discovery := &discoveryStub{}
	server := &Server{Authorizer: NewTokenAuthorizer("read-secret", "relay-secret"), Catalog: discovery}
	requestContext := &nativev1.RequestContext{RequestId: "request", CallerId: "caller",
		IdempotencyKey: "manifest-one", DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli()}
	publish := connect.NewRequest(&nativev1.PublishSoftwareWorkManifestRequest{Context: requestContext,
		CapabilityId: "capability", CanonicalCbor: []byte{1}})
	publish.Header().Set("Authorization", "Bearer read-secret")
	if _, err := server.PublishSoftwareWorkManifest(context.Background(), publish); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("read token published a manifest: %v", err)
	}
	publish.Header().Set("Authorization", "Bearer relay-secret")
	if _, err := server.PublishSoftwareWorkManifest(context.Background(), publish); err != nil {
		t.Fatal(err)
	}
	list := connect.NewRequest(&nativev1.ListCapabilitiesRequest{Context: requestContext})
	list.Header().Set("Authorization", "Bearer read-secret")
	if _, err := server.ListCapabilities(context.Background(), list); err != nil {
		t.Fatal(err)
	}
	get := connect.NewRequest(&nativev1.GetSoftwareWorkManifestRequest{Context: requestContext, ManifestDigest: "sha256:test"})
	get.Header().Set("Authorization", "Bearer read-secret")
	if _, err := server.GetSoftwareWorkManifest(context.Background(), get); err != nil {
		t.Fatal(err)
	}
	if discovery.published != 1 || discovery.listed != 1 || discovery.loaded != 1 {
		t.Fatal("discovery calls did not reach the expected bounded adapter")
	}
}
