package nativegateway

import (
	"context"
	"strings"
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

type discoveryStub struct{ published, listed, searched, loaded int }

type quoteStub struct{ calls int }

func (q *quoteStub) RequestQuoteProposal(context.Context, *nativev1.RequestQuoteProposalRequest) (*nativev1.QuoteProposalPackageV1, error) {
	q.calls++
	return &nativev1.QuoteProposalPackageV1{}, nil
}

func (d *discoveryStub) List(context.Context, uint32, string) (*capabilitycatalog.Page, error) {
	d.listed++
	return &capabilitycatalog.Page{}, nil
}

func TestQuoteExchangeRequiresReadPermissionAndCompletePreimages(t *testing.T) {
	source := &quoteStub{}
	server := &Server{Authorizer: NewTokenAuthorizer("read-secret", "relay-secret"), QuoteSource: source,
		Network: &nativev1.NetworkDomain{NetworkId: "test", GenesisRootHash: "sha256:" + strings.Repeat("11", 32),
			GenesisFileHash: "sha256:" + strings.Repeat("22", 32)}}
	requestContext := &nativev1.RequestContext{RequestId: "request", CallerId: "buyer",
		DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli()}
	request := connect.NewRequest(&nativev1.RequestQuoteProposalRequest{Context: requestContext,
		CapabilityId: "cap_" + strings.Repeat("33", 32), CapabilityVersion: "1.0.0",
		BuyerAddress: "0:" + strings.Repeat("44", 32)})
	if _, err := server.RequestQuoteProposal(context.Background(), request); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("missing token error=%v", err)
	}
	request.Header().Set("Authorization", "Bearer read-secret")
	if _, err := server.RequestQuoteProposal(context.Background(), request); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("incomplete preimage error=%v", err)
	}
	if source.calls != 1 {
		t.Fatalf("quote source calls=%d", source.calls)
	}
}
func (d *discoveryStub) Search(context.Context, string, uint32, string) (*capabilitycatalog.SearchPage, error) {
	d.searched++
	return &capabilitycatalog.SearchPage{}, nil
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
	search := connect.NewRequest(&nativev1.SearchCapabilitiesRequest{Context: requestContext, Query: "test"})
	search.Header().Set("Authorization", "Bearer read-secret")
	if _, err := server.SearchCapabilities(context.Background(), search); err != nil {
		t.Fatal(err)
	}
	get := connect.NewRequest(&nativev1.GetSoftwareWorkManifestRequest{Context: requestContext, ManifestDigest: "sha256:test"})
	get.Header().Set("Authorization", "Bearer read-secret")
	if _, err := server.GetSoftwareWorkManifest(context.Background(), get); err != nil {
		t.Fatal(err)
	}
	if discovery.published != 1 || discovery.listed != 1 || discovery.searched != 1 || discovery.loaded != 1 {
		t.Fatal("discovery calls did not reach the expected bounded adapter")
	}
}
