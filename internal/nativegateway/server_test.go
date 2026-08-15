package nativegateway

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
	"github.com/tosnetwork/tos-service-protocol/pkg/capabilitycatalog"
	"github.com/tosnetwork/tos-service-protocol/pkg/publicerrors"
)

type backendStub struct{ submissions, resolutions int }

func requirePublicDetail(t *testing.T, err error, code nativev1.NativeErrorCodeV1, retry nativev1.RetryDispositionV1) {
	t.Helper()
	detail, ok := publicerrors.Detail(err)
	if !ok || detail.Code != code || detail.RetryDisposition != retry {
		t.Fatalf("detail=%+v ok=%v err=%v", detail, ok, err)
	}
}

func TestPublicBoundaryErrorsCarryCanonicalRetryDetails(t *testing.T) {
	server := &Server{Authorizer: NewTokenAuthorizer("read-secret", "relay-secret")}
	_, err := server.SearchCapabilities(context.Background(), nil)
	requirePublicDetail(t, err, nativev1.NativeErrorCodeV1_NATIVE_ERROR_CODE_V1_PUBLIC_BAD_REQUEST,
		nativev1.RetryDispositionV1_RETRY_DISPOSITION_V1_NEVER)
	resolve := connect.NewRequest(&nativev1.ResolveNativeStateRequest{})
	resolve.Header().Set("Authorization", "Bearer read-secret")
	_, err = server.ResolveNativeState(context.Background(), resolve)
	requirePublicDetail(t, err, nativev1.NativeErrorCodeV1_NATIVE_ERROR_CODE_V1_PUBLIC_DEPENDENCY_UNAVAILABLE,
		nativev1.RetryDispositionV1_RETRY_DISPOSITION_V1_SAME_REQUEST_AFTER_BACKOFF)
	expired := connect.NewRequest(&nativev1.ListCapabilitiesRequest{Context: &nativev1.RequestContext{
		RequestId: "request", CallerId: "caller", DeadlineUnixMillis: time.Now().Add(-time.Second).UnixMilli()}})
	expired.Header().Set("Authorization", "Bearer read-secret")
	_, err = server.ListCapabilities(context.Background(), expired)
	requirePublicDetail(t, err, nativev1.NativeErrorCodeV1_NATIVE_ERROR_CODE_V1_PUBLIC_DEADLINE,
		nativev1.RetryDispositionV1_RETRY_DISPOSITION_V1_NEVER)
}

func TestPublicErrorDetailSurvivesConnectWire(t *testing.T) {
	server := &Server{Authorizer: NewTokenAuthorizer("read-secret", "relay-secret"), Catalog: &discoveryStub{}}
	path, handler := tosservicev1connect.NewCapabilityDiscoveryServiceHandler(server)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	client := tosservicev1connect.NewCapabilityDiscoveryServiceClient(httpServer.Client(), httpServer.URL)
	request := connect.NewRequest(&nativev1.SearchCapabilitiesRequest{Context: &nativev1.RequestContext{
		RequestId: "request", CallerId: "caller", DeadlineUnixMillis: time.Now().Add(-time.Second).UnixMilli()}})
	request.Header().Set("Authorization", "Bearer read-secret")
	_, err := client.SearchCapabilities(context.Background(), request)
	requirePublicDetail(t, err, nativev1.NativeErrorCodeV1_NATIVE_ERROR_CODE_V1_PUBLIC_DEADLINE,
		nativev1.RetryDispositionV1_RETRY_DISPOSITION_V1_NEVER)
}

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
