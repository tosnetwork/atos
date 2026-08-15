// Package nativegateway exposes the stateless atos_native_v1 transport.
// Authentication grants transport access only; it never decides Native state.
package nativegateway

import (
	"context"
	"crypto/subtle"
	"errors"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	nativev1 "github.com/tosnetwork/tos-protocol/gen/atos/native/v1"
	"github.com/tosnetwork/tos-protocol/pkg/capabilitycatalog"
	"github.com/tosnetwork/tos-protocol/pkg/publicerrors"
	"github.com/tosnetwork/tos-protocol/pkg/quoteexchange"
)

type Backend interface {
	SubmitNativeAction(context.Context, *connect.Request[nativev1.SubmitNativeActionRequest]) (*connect.Response[nativev1.SubmitNativeActionResponse], error)
	ResolveNativeState(context.Context, *connect.Request[nativev1.ResolveNativeStateRequest]) (*connect.Response[nativev1.ResolveNativeStateResponse], error)
}

type Permission uint8

const (
	PermissionRead Permission = 1 << iota
	PermissionRelay
)

type Authorizer interface {
	Authorize(header string, required Permission) error
}

type TokenAuthorizer struct {
	readToken  string
	relayToken string
}

func NewTokenAuthorizer(readToken, relayToken string) *TokenAuthorizer {
	return &TokenAuthorizer{readToken: strings.TrimSpace(readToken), relayToken: strings.TrimSpace(relayToken)}
}

func (a *TokenAuthorizer) Authorize(header string, required Permission) error {
	if a == nil || a.readToken == "" || a.relayToken == "" {
		return publicerrors.New(publicerrors.DependencyUnavailable, errors.New("gateway authentication is unavailable"), time.Second)
	}
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok || strings.TrimSpace(token) == "" {
		return publicerrors.New(publicerrors.Unauthenticated, errors.New("missing gateway bearer token"), 0)
	}
	token = strings.TrimSpace(token)
	readMatch := constantTimeEqual(token, a.readToken)
	relayMatch := constantTimeEqual(token, a.relayToken)
	if !readMatch && !relayMatch {
		return publicerrors.New(publicerrors.Unauthenticated, errors.New("invalid gateway bearer token"), 0)
	}
	if required == PermissionRelay && !relayMatch {
		return publicerrors.New(publicerrors.PermissionDenied, errors.New("gateway token lacks native.relay permission"), 0)
	}
	return nil
}

func constantTimeEqual(left, right string) bool {
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

type Server struct {
	Authorizer  Authorizer
	Backend     Backend
	Catalog     DiscoveryCatalog
	QuoteSource QuoteSource
	Network     *nativev1.NetworkDomain
}

type QuoteSource interface {
	RequestQuoteProposal(context.Context, *nativev1.RequestQuoteProposalRequest) (*nativev1.QuoteProposalPackageV1, error)
}

type DiscoveryCatalog interface {
	List(context.Context, uint32, string) (*capabilitycatalog.Page, error)
	Search(context.Context, string, uint32, string) (*capabilitycatalog.SearchPage, error)
	PublishManifest(context.Context, string, []byte) (*nativev1.NativeStateV1, string, error)
	Manifest(string) ([]byte, error)
}

func (s *Server) SearchCapabilities(ctx context.Context, request *connect.Request[nativev1.SearchCapabilitiesRequest]) (*connect.Response[nativev1.SearchCapabilitiesResponse], error) {
	if request == nil || request.Msg == nil {
		return nil, publicerrors.New(publicerrors.BadRequest, errors.New("Capability search request is required"), 0)
	}
	if err := s.authorizeDiscovery(request.Header().Get("Authorization"), request.Msg.Context, PermissionRead); err != nil {
		return nil, err
	}
	page, err := s.Catalog.Search(ctx, request.Msg.Query, request.Msg.PageSize, request.Msg.AfterCapabilityId)
	if err != nil {
		return nil, discoveryError(err, publicerrors.BadRequest)
	}
	return connect.NewResponse(&nativev1.SearchCapabilitiesResponse{Results: page.Results,
		NextAfterCapabilityId: page.NextToken}), nil
}

func (s *Server) SubmitNativeAction(ctx context.Context, request *connect.Request[nativev1.SubmitNativeActionRequest]) (*connect.Response[nativev1.SubmitNativeActionResponse], error) {
	if request == nil {
		return nil, publicerrors.New(publicerrors.BadRequest, errors.New("Native submission request is required"), 0)
	}
	if err := s.authorize(request.Header().Get("Authorization"), PermissionRelay); err != nil {
		return nil, err
	}
	if s.Backend == nil {
		return nil, publicerrors.New(publicerrors.DependencyUnavailable, errors.New("Native backend is unavailable"), time.Second)
	}
	return s.Backend.SubmitNativeAction(ctx, request)
}

func (s *Server) ResolveNativeState(ctx context.Context, request *connect.Request[nativev1.ResolveNativeStateRequest]) (*connect.Response[nativev1.ResolveNativeStateResponse], error) {
	if request == nil {
		return nil, publicerrors.New(publicerrors.BadRequest, errors.New("Native resolution request is required"), 0)
	}
	if err := s.authorize(request.Header().Get("Authorization"), PermissionRead); err != nil {
		return nil, err
	}
	if s.Backend == nil {
		return nil, publicerrors.New(publicerrors.DependencyUnavailable, errors.New("Native backend is unavailable"), time.Second)
	}
	return s.Backend.ResolveNativeState(ctx, request)
}

func (s *Server) authorize(header string, required Permission) error {
	if s == nil || s.Authorizer == nil {
		return publicerrors.New(publicerrors.DependencyUnavailable, errors.New("gateway authentication is unavailable"), time.Second)
	}
	return s.Authorizer.Authorize(header, required)
}

func (s *Server) ListCapabilities(ctx context.Context, request *connect.Request[nativev1.ListCapabilitiesRequest]) (*connect.Response[nativev1.ListCapabilitiesResponse], error) {
	if request == nil || request.Msg == nil {
		return nil, publicerrors.New(publicerrors.BadRequest, errors.New("Capability listing request is required"), 0)
	}
	if err := s.authorizeDiscovery(request.Header().Get("Authorization"), request.Msg.Context, PermissionRead); err != nil {
		return nil, err
	}
	page, err := s.Catalog.List(ctx, request.Msg.PageSize, request.Msg.AfterCapabilityId)
	if err != nil {
		return nil, discoveryError(err, publicerrors.DependencyUnavailable)
	}
	return connect.NewResponse(&nativev1.ListCapabilitiesResponse{Capabilities: page.Capabilities,
		NextAfterCapabilityId: page.NextToken}), nil
}

func (s *Server) PublishSoftwareWorkManifest(ctx context.Context, request *connect.Request[nativev1.PublishSoftwareWorkManifestRequest]) (*connect.Response[nativev1.PublishSoftwareWorkManifestResponse], error) {
	if request == nil || request.Msg == nil {
		return nil, publicerrors.New(publicerrors.BadRequest, errors.New("manifest publication request is required"), 0)
	}
	if err := s.authorizeDiscovery(request.Header().Get("Authorization"), request.Msg.Context, PermissionRelay); err != nil {
		return nil, err
	}
	if request.Msg.Context.IdempotencyKey == "" || len(request.Msg.Context.IdempotencyKey) > 256 {
		return nil, publicerrors.New(publicerrors.BadRequest, errors.New("manifest publication requires a bounded idempotency key"), 0)
	}
	state, digest, err := s.Catalog.PublishManifest(ctx, request.Msg.CapabilityId, request.Msg.CanonicalCbor)
	if err != nil {
		return nil, discoveryError(err, publicerrors.Conflict)
	}
	return connect.NewResponse(&nativev1.PublishSoftwareWorkManifestResponse{ManifestDigest: digest, Capability: state}), nil
}

func (s *Server) GetSoftwareWorkManifest(_ context.Context, request *connect.Request[nativev1.GetSoftwareWorkManifestRequest]) (*connect.Response[nativev1.GetSoftwareWorkManifestResponse], error) {
	if request == nil || request.Msg == nil {
		return nil, publicerrors.New(publicerrors.BadRequest, errors.New("manifest retrieval request is required"), 0)
	}
	if err := s.authorizeDiscovery(request.Header().Get("Authorization"), request.Msg.Context, PermissionRead); err != nil {
		return nil, err
	}
	raw, err := s.Catalog.Manifest(request.Msg.ManifestDigest)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, publicerrors.New(publicerrors.NotFound, errors.New("software-work manifest was not found"), 0)
		}
		return nil, discoveryError(err, publicerrors.Conflict)
	}
	return connect.NewResponse(&nativev1.GetSoftwareWorkManifestResponse{ManifestDigest: request.Msg.ManifestDigest, CanonicalCbor: raw}), nil
}

func (s *Server) RequestQuoteProposal(ctx context.Context, request *connect.Request[nativev1.RequestQuoteProposalRequest]) (*connect.Response[nativev1.RequestQuoteProposalResponse], error) {
	if request == nil || request.Msg == nil {
		return nil, publicerrors.New(publicerrors.BadRequest, errors.New("Quote Proposal request is required"), 0)
	}
	if err := s.authorizeRequestContext(request.Header().Get("Authorization"), request.Msg.Context, PermissionRead); err != nil {
		return nil, err
	}
	if s.QuoteSource == nil || s.Network == nil {
		return nil, publicerrors.New(publicerrors.DependencyUnavailable, errors.New("Quote Proposal source is unavailable"), time.Second)
	}
	proposal, err := s.QuoteSource.RequestQuoteProposal(ctx, request.Msg)
	if err != nil {
		return nil, discoveryError(err, publicerrors.DependencyUnavailable)
	}
	if _, err := quoteexchange.Validate(s.Network, request.Msg, proposal, time.Now()); err != nil {
		return nil, publicerrors.New(publicerrors.Conflict, errors.New("Quote Proposal source returned conflicting preimages"), 0)
	}
	return connect.NewResponse(&nativev1.RequestQuoteProposalResponse{Package: proposal}), nil
}

func (s *Server) authorizeDiscovery(header string, requestContext *nativev1.RequestContext, required Permission) error {
	if err := s.authorizeRequestContext(header, requestContext, required); err != nil {
		return err
	}
	if s.Catalog == nil {
		return publicerrors.New(publicerrors.DependencyUnavailable, errors.New("Capability discovery is unavailable"), time.Second)
	}
	return nil
}

func (s *Server) authorizeRequestContext(header string, requestContext *nativev1.RequestContext, required Permission) error {
	if err := s.authorize(header, required); err != nil {
		return err
	}
	now := time.Now()
	if requestContext == nil || requestContext.RequestId == "" || len(requestContext.RequestId) > 128 ||
		requestContext.CallerId == "" || len(requestContext.CallerId) > 256 ||
		requestContext.DeadlineUnixMillis > now.Add(15*time.Minute).UnixMilli() {
		return publicerrors.New(publicerrors.BadRequest, errors.New("valid bounded discovery request context is required"), 0)
	}
	if requestContext.DeadlineUnixMillis <= now.UnixMilli() {
		return publicerrors.New(publicerrors.Deadline, errors.New("discovery request deadline expired"), 0)
	}
	return nil
}

func discoveryError(err error, fallback publicerrors.Kind) error {
	if err == nil {
		return nil
	}
	if _, ok := publicerrors.Detail(err); ok {
		return err
	}
	retry := time.Duration(0)
	if fallback == publicerrors.DependencyUnavailable || fallback == publicerrors.Capacity {
		retry = time.Second
	}
	return publicerrors.New(fallback, err, retry)
}
