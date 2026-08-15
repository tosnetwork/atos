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
		return connect.NewError(connect.CodeUnavailable, errors.New("gateway authentication is unavailable"))
	}
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok || strings.TrimSpace(token) == "" {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("missing gateway bearer token"))
	}
	token = strings.TrimSpace(token)
	readMatch := constantTimeEqual(token, a.readToken)
	relayMatch := constantTimeEqual(token, a.relayToken)
	if !readMatch && !relayMatch {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("invalid gateway bearer token"))
	}
	if required == PermissionRelay && !relayMatch {
		return connect.NewError(connect.CodePermissionDenied, errors.New("gateway token lacks native.relay permission"))
	}
	return nil
}

func constantTimeEqual(left, right string) bool {
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

type Server struct {
	Authorizer Authorizer
	Backend    Backend
	Catalog    DiscoveryCatalog
}

type DiscoveryCatalog interface {
	List(context.Context, uint32, string) (*capabilitycatalog.Page, error)
	PublishManifest(context.Context, string, []byte) (*nativev1.NativeStateV1, string, error)
	Manifest(string) ([]byte, error)
}

func (s *Server) SubmitNativeAction(ctx context.Context, request *connect.Request[nativev1.SubmitNativeActionRequest]) (*connect.Response[nativev1.SubmitNativeActionResponse], error) {
	if request == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("Native submission request is required"))
	}
	if err := s.authorize(request.Header().Get("Authorization"), PermissionRelay); err != nil {
		return nil, err
	}
	if s.Backend == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("Native backend is unavailable"))
	}
	return s.Backend.SubmitNativeAction(ctx, request)
}

func (s *Server) ResolveNativeState(ctx context.Context, request *connect.Request[nativev1.ResolveNativeStateRequest]) (*connect.Response[nativev1.ResolveNativeStateResponse], error) {
	if request == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("Native resolution request is required"))
	}
	if err := s.authorize(request.Header().Get("Authorization"), PermissionRead); err != nil {
		return nil, err
	}
	if s.Backend == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("Native backend is unavailable"))
	}
	return s.Backend.ResolveNativeState(ctx, request)
}

func (s *Server) authorize(header string, required Permission) error {
	if s == nil || s.Authorizer == nil {
		return connect.NewError(connect.CodeUnavailable, errors.New("gateway authentication is unavailable"))
	}
	return s.Authorizer.Authorize(header, required)
}

func (s *Server) ListCapabilities(ctx context.Context, request *connect.Request[nativev1.ListCapabilitiesRequest]) (*connect.Response[nativev1.ListCapabilitiesResponse], error) {
	if request == nil || request.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("Capability listing request is required"))
	}
	if err := s.authorizeDiscovery(request.Header().Get("Authorization"), request.Msg.Context, PermissionRead); err != nil {
		return nil, err
	}
	page, err := s.Catalog.List(ctx, request.Msg.PageSize, request.Msg.AfterCapabilityId)
	if err != nil {
		return nil, discoveryError(err, connect.CodeUnavailable)
	}
	return connect.NewResponse(&nativev1.ListCapabilitiesResponse{Capabilities: page.Capabilities,
		NextAfterCapabilityId: page.NextToken}), nil
}

func (s *Server) PublishSoftwareWorkManifest(ctx context.Context, request *connect.Request[nativev1.PublishSoftwareWorkManifestRequest]) (*connect.Response[nativev1.PublishSoftwareWorkManifestResponse], error) {
	if request == nil || request.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("manifest publication request is required"))
	}
	if err := s.authorizeDiscovery(request.Header().Get("Authorization"), request.Msg.Context, PermissionRelay); err != nil {
		return nil, err
	}
	if request.Msg.Context.IdempotencyKey == "" || len(request.Msg.Context.IdempotencyKey) > 256 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("manifest publication requires a bounded idempotency key"))
	}
	state, digest, err := s.Catalog.PublishManifest(ctx, request.Msg.CapabilityId, request.Msg.CanonicalCbor)
	if err != nil {
		return nil, discoveryError(err, connect.CodeFailedPrecondition)
	}
	return connect.NewResponse(&nativev1.PublishSoftwareWorkManifestResponse{ManifestDigest: digest, Capability: state}), nil
}

func (s *Server) GetSoftwareWorkManifest(_ context.Context, request *connect.Request[nativev1.GetSoftwareWorkManifestRequest]) (*connect.Response[nativev1.GetSoftwareWorkManifestResponse], error) {
	if request == nil || request.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("manifest retrieval request is required"))
	}
	if err := s.authorizeDiscovery(request.Header().Get("Authorization"), request.Msg.Context, PermissionRead); err != nil {
		return nil, err
	}
	raw, err := s.Catalog.Manifest(request.Msg.ManifestDigest)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("software-work manifest was not found"))
		}
		return nil, discoveryError(err, connect.CodeFailedPrecondition)
	}
	return connect.NewResponse(&nativev1.GetSoftwareWorkManifestResponse{ManifestDigest: request.Msg.ManifestDigest, CanonicalCbor: raw}), nil
}

func (s *Server) authorizeDiscovery(header string, requestContext *nativev1.RequestContext, required Permission) error {
	if err := s.authorize(header, required); err != nil {
		return err
	}
	if s.Catalog == nil {
		return connect.NewError(connect.CodeUnavailable, errors.New("Capability discovery is unavailable"))
	}
	now := time.Now()
	if requestContext == nil || requestContext.RequestId == "" || len(requestContext.RequestId) > 128 ||
		requestContext.CallerId == "" || len(requestContext.CallerId) > 256 ||
		requestContext.DeadlineUnixMillis <= now.UnixMilli() ||
		requestContext.DeadlineUnixMillis > now.Add(15*time.Minute).UnixMilli() {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("valid bounded discovery request context is required"))
	}
	return nil
}

func discoveryError(err error, fallback connect.Code) error {
	if err == nil {
		return nil
	}
	if code := connect.CodeOf(err); code != connect.CodeUnknown {
		return err
	}
	return connect.NewError(fallback, err)
}
