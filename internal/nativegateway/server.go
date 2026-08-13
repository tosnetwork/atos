// Package nativegateway exposes the one-mode atos_native_v1 transport. It is
// deliberately separate from the hosted legacy mode APIs and storage.
package nativegateway

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/auth"
	nativev1 "github.com/tosnetwork/tos-protocol/gen/atos/native/v1"
)

type Backend interface {
	SubmitNativeAction(context.Context, *connect.Request[nativev1.SubmitNativeActionRequest]) (*connect.Response[nativev1.SubmitNativeActionResponse], error)
	ResolveNativeState(context.Context, *connect.Request[nativev1.ResolveNativeStateRequest]) (*connect.Response[nativev1.ResolveNativeStateResponse], error)
}

type Server struct {
	Auth    *auth.Service
	Backend Backend
}

func (s *Server) SubmitNativeAction(ctx context.Context, request *connect.Request[nativev1.SubmitNativeActionRequest]) (*connect.Response[nativev1.SubmitNativeActionResponse], error) {
	if err := s.authorize(request.Header().Get("Authorization"), auth.ScopeNativeRelay); err != nil {
		return nil, err
	}
	if s.Backend == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("Native backend is unavailable"))
	}
	return s.Backend.SubmitNativeAction(ctx, request)
}

func (s *Server) ResolveNativeState(ctx context.Context, request *connect.Request[nativev1.ResolveNativeStateRequest]) (*connect.Response[nativev1.ResolveNativeStateResponse], error) {
	if err := s.authorize(request.Header().Get("Authorization"), auth.ScopeNativeRead); err != nil {
		return nil, err
	}
	if s.Backend == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("Native backend is unavailable"))
	}
	return s.Backend.ResolveNativeState(ctx, request)
}

func (s *Server) authorize(header string, required auth.Scope) error {
	if s == nil || s.Auth == nil {
		return connect.NewError(connect.CodeUnavailable, errors.New("gateway authentication is unavailable"))
	}
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok || strings.TrimSpace(token) == "" {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("missing gateway bearer token"))
	}
	principal, err := s.Auth.Authenticate(strings.TrimSpace(token))
	if err != nil {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("invalid gateway bearer token"))
	}
	if !principal.HasAll(required) {
		return connect.NewError(connect.CodePermissionDenied, errors.New("gateway token lacks Native transport scope"))
	}
	return nil
}
