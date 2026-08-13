// Package nativegateway exposes the stateless atos_native_v1 transport.
// Authentication grants transport access only; it never decides Native state.
package nativegateway

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"

	"connectrpc.com/connect"
	nativev1 "github.com/tosnetwork/tos-protocol/gen/atos/native/v1"
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
