package mcp

import (
	"context"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

func (s *Server) toolOpenCertification(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	idempotencyKey := argString(args, "idempotency_key")
	if idempotencyKey == "" {
		return nil, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}
	return s.Certifications.Open(ctx, service.OpenCertificationInput{
		ProviderID: principal.ID, CapabilityID: argString(args, "capability_id"),
		Transport: domain.EndpointAdapterType(argString(args, "transport")), IdempotencyKey: idempotencyKey,
	})
}

func (s *Server) toolGetCertificationStatus(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	return s.Certifications.PublicStatus(ctx, principal.ID, argString(args, "capability_id"))
}
