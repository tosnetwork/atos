package mcp

import (
	"context"
	"encoding/base64"
	"strings"
	"time"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// decodeSignerPublicKeyArg mirrors httpapi's decodeSignerPublicKey exactly
// -- both packages independently translate the same public wire contract
// (docs/API.md §2.1's "base64:..." form) into the shared
// service.AuthorizeSignerInput/RotateSignerInput, the same way every
// other dual REST/MCP surface in this repository already does.
func decodeSignerPublicKeyArg(args map[string]any) ([]byte, error) {
	raw := strings.TrimPrefix(argString(args, "signer_public_key"), "base64:")
	if raw == "" {
		return nil, domain.NewError(domain.ErrValidationFailed, "signer_public_key is required", false)
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, domain.NewError(domain.ErrValidationFailed, "signer_public_key must be base64-encoded", false)
	}
	return decoded, nil
}

const defaultSignerValidity = 365 * 24 * time.Hour

func signerValidityWindowArg(args map[string]any) (time.Time, time.Time, error) {
	from := time.Now().UTC()
	if raw := argString(args, "valid_from"); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return time.Time{}, time.Time{}, domain.NewError(domain.ErrValidationFailed, "valid_from must be RFC3339", false)
		}
		from = parsed.UTC()
	}
	until := from.Add(defaultSignerValidity)
	if raw := argString(args, "valid_until"); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return time.Time{}, time.Time{}, domain.NewError(domain.ErrValidationFailed, "valid_until must be RFC3339", false)
		}
		until = parsed.UTC()
	}
	return from, until, nil
}

// checkSignerCapabilityVersionArg mirrors httpapi's identically-named
// method exactly -- see its doc comment.
func (s *Server) checkSignerCapabilityVersionArg(ctx context.Context, capabilityID string, args map[string]any) error {
	asserted := argString(args, "capability_version")
	if asserted == "" {
		return nil
	}
	cap, err := s.Capabilities.Get(ctx, capabilityID)
	if err != nil {
		return err
	}
	if cap.Version != asserted {
		return domain.NewError(domain.ErrValidationFailed, "capability_version does not match the capability's current version", false)
	}
	return nil
}

func (s *Server) toolAuthorizeExecutionSigner(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	capabilityID := argString(args, "capability_id")
	if err := s.checkSignerCapabilityVersionArg(ctx, capabilityID, args); err != nil {
		return nil, err
	}
	publicKey, err := decodeSignerPublicKeyArg(args)
	if err != nil {
		return nil, err
	}
	validFrom, validUntil, err := signerValidityWindowArg(args)
	if err != nil {
		return nil, err
	}
	if _, err := s.ExecutionSigners.Authorize(ctx, service.AuthorizeSignerInput{
		ProviderID: principal.ID, CapabilityID: capabilityID,
		ExecutionSignerID: argString(args, "execution_signer_id"), SignerPublicKey: publicKey,
		SignatureAlgorithm: argString(args, "signature_algorithm"), ValidFrom: validFrom, ValidUntil: validUntil,
		IdempotencyKey: argString(args, "idempotency_key"),
	}); err != nil {
		return nil, err
	}
	return s.ExecutionSigners.PublicStatus(ctx, principal.ID, capabilityID)
}

func (s *Server) toolRotateExecutionSigner(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	capabilityID := argString(args, "capability_id")
	if err := s.checkSignerCapabilityVersionArg(ctx, capabilityID, args); err != nil {
		return nil, err
	}
	publicKey, err := decodeSignerPublicKeyArg(args)
	if err != nil {
		return nil, err
	}
	validFrom, validUntil, err := signerValidityWindowArg(args)
	if err != nil {
		return nil, err
	}
	if _, err := s.ExecutionSigners.Rotate(ctx, service.RotateSignerInput{
		ProviderID: principal.ID, CapabilityID: capabilityID,
		NewExecutionSignerID: argString(args, "execution_signer_id"), NewSignerPublicKey: publicKey,
		NewSignatureAlgorithm: argString(args, "signature_algorithm"), NewValidFrom: validFrom, NewValidUntil: validUntil,
		RevocationReasonCode: argString(args, "reason_code"), IdempotencyKey: argString(args, "idempotency_key"),
	}); err != nil {
		return nil, err
	}
	return s.ExecutionSigners.PublicStatus(ctx, principal.ID, capabilityID)
}

func (s *Server) toolRevokeExecutionSigner(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	capabilityID := argString(args, "capability_id")
	if err := s.checkSignerCapabilityVersionArg(ctx, capabilityID, args); err != nil {
		return nil, err
	}
	if _, err := s.ExecutionSigners.Revoke(ctx, service.RevokeSignerInput{
		ProviderID: principal.ID, CapabilityID: capabilityID,
		ReasonCode: argString(args, "reason_code"), IdempotencyKey: argString(args, "idempotency_key"),
	}); err != nil {
		return nil, err
	}
	return s.ExecutionSigners.PublicStatus(ctx, principal.ID, capabilityID)
}

func (s *Server) toolGetExecutionSignerStatus(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	return s.ExecutionSigners.PublicStatus(ctx, principal.ID, argString(args, "capability_id"))
}
