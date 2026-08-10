package mcp

import (
	"context"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
)

// evaluateActivationResult mirrors httpapi's evaluateActivationResponse
// (atos-spec docs/API.md §2.2): granted false is a normal result, never an
// MCP isError:true outcome.
type evaluateActivationResult struct {
	Granted     bool                    `json:"granted"`
	ReasonCode  string                  `json:"reason_code,omitempty"`
	ModeSupport domain.ModeSupportEntry `json:"mode_support"`
}

func (s *Server) toolEvaluateActivation(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	mode := domain.TrustMode(argString(args, "mode"))
	if mode != domain.TrustModeVerified && mode != domain.TrustModeNative {
		return nil, domain.NewError(domain.ErrValidationFailed, "mode must be verified or native", false)
	}
	capabilityID := argString(args, "capability_id")
	granted, reasonCode, err := s.Capabilities.EvaluateActivation(ctx, s.ActivationAuthority, capabilityID, mode)
	if err != nil {
		return nil, err
	}
	cap, err := s.Capabilities.Get(ctx, capabilityID)
	if err != nil {
		return nil, err
	}
	return evaluateActivationResult{Granted: granted, ReasonCode: reasonCode, ModeSupport: cap.ModeSupport.Entry(mode)}, nil
}
