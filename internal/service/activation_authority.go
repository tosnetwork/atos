package service

import (
	"context"

	"github.com/tosnetwork/atos/internal/domain"
)

// FailClosedActivationAuthority is the only domain.ActivationAuthority
// implementation wired into production configuration (see
// cmd/api/main.go): it never grants verified/native activation, because
// Phase 4 has not shipped a real TOS-backed activation authority yet. It
// always returns domain.ActivationAuthorityUnavailable. See atos-spec
// docs/IMPLEMENTATION_ROADMAP.md §7.2.1.
type FailClosedActivationAuthority struct{}

func (FailClosedActivationAuthority) Evaluate(ctx context.Context, providerID, capabilityID, capabilityVersion string, mode domain.TrustMode) (granted bool, reasonCode string, err error) {
	return false, domain.ActivationAuthorityUnavailable, nil
}
