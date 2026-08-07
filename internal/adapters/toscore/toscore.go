// Package toscore defines the trust/economy interface contract from
// ~/atos-spec/docs/ARCHITECTURE.md: identity, registry, reputation,
// escrow, receipt verification, settlement and proof. Nothing about
// execution belongs here — see internal/adapters/tosai for that half.
//
// Two call directions share this interface per the spec: tos-ai -> tos-core
// (receipt lifecycle) and ATOS -> tos-core (direct trust reads/writes). A
// single interface is fine here because both callers live in this same
// process for Phase 0/1 — split them if/when tos-ai becomes a separate
// service in Phase 2+.
package toscore

import (
	"context"

	"github.com/tosnetwork/atos/internal/domain"
)

type CreateEscrowRequest struct {
	QuoteID      string
	CapabilityID string
	PrincipalID  string
	ProviderID   string
	Reserved     domain.Money
}

// VerifyExecutionReceiptResult is intentionally the *only* thing
// VerifyExecutionReceipt returns beyond an error: a stateless pass/fail
// judgment. It must never carry a side effect — see docs/SETTLEMENT.md's
// verify/apply separation.
type VerifyExecutionReceiptResult struct {
	Valid  bool
	Reason string // populated when Valid is false
}

type SettleJobRequest struct {
	EscrowID string
	JobID    string
	// ActualCost is what the job really cost per the verified receipt; it
	// may be less than the escrow's Reserved amount (metered/per_unit
	// pricing), never more.
	ActualCost domain.Money
}

type SettleJobResult struct {
	Receipt domain.Receipt
}

type Core interface {
	// Identity / registry / reputation (ATOS -> tos-core direct reads).
	ResolveAgent(ctx context.Context, principalID string) (agentID string, err error)
	ResolveCapability(ctx context.Context, capabilityID string) (domain.Trust, error)
	ReadReputation(ctx context.Context, providerID string) (domain.Trust, error)
	VerifyCapabilityOwnership(ctx context.Context, capabilityID, providerID string) (bool, error)
	UpdateReputationEvidence(ctx context.Context, providerID string, evidence string) error

	// Escrow / settlement (both call directions).
	CreateEscrow(ctx context.Context, req CreateEscrowRequest) (domain.Escrow, error)
	ReleaseEscrow(ctx context.Context, escrowID string) (domain.Receipt, error)
	VerifyExecutionReceipt(ctx context.Context, escrowID string, receipt domain.ExecutionReceipt) (VerifyExecutionReceiptResult, error)
	SettleJob(ctx context.Context, req SettleJobRequest) (SettleJobResult, error)
	ReadSettlementStatus(ctx context.Context, escrowID string) (domain.EscrowStatus, error)
	ReadProof(ctx context.Context, receiptID string) (map[string]any, error)
}
