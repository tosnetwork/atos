// Package toscore defines the ATOS/tos-ai -> tos-protocol trust, economy and
// proof boundary. Execution itself belongs behind internal/adapters/tosai.
package toscore

import (
	"context"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

type CreateEscrowRequest struct {
	QuoteID           string
	JobID             string
	CapabilityID      string
	CapabilityVersion string
	PrincipalID       string
	ProviderID        string
	TrustMode         domain.TrustMode
	ProofProfile      domain.ProofProfile
	Settlement        domain.SettlementDescriptor
	Reserved          domain.Money
}

type VerifyExecutionReceiptResult struct {
	Valid    bool
	Reason   string
	ProofRef string
}

type SettleJobRequest struct {
	EscrowID   string
	JobID      string
	ReceiptID  string
	ActualCost domain.Money
}

type SettleJobResult struct {
	Receipt domain.Receipt
}

type ExecutionSignerAuthorization struct {
	AuthorizationID   string
	ProviderID        string
	CapabilityID      string
	CapabilityVersion string
	ExecutionSignerID string
	ValidFrom         time.Time
	ValidUntil        time.Time
	AuthorizationRef  string
	Revoked           bool
	RevocationRef     string
}

// AuthorizeExecutionSignerRequest mirrors
// atos.tos.v1.ExecutionSignerAuthorizationInput (already normative and
// version-bound in atos-spec's trust.proto). AuthorizationID is the stable
// idempotency identity ATOS generates once and durably persists before ever
// calling this method -- a retry with the same AuthorizationID and
// identical fields MUST return the same result (tos-protocol's own
// AuthorizeExecutionSigner is already idempotent on this key via its
// shared atomicMutation machinery); a retry with the same AuthorizationID
// and DIFFERENT fields is a semantic conflict.
type AuthorizeExecutionSignerRequest struct {
	AuthorizationID    string
	ProviderID         string
	CapabilityID       string
	CapabilityVersion  string
	ExecutionSignerID  string
	SignerPublicKey    []byte
	SignatureAlgorithm string
	ValidFrom          time.Time
	ValidUntil         time.Time
}

type RevokeExecutionSignerRequest struct {
	AuthorizationID string
	ProviderID      string
	ReasonCode      string
}

type Core interface {
	// Identity and capability trust facts.
	ResolveAgent(ctx context.Context, principalID string) (agentID string, err error)
	ResolveCapability(ctx context.Context, capabilityID string) (domain.Trust, error)
	ReadReputation(ctx context.Context, providerID string) (domain.Trust, error)
	VerifyCapabilityOwnership(ctx context.Context, capabilityID, providerID string) (bool, error)
	UpdateReputationEvidence(ctx context.Context, providerID string, evidence string) error

	// Quote and execution-signer trust.
	CommitQuote(ctx context.Context, quote domain.Quote) (proofRef string, err error)
	ResolveExecutionSignerAuthorization(ctx context.Context, providerID, capabilityID, capabilityVersion, signerID string, at time.Time) (ExecutionSignerAuthorization, bool, error)
	// AuthorizeExecutionSigner and RevokeExecutionSigner are the only path
	// permitted to mutate trust-side signer state -- ordinary ATOS provider
	// business logic MUST NOT write it directly (atos-spec
	// docs/IMPLEMENTATION_ROADMAP.md §7.2.2). created/revoked mirror
	// tos-protocol's own AuthorizeExecutionSignerResponse.created /
	// RevokeExecutionSignerResponse.revoked -- false on an idempotent
	// replay of an already-completed request, true on first application.
	AuthorizeExecutionSigner(ctx context.Context, req AuthorizeExecutionSignerRequest) (authorization ExecutionSignerAuthorization, created bool, err error)
	RevokeExecutionSigner(ctx context.Context, req RevokeExecutionSignerRequest) (authorization ExecutionSignerAuthorization, revoked bool, err error)

	// Escrow, receipt and settlement.
	CreateEscrow(ctx context.Context, req CreateEscrowRequest) (domain.Escrow, error)
	ReleaseEscrow(ctx context.Context, escrowID string) (domain.Receipt, error)
	CommitExecutionReceipt(ctx context.Context, receipt domain.ExecutionReceipt) (proofRef string, err error)
	VerifyExecutionReceipt(ctx context.Context, escrowID string, receipt domain.ExecutionReceipt) (VerifyExecutionReceiptResult, error)
	SettleJob(ctx context.Context, req SettleJobRequest) (SettleJobResult, error)
	CommitProofOfServiceEvidence(ctx context.Context, receipt domain.ExecutionReceipt) (evidenceRef string, err error)
	ReadSettlementStatus(ctx context.Context, escrowID string) (domain.EscrowStatus, error)
	ReadProof(ctx context.Context, receiptID string) (map[string]any, error)
}
