// Package toscore defines the ATOS/tos-ai -> tos-protocol trust, economy and
// proof boundary. Execution itself belongs behind internal/adapters/tosai.
package toscore

import (
	"context"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
)

type CreateEscrowRequest struct {
	Quote             domain.Quote
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

type GetEscrowRequest struct {
	Quote                     domain.Quote
	JobID                     string
	EscrowID                  string
	ExpectedReservationDigest string
	ExpectedEscrowRef         string
}

type ReleaseEscrowRequest struct {
	Quote      domain.Quote
	JobID      string
	EscrowID   string
	ReasonCode string
}
type ReleaseEscrowResult struct {
	Escrow  domain.Escrow
	Receipt domain.Receipt
}

type VerifyExecutionReceiptResult struct {
	Valid               bool
	Reason              string
	ProofRef            string
	Network             string
	Finalized           bool
	FinalizedCheckpoint uint64
}

type CanonicalEvidence struct {
	Network, Reference, Digest string
	Finalized                  bool
	FinalizedCheckpoint        uint64
}
type ProofOfServiceEvidence struct {
	EvidenceID, ReceiptID, ProviderID, CapabilityID, CapabilityVersion, ContentDigest string
	CanonicalEvidence
}
type PortableReceiptEvidence struct {
	CanonicalCBOR []byte
	Digest        string
}

type SettleJobRequest struct {
	EscrowID   string
	JobID      string
	ReceiptID  string
	ActualCost domain.Money
	Quote      domain.Quote
}

type SettleJobResult struct {
	Receipt domain.Receipt
}

type ExecutionSignerAuthorization struct {
	AuthorizationID     string
	ProviderID          string
	CapabilityID        string
	CapabilityVersion   string
	ExecutionSignerID   string
	ValidFrom           time.Time
	ValidUntil          time.Time
	AuthorizationRef    string
	Revoked             bool
	RevocationRef       string
	SignerPublicKey     []byte
	SignatureAlgorithm  string
	FinalizedCheckpoint uint64
}

type QuoteCommitment struct {
	Quote               domain.Quote
	Network             string
	Reference           string
	Digest              string
	ExpectedDigest      string
	Finalized           bool
	FinalizedCheckpoint uint64
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
	// Network is this deployment's configured TOS network identity, as
	// reported by the underlying connection (empty for the mock/dev
	// backend, which never anchors anything to a real network). Phase 4A's
	// TOSBackedActivationAuthority rejects activation when this is empty --
	// an unconfigured network must fail closed, never be treated as a
	// wildcard that matches any provider identity binding.
	Network() string
	// Identity and capability trust facts.
	ResolveAgent(ctx context.Context, principalID string) (agentID string, err error)
	ResolveCapability(ctx context.Context, capabilityID string) (domain.Trust, error)
	ReadReputation(ctx context.Context, providerID string) (domain.Trust, error)
	// CommitCapabilityManifest anchors capability's exact
	// manifest/version/ownership commitment (docs/CAPABILITIES.md §11) --
	// idempotent and safe to call on every Register/Update, mirroring the
	// remote service's own CommitCapabilityManifest idempotency (a replay
	// with identical provider_id/manifest digest returns the existing
	// commitment; a conflicting replay under the same capability_id+version
	// errors). ownershipRef is opaque "network:reference" (this codebase's
	// standing convention for TOS reference fields).
	CommitCapabilityManifest(ctx context.Context, capability domain.Capability) (ownershipRef string, err error)
	// VerifyCapabilityOwnership checks that providerID owns capabilityID
	// at EXACTLY version (never "whatever is current now" -- a Job/Quote
	// executing against an older version, or a re-anchoring race with a
	// concurrent Update, both need to verify a specific historical
	// version's commitment, not silently default to latest) and, when
	// expectedManifestDigest is non-empty ("sha256:<hex>", matching
	// capabilityManifestCommitment's format), that it matches the digest
	// anchored for that exact version -- this is the manifest/version
	// TOCTOU check Phase 4A's ActivationAuthority requires (a provider
	// mutating a capability after committing must not keep a stale
	// activation valid). version="" means "whatever is currently latest",
	// matching the remote service's own version-defaulting convention.
	// reasonCode is non-empty whenever verified is false
	// (NOT_FOUND/PROVIDER_MISMATCH/MANIFEST_MISMATCH).
	VerifyCapabilityOwnership(ctx context.Context, capabilityID, providerID, version, expectedManifestDigest string) (verified bool, reasonCode string, err error)
	ResolveCapabilityOwnershipEvidence(ctx context.Context, capabilityID, providerID, version, expectedManifestDigest string) (CanonicalEvidence, bool, error)
	UpdateReputationEvidence(ctx context.Context, providerID string, evidence string) error

	// Phase 4A identity binding (docs/IMPLEMENTATION_ROADMAP.md §8.1).
	// Deliberately separate from ResolveAgent above, which silently falls
	// back to treating principal_id itself as the agent identity for
	// Managed-compatible callers -- a Phase 4A authority decision must
	// never accept that fallback silently, so it uses
	// ResolvePrincipalBindingStatus instead.
	ResolvePrincipalBindingStatus(ctx context.Context, principalID string) (binding domain.PrincipalIdentityBinding, bound, revoked bool, revocationReasonCode string, err error)
	CreatePrincipalBinding(ctx context.Context, callerID, idempotencyKey, principalID, agentID string) (domain.PrincipalIdentityBinding, bool, error)
	RevokePrincipalBinding(ctx context.Context, callerID, idempotencyKey, principalID, reasonCode string) (revoked bool, revocationNetwork, revocationRef string, err error)

	// Quote and execution-signer trust.
	CommitQuote(ctx context.Context, quote domain.Quote) (QuoteCommitment, error)
	GetQuoteCommitment(ctx context.Context, quote domain.Quote) (QuoteCommitment, bool, error)
	ResolveExecutionSignerAuthorization(ctx context.Context, providerID, capabilityID, capabilityVersion, signerID string, at time.Time) (ExecutionSignerAuthorization, bool, error)
	// AuthorizeExecutionSigner and RevokeExecutionSigner are the only path
	// permitted to mutate trust-side signer state -- ordinary ATOS provider
	// business logic MUST NOT write it directly (atos-spec
	// docs/IMPLEMENTATION_ROADMAP.md §7.2.2). created/revoked mirror
	// tos-protocol's own AuthorizeExecutionSignerResponse.created /
	// RevokeExecutionSignerResponse.revoked, but neither is a reliable "did
	// THIS call do the work" signal: a literal retry with the same
	// caller_id+idempotency_key replays the original response verbatim (so
	// created keeps whatever value the original call set, not necessarily
	// false), and RevokeExecutionSigner in particular always reports
	// revoked=true once the signer is revoked, by any call -- there is no
	// revoked=false path once a signer has ever been revoked. Verified
	// directly against a real tos-protocol server, not only the Go-interface
	// mock, by internal/adapters/tosprotocol/signer_integration_test.go. No
	// caller in this codebase currently branches on either value.
	AuthorizeExecutionSigner(ctx context.Context, req AuthorizeExecutionSignerRequest) (authorization ExecutionSignerAuthorization, created bool, err error)
	RevokeExecutionSigner(ctx context.Context, req RevokeExecutionSignerRequest) (authorization ExecutionSignerAuthorization, revoked bool, err error)

	// Escrow, receipt and settlement.
	CreateEscrow(ctx context.Context, req CreateEscrowRequest) (domain.Escrow, error)
	GetEscrow(ctx context.Context, req GetEscrowRequest) (domain.Escrow, bool, error)
	ReleaseEscrow(ctx context.Context, req ReleaseEscrowRequest) (ReleaseEscrowResult, error)
	CommitExecutionReceipt(ctx context.Context, receipt domain.ExecutionReceipt) (proofRef string, err error)
	VerifyExecutionReceipt(ctx context.Context, escrowID string, receipt domain.ExecutionReceipt) (VerifyExecutionReceiptResult, error)
	PortableReceiptEvidence(context.Context, domain.ExecutionReceipt) (PortableReceiptEvidence, error)
	ResolveExecutionReceiptEvidence(context.Context, domain.ExecutionReceipt) (CanonicalEvidence, bool, error)
	SettleJob(ctx context.Context, req SettleJobRequest) (SettleJobResult, error)
	CommitProofOfServiceEvidence(ctx context.Context, receipt domain.ExecutionReceipt) (evidenceRef string, err error)
	ReadProofOfServiceEvidence(ctx context.Context, receipt domain.ExecutionReceipt) (ProofOfServiceEvidence, bool, error)
	ReadSettlementStatus(ctx context.Context, escrowID string) (domain.EscrowStatus, error)
	ReadProof(ctx context.Context, receiptID string) (map[string]any, error)
}
