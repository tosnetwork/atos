// Package mock is a synchronous in-process stand-in for tos-protocol/tos-core.
// It enforces the v0.2 state and binding invariants but deliberately does not
// claim that Managed transactions are TOS-verifiable.
package mock

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
)

const settlementDecimals = 2

type Core struct {
	mu        sync.Mutex
	network   string
	store     store.Store
	verified  map[string]domain.ExecutionReceipt
	quotes    map[string]domain.Quote
	modes     map[domain.TrustMode]bool
	simulated bool
	// signers is keyed by AuthorizationID, mirroring tos-protocol's own
	// idempotency key for AuthorizeExecutionSigner/RevokeExecutionSigner --
	// a real stateful registry (not the old always-authorized stub) so
	// Phase 3B's signer lifecycle/crash-recovery tests can exercise
	// meaningful authorize/revoke/idempotency-conflict behavior against
	// this mock without needing the real tos-protocol RPC server.
	signers map[string]toscore.ExecutionSignerAuthorization
	// agentIdentities simulates the remote service's identity store: an
	// agent_id must be "seeded" (present here) before CreatePrincipalBinding
	// will bind to it, mirroring the real server's identical requirement.
	agentIdentities map[string]bool
	// principalBindings/revokedBindings mirror the remote service's own
	// current-bindings/revocation-history split exactly, for the same
	// "never bound" vs "bound then revoked" distinction.
	principalBindings map[string]domain.PrincipalIdentityBinding
	// principalBindingCreationKey records which idempotency_key ORIGINALLY
	// created each current binding, so a crash-recovery retry (driveBind
	// re-calling CreatePrincipalBinding with the SAME key after a crash
	// between the mock's write and the caller's own checkpoint advance)
	// can honestly report created=true, matching the real server's
	// atomicMutation cache (which replays the ORIGINAL response rather
	// than re-deriving it from current state) instead of always reporting
	// created=false merely because a binding now exists.
	principalBindingCreationKey map[string]string
	revokedBindings             map[string]revokedBindingRecord
	// manifestCommitments is keyed by "capability_id@version", mirroring
	// the remote service's own capability-key bucketing for
	// CommitCapabilityManifest.
	manifestCommitments map[string]manifestCommitmentRecord
}

type manifestCommitmentRecord struct {
	providerID     string
	manifestDigest string
	ownershipRef   string
}

// revokedBindingRecord mirrors the remote service's own revocation record:
// storing the ref at write time (not recomputing on replay) so a retry
// with a DIFFERENT idempotency_key -- the documented safe lost-response
// retry pattern -- honestly replays the ORIGINAL revocation_ref, matching
// the real server's own RevokePrincipalBinding behavior.
type revokedBindingRecord struct {
	ReasonCode string
	Network    string
	Ref        string
}

func New(s store.Store) *Core {
	return newCore(s, false, domain.TrustModeManaged)
}

// NewContractFixture creates a deliberately simulated Core for Phase 0
// contract/conformance tests. It never becomes part of runtime composition and
// its simulated references are not TOS proofs.
func NewContractFixture(s store.Store) *Core {
	return newCore(s, true, domain.TrustModeManaged, domain.TrustModeVerified, domain.TrustModeNative)
}

func newCore(s store.Store, simulated bool, modes ...domain.TrustMode) *Core {
	allowed := make(map[domain.TrustMode]bool, len(modes))
	for _, mode := range modes {
		allowed[mode] = true
	}
	return &Core{
		store: s, verified: make(map[string]domain.ExecutionReceipt),
		quotes: make(map[string]domain.Quote), modes: allowed, simulated: simulated,
		signers:                     make(map[string]toscore.ExecutionSignerAuthorization),
		agentIdentities:             make(map[string]bool),
		principalBindings:           make(map[string]domain.PrincipalIdentityBinding),
		principalBindingCreationKey: make(map[string]string),
		revokedBindings:             make(map[string]revokedBindingRecord),
		manifestCommitments:         make(map[string]manifestCommitmentRecord),
	}
}

// Network returns the mock's configured network -- empty unless
// SetNetwork was called, matching the real client's "empty means
// unconfigured, never a wildcard" contract.
func (c *Core) Network() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.network
}

// SetNetwork configures the mock's reported network for tests exercising
// TOSBackedActivationAuthority's network-binding check.
func (c *Core) SetNetwork(network string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.network = network
}

// CommitCapabilityManifest simulates the remote service's idempotent
// manifest commitment: identical (capability_id, version, provider_id, manifest
// digest) replays the same ownershipRef; a conflicting replay under the
// same capability_id+version errors, mirroring the real
// CommitCapabilityManifest's ALREADY_EXISTS behavior.
func (c *Core) CommitCapabilityManifest(ctx context.Context, capability domain.Capability) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if capability.ID == "" || capability.ProviderID == "" || capability.Version == "" || capability.ManifestCommitment == "" {
		return "", domain.NewError(domain.ErrValidationFailed, "capability_id, provider_id, version and manifest_commitment are required", false)
	}
	key := capability.ID + "@" + capability.Version
	if existing, ok := c.manifestCommitments[key]; ok {
		if existing.providerID != capability.ProviderID || existing.manifestDigest != capability.ManifestCommitment {
			return "", domain.NewError(domain.ErrIdempotencyConflict, "capability version is already committed with different ownership or manifest", false)
		}
		return existing.ownershipRef, nil
	}
	ref := simulatedRef("capability-ownership", domain.TrustModeManaged, key)
	c.manifestCommitments[key] = manifestCommitmentRecord{providerID: capability.ProviderID, manifestDigest: capability.ManifestCommitment, ownershipRef: ref}
	return ref, nil
}

// SeedAgentIdentity registers agentID as an existing TOS Agent Identity,
// mirroring the remote service's identity bootstrap path -- tests must call
// this before CreatePrincipalBinding will succeed, exactly like the real
// server requires an identity to already resolve before it can be bound.
func (c *Core) SeedAgentIdentity(agentID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.agentIdentities[agentID] = true
}

func (c *Core) supports(mode domain.TrustMode) bool {
	return c != nil && c.modes[mode]
}

func simulatedRef(kind string, mode domain.TrustMode, id string) string {
	return "simulated:atos-v0.2:" + string(mode) + ":" + kind + ":" + id
}

func (c *Core) ResolveAgent(ctx context.Context, principalID string) (string, error) {
	return principalID, nil
}

func (c *Core) ResolvePrincipalBindingStatus(ctx context.Context, principalID string) (domain.PrincipalIdentityBinding, bool, bool, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.principalBindings[principalID]; ok {
		return b, true, false, "", nil
	}
	if record, ok := c.revokedBindings[principalID]; ok {
		return domain.PrincipalIdentityBinding{}, false, true, record.ReasonCode, nil
	}
	return domain.PrincipalIdentityBinding{}, false, false, "", nil
}

func (c *Core) CreatePrincipalBinding(ctx context.Context, callerID, idempotencyKey, principalID, agentID string) (domain.PrincipalIdentityBinding, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.agentIdentities[agentID] {
		return domain.PrincipalIdentityBinding{}, false, domain.NewError(domain.ErrNotFound, "agent identity does not exist; it must be established before it can be bound", false)
	}
	if existing, ok := c.principalBindings[principalID]; ok {
		if existing.AgentID != agentID {
			return domain.PrincipalIdentityBinding{}, false, domain.NewError(domain.ErrIdempotencyConflict, "principal is already bound to a different TOS Agent Identity; revoke the existing binding first", false)
		}
		// Replay the ORIGINAL outcome for the exact key that created this
		// binding (a crash-recovery retry with the SAME idempotency_key
		// after a crash between this write and the caller's own checkpoint
		// advance) -- any OTHER key naming the same principal+agent is a
		// genuine no-op rebind, correctly created=false.
		created := c.principalBindingCreationKey[principalID] == idempotencyKey
		return existing, created, nil
	}
	binding := domain.PrincipalIdentityBinding{
		PrincipalID: principalID, AgentID: agentID, Network: c.network,
		BindingRef: simulatedRef("principal-binding", domain.TrustModeManaged, principalID+":"+agentID),
	}
	c.principalBindings[principalID] = binding
	c.principalBindingCreationKey[principalID] = idempotencyKey
	delete(c.revokedBindings, principalID)
	return binding, true, nil
}

func (c *Core) RevokePrincipalBinding(ctx context.Context, callerID, idempotencyKey, principalID, reasonCode string) (bool, string, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.principalBindings[principalID]; !ok {
		// No active binding to revoke -- but if this principal was ALREADY
		// revoked (e.g. a lost-response retry under a fresh idempotency_key,
		// the documented safe retry pattern), honestly replay the ORIGINAL
		// revocation_ref rather than reporting revoked=false, matching the
		// real remote server's RevokePrincipalBinding exactly: a
		// caller must see the same "revoked=true" fact regardless of which
		// attempt (original or retry) they're looking at.
		if record, ok := c.revokedBindings[principalID]; ok {
			return true, record.Network, record.Ref, nil
		}
		return false, "", "", nil
	}
	delete(c.principalBindings, principalID)
	ref := simulatedRef("principal-binding-revocation", domain.TrustModeManaged, principalID+":"+idempotencyKey)
	c.revokedBindings[principalID] = revokedBindingRecord{ReasonCode: reasonCode, Network: c.network, Ref: ref}
	return true, c.network, ref, nil
}

func (c *Core) ResolveCapability(ctx context.Context, capabilityID string) (domain.Trust, error) {
	cap, err := c.store.Get(ctx, capabilityID)
	if err != nil {
		return domain.Trust{}, err
	}
	return cap.Trust, nil
}

func (c *Core) ReadReputation(ctx context.Context, providerID string) (domain.Trust, error) {
	return domain.Trust{Score: 0.8, Level: domain.TrustSelfAsserted}, nil
}

// VerifyCapabilityOwnership mirrors the remote service's real semantics: a
// manifest digest can only verify if CommitCapabilityManifest actually
// anchored it first -- comparing against the capability's own
// locally-stored ManifestCommitment field (which Register/Update set
// unconditionally, anchored or not) would trivially "verify" a capability
// that was never anchored at all, defeating the whole point of the
// manifest/version TOCTOU check this method exists for.
func (c *Core) VerifyCapabilityOwnership(ctx context.Context, capabilityID, providerID, version, expectedManifestDigest string) (bool, string, error) {
	cap, err := c.store.Get(ctx, capabilityID)
	if errors.Is(err, store.ErrNotFound) {
		return false, "NOT_FOUND", nil
	}
	if err != nil {
		return false, "", err
	}
	if cap.ProviderID != providerID {
		return false, "PROVIDER_MISMATCH", nil
	}
	if version == "" {
		version = cap.Version
	}
	if expectedManifestDigest == "" {
		return true, "", nil
	}
	c.mu.Lock()
	commitment, found := c.manifestCommitments[capabilityID+"@"+version]
	c.mu.Unlock()
	if !found {
		return false, "MANIFEST_MISMATCH", nil
	}
	if commitment.providerID != providerID || commitment.manifestDigest != expectedManifestDigest {
		return false, "MANIFEST_MISMATCH", nil
	}
	return true, "", nil
}

func (c *Core) UpdateReputationEvidence(ctx context.Context, providerID string, evidence string) error {
	return nil
}

func (c *Core) CommitQuote(ctx context.Context, quote domain.Quote) (toscore.QuoteCommitment, error) {
	if !c.supports(quote.TrustMode) {
		return toscore.QuoteCommitment{}, domain.NewError(domain.ErrNetworkUnavailable, "mock tos-core is not configured for this trust mode", true)
	}
	if err := domain.ValidateCommittedTrust(quote.TrustMode, quote.ProofProfile); err != nil {
		return toscore.QuoteCommitment{}, err
	}
	c.mu.Lock()
	if existing, ok := c.quotes[quote.ID]; ok && !sameQuoteSemantics(existing, quote) {
		c.mu.Unlock()
		return toscore.QuoteCommitment{}, domain.NewError(domain.ErrIdempotencyConflict, "quote ID is already committed to different terms", false)
	}
	c.quotes[quote.ID] = quote
	c.mu.Unlock()
	if c.simulated && quote.TrustMode != domain.TrustModeManaged {
		return toscore.QuoteCommitment{Quote: quote, Network: quote.NetworkID, Reference: simulatedRef("quote", quote.TrustMode, quote.ID), Digest: quote.TermsHash, Finalized: true, FinalizedCheckpoint: 1}, nil
	}
	return toscore.QuoteCommitment{Quote: quote}, nil
}

func sameQuoteSemantics(a, b domain.Quote) bool {
	a.Commitment = nil
	b.Commitment = nil
	a.CreatedAt = time.Time{}
	b.CreatedAt = time.Time{}
	return reflect.DeepEqual(a, b)
}

func (c *Core) GetQuoteCommitment(_ context.Context, quoteID string) (toscore.QuoteCommitment, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	q, ok := c.quotes[quoteID]
	if !ok {
		return toscore.QuoteCommitment{}, false, nil
	}
	return toscore.QuoteCommitment{Quote: q, Network: q.NetworkID, Reference: simulatedRef("quote", q.TrustMode, q.ID), Digest: q.TermsHash, Finalized: true, FinalizedCheckpoint: 1}, true, nil
}

// ResolveExecutionSignerAuthorization scans the real, mutable signer
// registry populated by AuthorizeExecutionSigner/RevokeExecutionSigner --
// not a hardcoded always-authorized stub -- so a resolve genuinely reflects
// whether the signer was ever authorized, has since been revoked, and is
// within its validity window, exactly like tos-protocol's real
// implementation.
func (c *Core) ResolveExecutionSignerAuthorization(
	ctx context.Context,
	providerID, capabilityID, capabilityVersion, signerID string,
	at time.Time,
) (toscore.ExecutionSignerAuthorization, bool, error) {
	if signerID == "" {
		return toscore.ExecutionSignerAuthorization{}, false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, auth := range c.signers {
		if auth.ProviderID != providerID || auth.CapabilityID != capabilityID ||
			auth.CapabilityVersion != capabilityVersion || auth.ExecutionSignerID != signerID {
			continue
		}
		if auth.Revoked || at.Before(auth.ValidFrom) || !at.Before(auth.ValidUntil) {
			return toscore.ExecutionSignerAuthorization{}, false, nil
		}
		return auth, true, nil
	}
	return toscore.ExecutionSignerAuthorization{}, false, nil
}

// AuthorizeExecutionSigner mirrors tos-protocol's own idempotency
// contract: a replay of the same AuthorizationID with identical fields
// returns the existing record (created=false); a replay with the same
// AuthorizationID but a different field is an idempotency conflict.
func (c *Core) AuthorizeExecutionSigner(ctx context.Context, req toscore.AuthorizeExecutionSignerRequest) (toscore.ExecutionSignerAuthorization, bool, error) {
	if req.AuthorizationID == "" {
		return toscore.ExecutionSignerAuthorization{}, false, domain.NewError(domain.ErrValidationFailed, "authorization_id is required", false)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.signers[req.AuthorizationID]; ok {
		if existing.ProviderID != req.ProviderID || existing.CapabilityID != req.CapabilityID ||
			existing.CapabilityVersion != req.CapabilityVersion || existing.ExecutionSignerID != req.ExecutionSignerID ||
			!existing.ValidFrom.Equal(req.ValidFrom) || !existing.ValidUntil.Equal(req.ValidUntil) {
			return toscore.ExecutionSignerAuthorization{}, false, domain.NewError(domain.ErrIdempotencyConflict, "authorization_id reused with a different execution signer request", false)
		}
		return existing, false, nil
	}
	authorizationRef := "atos:mock:signer-auth:" + req.AuthorizationID
	if c.simulated {
		authorizationRef = simulatedRef("signer-auth", domain.TrustModeVerified, req.AuthorizationID)
	}
	auth := toscore.ExecutionSignerAuthorization{
		AuthorizationID: req.AuthorizationID, ProviderID: req.ProviderID,
		CapabilityID: req.CapabilityID, CapabilityVersion: req.CapabilityVersion,
		ExecutionSignerID: req.ExecutionSignerID,
		ValidFrom:         req.ValidFrom, ValidUntil: req.ValidUntil,
		AuthorizationRef: authorizationRef,
	}
	c.signers[req.AuthorizationID] = auth
	return auth, true, nil
}

// RevokeExecutionSigner marks the record identified by req.AuthorizationID
// revoked. Idempotent: revoking an already-revoked record returns the
// existing (already-revoked) record with revoked=false, mirroring
// tos-protocol's own "true only on first application" contract.
func (c *Core) RevokeExecutionSigner(ctx context.Context, req toscore.RevokeExecutionSignerRequest) (toscore.ExecutionSignerAuthorization, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, ok := c.signers[req.AuthorizationID]
	if !ok {
		return toscore.ExecutionSignerAuthorization{}, false, domain.NewError(domain.ErrNotFound, "execution signer authorization not found", false)
	}
	if existing.Revoked {
		return existing, false, nil
	}
	existing.Revoked = true
	existing.RevocationRef = "atos:mock:signer-revocation:" + req.AuthorizationID
	if c.simulated {
		existing.RevocationRef = simulatedRef("signer-revocation", domain.TrustModeVerified, req.AuthorizationID)
	}
	c.signers[req.AuthorizationID] = existing
	return existing, true, nil
}

func (c *Core) CreateEscrow(ctx context.Context, req toscore.CreateEscrowRequest) (domain.Escrow, error) {
	if req.TrustMode == "" {
		req.TrustMode = domain.TrustModeManaged
	}
	if !req.TrustMode.Valid() {
		return domain.Escrow{}, domain.NewError(domain.ErrQuoteModeMismatch, "escrow requires a concrete trust mode", false)
	}
	if !c.supports(req.TrustMode) {
		return domain.Escrow{}, domain.NewError(domain.ErrNetworkUnavailable, "mock tos-core is not configured for this trust mode", true)
	}
	if err := domain.ValidateCommittedTrust(req.TrustMode, req.ProofProfile); err != nil {
		return domain.Escrow{}, err
	}
	if existing, err := c.store.EscrowByJob(ctx, req.JobID); err == nil {
		if existing.QuoteID != req.QuoteID || existing.PrincipalID != req.PrincipalID ||
			existing.ProviderID != req.ProviderID || existing.CapabilityID != req.CapabilityID ||
			existing.CapabilityVersion != req.CapabilityVersion || existing.TrustMode != req.TrustMode ||
			existing.ProofProfile != req.ProofProfile || existing.Reserved != req.Reserved {
			return domain.Escrow{}, domain.NewError(domain.ErrIdempotencyConflict, "existing escrow does not match replayed request", false)
		}
		return existing, nil
	} else if err != store.ErrNotFound {
		return domain.Escrow{}, err
	}

	now := time.Now().UTC()
	e := domain.Escrow{
		ID:                "esc_" + uuid.NewString(),
		QuoteID:           req.QuoteID,
		JobID:             req.JobID,
		PrincipalID:       req.PrincipalID,
		ProviderID:        req.ProviderID,
		CapabilityID:      req.CapabilityID,
		CapabilityVersion: req.CapabilityVersion,
		TrustMode:         req.TrustMode,
		ProofProfile:      req.ProofProfile,
		Settlement:        req.Settlement,
		Reserved:          req.Reserved,
		Status:            domain.EscrowReserved,
		CreatedAt:         now,
		ExpiresAt:         now.Add(24 * time.Hour),
	}
	if c.simulated && req.TrustMode != domain.TrustModeManaged {
		e.NetworkProofRef = simulatedRef("escrow", req.TrustMode, e.ID)
	}
	if err := c.store.PutEscrow(ctx, e); err != nil {
		return domain.Escrow{}, err
	}
	return e, nil
}

func (c *Core) ReleaseEscrow(ctx context.Context, escrowID string) (domain.Receipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, err := c.store.GetEscrow(ctx, escrowID)
	if err != nil {
		return domain.Receipt{}, err
	}
	if e.Status == domain.EscrowReleased {
		if receipt, replayErr := c.store.ReceiptByJob(ctx, e.JobID); replayErr == nil && receipt.EscrowID == e.ID && receipt.Status == domain.ReceiptReleased {
			return receipt, nil
		}
	}
	if e.Status.Terminal() {
		return domain.Receipt{}, domain.NewError(domain.ErrSettlementFailed, "escrow already in a terminal state", false)
	}
	now := time.Now().UTC()
	e.Status = domain.EscrowReleased
	e.SettledAt = &now
	if e.TrustMode == domain.TrustModeManaged {
		e.NetworkProofRef = ""
	}
	if err := c.store.PutEscrow(ctx, e); err != nil {
		return domain.Receipt{}, err
	}
	delete(c.verified, escrowID)

	receipt := domain.Receipt{
		ID:           "rcpt_release_" + e.ID,
		QuoteID:      e.QuoteID,
		EscrowID:     e.ID,
		JobID:        e.JobID,
		PrincipalID:  e.PrincipalID,
		TrustMode:    e.TrustMode,
		ProofProfile: e.ProofProfile,
		Charged:      domain.Money{Amount: "0", Currency: e.Reserved.Currency},
		Refunded:     e.Reserved,
		Status:       domain.ReceiptReleased,
		ProofStatus:  domain.ProofNotRequired,
		CreatedAt:    now,
	}
	if err := c.store.PutReceipt(ctx, receipt); err != nil {
		return domain.Receipt{}, err
	}
	return receipt, nil
}

func (c *Core) CommitExecutionReceipt(ctx context.Context, receipt domain.ExecutionReceipt) (string, error) {
	if !c.supports(receipt.TrustMode) {
		return "", domain.NewError(domain.ErrNetworkUnavailable, "mock tos-core is not configured for this trust mode", true)
	}
	if c.simulated && receipt.TrustMode != domain.TrustModeManaged {
		return simulatedRef("receipt", receipt.TrustMode, receipt.ID), nil
	}
	return "", nil
}

func (c *Core) VerifyExecutionReceipt(ctx context.Context, escrowID string, receipt domain.ExecutionReceipt) (toscore.VerifyExecutionReceiptResult, error) {
	e, err := c.store.GetEscrow(ctx, escrowID)
	if err != nil {
		return toscore.VerifyExecutionReceiptResult{}, err
	}
	if e.Status.Terminal() {
		return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "escrow already terminal"}, nil
	}
	mismatches := []struct {
		ok     bool
		reason string
	}{
		{receipt.EscrowID == e.ID, "escrow mismatch"},
		{receipt.QuoteID == e.QuoteID, "quote mismatch"},
		{receipt.JobID == e.JobID, "job mismatch"},
		{receipt.ProviderID == e.ProviderID, "provider mismatch"},
		{receipt.CapabilityID == e.CapabilityID, "capability mismatch"},
		{receipt.CapabilityVersion == e.CapabilityVersion, "capability version mismatch"},
		{receipt.TrustMode == e.TrustMode, "trust mode mismatch"},
		{receipt.ProofProfile == e.ProofProfile, "proof profile mismatch"},
	}
	for _, check := range mismatches {
		if !check.ok {
			return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: check.reason}, nil
		}
	}
	if receipt.Signature == "" || receipt.OutputHash == "" || receipt.InputHash == "" {
		return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "receipt missing signature or input/output commitment"}, nil
	}
	if receipt.Result != "" && receipt.Result != domain.ExecutionSuccess {
		return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "receipt is not successful"}, nil
	}
	// Managed never requires TOS-backed execution-signer authorization --
	// its receipts use an ATOS-self-signed synthetic signer identity (see
	// internal/service/job.go's synthesizeDeliveredReceipt and
	// internal/adapters/tosai/dispatch's synthesizeReceipt), never a
	// signer this registry (or the real tos-protocol TrustService) was
	// ever asked to authorize. Only Verified/Native require a real,
	// resolvable authorization (atos-spec docs/IMPLEMENTATION_ROADMAP.md
	// §7.2's Managed-stability rule).
	if receipt.TrustMode != domain.TrustModeManaged {
		auth, authorized, err := c.ResolveExecutionSignerAuthorization(
			ctx, receipt.ProviderID, receipt.CapabilityID, receipt.CapabilityVersion,
			receipt.ExecutionSignerID, receipt.CompletedAt,
		)
		if err != nil {
			return toscore.VerifyExecutionReceiptResult{}, err
		}
		if !authorized {
			return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "execution signer is not authorized"}, nil
		}
		if receipt.SignerAuthorizationID != "" && receipt.SignerAuthorizationID != auth.AuthorizationID {
			return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "signer authorization mismatch"}, nil
		}
	}

	c.mu.Lock()
	c.verified[escrowID] = receipt
	c.mu.Unlock()
	proofRef := ""
	if c.simulated && receipt.TrustMode != domain.TrustModeManaged {
		proofRef = simulatedRef("receipt-verified", receipt.TrustMode, receipt.ID)
	}
	return toscore.VerifyExecutionReceiptResult{Valid: true, ProofRef: proofRef}, nil
}

// SettleJob is replayable: a lost response after a committed settlement
// returns the original terminal receipt instead of requiring the caller to
// guess whether the economic side effect happened.
func (c *Core) SettleJob(ctx context.Context, req toscore.SettleJobRequest) (toscore.SettleJobResult, error) {
	if receipt, err := c.store.ReceiptByJob(ctx, req.JobID); err == nil && receipt.EscrowID == req.EscrowID && receipt.Status == domain.ReceiptSettled {
		return toscore.SettleJobResult{Receipt: receipt}, nil
	}
	c.mu.Lock()
	verifiedReceipt, ok := c.verified[req.EscrowID]
	c.mu.Unlock()
	if !ok {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrSettlementFailed, "no verified receipt for this escrow", false)
	}
	if verifiedReceipt.JobID != req.JobID {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrSettlementFailed, "verified receipt belongs to a different job", false)
	}
	if req.ReceiptID != "" && verifiedReceipt.ID != "" && req.ReceiptID != verifiedReceipt.ID {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrSettlementFailed, "verified receipt id mismatch", false)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	e, err := c.store.GetEscrow(ctx, req.EscrowID)
	if err != nil {
		return toscore.SettleJobResult{}, err
	}
	if e.Status.Terminal() {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrSettlementFailed, "escrow already in a terminal state", false)
	}

	reserved, err := money.Parse(e.Reserved.Amount, e.Reserved.Currency, settlementDecimals)
	if err != nil {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrSettlementFailed, "invalid reserved amount: "+err.Error(), false)
	}
	charged, err := money.Parse(req.ActualCost.Amount, req.ActualCost.Currency, settlementDecimals)
	if err != nil {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrValidationFailed, "invalid actual cost: "+err.Error(), false)
	}
	if charged.Currency != reserved.Currency {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrSettlementFailed, "currency mismatch between escrow and actual cost", false)
	}
	if charged.Cmp(reserved) > 0 {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrSettlementFailed, "actual cost exceeds reserved escrow", false)
	}
	refunded, err := reserved.Sub(charged)
	if err != nil {
		return toscore.SettleJobResult{}, err
	}

	now := time.Now().UTC()
	e.Status = domain.EscrowSettled
	e.SettledAt = &now
	if err := c.store.PutEscrow(ctx, e); err != nil {
		return toscore.SettleJobResult{}, err
	}

	proofStatus := domain.ProofNotRequired
	if e.TrustMode != domain.TrustModeManaged {
		proofStatus = domain.ProofVerified
	}
	settlementReceiptID := "rcpt_settle_" + e.ID
	receipt := domain.Receipt{
		ID:                     settlementReceiptID,
		QuoteID:                e.QuoteID,
		EscrowID:               e.ID,
		JobID:                  req.JobID,
		PrincipalID:            e.PrincipalID,
		TrustMode:              e.TrustMode,
		ProofProfile:           e.ProofProfile,
		Charged:                req.ActualCost,
		Refunded:               domain.Money{Amount: refunded.String(), Currency: e.Reserved.Currency},
		Status:                 domain.ReceiptSettled,
		ProofStatus:            proofStatus,
		NetworkProofRef:        verifiedReceipt.NetworkProofRef,
		ExecutionSignerID:      verifiedReceipt.ExecutionSignerID,
		SignerAuthorizationRef: verifiedReceipt.SignerAuthorizationRef,
		InputCommitment:        verifiedReceipt.InputHash,
		OutputCommitment:       verifiedReceipt.OutputHash,
		UsageCommitment:        verifiedReceipt.UsageCommitment,
		CreatedAt:              now,
	}
	if c.simulated && e.TrustMode != domain.TrustModeManaged {
		receipt.NetworkProofRef = simulatedRef("settlement", e.TrustMode, settlementReceiptID)
	}
	if err := c.store.PutReceipt(ctx, receipt); err != nil {
		return toscore.SettleJobResult{}, err
	}
	delete(c.verified, req.EscrowID)
	return toscore.SettleJobResult{Receipt: receipt}, nil
}

func (c *Core) CommitProofOfServiceEvidence(ctx context.Context, receipt domain.ExecutionReceipt) (string, error) {
	if receipt.ID == "" {
		return "", domain.NewError(domain.ErrValidationFailed, "receipt_id is required for Proof-of-Service evidence", false)
	}
	if receipt.TrustMode == domain.TrustModeManaged {
		return "atos:managed:pos:" + receipt.ID, nil
	}
	if c.simulated && c.supports(receipt.TrustMode) {
		return simulatedRef("proof-of-service", receipt.TrustMode, receipt.ID), nil
	}
	return "", domain.NewError(domain.ErrNetworkUnavailable, "mock tos-core cannot commit portable Proof-of-Service evidence", true)
}

func (c *Core) ReadSettlementStatus(ctx context.Context, escrowID string) (domain.EscrowStatus, error) {
	e, err := c.store.GetEscrow(ctx, escrowID)
	if err != nil {
		return "", err
	}
	return e.Status, nil
}

func (c *Core) ReadProof(ctx context.Context, receiptID string) (map[string]any, error) {
	r, err := c.store.GetReceipt(ctx, receiptID)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"receipt_id":        r.ID,
		"escrow_id":         r.EscrowID,
		"trust_mode":        r.TrustMode,
		"proof_profile":     r.ProofProfile,
		"proof_status":      r.ProofStatus,
		"network_proof_ref": r.NetworkProofRef,
		"attested":          false,
		"note":              fmt.Sprintf("Phase 0 contract simulation; %s evidence is not a TOS network proof", r.TrustMode),
	}, nil
}
