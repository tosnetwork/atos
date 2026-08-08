// Package mock is a synchronous in-process stand-in for tos-protocol/tos-core.
// It enforces the v0.2 state and binding invariants but deliberately does not
// claim that Managed transactions are TOS-verifiable.
package mock

import (
	"context"
	"fmt"
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
	store     store.Store
	verified  map[string]domain.ExecutionReceipt
	quotes    map[string]domain.Quote
	modes     map[domain.TrustMode]bool
	simulated bool
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
	}
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

func (c *Core) VerifyCapabilityOwnership(ctx context.Context, capabilityID, providerID string) (bool, error) {
	cap, err := c.store.Get(ctx, capabilityID)
	if err != nil {
		return false, err
	}
	return cap.ProviderID == providerID, nil
}

func (c *Core) UpdateReputationEvidence(ctx context.Context, providerID string, evidence string) error {
	return nil
}

func (c *Core) CommitQuote(ctx context.Context, quote domain.Quote) (string, error) {
	if !c.supports(quote.TrustMode) {
		return "", domain.NewError(domain.ErrNetworkUnavailable, "mock tos-core is not configured for this trust mode", true)
	}
	if err := domain.ValidateCommittedTrust(quote.TrustMode, quote.ProofProfile); err != nil {
		return "", err
	}
	c.mu.Lock()
	c.quotes[quote.ID] = quote
	c.mu.Unlock()
	if c.simulated && quote.TrustMode != domain.TrustModeManaged {
		return simulatedRef("quote", quote.TrustMode, quote.ID), nil
	}
	return "", nil
}

func (c *Core) ResolveExecutionSignerAuthorization(
	ctx context.Context,
	providerID, capabilityID, capabilityVersion, signerID string,
	at time.Time,
) (toscore.ExecutionSignerAuthorization, bool, error) {
	if signerID == "" {
		return toscore.ExecutionSignerAuthorization{}, false, nil
	}
	if !c.supports(domain.TrustModeManaged) && !c.simulated {
		return toscore.ExecutionSignerAuthorization{}, false, nil
	}
	authorizationRef := "atos:managed:signer:" + signerID
	if c.simulated {
		authorizationRef = simulatedRef("signer", domain.TrustModeVerified, "auth_mock_"+capabilityID)
	}
	auth := toscore.ExecutionSignerAuthorization{
		AuthorizationID:   "auth_mock_" + capabilityID,
		ProviderID:        providerID,
		CapabilityID:      capabilityID,
		CapabilityVersion: capabilityVersion,
		ExecutionSignerID: signerID,
		ValidFrom:         at.Add(-24 * time.Hour),
		ValidUntil:        at.Add(24 * time.Hour),
		AuthorizationRef:  authorizationRef,
	}
	return auth, true, nil
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
		ID:           "rcpt_" + uuid.NewString(),
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

	c.mu.Lock()
	c.verified[escrowID] = receipt
	c.mu.Unlock()
	proofRef := ""
	if c.simulated && receipt.TrustMode != domain.TrustModeManaged {
		proofRef = simulatedRef("receipt-verified", receipt.TrustMode, receipt.ID)
	}
	return toscore.VerifyExecutionReceiptResult{Valid: true, ProofRef: proofRef}, nil
}

func (c *Core) SettleJob(ctx context.Context, req toscore.SettleJobRequest) (toscore.SettleJobResult, error) {
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
	settlementReceiptID := "rcpt_" + uuid.NewString()
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
