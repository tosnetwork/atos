// Package mock is a synchronous, in-process stand-in for tos-core: a local
// ledger that actually enforces the escrow state machine and the
// verify/settle phase separation from ~/atos-spec/docs/SETTLEMENT.md, just
// without any real cryptography or chain commitment behind it yet.
package mock

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
)

// settlementDecimals is the minor-unit precision assumed for every
// currency this Phase 0/1 mock handles. Real multi-currency decimal tables
// arrive alongside a real settlement backend.
const settlementDecimals = 2

type Core struct {
	mu    sync.Mutex
	store store.Store
	// verified tracks which (escrowID) have passed VerifyExecutionReceipt,
	// so SettleJob can refuse to run against an unverified receipt — this
	// is the enforcement point for the phase-separation rule.
	verified map[string]domain.ExecutionReceipt
}

func New(s store.Store) *Core {
	return &Core{store: s, verified: make(map[string]domain.ExecutionReceipt)}
}

func (c *Core) ResolveAgent(ctx context.Context, principalID string) (string, error) {
	// Phase 1/2: principal_id IS the agent identifier (see
	// docs/AUTH.md "Agent Identity Migration"). A real tos-core binding
	// replaces this 1:1 passthrough without changing the interface.
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
	// Phase 0: flat, optimistic reputation for every provider until real
	// evidence accumulates.
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
	// No-op store for Phase 0 — real reputation aggregation is Phase 3+.
	return nil
}

func (c *Core) CreateEscrow(ctx context.Context, req toscore.CreateEscrowRequest) (domain.Escrow, error) {
	now := time.Now().UTC()
	e := domain.Escrow{
		ID:           "esc_" + uuid.NewString(),
		QuoteID:      req.QuoteID,
		PrincipalID:  req.PrincipalID,
		ProviderID:   req.ProviderID,
		CapabilityID: req.CapabilityID,
		Reserved:     req.Reserved,
		Status:       domain.EscrowReserved,
		CreatedAt:    now,
		ExpiresAt:    now.Add(24 * time.Hour),
	}
	if err := c.store.PutEscrow(ctx, e); err != nil {
		return domain.Escrow{}, err
	}
	return e, nil
}

// ReleaseEscrow returns the full reserved amount to the client — the path
// taken by cancellation, expiry, or a client-favorable dispute resolution.
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
	if err := c.store.PutEscrow(ctx, e); err != nil {
		return domain.Receipt{}, err
	}
	// Clear any prior verification for this escrow — it can no longer be
	// settled, so a lingering verified entry would be dead state at best
	// and a confusing footgun at worst.
	delete(c.verified, escrowID)

	receipt := domain.Receipt{
		ID:        "rcpt_" + uuid.NewString(),
		QuoteID:   e.QuoteID,
		EscrowID:  e.ID,
		Charged:   domain.Money{Amount: "0", Currency: e.Reserved.Currency},
		Refunded:  e.Reserved,
		Status:    domain.ReceiptReleased,
		CreatedAt: now,
	}
	if err := c.store.PutReceipt(ctx, receipt); err != nil {
		return domain.Receipt{}, err
	}
	return receipt, nil
}

// VerifyExecutionReceipt is stateless and read-only by design (see
// docs/SETTLEMENT.md): it only checks the receipt against the escrow it
// claims to belong to and records the outcome for SettleJob to consult. It
// never moves funds.
func (c *Core) VerifyExecutionReceipt(ctx context.Context, escrowID string, receipt domain.ExecutionReceipt) (toscore.VerifyExecutionReceiptResult, error) {
	e, err := c.store.GetEscrow(ctx, escrowID)
	if err != nil {
		return toscore.VerifyExecutionReceiptResult{}, err
	}
	if e.Status.Terminal() {
		return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "escrow already terminal"}, nil
	}
	if e.CapabilityID != receipt.CapabilityID {
		return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "capability mismatch"}, nil
	}
	// Bind verification to the exact provider the escrow was reserved
	// for and the exact job it was reserved for — without this, a
	// receipt produced for a different job/provider that happens to
	// target the same capability could authorize settlement of this
	// escrow (or be misattributed to the wrong job at settlement time).
	if e.ProviderID != receipt.ProviderID {
		return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "provider mismatch"}, nil
	}
	if receipt.JobID == "" {
		return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "receipt missing job_id"}, nil
	}
	if receipt.Signature == "" || receipt.OutputHash == "" || receipt.InputHash == "" {
		return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "receipt missing signature or input/output hash"}, nil
	}

	c.mu.Lock()
	c.verified[escrowID] = receipt
	c.mu.Unlock()

	return toscore.VerifyExecutionReceiptResult{Valid: true}, nil
}

// SettleJob is the only method in this package allowed to move funds, and
// only runs if VerifyExecutionReceipt already approved this exact escrow —
// enforcing the verify/apply separation at the type level, not just by
// convention.
func (c *Core) SettleJob(ctx context.Context, req toscore.SettleJobRequest) (toscore.SettleJobResult, error) {
	c.mu.Lock()
	verifiedReceipt, ok := c.verified[req.EscrowID]
	c.mu.Unlock()
	if !ok {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrSettlementFailed, "no verified receipt for this escrow", false)
	}
	// The verified receipt must be for the exact job being settled — an
	// escrow is 1:1 with a job, but this check is the actual enforcement
	// of that invariant rather than an assumption.
	if verifiedReceipt.JobID != req.JobID {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrSettlementFailed, "verified receipt belongs to a different job", false)
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

	receipt := domain.Receipt{
		ID:        "rcpt_" + uuid.NewString(),
		QuoteID:   e.QuoteID,
		EscrowID:  e.ID,
		JobID:     req.JobID,
		Charged:   req.ActualCost,
		Refunded:  domain.Money{Amount: refunded.String(), Currency: e.Reserved.Currency},
		Status:    domain.ReceiptSettled,
		CreatedAt: now,
	}
	if err := c.store.PutReceipt(ctx, receipt); err != nil {
		return toscore.SettleJobResult{}, err
	}

	// Consume the verification — an escrow only settles once, and clearing
	// this prevents a stale verified entry from being reused if the same
	// escrow ID were ever revisited (defense in depth alongside the
	// escrow's own Terminal() check above).
	delete(c.verified, req.EscrowID)

	return toscore.SettleJobResult{Receipt: receipt}, nil
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
	// Phase 0/1: no TOS Network commitment exists yet, so the "proof" is
	// just the receipt itself, explicitly labeled as unattested. Phase 4
	// replaces this body with a real tos-core settlement-proof payload
	// without changing ReadProof's signature.
	return map[string]any{
		"receipt_id": r.ID,
		"escrow_id":  r.EscrowID,
		"attested":   false,
		"note":       "no TOS Network commitment yet (Phase 0/1 mock tos-core)",
	}, nil
}
