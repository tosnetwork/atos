package financial

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type ledgerClient interface {
	Submit(context.Context, Event, bool) (LedgerTransaction, error)
	Lookup(context.Context, string) (LedgerTransaction, bool, error)
	Balance(context.Context, string, string) (string, bool, error)
	Verify(context.Context, Event, LedgerTransaction) error
}

type ledgerReconciler interface {
	ReconcileLedger(context.Context, []Event) (string, error)
}

type ledgerChainReader interface {
	ChainEvidence(context.Context) (LedgerChainEvidence, error)
}

type Adapter struct {
	repository *Repository
	ledger     ledgerClient
	closed     atomic.Bool
}

func NewAdapter(repository *Repository, ledger ledgerClient) (*Adapter, error) {
	if repository == nil || ledger == nil {
		return nil, errors.New("financial: adapter requires repository and ledger")
	}
	return &Adapter{repository: repository, ledger: ledger}, nil
}

func (a *Adapter) execute(ctx context.Context, request TransferRequest) (Event, error) {
	if a.closed.Load() {
		return Event{}, errors.New("financial: adapter is closed")
	}
	mutationDB, unlock, err := a.repository.LockMutationEvent(ctx, request.IdempotencyIdentity)
	if err != nil {
		return Event{}, err
	}
	defer unlock()
	event, err := a.repository.openIntentWith(ctx, mutationDB, request)
	if errors.Is(err, ErrSafeMode) {
		if event.LedgerReference == "" {
			return Event{}, ErrSafeMode
		}
		transaction, found, lookupErr := a.ledger.Lookup(ctx, event.LedgerReference)
		if lookupErr != nil {
			return Event{}, errors.Join(ErrLedgerUncertain, lookupErr)
		}
		if !found {
			return Event{}, ErrSafeMode
		}
		return a.observe(ctx, mutationDB, event, transaction)
	}
	if err != nil || event.State == "finalized" {
		return event, err
	}
	event, err = a.repository.lookupWith(ctx, mutationDB, request.IdempotencyIdentity)
	if err != nil || event.State == "finalized" {
		return event, err
	}
	transaction, found, lookupErr := a.ledger.Lookup(ctx, event.LedgerReference)
	if lookupErr == nil && found {
		return a.observe(ctx, mutationDB, event, transaction)
	}
	if lookupErr != nil {
		_ = a.repository.markUncertainWith(ctx, mutationDB, request.IdempotencyIdentity, lookupErr)
		return Event{}, errors.Join(ErrLedgerUncertain, lookupErr)
	}
	// Blnk's authoritative journal chain must advance in the same order as
	// ATOS commitment sequence. A later intent may be allocated concurrently,
	// but it cannot submit until every lower sequence is durably finalized.
	// This makes adjacent retained Blnk chain segments gap-free.
	orderTicker := time.NewTicker(10 * time.Millisecond)
	defer orderTicker.Stop()
	for {
		var safeMode, priorPending bool
		if orderErr := mutationDB.QueryRow(ctx, `SELECT safe_mode,
 EXISTS(SELECT 1 FROM financial_events WHERE sequence<$1 AND state<>'finalized')
 FROM financial_integrity_state WHERE singleton=TRUE`, event.Sequence).Scan(&safeMode, &priorPending); orderErr != nil {
			return Event{}, orderErr
		}
		if safeMode {
			return Event{}, ErrSafeMode
		}
		if !priorPending {
			break
		}
		select {
		case <-ctx.Done():
			return Event{}, errors.Join(ErrLedgerUncertain, ctx.Err())
		case <-orderTicker.C:
		}
	}
	if err := a.repository.markSubmittingWith(ctx, mutationDB, request.IdempotencyIdentity); err != nil {
		return Event{}, err
	}
	transaction, err = a.ledger.Submit(ctx, event, event.AllowOverdraft)
	if err == nil {
		return a.observe(ctx, mutationDB, event, transaction)
	}

	// A transport error or duplicate response is not proof that the ledger did
	// not commit. Resolve the stable reference before returning.
	transaction, found, lookupErr = a.ledger.Lookup(ctx, event.LedgerReference)
	if lookupErr == nil && found {
		return a.observe(ctx, mutationDB, event, transaction)
	}
	_ = a.repository.markUncertainWith(ctx, mutationDB, request.IdempotencyIdentity, err)
	if lookupErr != nil {
		return Event{}, errors.Join(ErrLedgerUncertain, err, lookupErr)
	}
	return Event{}, errors.Join(ErrLedgerUncertain, err)
}

func (a *Adapter) observe(ctx context.Context, db repositoryDB, event Event, transaction LedgerTransaction) (Event, error) {
	if transaction.Status == "REJECTED" {
		err := fmt.Errorf("financial: Blnk rejected transaction %s", event.LedgerTransactionID)
		_ = a.repository.markUncertainWith(ctx, db, event.IdempotencyIdentity, err)
		return Event{}, err
	}
	if err := a.ledger.Verify(ctx, event, transaction); err != nil {
		_, incidentErr := a.repository.enterSafeModeWith(ctx, db, "ledger_semantic_mismatch", event, transaction)
		return Event{}, errors.Join(err, incidentErr)
	}
	return a.repository.markFinalizedWith(ctx, db, event.IdempotencyIdentity, transaction)
}

func requireTransfer(request TransferRequest, event EventType, source AccountCode, destinations ...AccountCode) error {
	if request.EventType != event || request.SourceCode != source {
		return fmt.Errorf("financial: %s has invalid event or source account", event)
	}
	for _, destination := range destinations {
		if request.DestinationCode == destination {
			return nil
		}
	}
	return fmt.Errorf("financial: %s has invalid destination account", event)
}

func (a *Adapter) ProvisionAccount(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventAccountGenesis, GatewayCreditIssuance, PrincipalAvailable); err != nil || !request.AllowOverdraft {
		if err != nil {
			return Event{}, err
		}
		return Event{}, errors.New("financial: account genesis requires explicit issuance overdraft")
	}
	return a.execute(ctx, request)
}

func (a *Adapter) Balance(ctx context.Context, code AccountCode, ownerID, asset string, decimals int) (Balance, error) {
	if !validAccountCodes[code] || ownerID == "" || !assetPattern.MatchString(asset) || decimals < 0 || decimals > 18 {
		return Balance{}, errors.New("financial: invalid balance identity")
	}
	indicator, err := AccountIndicator(a.repository.gatewayID, a.repository.networkID, code, ownerID, asset)
	if err != nil {
		return Balance{}, err
	}
	amount, found, err := a.ledger.Balance(ctx, indicator, asset)
	if err != nil {
		return Balance{}, err
	}
	if !found {
		amount = "0"
	}
	return Balance{AccountCode: code, AccountOwnerID: ownerID, Asset: asset, AtomicAmount: amount, Decimals: decimals}, nil
}

func (a *Adapter) Reserve(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventReserve, PrincipalAvailable, PrincipalReserved); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}
func (a *Adapter) ReleaseReservation(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventReservationRelease, PrincipalReserved, PrincipalAvailable); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}
func (a *Adapter) FundEscrow(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventEscrowFund, PrincipalReserved, ManagedEscrow); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}
func (a *Adapter) ReleaseEscrow(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventEscrowRelease, ManagedEscrow, PrincipalAvailable); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}
func (a *Adapter) Settle(ctx context.Context, request TransferRequest) (Event, error) {
	var destination AccountCode
	switch request.EventType {
	case EventSettlementProvider:
		destination = ProviderPayable
	case EventSettlementFee:
		destination = GatewayFeeRevenue
	case EventSettlementRefund:
		destination = PrincipalAvailable
	default:
		return Event{}, errors.New("financial: invalid settlement leg")
	}
	if err := requireTransfer(request, request.EventType, ManagedEscrow, destination); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}
func (a *Adapter) PartialRefund(ctx context.Context, request TransferRequest) (Event, error) {
	if request.SourceCode != ProviderPayable && request.SourceCode != ProviderDisputed {
		return Event{}, errors.New("financial: partial refund source must be payable or disputed")
	}
	if err := requireTransfer(request, EventPartialRefund, request.SourceCode, PrincipalAvailable); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}
func (a *Adapter) FullRefund(ctx context.Context, request TransferRequest) (Event, error) {
	if request.SourceCode != ProviderPayable && request.SourceCode != ProviderDisputed {
		return Event{}, errors.New("financial: full refund source must be payable or disputed")
	}
	if err := requireTransfer(request, EventFullRefund, request.SourceCode, PrincipalAvailable); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}
func (a *Adapter) CompensatingReversal(ctx context.Context, request TransferRequest) (Event, error) {
	if request.EventType != EventCompensatingReversal || request.ReversesEventID == "" {
		return Event{}, errors.New("financial: reversal must identify the reversed event")
	}
	original, err := a.repository.LookupEventID(ctx, request.ReversesEventID)
	if err != nil {
		return Event{}, fmt.Errorf("financial: reversed event lookup: %w", err)
	}
	if original.State != "finalized" || len(original.Postings) != 2 ||
		original.Postings[0].Direction != "debit" || original.Postings[1].Direction != "credit" ||
		request.Asset != original.Asset || request.AtomicAmount != original.AtomicAmount || request.AllowOverdraft ||
		request.SourceCode != original.Postings[1].AccountCode || request.SourceOwnerID != original.Postings[1].AccountOwnerID ||
		request.DestinationCode != original.Postings[0].AccountCode || request.DestinationOwnerID != original.Postings[0].AccountOwnerID {
		return Event{}, errors.New("financial: reversal must exactly invert one finalized event")
	}
	return a.execute(ctx, request)
}
func (a *Adapter) FundGatewayRefund(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventGatewayRefundFund, GatewayFeeRevenue, GatewayRefundLiability); err != nil || request.SourceOwnerID != "_" || request.DestinationOwnerID != "_" {
		if err != nil {
			return Event{}, err
		}
		return Event{}, errors.New("financial: gateway refund funding requires singleton gateway accounts")
	}
	return a.execute(ctx, request)
}
func (a *Adapter) PayGatewayRefund(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventGatewayRefundPay, GatewayRefundLiability, PrincipalAvailable); err != nil || request.SourceOwnerID != "_" {
		if err != nil {
			return Event{}, err
		}
		return Event{}, errors.New("financial: gateway refund payment requires the singleton liability account")
	}
	return a.execute(ctx, request)
}
func (a *Adapter) HoldDispute(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventDisputeHold, ProviderPayable, ProviderDisputed); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}
func (a *Adapter) ReleaseDispute(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventDisputeRelease, ProviderDisputed, ProviderPayable); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}
func (a *Adapter) BeginPayout(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventPayoutIntent, ProviderPayable, PayoutClearing); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}
func (a *Adapter) CompletePayout(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventPayoutSuccess, PayoutClearing, PayoutDisbursed); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}
func (a *Adapter) FailPayout(ctx context.Context, request TransferRequest) (Event, error) {
	if err := requireTransfer(request, EventPayoutFailure, PayoutClearing, ProviderPayable); err != nil {
		return Event{}, err
	}
	return a.execute(ctx, request)
}

func (a *Adapter) Lookup(ctx context.Context, identity string) (Event, error) {
	return a.repository.Lookup(ctx, identity)
}

func (a *Adapter) Reconcile(ctx context.Context, limit int) (ReconcileResult, error) {
	return a.reconcile(ctx, limit, true)
}

// RecoverPending converges durable intents without performing an O(history)
// integrity audit. The API process runs this frequently for lost responses;
// Reconcile remains the less-frequent, full-snapshot integrity boundary.
func (a *Adapter) RecoverPending(ctx context.Context, limit int) (ReconcileResult, error) {
	return a.reconcile(ctx, limit, false)
}

func (a *Adapter) reconcile(ctx context.Context, limit int, fullAudit bool) (ReconcileResult, error) {
	reconciliationDB, unlockReconciliation, err := a.repository.LockReconciliation(ctx)
	if err != nil {
		return ReconcileResult{}, err
	}
	defer unlockReconciliation()
	ownerBytes := make([]byte, 16)
	if _, err := rand.Read(ownerBytes); err != nil {
		return ReconcileResult{}, fmt.Errorf("financial: create reconciliation owner: %w", err)
	}
	owner := "frecon_" + hex.EncodeToString(ownerBytes)
	cursor, err := a.repository.claimReconciliationWith(ctx, reconciliationDB, owner, 15*time.Minute)
	if err != nil {
		return ReconcileResult{}, err
	}
	completed := false
	defer func() {
		if !completed {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = a.repository.releaseReconciliationWith(releaseCtx, reconciliationDB, owner)
		}
	}()
	pending, err := a.repository.pendingWith(ctx, reconciliationDB, limit)
	if err != nil {
		return ReconcileResult{}, err
	}
	result := ReconcileResult{Checked: len(pending)}
	safeMode, _, err := a.repository.safeModeWith(ctx, reconciliationDB)
	if err != nil {
		return result, err
	}
	for _, event := range pending {
		current, currentErr := a.repository.lookupWith(ctx, reconciliationDB, event.IdempotencyIdentity)
		if currentErr != nil || current.State == "finalized" {
			continue
		}
		event = current
		transaction, found, lookupErr := a.ledger.Lookup(ctx, event.LedgerReference)
		if lookupErr != nil {
			_ = a.repository.markUncertainWith(ctx, reconciliationDB, event.IdempotencyIdentity, lookupErr)
			continue
		}
		if !found {
			if safeMode {
				continue
			}
			result.Retried++
			transaction, lookupErr = a.ledger.Submit(ctx, event, event.AllowOverdraft)
			if lookupErr != nil {
				_ = a.repository.markUncertainWith(ctx, reconciliationDB, event.IdempotencyIdentity, lookupErr)
				continue
			}
		}
		if _, observeErr := a.observe(ctx, reconciliationDB, event, transaction); observeErr != nil {
			if errors.Is(observeErr, ErrIdempotencyConflict) {
				result.Mismatches++
				safeMode = true
			}
			continue
		}
		result.Finalized++
	}
	if fullAudit {
		audited, auditErr := a.auditIntegrity(ctx, reconciliationDB)
		result.Checked += audited
		if auditErr != nil {
			// Availability uncertainty is recoverable; it is not positive evidence
			// of corruption and therefore does not itself enter safe mode.
			if errors.Is(auditErr, ErrLedgerUncertain) {
				return result, auditErr
			}
			result.Mismatches++
			_, incidentErr := a.repository.enterSafeModeWith(ctx, reconciliationDB, "financial_reconciliation_mismatch", map[string]any{"authority": "blnk"}, map[string]any{"error": auditErr.Error()})
			result.SafeMode = true
			return result, errors.Join(auditErr, incidentErr)
		}
		if err = reconciliationDB.QueryRow(ctx, `SELECT next_sequence-1 FROM financial_chain_state WHERE singleton=TRUE`).Scan(&cursor); err != nil {
			return result, err
		}
	}
	result.SafeMode, _, err = a.repository.safeModeWith(ctx, reconciliationDB)
	if err != nil {
		return result, err
	}
	if err = a.repository.completeReconciliationWith(ctx, reconciliationDB, owner, cursor); err != nil {
		return result, err
	}
	completed = true
	return result, nil
}
