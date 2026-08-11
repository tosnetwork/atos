package financial

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
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
	event, err := a.repository.OpenIntent(ctx, request)
	if err != nil || event.State == "finalized" {
		return event, err
	}
	unlock, err := a.repository.LockEvent(ctx, request.IdempotencyIdentity)
	if err != nil {
		return Event{}, err
	}
	defer unlock()
	event, err = a.repository.Lookup(ctx, request.IdempotencyIdentity)
	if err != nil || event.State == "finalized" {
		return event, err
	}
	transaction, found, lookupErr := a.ledger.Lookup(ctx, event.LedgerReference)
	if lookupErr == nil && found {
		return a.observe(ctx, event, transaction)
	}
	if lookupErr != nil {
		_ = a.repository.MarkUncertain(ctx, request.IdempotencyIdentity, lookupErr)
		return Event{}, errors.Join(ErrLedgerUncertain, lookupErr)
	}
	if err := a.repository.MarkSubmitting(ctx, request.IdempotencyIdentity); err != nil {
		return Event{}, err
	}
	transaction, err = a.ledger.Submit(ctx, event, event.AllowOverdraft)
	if err == nil {
		return a.observe(ctx, event, transaction)
	}

	// A transport error or duplicate response is not proof that the ledger did
	// not commit. Resolve the stable reference before returning.
	transaction, found, lookupErr = a.ledger.Lookup(ctx, event.LedgerReference)
	if lookupErr == nil && found {
		return a.observe(ctx, event, transaction)
	}
	_ = a.repository.MarkUncertain(ctx, request.IdempotencyIdentity, err)
	if lookupErr != nil {
		return Event{}, errors.Join(ErrLedgerUncertain, err, lookupErr)
	}
	return Event{}, errors.Join(ErrLedgerUncertain, err)
}

func (a *Adapter) observe(ctx context.Context, event Event, transaction LedgerTransaction) (Event, error) {
	if transaction.Status == "REJECTED" {
		err := fmt.Errorf("financial: Blnk rejected transaction %s", event.LedgerTransactionID)
		_ = a.repository.MarkUncertain(ctx, event.IdempotencyIdentity, err)
		return Event{}, err
	}
	if err := a.ledger.Verify(ctx, event, transaction); err != nil {
		_, incidentErr := a.repository.EnterSafeMode(ctx, "ledger_semantic_mismatch", event, transaction)
		return Event{}, errors.Join(err, incidentErr)
	}
	return a.repository.MarkFinalized(ctx, event.IdempotencyIdentity, transaction)
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
	pending, err := a.repository.Pending(ctx, limit)
	if err != nil {
		return ReconcileResult{}, err
	}
	result := ReconcileResult{Checked: len(pending)}
	for _, event := range pending {
		unlock, lockErr := a.repository.LockEvent(ctx, event.IdempotencyIdentity)
		if lockErr != nil {
			continue
		}
		current, currentErr := a.repository.Lookup(ctx, event.IdempotencyIdentity)
		if currentErr != nil || current.State == "finalized" {
			unlock()
			continue
		}
		event = current
		transaction, found, lookupErr := a.ledger.Lookup(ctx, event.LedgerReference)
		if lookupErr != nil {
			_ = a.repository.MarkUncertain(ctx, event.IdempotencyIdentity, lookupErr)
			unlock()
			continue
		}
		if !found {
			result.Retried++
			transaction, lookupErr = a.ledger.Submit(ctx, event, event.AllowOverdraft)
			if lookupErr != nil {
				_ = a.repository.MarkUncertain(ctx, event.IdempotencyIdentity, lookupErr)
				unlock()
				continue
			}
		}
		if _, observeErr := a.observe(ctx, event, transaction); observeErr != nil {
			if errors.Is(observeErr, ErrIdempotencyConflict) {
				result.Mismatches++
			}
			unlock()
			continue
		}
		result.Finalized++
		unlock()
	}
	audited, auditErr := a.auditIntegrity(ctx)
	result.Checked += audited
	if auditErr != nil {
		// Availability uncertainty is recoverable; it is not positive evidence
		// of corruption and therefore does not itself enter safe mode.
		if errors.Is(auditErr, ErrLedgerUncertain) {
			return result, auditErr
		}
		result.Mismatches++
		_, incidentErr := a.repository.EnterSafeMode(ctx, "financial_reconciliation_mismatch", map[string]any{"authority": "blnk"}, map[string]any{"error": auditErr.Error()})
		result.SafeMode = true
		return result, errors.Join(auditErr, incidentErr)
	}
	result.SafeMode, _, err = a.repository.SafeMode(ctx)
	return result, err
}
