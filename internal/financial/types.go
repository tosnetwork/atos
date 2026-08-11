// Package financial implements the sole ATOS boundary to Blnk's authoritative
// double-entry ledger. Local values are rebuildable projections and evidence.
package financial

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	CommitmentVersion = "atos_financial_commitment_v1"
	Canonicalization  = "rfc8949_core_deterministic_cbor"
	CommitmentDomain  = "tos.atos.financial.commitment.v1"
	GenesisDigest     = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
)

type AccountCode string

const (
	PrincipalAvailable     AccountCode = "principal_available"
	PrincipalReserved      AccountCode = "principal_reserved"
	ManagedEscrow          AccountCode = "managed_escrow"
	ProviderPayable        AccountCode = "provider_payable"
	ProviderDisputed       AccountCode = "provider_disputed"
	GatewayFeeRevenue      AccountCode = "gateway_fee_revenue"
	GatewayRefundLiability AccountCode = "gateway_refund_liability"
	PayoutClearing         AccountCode = "payout_clearing"
	PayoutDisbursed        AccountCode = "payout_disbursed"
	GatewayCreditIssuance  AccountCode = "gateway_credit_issuance"
)

var validAccountCodes = map[AccountCode]bool{
	PrincipalAvailable: true, PrincipalReserved: true, ManagedEscrow: true,
	ProviderPayable: true, ProviderDisputed: true, GatewayFeeRevenue: true,
	GatewayRefundLiability: true, PayoutClearing: true, PayoutDisbursed: true,
	GatewayCreditIssuance: true,
}

type EventType string

const (
	EventAccountGenesis       EventType = "account_genesis"
	EventReserve              EventType = "reserve"
	EventReservationRelease   EventType = "reservation_release"
	EventEscrowFund           EventType = "escrow_fund"
	EventEscrowRelease        EventType = "escrow_release"
	EventSettlementProvider   EventType = "settlement_provider"
	EventSettlementFee        EventType = "settlement_fee"
	EventSettlementRefund     EventType = "settlement_refund"
	EventPartialRefund        EventType = "partial_refund"
	EventFullRefund           EventType = "full_refund"
	EventCompensatingReversal EventType = "compensating_reversal"
	EventDisputeHold          EventType = "dispute_hold"
	EventDisputeRelease       EventType = "dispute_release"
	EventPayoutIntent         EventType = "payout_intent"
	EventPayoutSuccess        EventType = "payout_success"
	EventPayoutFailure        EventType = "payout_failure"
	EventGatewayRefundFund    EventType = "gateway_refund_fund"
	EventGatewayRefundPay     EventType = "gateway_refund_pay"
	EventManualAdjustment     EventType = "manual_adjustment"
)

var validEventTypes = map[EventType]bool{
	EventAccountGenesis: true, EventReserve: true, EventReservationRelease: true,
	EventEscrowFund: true, EventEscrowRelease: true, EventSettlementProvider: true,
	EventSettlementFee: true, EventSettlementRefund: true, EventPartialRefund: true,
	EventFullRefund: true, EventCompensatingReversal: true, EventDisputeHold: true,
	EventDisputeRelease: true, EventPayoutIntent: true, EventPayoutSuccess: true,
	EventPayoutFailure: true, EventManualAdjustment: true,
	EventGatewayRefundFund: true, EventGatewayRefundPay: true,
}

type Identities struct {
	PrincipalID        string `json:"principal_id"`
	ProviderID         string `json:"provider_id"`
	JobID              string `json:"job_id"`
	QuoteID            string `json:"quote_id"`
	CapabilityID       string `json:"capability_id"`
	CapabilityVersion  string `json:"capability_version"`
	BillingSnapshotID  string `json:"billing_snapshot_id"`
	ExecutionReceiptID string `json:"execution_receipt_id"`
	SettlementID       string `json:"settlement_id"`
	ProviderEarningID  string `json:"provider_earning_id"`
	DisputeID          string `json:"dispute_id"`
	PayoutID           string `json:"payout_id"`
}

type Posting struct {
	EntryIndex     int         `json:"entry_index"`
	AccountCode    AccountCode `json:"account_code"`
	AccountOwnerID string      `json:"account_owner_id"`
	Direction      string      `json:"direction"`
	AtomicAmount   string      `json:"atomic_amount"`
}

type Commitment struct {
	Version              string     `json:"version"`
	Canonicalization     string     `json:"canonicalization"`
	GatewayID            string     `json:"gateway_id"`
	NetworkID            string     `json:"network_id"`
	Sequence             int64      `json:"sequence"`
	PreviousCommitment   string     `json:"previous_commitment"`
	EventID              string     `json:"event_id"`
	EventType            EventType  `json:"event_type"`
	IdempotencyIdentity  string     `json:"idempotency_identity"`
	OccurredUnixMillis   int64      `json:"occurred_unix_millis"`
	LedgerReference      string     `json:"ledger_reference"`
	LedgerTransactionIDs []string   `json:"ledger_transaction_ids"`
	Asset                string     `json:"asset"`
	AtomicAmount         string     `json:"atomic_amount"`
	Identities           Identities `json:"identities"`
	Postings             []Posting  `json:"postings"`
	ReversesEventID      string     `json:"reverses_event_id"`
}

type TransferRequest struct {
	EventType           EventType
	IdempotencyIdentity string
	Identities          Identities
	Asset               string
	Decimals            int
	AtomicAmount        string
	SourceCode          AccountCode
	SourceOwnerID       string
	DestinationCode     AccountCode
	DestinationOwnerID  string
	AllowOverdraft      bool
	ReversesEventID     string
	OccurredAt          time.Time
}

type Event struct {
	Commitment
	Digest               string
	CanonicalCBOR        []byte
	SemanticDigest       string
	LedgerTransactionID  string
	SourceIndicator      string
	DestinationIndicator string
	Decimals             int
	AllowOverdraft       bool
	State                string
	Attempts             int
	LastError            string
	FinalizedAt          *time.Time
}

type Balance struct {
	AccountCode    AccountCode
	AccountOwnerID string
	Asset          string
	AtomicAmount   string
	Decimals       int
}

var assetPattern = regexp.MustCompile(`^[A-Z][A-Z0-9]{1,15}$`)
var gatewayNetworkIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func validGatewayNetworkIDs(gatewayID, networkID string) bool {
	return len(gatewayID) <= 253 && len(networkID) <= 128 &&
		gatewayNetworkIDPattern.MatchString(gatewayID) && gatewayNetworkIDPattern.MatchString(networkID)
}

func (r TransferRequest) Validate() error {
	if !validEventTypes[r.EventType] {
		return fmt.Errorf("financial: unsupported event type %q", r.EventType)
	}
	if len(r.IdempotencyIdentity) < 1 || len(r.IdempotencyIdentity) > 512 {
		return errors.New("financial: idempotency identity must contain 1-512 bytes")
	}
	if !assetPattern.MatchString(r.Asset) || r.Decimals < 0 || r.Decimals > 18 {
		return errors.New("financial: invalid asset or decimal precision")
	}
	amount, ok := new(big.Int).SetString(r.AtomicAmount, 10)
	if !ok || amount.Sign() < 0 || (len(r.AtomicAmount) > 1 && strings.HasPrefix(r.AtomicAmount, "0")) {
		return errors.New("financial: atomic amount must be canonical non-negative base-10")
	}
	if !validAccountCodes[r.SourceCode] || !validAccountCodes[r.DestinationCode] || r.SourceCode == r.DestinationCode && r.SourceOwnerID == r.DestinationOwnerID {
		return errors.New("financial: invalid or identical posting accounts")
	}
	if r.SourceOwnerID == "" || r.DestinationOwnerID == "" {
		return errors.New("financial: posting owner is empty")
	}
	if err := r.validateIdentityBindings(); err != nil {
		return err
	}
	return nil
}

func (r TransferRequest) validateIdentityBindings() error {
	require := func(values ...string) error {
		for _, value := range values {
			if strings.TrimSpace(value) == "" {
				return errors.New("financial: economic event is missing an applicable immutable identity")
			}
		}
		return nil
	}
	ids := r.Identities
	ownersMatch := true
	var err error
	switch r.EventType {
	case EventAccountGenesis:
		err = require(ids.PrincipalID)
		ownersMatch = r.SourceOwnerID == "_" && r.DestinationOwnerID == ids.PrincipalID
	case EventReserve, EventReservationRelease:
		err = require(ids.PrincipalID, ids.ProviderID, ids.JobID, ids.QuoteID, ids.CapabilityID, ids.CapabilityVersion)
		ownersMatch = r.SourceOwnerID == ids.PrincipalID && r.DestinationOwnerID == ids.PrincipalID
	case EventEscrowFund:
		err = require(ids.PrincipalID, ids.ProviderID, ids.JobID, ids.QuoteID, ids.CapabilityID, ids.CapabilityVersion)
		ownersMatch = r.SourceOwnerID == ids.PrincipalID && r.DestinationOwnerID == ids.JobID
	case EventEscrowRelease:
		err = require(ids.PrincipalID, ids.ProviderID, ids.JobID, ids.QuoteID, ids.CapabilityID, ids.CapabilityVersion)
		ownersMatch = r.SourceOwnerID == ids.JobID && r.DestinationOwnerID == ids.PrincipalID
	case EventSettlementProvider, EventSettlementFee, EventSettlementRefund:
		err = require(ids.PrincipalID, ids.ProviderID, ids.JobID, ids.QuoteID, ids.CapabilityID, ids.CapabilityVersion,
			ids.BillingSnapshotID, ids.ExecutionReceiptID, ids.SettlementID, ids.ProviderEarningID)
		ownersMatch = r.SourceOwnerID == ids.JobID
		switch r.EventType {
		case EventSettlementProvider:
			ownersMatch = ownersMatch && r.DestinationOwnerID == ids.ProviderID
		case EventSettlementFee:
			ownersMatch = ownersMatch && r.DestinationOwnerID == "_"
		case EventSettlementRefund:
			ownersMatch = ownersMatch && r.DestinationOwnerID == ids.PrincipalID
		}
	case EventDisputeHold, EventDisputeRelease:
		err = require(ids.PrincipalID, ids.ProviderID, ids.JobID, ids.QuoteID, ids.CapabilityID, ids.CapabilityVersion,
			ids.BillingSnapshotID, ids.ExecutionReceiptID, ids.SettlementID, ids.ProviderEarningID, ids.DisputeID)
		ownersMatch = r.SourceOwnerID == ids.ProviderID && r.DestinationOwnerID == ids.ProviderID
	case EventPartialRefund, EventFullRefund:
		err = require(ids.PrincipalID, ids.ProviderID, ids.JobID, ids.QuoteID, ids.CapabilityID, ids.CapabilityVersion,
			ids.BillingSnapshotID, ids.ExecutionReceiptID, ids.SettlementID, ids.ProviderEarningID, ids.DisputeID)
		ownersMatch = r.SourceOwnerID == ids.ProviderID && r.DestinationOwnerID == ids.PrincipalID
	case EventCompensatingReversal:
		err = require(ids.PrincipalID, ids.ProviderID, ids.JobID, ids.QuoteID, ids.CapabilityID, ids.CapabilityVersion,
			ids.BillingSnapshotID, ids.ExecutionReceiptID, ids.SettlementID, ids.ProviderEarningID, ids.DisputeID)
	case EventGatewayRefundFund, EventGatewayRefundPay:
		err = require(ids.PrincipalID, ids.ProviderID, ids.JobID, ids.QuoteID, ids.CapabilityID, ids.CapabilityVersion,
			ids.BillingSnapshotID, ids.ExecutionReceiptID, ids.SettlementID, ids.ProviderEarningID, ids.DisputeID)
		if r.EventType == EventGatewayRefundFund {
			ownersMatch = r.SourceOwnerID == "_" && r.DestinationOwnerID == "_"
		} else {
			ownersMatch = r.SourceOwnerID == "_" && r.DestinationOwnerID == ids.PrincipalID
		}
	case EventPayoutIntent, EventPayoutSuccess, EventPayoutFailure:
		err = require(ids.ProviderID, ids.JobID, ids.QuoteID, ids.CapabilityID, ids.CapabilityVersion,
			ids.BillingSnapshotID, ids.ExecutionReceiptID, ids.SettlementID, ids.ProviderEarningID, ids.PayoutID)
		switch r.EventType {
		case EventPayoutIntent:
			ownersMatch = r.SourceOwnerID == ids.ProviderID && r.DestinationOwnerID == ids.PayoutID
		case EventPayoutSuccess:
			ownersMatch = r.SourceOwnerID == ids.PayoutID && r.DestinationOwnerID == "_"
		case EventPayoutFailure:
			ownersMatch = r.SourceOwnerID == ids.PayoutID && r.DestinationOwnerID == ids.ProviderID
		}
	case EventManualAdjustment:
		return errors.New("financial: manual adjustment is not available through the runtime adapter")
	default:
		return nil
	}
	if err != nil {
		return err
	}
	if !ownersMatch {
		return errors.New("financial: posting owners do not match immutable economic identities")
	}
	return nil
}

func (c Commitment) Validate() error {
	if c.Version != CommitmentVersion || c.Canonicalization != Canonicalization || c.Sequence < 1 || !validGatewayNetworkIDs(c.GatewayID, c.NetworkID) {
		return errors.New("financial: invalid commitment envelope")
	}
	if len(c.LedgerTransactionIDs) == 0 || !sort.StringsAreSorted(c.LedgerTransactionIDs) {
		return errors.New("financial: ledger transaction IDs must be nonempty and sorted")
	}
	if len(c.Postings) < 2 {
		return errors.New("financial: commitment must contain balanced postings")
	}
	debits, credits := new(big.Int), new(big.Int)
	for i, p := range c.Postings {
		if p.EntryIndex != i || !validAccountCodes[p.AccountCode] || p.AccountOwnerID == "" || p.AtomicAmount != c.AtomicAmount {
			return errors.New("financial: invalid posting")
		}
		n, ok := new(big.Int).SetString(p.AtomicAmount, 10)
		if !ok || n.Sign() < 0 {
			return errors.New("financial: invalid posting amount")
		}
		switch p.Direction {
		case "debit":
			debits.Add(debits, n)
		case "credit":
			credits.Add(credits, n)
		default:
			return errors.New("financial: invalid posting direction")
		}
	}
	if debits.Cmp(credits) != 0 {
		return errors.New("financial: postings violate conservation")
	}
	return nil
}

type ATOSFinancialAdapter interface {
	ProvisionAccount(context.Context, TransferRequest) (Event, error)
	Balance(context.Context, AccountCode, string, string, int) (Balance, error)
	Reserve(context.Context, TransferRequest) (Event, error)
	ReleaseReservation(context.Context, TransferRequest) (Event, error)
	FundEscrow(context.Context, TransferRequest) (Event, error)
	ReleaseEscrow(context.Context, TransferRequest) (Event, error)
	Settle(context.Context, TransferRequest) (Event, error)
	PartialRefund(context.Context, TransferRequest) (Event, error)
	FullRefund(context.Context, TransferRequest) (Event, error)
	CompensatingReversal(context.Context, TransferRequest) (Event, error)
	FundGatewayRefund(context.Context, TransferRequest) (Event, error)
	PayGatewayRefund(context.Context, TransferRequest) (Event, error)
	HoldDispute(context.Context, TransferRequest) (Event, error)
	ReleaseDispute(context.Context, TransferRequest) (Event, error)
	BeginPayout(context.Context, TransferRequest) (Event, error)
	CompletePayout(context.Context, TransferRequest) (Event, error)
	FailPayout(context.Context, TransferRequest) (Event, error)
	Lookup(context.Context, string) (Event, error)
	Reconcile(context.Context, int) (ReconcileResult, error)
}

type ReconcileResult struct {
	Checked, Finalized, Retried, Mismatches int
	SafeMode                                bool
}

var (
	ErrIdempotencyConflict = errors.New("financial: idempotency identity reused with changed semantics")
	ErrSafeMode            = errors.New("financial: safe mode rejects financial writes")
	ErrLedgerUncertain     = errors.New("financial: ledger outcome is uncertain")
)
