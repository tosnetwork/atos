package financial

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/tosnetwork/tos-protocol/pkg/codec"
)

type semanticIntent struct {
	EventType           EventType   `json:"event_type"`
	IdempotencyIdentity string      `json:"idempotency_identity"`
	Identities          Identities  `json:"identities"`
	Asset               string      `json:"asset"`
	Decimals            int         `json:"decimals"`
	AtomicAmount        string      `json:"atomic_amount"`
	SourceCode          AccountCode `json:"source_code"`
	SourceOwnerID       string      `json:"source_owner_id"`
	DestinationCode     AccountCode `json:"destination_code"`
	DestinationOwnerID  string      `json:"destination_owner_id"`
	AllowOverdraft      bool        `json:"allow_overdraft"`
	ReversesEventID     string      `json:"reverses_event_id"`
}

func SemanticDigest(request TransferRequest) (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	return codec.Digest("tos.atos.financial.intent.v1", semanticIntent{
		EventType: request.EventType, IdempotencyIdentity: request.IdempotencyIdentity,
		Identities: request.Identities, Asset: request.Asset, Decimals: request.Decimals,
		AtomicAmount: request.AtomicAmount, SourceCode: request.SourceCode,
		SourceOwnerID: request.SourceOwnerID, DestinationCode: request.DestinationCode,
		DestinationOwnerID: request.DestinationOwnerID, AllowOverdraft: request.AllowOverdraft,
		ReversesEventID: request.ReversesEventID,
	})
}

func stableID(prefix, domain, identity string) (string, error) {
	digest, err := codec.Digest(domain, map[string]string{"identity": identity})
	if err != nil {
		return "", err
	}
	return prefix + strings.TrimPrefix(digest, "sha256:"), nil
}

func EventID(identity string) (string, error) {
	return stableID("fevt_", "tos.atos.financial.event-id.v1", identity)
}

func LedgerTransactionID(identity string) (string, error) {
	return stableID("txn_atos_", "tos.atos.financial.ledger-transaction-id.v1", identity)
}

func LedgerReference(eventID string) string { return "atos-fevt-" + eventID }

func AccountIndicator(gatewayID, networkID string, code AccountCode, ownerID, asset string) (string, error) {
	digest, err := codec.Digest("tos.atos.financial.account-indicator.v1", struct {
		GatewayID string      `json:"gateway_id"`
		NetworkID string      `json:"network_id"`
		Code      AccountCode `json:"account_code"`
		OwnerID   string      `json:"account_owner_id"`
		Asset     string      `json:"asset"`
	}{gatewayID, networkID, code, ownerID, asset})
	if err != nil {
		return "", err
	}
	return "@atos_" + strings.TrimPrefix(digest, "sha256:"), nil
}

func BuildCommitment(gatewayID, networkID string, sequence int64, previous string, occurredMillis int64, request TransferRequest) (Event, error) {
	semanticDigest, err := SemanticDigest(request)
	if err != nil {
		return Event{}, err
	}
	eventID, err := EventID(request.IdempotencyIdentity)
	if err != nil {
		return Event{}, err
	}
	txnID, err := LedgerTransactionID(request.IdempotencyIdentity)
	if err != nil {
		return Event{}, err
	}
	source, err := AccountIndicator(gatewayID, networkID, request.SourceCode, request.SourceOwnerID, request.Asset)
	if err != nil {
		return Event{}, err
	}
	destination, err := AccountIndicator(gatewayID, networkID, request.DestinationCode, request.DestinationOwnerID, request.Asset)
	if err != nil {
		return Event{}, err
	}
	transactionIDs := []string{txnID}
	sort.Strings(transactionIDs)
	commitment := Commitment{
		Version: CommitmentVersion, Canonicalization: Canonicalization,
		GatewayID: gatewayID, NetworkID: networkID, Sequence: sequence,
		PreviousCommitment: previous, EventID: eventID, EventType: request.EventType,
		IdempotencyIdentity: request.IdempotencyIdentity, OccurredUnixMillis: occurredMillis,
		LedgerReference: LedgerReference(eventID), LedgerTransactionIDs: transactionIDs,
		Asset: request.Asset, AtomicAmount: request.AtomicAmount, Identities: request.Identities,
		Postings: []Posting{
			{EntryIndex: 0, AccountCode: request.SourceCode, AccountOwnerID: request.SourceOwnerID, Direction: "debit", AtomicAmount: request.AtomicAmount},
			{EntryIndex: 1, AccountCode: request.DestinationCode, AccountOwnerID: request.DestinationOwnerID, Direction: "credit", AtomicAmount: request.AtomicAmount},
		},
		ReversesEventID: request.ReversesEventID,
	}
	if err := commitment.Validate(); err != nil {
		return Event{}, err
	}
	canonical, err := codec.Marshal(commitment)
	if err != nil {
		return Event{}, err
	}
	digest, err := codec.Digest(CommitmentDomain, commitment)
	if err != nil {
		return Event{}, err
	}
	return Event{Commitment: commitment, Digest: digest, CanonicalCBOR: canonical,
		SemanticDigest: semanticDigest, LedgerTransactionID: txnID,
		SourceIndicator: source, DestinationIndicator: destination,
		Decimals: request.Decimals, AllowOverdraft: request.AllowOverdraft, State: "intent"}, nil
}

func DigestBytes(value string) ([]byte, error) {
	if !strings.HasPrefix(value, "sha256:") || len(value) != 71 {
		return nil, fmt.Errorf("financial: malformed digest %q", value)
	}
	out, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	if err != nil || len(out) != sha256.Size {
		return nil, fmt.Errorf("financial: malformed digest %q", value)
	}
	return out, nil
}
