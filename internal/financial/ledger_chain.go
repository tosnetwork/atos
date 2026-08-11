package financial

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tosnetwork/tos-protocol/pkg/codec"
)

const (
	blnkChainVersionCBORV2 = 2
	blnkChainVersionCBORV3 = 3
	blnkChainDomainCBORV2  = "blnk.transaction-chain.v2"
	blnkChainDomainCBORV3  = "blnk.transaction-chain.v3"
)

type LedgerChainState struct {
	ChainKey              string `json:"chain_key"`
	FirstSequence         int64  `json:"first_sequence"`
	LastSequence          int64  `json:"last_sequence"`
	PreviousHash          string `json:"previous_hash"`
	HeadHash              string `json:"head_hash"`
	GenesisHash           string `json:"genesis_hash"`
	UnchainedTransactions int64  `json:"unchained_transactions"`
}

type LedgerChainRow struct {
	Transaction       LedgerTransaction `json:"transaction"`
	Amount            string            `json:"ledger_amount"`
	ChainVersion      int               `json:"chain_version"`
	ChainSequence     int64             `json:"chain_sequence"`
	ChainPreviousHash string            `json:"chain_previous_hash"`
	ChainHash         string            `json:"chain_hash"`
}

type ledgerChainWireRow struct {
	Transaction struct {
		TransactionID        string      `json:"transaction_id"`
		Source               string      `json:"source"`
		Destination          string      `json:"destination"`
		SourceIndicator      string      `json:"source_indicator"`
		DestinationIndicator string      `json:"destination_indicator"`
		Amount               string      `json:"amount"`
		PreciseAmount        json.Number `json:"precise_amount"`
		Currency             string      `json:"currency"`
		Status               string      `json:"status"`
		Reference            string      `json:"reference"`
		Description          string      `json:"description"`
		CreatedAt            time.Time   `json:"created_at"`
	} `json:"transaction"`
	ChainVersion      int    `json:"chain_version"`
	ChainSequence     int64  `json:"chain_sequence"`
	ChainPreviousHash string `json:"chain_previous_hash"`
	ChainHash         string `json:"chain_hash"`
}

type LedgerChainEvidence struct {
	State        LedgerChainState `json:"state"`
	Transactions []LedgerChainRow `json:"transactions"`
}

func (c *BlnkClient) ChainEvidence(ctx context.Context) (LedgerChainEvidence, error) {
	var evidence LedgerChainEvidence
	if _, err := c.request(ctx, http.MethodGet, "/transactions/chain/state", nil, &evidence.State); err != nil {
		return LedgerChainEvidence{}, err
	}
	if evidence.State.LastSequence < 0 || evidence.State.UnchainedTransactions < 0 {
		return LedgerChainEvidence{}, errors.New("financial: invalid Blnk chain state")
	}
	evidence.State.FirstSequence = 1
	evidence.State.PreviousHash = evidence.State.GenesisHash
	for after := int64(0); after < evidence.State.LastSequence; {
		var page struct {
			Transactions []ledgerChainWireRow `json:"transactions"`
		}
		if _, err := c.request(ctx, http.MethodGet, fmt.Sprintf("/transactions/chain/evidence?after_sequence=%d&limit=1000", after), nil, &page); err != nil {
			return LedgerChainEvidence{}, err
		}
		if len(page.Transactions) == 0 {
			return LedgerChainEvidence{}, errors.New("financial: Blnk chain evidence ended before its head")
		}
		for _, row := range page.Transactions {
			// The page can race with a newly advanced head. Bind this snapshot to
			// the first state response and validate that bookmark again below.
			if row.ChainSequence > evidence.State.LastSequence {
				break
			}
			evidence.Transactions = append(evidence.Transactions, LedgerChainRow{
				Transaction: LedgerTransaction{TransactionID: row.Transaction.TransactionID, Source: row.Transaction.Source,
					Destination: row.Transaction.Destination, SourceIndicator: row.Transaction.SourceIndicator,
					DestinationIndicator: row.Transaction.DestinationIndicator, Reference: row.Transaction.Reference,
					PreciseAmount: row.Transaction.PreciseAmount, Currency: row.Transaction.Currency,
					Description: row.Transaction.Description, Status: row.Transaction.Status, CreatedAt: row.Transaction.CreatedAt},
				Amount: row.Transaction.Amount, ChainVersion: row.ChainVersion, ChainSequence: row.ChainSequence,
				ChainPreviousHash: row.ChainPreviousHash, ChainHash: row.ChainHash,
			})
			after = row.ChainSequence
		}
	}
	var confirmed LedgerChainState
	if _, err := c.request(ctx, http.MethodGet, "/transactions/chain/state", nil, &confirmed); err != nil {
		return LedgerChainEvidence{}, err
	}
	if confirmed.ChainKey != evidence.State.ChainKey || confirmed.LastSequence != evidence.State.LastSequence ||
		confirmed.HeadHash != evidence.State.HeadHash || confirmed.GenesisHash != evidence.State.GenesisHash ||
		confirmed.UnchainedTransactions != 0 {
		return LedgerChainEvidence{}, ErrLedgerUncertain
	}
	evidence.State.UnchainedTransactions = confirmed.UnchainedTransactions
	return evidence, nil
}

func ledgerChainHash(previous string, row LedgerChainRow) (string, error) {
	if row.ChainVersion != blnkChainVersionCBORV2 && row.ChainVersion != blnkChainVersionCBORV3 {
		return "", errors.New("financial: unsupported Blnk chain canonicalization")
	}
	created := row.Transaction.CreatedAt.UTC()
	domain := blnkChainDomainCBORV2
	if row.ChainVersion == blnkChainVersionCBORV3 {
		domain = blnkChainDomainCBORV3
	}
	canonical := []any{domain, uint64(row.ChainVersion), previous,
		row.Transaction.TransactionID, row.Transaction.Source, row.Transaction.Destination,
		row.Amount, row.Transaction.PreciseAmount.String(), row.Transaction.Currency,
		row.Transaction.Status, row.Transaction.Reference, row.Transaction.Description,
		created.Unix(), int64(created.Nanosecond())}
	if row.ChainVersion == blnkChainVersionCBORV3 {
		canonical = []any{domain, uint64(row.ChainVersion), previous,
			row.Transaction.TransactionID, row.Transaction.Source, row.Transaction.Destination,
			row.Transaction.SourceIndicator, row.Transaction.DestinationIndicator,
			row.Amount, row.Transaction.PreciseAmount.String(), row.Transaction.Currency,
			row.Transaction.Status, row.Transaction.Reference, row.Transaction.Description,
			created.Unix(), int64(created.Nanosecond())}
	}
	encoded, err := codec.Marshal(canonical)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:]), nil
}

func verifyLedgerChainLinks(evidence LedgerChainEvidence) error {
	if evidence.State.ChainKey != "global" || evidence.State.FirstSequence < 1 ||
		evidence.State.LastSequence-evidence.State.FirstSequence+1 != int64(len(evidence.Transactions)) ||
		evidence.State.GenesisHash == "" || evidence.State.PreviousHash == "" {
		return errors.New("financial: invalid Blnk chain checkpoint")
	}
	if evidence.State.UnchainedTransactions != 0 {
		return ErrLedgerUncertain
	}
	head := evidence.State.PreviousHash
	for index, row := range evidence.Transactions {
		sequence := evidence.State.FirstSequence + int64(index)
		if row.ChainSequence != sequence || row.ChainPreviousHash != head {
			return fmt.Errorf("financial: Blnk chain discontinuity at sequence %d", sequence)
		}
		digest, err := ledgerChainHash(head, row)
		if err != nil || digest != row.ChainHash {
			return fmt.Errorf("financial: Blnk chain mutation at sequence %d", sequence)
		}
		head = digest
	}
	if head != evidence.State.HeadHash {
		return errors.New("financial: Blnk chain head mismatch")
	}
	return nil
}

func verifyLedgerChainEvidence(evidence LedgerChainEvidence, expected map[string]LedgerTransaction) error {
	if err := verifyLedgerChainLinks(evidence); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(expected))
	for index, row := range evidence.Transactions {
		sequence := evidence.State.FirstSequence + int64(index)
		want, ok := expected[row.Transaction.TransactionID]
		if !ok {
			return fmt.Errorf("financial: unexpected_ledger_event at Blnk sequence %d", sequence)
		}
		if want.Source != row.Transaction.Source || want.Destination != row.Transaction.Destination ||
			want.SourceIndicator != row.Transaction.SourceIndicator || want.DestinationIndicator != row.Transaction.DestinationIndicator ||
			want.Reference != row.Transaction.Reference || want.PreciseAmount.String() != row.Transaction.PreciseAmount.String() ||
			want.Currency != row.Transaction.Currency || want.Description != row.Transaction.Description || want.Status != row.Transaction.Status {
			return fmt.Errorf("financial: Blnk chain semantic mismatch at sequence %d", sequence)
		}
		if _, duplicate := seen[row.Transaction.TransactionID]; duplicate {
			return fmt.Errorf("financial: duplicate Blnk transaction in chain at sequence %d", sequence)
		}
		seen[row.Transaction.TransactionID] = struct{}{}
	}
	if len(seen) != len(expected) {
		return errors.New("financial: expected ATOS transaction missing from Blnk chain")
	}
	return nil
}

func makeLedgerBatchEvidence(ctx context.Context, ledger interface {
	ledgerClient
	ledgerChainReader
}, events []Event) (LedgerChainEvidence, error) {
	full, err := ledger.ChainEvidence(ctx)
	if err != nil {
		return LedgerChainEvidence{}, err
	}
	if err := verifyLedgerChainLinks(full); err != nil {
		return LedgerChainEvidence{}, err
	}
	expected := make(map[string]LedgerTransaction, len(events))
	positions := make(map[string]int, len(events))
	for _, event := range events {
		transaction, found, lookupErr := ledger.Lookup(ctx, event.LedgerReference)
		if lookupErr != nil || !found {
			return LedgerChainEvidence{}, errors.Join(ErrLedgerUncertain, lookupErr)
		}
		if err := ledger.Verify(ctx, event, transaction); err != nil {
			return LedgerChainEvidence{}, err
		}
		transaction.SourceIndicator, transaction.DestinationIndicator = event.SourceIndicator, event.DestinationIndicator
		expected[transaction.TransactionID] = transaction
	}
	for index, row := range full.Transactions {
		if _, ok := expected[row.Transaction.TransactionID]; ok {
			positions[row.Transaction.TransactionID] = index
		}
	}
	if len(positions) != len(expected) {
		return LedgerChainEvidence{}, ErrLedgerUncertain
	}
	first, last := len(full.Transactions), -1
	for id := range expected {
		position := positions[id]
		if position < first {
			first = position
		}
		if position > last {
			last = position
		}
	}
	segmentRows := append([]LedgerChainRow(nil), full.Transactions[first:last+1]...)
	segment := LedgerChainEvidence{State: LedgerChainState{ChainKey: full.State.ChainKey,
		FirstSequence: segmentRows[0].ChainSequence, LastSequence: segmentRows[len(segmentRows)-1].ChainSequence,
		PreviousHash: segmentRows[0].ChainPreviousHash, HeadHash: segmentRows[len(segmentRows)-1].ChainHash,
		GenesisHash: full.State.GenesisHash}, Transactions: segmentRows}
	if err := verifyLedgerChainEvidence(segment, expected); err != nil {
		return LedgerChainEvidence{}, err
	}
	return segment, nil
}
