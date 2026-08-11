package financial

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestLedgerChainRejectsBalancedUnexpectedTransactions(t *testing.T) {
	created := time.Unix(1786406400, 123).UTC()
	expectedTransaction := LedgerTransaction{TransactionID: "txn_expected", Source: "balance_a", Destination: "balance_b",
		Reference: "atos-ref", PreciseAmount: json.Number("100"), Currency: "USD",
		Description: "atos-financial-v1:sha256:expected", Status: "APPLIED", CreatedAt: created}
	rows := []LedgerChainRow{
		{Transaction: expectedTransaction, Amount: "1", ChainVersion: blnkChainVersionCBORV2, ChainSequence: 1},
		{Transaction: LedgerTransaction{TransactionID: "txn_attack_1", Source: "balance_a", Destination: "balance_b", Reference: "attack-1", PreciseAmount: json.Number("100"), Currency: "USD", Description: "unauthorized", Status: "APPLIED", CreatedAt: created.Add(time.Nanosecond)}, Amount: "1", ChainVersion: blnkChainVersionCBORV2, ChainSequence: 2},
		{Transaction: LedgerTransaction{TransactionID: "txn_attack_2", Source: "balance_b", Destination: "balance_a", Reference: "attack-2", PreciseAmount: json.Number("100"), Currency: "USD", Description: "unauthorized", Status: "APPLIED", CreatedAt: created.Add(2 * time.Nanosecond)}, Amount: "1", ChainVersion: blnkChainVersionCBORV2, ChainSequence: 3},
	}
	head := strings.Repeat("0", 64)
	for index := range rows {
		rows[index].ChainPreviousHash = head
		var err error
		head, err = ledgerChainHash(head, rows[index])
		if err != nil {
			t.Fatal(err)
		}
		rows[index].ChainHash = head
	}
	evidence := LedgerChainEvidence{State: LedgerChainState{ChainKey: "global", FirstSequence: 1, LastSequence: 3, PreviousHash: strings.Repeat("0", 64), GenesisHash: strings.Repeat("0", 64), HeadHash: head}, Transactions: rows}
	err := verifyLedgerChainEvidence(evidence, map[string]LedgerTransaction{expectedTransaction.TransactionID: expectedTransaction})
	if err == nil || !strings.Contains(err.Error(), "unexpected_ledger_event") {
		t.Fatalf("balanced unauthorized pair was not rejected: %v", err)
	}
}
