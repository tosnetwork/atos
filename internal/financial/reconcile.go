package financial

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/tosnetwork/tos-protocol/pkg/codec"
)

type projectionKey struct {
	code  AccountCode
	owner string
	asset string
}

type projectionValue struct {
	amount       *big.Int
	lastSequence int64
}

// auditIntegrity deterministically rebuilds every projection from the sealed
// commitment chain and compares it with Blnk. It never chooses a newer side.
func (a *Adapter) auditIntegrity(ctx context.Context, db repositoryDB) (int, error) {
	rows, err := db.Query(ctx, `SELECT commitment,commitment_digest,canonical_cbor,semantic_digest,
 ledger_transaction_id,source_indicator,destination_indicator,decimals,allow_overdraft,state,attempts,last_error,finalized_at
 FROM financial_events ORDER BY sequence`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	previous := GenesisDigest
	expectedSequence := int64(1)
	expected := make(map[projectionKey]projectionValue)
	finalized := make([]Event, 0)
	checked := 0
	for rows.Next() {
		event, scanErr := scanEvent(rows)
		if scanErr != nil {
			return checked, scanErr
		}
		if event.Sequence != expectedSequence || event.PreviousCommitment != previous || event.GatewayID != a.repository.gatewayID || event.NetworkID != a.repository.networkID {
			return checked, fmt.Errorf("financial: commitment chain discontinuity at sequence %d", expectedSequence)
		}
		if err := event.Commitment.Validate(); err != nil {
			return checked, fmt.Errorf("financial: invalid commitment at sequence %d: %w", event.Sequence, err)
		}
		canonical, marshalErr := codec.Marshal(event.Commitment)
		digest, digestErr := codec.Digest(CommitmentDomain, event.Commitment)
		if marshalErr != nil || digestErr != nil || !bytes.Equal(canonical, event.CanonicalCBOR) || digest != event.Digest {
			return checked, fmt.Errorf("financial: commitment mutation at sequence %d", event.Sequence)
		}
		previous = event.Digest
		expectedSequence++
		if event.State != "finalized" {
			continue
		}
		transaction, found, lookupErr := a.ledger.Lookup(ctx, event.LedgerReference)
		if lookupErr != nil {
			return checked, errors.Join(ErrLedgerUncertain, lookupErr)
		}
		if !found {
			return checked, fmt.Errorf("financial: finalized ledger transaction missing at sequence %d", event.Sequence)
		}
		if err := a.ledger.Verify(ctx, event, transaction); err != nil {
			return checked, fmt.Errorf("financial: finalized ledger transaction mismatch at sequence %d: %w", event.Sequence, err)
		}
		finalized = append(finalized, event)
		for _, posting := range event.Postings {
			key := projectionKey{posting.AccountCode, posting.AccountOwnerID, event.Asset}
			value := expected[key]
			if value.amount == nil {
				value.amount = new(big.Int)
			}
			amount, ok := new(big.Int).SetString(posting.AtomicAmount, 10)
			if !ok {
				return checked, fmt.Errorf("financial: invalid posting amount at sequence %d", event.Sequence)
			}
			if posting.Direction == "credit" {
				value.amount.Add(value.amount, amount)
			} else {
				value.amount.Sub(value.amount, amount)
			}
			value.lastSequence = event.Sequence
			expected[key] = value
		}
		checked++
	}
	if err := rows.Err(); err != nil {
		return checked, err
	}
	var nextSequence int64
	var lastCommitment string
	if err := db.QueryRow(ctx, `SELECT next_sequence,last_commitment FROM financial_chain_state WHERE singleton=TRUE`).Scan(&nextSequence, &lastCommitment); err != nil {
		return checked, err
	}
	if nextSequence != expectedSequence || lastCommitment != previous {
		return checked, errors.New("financial: chain checkpoint does not match rebuilt commitment chain")
	}
	if reconciler, ok := a.ledger.(ledgerReconciler); ok {
		for offset := 0; offset < len(finalized); offset += 10000 {
			end := min(offset+10000, len(finalized))
			if _, err := reconciler.ReconcileLedger(ctx, finalized[offset:end]); err != nil {
				return checked, err
			}
		}
	}

	projectionRows, err := db.Query(ctx, `SELECT account_code,account_owner_id,asset,atomic_balance::text,last_sequence FROM financial_projections`)
	if err != nil {
		return checked, err
	}
	defer projectionRows.Close()
	observed := make(map[projectionKey]projectionValue)
	for projectionRows.Next() {
		var key projectionKey
		var amount string
		var value projectionValue
		if err := projectionRows.Scan(&key.code, &key.owner, &key.asset, &amount, &value.lastSequence); err != nil {
			return checked, err
		}
		value.amount, _ = new(big.Int).SetString(amount, 10)
		observed[key] = value
	}
	if err := projectionRows.Err(); err != nil {
		return checked, err
	}
	if len(observed) != len(expected) {
		return checked, errors.New("financial: projection account set mismatch")
	}
	for key, want := range expected {
		got, ok := observed[key]
		if !ok || got.amount == nil || got.amount.Cmp(want.amount) != 0 || got.lastSequence != want.lastSequence {
			return checked, fmt.Errorf("financial: projection mismatch for %s/%s/%s", key.code, key.owner, key.asset)
		}
		indicator, indicatorErr := AccountIndicator(a.repository.gatewayID, a.repository.networkID, key.code, key.owner, key.asset)
		if indicatorErr != nil {
			return checked, indicatorErr
		}
		ledgerAmount, found, balanceErr := a.ledger.Balance(ctx, indicator, key.asset)
		if balanceErr != nil {
			return checked, errors.Join(ErrLedgerUncertain, balanceErr)
		}
		ledgerValue, valid := new(big.Int).SetString(ledgerAmount, 10)
		if !found || !valid || ledgerValue.Cmp(want.amount) != 0 {
			return checked, fmt.Errorf("financial: authoritative Blnk balance mismatch for %s/%s/%s", key.code, key.owner, key.asset)
		}
	}
	return checked, nil
}

// RebuildProjections is an operator recovery primitive. It first proves the
// full commitment chain and every finalized Blnk transaction, then replaces
// only the disposable ATOS projection in one transaction. Safe mode remains
// enabled until a separately reviewed incident-resolution migration clears it.
func (a *Adapter) RebuildProjections(ctx context.Context) error {
	reconciliationDB, unlockReconciliation, err := a.repository.LockReconciliation(ctx)
	if err != nil {
		return err
	}
	defer unlockReconciliation()
	rows, err := reconciliationDB.Query(ctx, `SELECT commitment,commitment_digest,canonical_cbor,semantic_digest,
 ledger_transaction_id,source_indicator,destination_indicator,decimals,allow_overdraft,state,attempts,last_error,finalized_at
 FROM financial_events ORDER BY sequence`)
	if err != nil {
		return err
	}
	previous := GenesisDigest
	sequence := int64(1)
	for rows.Next() {
		event, scanErr := scanEvent(rows)
		if scanErr != nil {
			rows.Close()
			return scanErr
		}
		canonical, marshalErr := codec.Marshal(event.Commitment)
		digest, digestErr := codec.Digest(CommitmentDomain, event.Commitment)
		if event.Sequence != sequence || event.PreviousCommitment != previous ||
			event.GatewayID != a.repository.gatewayID || event.NetworkID != a.repository.networkID ||
			marshalErr != nil || digestErr != nil || !bytes.Equal(canonical, event.CanonicalCBOR) || digest != event.Digest {
			rows.Close()
			return fmt.Errorf("financial: cannot rebuild from invalid commitment sequence %d", sequence)
		}
		previous, sequence = event.Digest, sequence+1
		if event.State == "finalized" {
			transaction, found, lookupErr := a.ledger.Lookup(ctx, event.LedgerReference)
			if lookupErr != nil || !found {
				rows.Close()
				return errors.Join(ErrLedgerUncertain, lookupErr)
			}
			if err := a.ledger.Verify(ctx, event, transaction); err != nil {
				rows.Close()
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	tx, err := reconciliationDB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var nextSequence int64
	var lastCommitment string
	if err := tx.QueryRow(ctx, `SELECT next_sequence,last_commitment FROM financial_chain_state WHERE singleton=TRUE FOR UPDATE`).Scan(&nextSequence, &lastCommitment); err != nil {
		return err
	}
	if nextSequence != sequence || lastCommitment != previous {
		return errors.New("financial: chain changed during projection rebuild")
	}
	if _, err := tx.Exec(ctx, `DELETE FROM financial_projections`); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO financial_projections(account_code,account_owner_id,asset,atomic_balance,last_sequence)
SELECT posting->>'account_code',posting->>'account_owner_id',event.asset,
 SUM(CASE posting->>'direction' WHEN 'credit' THEN (posting->>'atomic_amount')::numeric ELSE -(posting->>'atomic_amount')::numeric END),
 MAX(event.sequence)
FROM financial_events event CROSS JOIN LATERAL jsonb_array_elements(event.commitment->'postings') posting
WHERE event.state='finalized'
GROUP BY posting->>'account_code',posting->>'account_owner_id',event.asset`)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	_, err = a.auditIntegrity(ctx, reconciliationDB)
	return err
}
