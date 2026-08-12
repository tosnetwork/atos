package financial

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tosnetwork/tos-protocol/pkg/codec"
)

const (
	BatchVersion         = "atos_financial_batch_v2"
	BatchDomain          = "tos.atos.financial.batch.v2"
	BatchSignatureDomain = "tos.atos.financial.batch-signature.v2"
)

type BatchManifest struct {
	Version              string   `json:"version"`
	Canonicalization     string   `json:"canonicalization"`
	GatewayID            string   `json:"gateway_id"`
	NetworkID            string   `json:"network_id"`
	BatchSequence        int64    `json:"batch_sequence"`
	BatchID              string   `json:"batch_id"`
	FirstSequence        int64    `json:"first_sequence"`
	LastSequence         int64    `json:"last_sequence"`
	CommitmentCount      int      `json:"commitment_count"`
	PreviousBatchID      string   `json:"previous_batch_id"`
	PreviousMerkleRoot   string   `json:"previous_merkle_root"`
	MerkleRoot           string   `json:"merkle_root"`
	CommitmentDigests    []string `json:"commitment_digests"`
	LedgerEvidenceDigest string   `json:"ledger_evidence_digest"`
	CreatedUnixMillis    int64    `json:"created_unix_millis"`
}

type batchIdentity struct {
	Version              string   `json:"version"`
	Canonicalization     string   `json:"canonicalization"`
	GatewayID            string   `json:"gateway_id"`
	NetworkID            string   `json:"network_id"`
	BatchSequence        int64    `json:"batch_sequence"`
	FirstSequence        int64    `json:"first_sequence"`
	LastSequence         int64    `json:"last_sequence"`
	CommitmentCount      int      `json:"commitment_count"`
	PreviousBatchID      string   `json:"previous_batch_id"`
	PreviousMerkleRoot   string   `json:"previous_merkle_root"`
	MerkleRoot           string   `json:"merkle_root"`
	CommitmentDigests    []string `json:"commitment_digests"`
	LedgerEvidenceDigest string   `json:"ledger_evidence_digest"`
}

type Batch struct {
	Manifest       BatchManifest
	ManifestDigest string
	ManifestCBOR   []byte
	Commitments    []Commitment
	LedgerEvidence LedgerChainEvidence
	State          string
}

func (r *Repository) pendingBatchWith(ctx context.Context, db repositoryDB) (Batch, *SignatureEnvelope, error) {
	var batch Batch
	var manifestRaw, signatureRaw []byte
	var ledgerEvidenceRaw []byte
	err := db.QueryRow(ctx, `SELECT manifest,manifest_digest,manifest_cbor,ledger_evidence,state,COALESCE(signature_envelope,'null'::jsonb)
 FROM financial_batches WHERE state<>'anchored' ORDER BY batch_sequence LIMIT 1`).Scan(&manifestRaw, &batch.ManifestDigest, &batch.ManifestCBOR, &ledgerEvidenceRaw, &batch.State, &signatureRaw)
	if err != nil {
		return Batch{}, nil, err
	}
	if err := json.Unmarshal(manifestRaw, &batch.Manifest); err != nil {
		return Batch{}, nil, err
	}
	if err := json.Unmarshal(ledgerEvidenceRaw, &batch.LedgerEvidence); err != nil {
		return Batch{}, nil, err
	}
	rows, err := db.Query(ctx, `SELECT commitment FROM financial_events WHERE batch_id=$1 ORDER BY sequence`, batch.Manifest.BatchID)
	if err != nil {
		return Batch{}, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return Batch{}, nil, err
		}
		var commitment Commitment
		if err := json.Unmarshal(raw, &commitment); err != nil {
			return Batch{}, nil, err
		}
		batch.Commitments = append(batch.Commitments, commitment)
	}
	if err := rows.Err(); err != nil {
		return Batch{}, nil, err
	}
	var signature *SignatureEnvelope
	if string(signatureRaw) != "null" {
		signature = new(SignatureEnvelope)
		if err := json.Unmarshal(signatureRaw, signature); err != nil {
			return Batch{}, nil, err
		}
	}
	return batch, signature, nil
}

func MerkleRoot(digests []string) (string, error) {
	if len(digests) == 0 || len(digests) > 4096 {
		return "", errors.New("financial: Merkle batch must contain 1-4096 leaves")
	}
	level := make([][]byte, len(digests))
	for i, digest := range digests {
		raw, err := DigestBytes(digest)
		if err != nil {
			return "", err
		}
		leaf := sha256.Sum256(append([]byte{0}, raw...))
		level[i] = leaf[:]
	}
	for len(level) > 1 {
		next := make([][]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			right := level[i]
			if i+1 < len(level) {
				right = level[i+1]
			}
			input := make([]byte, 1, 1+len(level[i])+len(right))
			input[0] = 1
			input = append(input, level[i]...)
			input = append(input, right...)
			node := sha256.Sum256(input)
			next = append(next, node[:])
		}
		level = next
	}
	return "sha256:" + hex.EncodeToString(level[0]), nil
}

func makeBatchManifest(gatewayID, networkID string, batchSequence int64, previousBatchID, previousRoot, ledgerEvidenceDigest string, commitments []Commitment, created time.Time) (Batch, error) {
	digests := make([]string, len(commitments))
	for i, commitment := range commitments {
		if commitment.Sequence != commitments[0].Sequence+int64(i) {
			return Batch{}, errors.New("financial: non-contiguous commitment sequence")
		}
		digest, err := codec.Digest(CommitmentDomain, commitment)
		if err != nil {
			return Batch{}, err
		}
		digests[i] = digest
	}
	root, err := MerkleRoot(digests)
	if err != nil {
		return Batch{}, err
	}
	identity := batchIdentity{Version: BatchVersion, Canonicalization: Canonicalization, GatewayID: gatewayID, NetworkID: networkID, BatchSequence: batchSequence,
		FirstSequence: commitments[0].Sequence, LastSequence: commitments[len(commitments)-1].Sequence, CommitmentCount: len(commitments),
		PreviousBatchID: previousBatchID, PreviousMerkleRoot: previousRoot, MerkleRoot: root, CommitmentDigests: digests,
		LedgerEvidenceDigest: ledgerEvidenceDigest}
	identityBytes, err := codec.Marshal(identity)
	if err != nil {
		return Batch{}, err
	}
	idHash := sha256.Sum256(identityBytes)
	manifest := BatchManifest{Version: BatchVersion, Canonicalization: Canonicalization, GatewayID: gatewayID, NetworkID: networkID, BatchSequence: batchSequence,
		BatchID: "fbat_" + hex.EncodeToString(idHash[:]), FirstSequence: identity.FirstSequence, LastSequence: identity.LastSequence,
		CommitmentCount: len(commitments), PreviousBatchID: previousBatchID, PreviousMerkleRoot: previousRoot,
		MerkleRoot: root, CommitmentDigests: digests, LedgerEvidenceDigest: ledgerEvidenceDigest, CreatedUnixMillis: created.UTC().UnixMilli()}
	canonical, err := codec.Marshal(manifest)
	if err != nil {
		return Batch{}, err
	}
	digest, err := codec.Digest(BatchDomain, manifest)
	if err != nil {
		return Batch{}, err
	}
	return Batch{Manifest: manifest, ManifestDigest: digest, ManifestCBOR: canonical, Commitments: commitments, State: "created"}, nil
}

func (r *Repository) createBatchWith(ctx context.Context, db repositoryDB, limit int, ledger interface {
	ledgerClient
	ledgerChainReader
}) (Batch, error) {
	if limit < 1 || limit > 4096 {
		return Batch{}, errors.New("financial: batch limit must be 1-4096")
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Batch{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var batchSequence int64
	var previousBatchID, previousRoot string
	if err := tx.QueryRow(ctx, `SELECT next_batch_sequence,last_batch_id,last_batch_root FROM financial_chain_state WHERE singleton=TRUE FOR UPDATE`).Scan(&batchSequence, &previousBatchID, &previousRoot); err != nil {
		return Batch{}, err
	}
	rows, err := tx.Query(ctx, `SELECT commitment,commitment_digest,canonical_cbor,semantic_digest,
 ledger_transaction_id,source_indicator,destination_indicator,decimals,allow_overdraft,state,attempts,last_error,finalized_at
 FROM financial_events WHERE state='finalized' AND batch_id='' ORDER BY sequence LIMIT $1 FOR UPDATE`, limit)
	if err != nil {
		return Batch{}, err
	}
	var commitments []Commitment
	var events []Event
	for rows.Next() {
		event, scanErr := scanEvent(rows)
		if scanErr != nil {
			rows.Close()
			return Batch{}, scanErr
		}
		events = append(events, event)
		commitments = append(commitments, event.Commitment)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Batch{}, err
	}
	if len(commitments) == 0 {
		return Batch{}, pgx.ErrNoRows
	}
	if ledger == nil {
		return Batch{}, errors.New("financial: Blnk chain evidence source is required")
	}
	ledgerEvidence, err := makeLedgerBatchEvidence(ctx, ledger, events)
	if err != nil {
		return Batch{}, err
	}
	ledgerEvidenceDigest, err := ledgerEvidenceDigest(ledgerEvidence)
	if err != nil {
		return Batch{}, err
	}
	if batchSequence > 1 {
		var expected int64
		var previousLedgerSequence int64
		var previousLedgerHead string
		if err := tx.QueryRow(ctx, `SELECT last_sequence+1,
 (ledger_evidence->'state'->>'last_sequence')::bigint,
 ledger_evidence->'state'->>'head_hash'
 FROM financial_batches WHERE batch_sequence=$1`, batchSequence-1).Scan(&expected, &previousLedgerSequence, &previousLedgerHead); err != nil {
			return Batch{}, err
		}
		if commitments[0].Sequence != expected {
			return Batch{}, errors.New("financial: batch sequence gap")
		}
		if ledgerEvidence.State.FirstSequence != previousLedgerSequence+1 || ledgerEvidence.State.PreviousHash != previousLedgerHead {
			return Batch{}, errors.New("financial: Blnk chain gap or unexpected transaction between batches")
		}
	} else if commitments[0].Sequence != 1 {
		return Batch{}, errors.New("financial: first batch must begin at commitment sequence 1")
	} else if ledgerEvidence.State.FirstSequence != 1 || ledgerEvidence.State.PreviousHash != ledgerEvidence.State.GenesisHash {
		return Batch{}, errors.New("financial: first batch does not begin at the Blnk genesis link")
	}
	batch, err := makeBatchManifest(r.gatewayID, r.networkID, batchSequence, previousBatchID, previousRoot, ledgerEvidenceDigest, commitments, r.now())
	if err != nil {
		return Batch{}, err
	}
	manifestJSON, _ := json.Marshal(batch.Manifest)
	ledgerEvidenceJSON, _ := json.Marshal(ledgerEvidence)
	batch.LedgerEvidence = ledgerEvidence
	_, err = tx.Exec(ctx, `INSERT INTO financial_batches(batch_id,batch_sequence,first_sequence,last_sequence,commitment_count,
 previous_batch_id,previous_merkle_root,merkle_root,manifest_digest,manifest_cbor,manifest,ledger_evidence,created_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, batch.Manifest.BatchID, batch.Manifest.BatchSequence,
		batch.Manifest.FirstSequence, batch.Manifest.LastSequence, batch.Manifest.CommitmentCount,
		batch.Manifest.PreviousBatchID, batch.Manifest.PreviousMerkleRoot, batch.Manifest.MerkleRoot,
		batch.ManifestDigest, batch.ManifestCBOR, manifestJSON, ledgerEvidenceJSON, time.UnixMilli(batch.Manifest.CreatedUnixMillis))
	if err != nil {
		return Batch{}, err
	}
	result, err := tx.Exec(ctx, `UPDATE financial_events SET batch_id=$1,updated_at=now() WHERE sequence BETWEEN $2 AND $3 AND state='finalized' AND batch_id=''`, batch.Manifest.BatchID, batch.Manifest.FirstSequence, batch.Manifest.LastSequence)
	if err != nil {
		return Batch{}, fmt.Errorf("financial: claim batch commitments: %w", err)
	}
	if result.RowsAffected() != int64(batch.Manifest.CommitmentCount) {
		return Batch{}, errors.New("financial: batch commitment claim count changed concurrently")
	}
	_, err = tx.Exec(ctx, `UPDATE financial_chain_state SET next_batch_sequence=$1,last_batch_id=$2,last_batch_root=$3,updated_at=now() WHERE singleton=TRUE`, batchSequence+1, batch.Manifest.BatchID, batch.Manifest.MerkleRoot)
	if err != nil {
		return Batch{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Batch{}, err
	}
	return batch, nil
}
