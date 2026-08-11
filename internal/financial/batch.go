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
	BatchVersion         = "atos_financial_batch_v1"
	BatchDomain          = "tos.atos.financial.batch.v1"
	BatchSignatureDomain = "tos.atos.financial.batch-signature.v1"
)

type BatchManifest struct {
	Version            string   `json:"version"`
	Canonicalization   string   `json:"canonicalization"`
	GatewayID          string   `json:"gateway_id"`
	NetworkID          string   `json:"network_id"`
	BatchSequence      int64    `json:"batch_sequence"`
	BatchID            string   `json:"batch_id"`
	FirstSequence      int64    `json:"first_sequence"`
	LastSequence       int64    `json:"last_sequence"`
	CommitmentCount    int      `json:"commitment_count"`
	PreviousBatchID    string   `json:"previous_batch_id"`
	PreviousMerkleRoot string   `json:"previous_merkle_root"`
	MerkleRoot         string   `json:"merkle_root"`
	CommitmentDigests  []string `json:"commitment_digests"`
	CreatedUnixMillis  int64    `json:"created_unix_millis"`
}

type batchIdentity struct {
	Version            string   `json:"version"`
	Canonicalization   string   `json:"canonicalization"`
	GatewayID          string   `json:"gateway_id"`
	NetworkID          string   `json:"network_id"`
	BatchSequence      int64    `json:"batch_sequence"`
	FirstSequence      int64    `json:"first_sequence"`
	LastSequence       int64    `json:"last_sequence"`
	CommitmentCount    int      `json:"commitment_count"`
	PreviousBatchID    string   `json:"previous_batch_id"`
	PreviousMerkleRoot string   `json:"previous_merkle_root"`
	MerkleRoot         string   `json:"merkle_root"`
	CommitmentDigests  []string `json:"commitment_digests"`
}

type Batch struct {
	Manifest       BatchManifest
	ManifestDigest string
	ManifestCBOR   []byte
	Commitments    []Commitment
	State          string
}

func (r *Repository) PendingBatch(ctx context.Context) (Batch, *SignatureEnvelope, error) {
	var batch Batch
	var manifestRaw, signatureRaw []byte
	err := r.pool.QueryRow(ctx, `SELECT manifest,manifest_digest,manifest_cbor,state,COALESCE(signature_envelope,'null'::jsonb)
 FROM financial_batches WHERE state<>'anchored' ORDER BY batch_sequence LIMIT 1`).Scan(&manifestRaw, &batch.ManifestDigest, &batch.ManifestCBOR, &batch.State, &signatureRaw)
	if err != nil {
		return Batch{}, nil, err
	}
	if err := json.Unmarshal(manifestRaw, &batch.Manifest); err != nil {
		return Batch{}, nil, err
	}
	rows, err := r.pool.Query(ctx, `SELECT commitment FROM financial_events WHERE batch_id=$1 ORDER BY sequence`, batch.Manifest.BatchID)
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

func makeBatchManifest(gatewayID, networkID string, batchSequence int64, previousBatchID, previousRoot string, commitments []Commitment, created time.Time) (Batch, error) {
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
	identity := batchIdentity{BatchVersion, Canonicalization, gatewayID, networkID, batchSequence,
		commitments[0].Sequence, commitments[len(commitments)-1].Sequence, len(commitments),
		previousBatchID, previousRoot, root, digests}
	identityBytes, err := codec.Marshal(identity)
	if err != nil {
		return Batch{}, err
	}
	idHash := sha256.Sum256(identityBytes)
	manifest := BatchManifest{BatchVersion, Canonicalization, gatewayID, networkID, batchSequence,
		"fbat_" + hex.EncodeToString(idHash[:]), identity.FirstSequence, identity.LastSequence,
		len(commitments), previousBatchID, previousRoot, root, digests, created.UTC().UnixMilli()}
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

func (r *Repository) CreateBatch(ctx context.Context, limit int) (Batch, error) {
	if limit < 1 || limit > 4096 {
		return Batch{}, errors.New("financial: batch limit must be 1-4096")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Batch{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var batchSequence int64
	var previousBatchID, previousRoot string
	if err := tx.QueryRow(ctx, `SELECT next_batch_sequence,last_batch_id,last_batch_root FROM financial_chain_state WHERE singleton=TRUE FOR UPDATE`).Scan(&batchSequence, &previousBatchID, &previousRoot); err != nil {
		return Batch{}, err
	}
	rows, err := tx.Query(ctx, `SELECT commitment FROM financial_events WHERE state='finalized' AND batch_id='' ORDER BY sequence LIMIT $1 FOR UPDATE`, limit)
	if err != nil {
		return Batch{}, err
	}
	var commitments []Commitment
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return Batch{}, err
		}
		var commitment Commitment
		if err := json.Unmarshal(raw, &commitment); err != nil {
			rows.Close()
			return Batch{}, err
		}
		commitments = append(commitments, commitment)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Batch{}, err
	}
	if len(commitments) == 0 {
		return Batch{}, pgx.ErrNoRows
	}
	if batchSequence > 1 {
		var expected int64
		if err := tx.QueryRow(ctx, `SELECT last_sequence+1 FROM financial_batches WHERE batch_sequence=$1`, batchSequence-1).Scan(&expected); err != nil {
			return Batch{}, err
		}
		if commitments[0].Sequence != expected {
			return Batch{}, errors.New("financial: batch sequence gap")
		}
	} else if commitments[0].Sequence != 1 {
		return Batch{}, errors.New("financial: first batch must begin at commitment sequence 1")
	}
	batch, err := makeBatchManifest(r.gatewayID, r.networkID, batchSequence, previousBatchID, previousRoot, commitments, r.now())
	if err != nil {
		return Batch{}, err
	}
	manifestJSON, _ := json.Marshal(batch.Manifest)
	_, err = tx.Exec(ctx, `INSERT INTO financial_batches(batch_id,batch_sequence,first_sequence,last_sequence,commitment_count,
 previous_batch_id,previous_merkle_root,merkle_root,manifest_digest,manifest_cbor,manifest,created_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, batch.Manifest.BatchID, batch.Manifest.BatchSequence,
		batch.Manifest.FirstSequence, batch.Manifest.LastSequence, batch.Manifest.CommitmentCount,
		batch.Manifest.PreviousBatchID, batch.Manifest.PreviousMerkleRoot, batch.Manifest.MerkleRoot,
		batch.ManifestDigest, batch.ManifestCBOR, manifestJSON, time.UnixMilli(batch.Manifest.CreatedUnixMillis))
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
