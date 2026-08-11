package financial

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/tosnetwork/tos-protocol/pkg/codec"
)

const (
	ManagedAnchorVersion = "atos_managed_financial_anchor_v1"
	ManagedAnchorDomain  = "tos.atos.managed-financial-anchor.v1"
)

type ManagedAnchor struct {
	Version            string `json:"version"`
	AnchorID           string `json:"anchor_id,omitempty"`
	BatchID            string `json:"batch_id"`
	BatchSequence      uint64 `json:"batch_sequence"`
	FirstSequence      uint64 `json:"first_sequence"`
	LastSequence       uint64 `json:"last_sequence"`
	CommitmentCount    uint32 `json:"commitment_count"`
	PreviousAnchorID   string `json:"previous_anchor_id"`
	PreviousMerkleRoot string `json:"previous_merkle_root"`
	MerkleRoot         string `json:"merkle_root"`
	ManifestDigest     string `json:"manifest_digest"`
	SignatureDigest    string `json:"signature_digest"`
	SigningKeyID       string `json:"signing_key_id"`
	Canonicalization   string `json:"canonicalization"`
	GatewayID          string `json:"gateway_id"`
	NetworkID          string `json:"network_id"`
}

type anchorIdentity struct {
	Version            string `json:"version"`
	BatchID            string `json:"batch_id"`
	BatchSequence      uint64 `json:"batch_sequence"`
	FirstSequence      uint64 `json:"first_sequence"`
	LastSequence       uint64 `json:"last_sequence"`
	CommitmentCount    uint32 `json:"commitment_count"`
	PreviousAnchorID   string `json:"previous_anchor_id"`
	PreviousMerkleRoot string `json:"previous_merkle_root"`
	MerkleRoot         string `json:"merkle_root"`
	ManifestDigest     string `json:"manifest_digest"`
	SignatureDigest    string `json:"signature_digest"`
	SigningKeyID       string `json:"signing_key_id"`
	Canonicalization   string `json:"canonicalization"`
	GatewayID          string `json:"gateway_id"`
	NetworkID          string `json:"network_id"`
}

type AnchorReceipt struct {
	Anchor              ManagedAnchor `json:"anchor"`
	PayloadDigest       string        `json:"payload_digest"`
	NetworkReferenceID  string        `json:"network_reference_id"`
	NetworkID           string        `json:"network_id"`
	Finalized           bool          `json:"finalized"`
	FinalizedCheckpoint uint64        `json:"finalized_checkpoint"`
}

type AnchorPublisher interface {
	PublishManagedFinancialAnchor(context.Context, ManagedAnchor) (AnchorReceipt, error)
	ResolveManagedFinancialAnchor(context.Context, ManagedAnchor) (AnchorReceipt, bool, error)
}

func SignatureEnvelopeDigest(envelope SignatureEnvelope) (string, error) {
	return codec.Digest("tos.atos.financial.signature-envelope.v1", envelope)
}

func BuildManagedAnchor(batch Batch, signature SignatureEnvelope, previousAnchorID string) (ManagedAnchor, error) {
	signatureDigest, err := SignatureEnvelopeDigest(signature)
	if err != nil {
		return ManagedAnchor{}, err
	}
	identity := anchorIdentity{ManagedAnchorVersion, batch.Manifest.BatchID, uint64(batch.Manifest.BatchSequence),
		uint64(batch.Manifest.FirstSequence), uint64(batch.Manifest.LastSequence), uint32(batch.Manifest.CommitmentCount),
		previousAnchorID, batch.Manifest.PreviousMerkleRoot, batch.Manifest.MerkleRoot, batch.ManifestDigest,
		signatureDigest, signature.SigningKeyID, Canonicalization, batch.Manifest.GatewayID, batch.Manifest.NetworkID}
	canonical, err := codec.Marshal(identity)
	if err != nil {
		return ManagedAnchor{}, err
	}
	hash := sha256.Sum256(canonical)
	return ManagedAnchor{identity.Version, "fanchor_" + hex.EncodeToString(hash[:]), identity.BatchID,
		identity.BatchSequence, identity.FirstSequence, identity.LastSequence, identity.CommitmentCount,
		identity.PreviousAnchorID, identity.PreviousMerkleRoot, identity.MerkleRoot, identity.ManifestDigest,
		identity.SignatureDigest, identity.SigningKeyID, identity.Canonicalization, identity.GatewayID, identity.NetworkID}, nil
}

func AnchorPayloadDigest(anchor ManagedAnchor) (string, error) {
	return codec.Digest(ManagedAnchorDomain, anchor)
}

func (r *Repository) AnchorBatch(ctx context.Context, batch Batch, signature SignatureEnvelope, publisher AnchorPublisher, retainer Retainer) (AnchorReceipt, error) {
	if publisher == nil || retainer == nil {
		return AnchorReceipt{}, errors.New("financial: anchor publisher and retainer are required")
	}
	var previousAnchorID string
	if batch.Manifest.BatchSequence > 1 {
		if err := r.pool.QueryRow(ctx, `SELECT anchor_id FROM financial_batches WHERE batch_sequence=$1 AND state='anchored'`, batch.Manifest.BatchSequence-1).Scan(&previousAnchorID); err != nil {
			return AnchorReceipt{}, err
		}
	}
	anchor, err := BuildManagedAnchor(batch, signature, previousAnchorID)
	if err != nil {
		return AnchorReceipt{}, err
	}
	receipt, found, resolveErr := publisher.ResolveManagedFinancialAnchor(ctx, anchor)
	if resolveErr == nil && !found {
		receipt, resolveErr = publisher.PublishManagedFinancialAnchor(ctx, anchor)
	}
	if resolveErr != nil {
		return AnchorReceipt{}, resolveErr
	}
	if receipt.Anchor != anchor || !receipt.Finalized || receipt.NetworkID != anchor.NetworkID || receipt.FinalizedCheckpoint == 0 {
		return AnchorReceipt{}, errors.New("financial: TOS anchor is absent, changed, wrong-network, or not finalized")
	}
	expectedPayload, err := AnchorPayloadDigest(anchor)
	if err != nil || receipt.PayloadDigest != expectedPayload {
		return AnchorReceipt{}, errors.New("financial: TOS anchor payload mismatch")
	}
	body, _ := json.Marshal(receipt)
	hash := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	key := "atos-financial/v1/" + anchor.GatewayID + "/" + anchor.NetworkID + "/anchors/" + anchor.AnchorID + ".json"
	if _, err := retainer.PutIfAbsent(ctx, key, body, digest); err != nil {
		return AnchorReceipt{}, err
	}
	result, err := r.pool.Exec(ctx, `UPDATE financial_batches SET anchor_id=$2,state='anchored',updated_at=now() WHERE batch_id=$1 AND state IN ('retained','anchored') AND (anchor_id='' OR anchor_id=$2)`, batch.Manifest.BatchID, anchor.AnchorID)
	if err != nil {
		return AnchorReceipt{}, err
	}
	if result.RowsAffected() != 1 {
		return AnchorReceipt{}, ErrIdempotencyConflict
	}
	_, _ = r.pool.Exec(ctx, `UPDATE financial_chain_state SET last_anchor_id=$1,updated_at=now() WHERE singleton=TRUE`, anchor.AnchorID)
	return receipt, nil
}
