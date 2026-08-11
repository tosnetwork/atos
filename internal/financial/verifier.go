package financial

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/tosnetwork/tos-protocol/pkg/codec"
)

type VerifyOptions struct {
	GatewayID                      string
	NetworkID                      string
	TrustedPublicKeys              map[string]string
	ExpectedPreviousAnchorID       string
	ExpectedPreviousRoot           string
	ExpectedPreviousCommitment     string
	ExpectedPreviousLedgerHead     string
	ExpectedPreviousLedgerSequence int64
	Resolver                       AnchorPublisher
	RetentionResolver              RetentionResolver
	RetainedVersionID              string
	MinimumRetention               time.Duration
}

func DecodeEvidenceBundle(data []byte) (EvidenceBundle, error) {
	var bundle EvidenceBundle
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return EvidenceBundle{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return EvidenceBundle{}, errors.New("financial verifier: trailing evidence")
	}
	return bundle, nil
}

func VerifyEvidence(ctx context.Context, bundle EvidenceBundle, anchorReceipt AnchorReceipt, options VerifyOptions) error {
	if bundle.Version != "atos_financial_evidence_bundle_v2" || options.GatewayID == "" || options.NetworkID == "" {
		return errors.New("financial verifier: invalid version or expected domain")
	}
	manifest := bundle.Manifest
	if manifest.Version != BatchVersion || manifest.Canonicalization != Canonicalization || manifest.GatewayID != options.GatewayID || manifest.NetworkID != options.NetworkID {
		return errors.New("financial verifier: wrong batch version, gateway, network, or canonicalization")
	}
	if manifest.CommitmentCount != len(bundle.Commitments) || manifest.CommitmentCount != len(manifest.CommitmentDigests) || len(bundle.Commitments) == 0 {
		return errors.New("financial verifier: commitment count mismatch")
	}
	canonical, err := codec.Marshal(manifest)
	if err != nil {
		return err
	}
	retainedCanonical, err := base64.StdEncoding.Strict().DecodeString(bundle.ManifestCBOR)
	if err != nil || !bytes.Equal(canonical, retainedCanonical) {
		return errors.New("financial verifier: retained manifest canonical bytes mismatch")
	}
	manifestDigest, err := codec.Digest(BatchDomain, manifest)
	if err != nil || manifestDigest != bundle.Signature.ManifestDigest {
		return errors.New("financial verifier: manifest digest mismatch")
	}
	ledgerEvidenceDigest, err := codec.Digest("tos.atos.financial.blnk-evidence.v1", bundle.LedgerEvidence)
	if err != nil || ledgerEvidenceDigest != manifest.LedgerEvidenceDigest {
		return errors.New("financial verifier: Blnk ledger evidence digest mismatch")
	}
	if err := verifyBundleLedgerEvidence(bundle.Commitments, bundle.LedgerEvidence); err != nil {
		return err
	}
	if manifest.BatchSequence == 1 {
		if bundle.LedgerEvidence.State.FirstSequence != 1 || bundle.LedgerEvidence.State.PreviousHash != bundle.LedgerEvidence.State.GenesisHash {
			return errors.New("financial verifier: genesis batch does not begin at Blnk genesis")
		}
	} else if options.ExpectedPreviousLedgerSequence < 1 || options.ExpectedPreviousLedgerHead == "" ||
		bundle.LedgerEvidence.State.FirstSequence != options.ExpectedPreviousLedgerSequence+1 ||
		bundle.LedgerEvidence.State.PreviousHash != options.ExpectedPreviousLedgerHead {
		return errors.New("financial verifier: previous Blnk chain checkpoint is missing or discontinuous")
	}
	previous := GenesisDigest
	if manifest.FirstSequence > 1 {
		if options.ExpectedPreviousCommitment == "" {
			return errors.New("financial verifier: previous commitment evidence is required")
		}
		previous = options.ExpectedPreviousCommitment
	}
	for i, commitment := range bundle.Commitments {
		if err := commitment.Validate(); err != nil {
			return fmt.Errorf("financial verifier: commitment %d: %w", i, err)
		}
		expectedSequence := manifest.FirstSequence + int64(i)
		if commitment.Sequence != expectedSequence || commitment.GatewayID != options.GatewayID || commitment.NetworkID != options.NetworkID || commitment.PreviousCommitment != previous {
			return fmt.Errorf("financial verifier: commitment %d chain/domain mismatch", i)
		}
		digest, err := codec.Digest(CommitmentDomain, commitment)
		if err != nil || digest != manifest.CommitmentDigests[i] {
			return fmt.Errorf("financial verifier: commitment %d digest mismatch", i)
		}
		previous = digest
	}
	root, err := MerkleRoot(manifest.CommitmentDigests)
	if err != nil || root != manifest.MerkleRoot {
		return errors.New("financial verifier: Merkle root mismatch")
	}
	rebuilt, err := makeBatchManifest(manifest.GatewayID, manifest.NetworkID, manifest.BatchSequence, manifest.PreviousBatchID, manifest.PreviousMerkleRoot, manifest.LedgerEvidenceDigest, bundle.Commitments, time.UnixMilli(manifest.CreatedUnixMillis))
	if err != nil || rebuilt.Manifest.BatchID != manifest.BatchID || rebuilt.ManifestDigest != manifestDigest {
		return errors.New("financial verifier: batch identity mismatch")
	}
	if bundle.Signature.Version != "atos_financial_batch_signature_v2" || bundle.Signature.BatchID != manifest.BatchID || bundle.Signature.GatewayID != options.GatewayID || bundle.Signature.NetworkID != options.NetworkID {
		return errors.New("financial verifier: signature envelope domain mismatch")
	}
	expectedSigningDigest, err := signingDigest(manifest)
	if err != nil || expectedSigningDigest != bundle.Signature.SigningDigest {
		return errors.New("financial verifier: signing digest mismatch")
	}
	trusted, ok := options.TrustedPublicKeys[bundle.Signature.SigningKeyID]
	if !ok || subtle.ConstantTimeCompare([]byte(trusted), []byte(bundle.Signature.PublicKey)) != 1 {
		return errors.New("financial verifier: untrusted or substituted signing key")
	}
	if err := VerifySignature(bundle.Signature); err != nil {
		return err
	}
	bundleBytes, err := json.Marshal(bundle)
	if err != nil {
		return err
	}
	bundleHash := sha256.Sum256(bundleBytes)
	bundleDigest := "sha256:" + hex.EncodeToString(bundleHash[:])
	objectKey := fmt.Sprintf("atos-financial/v1/%s/%s/%d-%s.json", manifest.GatewayID, manifest.NetworkID, manifest.BatchSequence, manifest.BatchID)
	if options.RetentionResolver == nil || options.RetainedVersionID == "" || options.MinimumRetention <= 0 {
		return errors.New("financial verifier: immutable retention resolver and version are required")
	}
	retention, err := options.RetentionResolver.ResolveRetention(ctx, objectKey, options.RetainedVersionID, bundleDigest)
	if err != nil || retention.ObjectKey != objectKey || retention.VersionID != options.RetainedVersionID ||
		retention.Digest != bundleDigest || retention.LockMode != "COMPLIANCE" ||
		retention.RetainUntil.Before(time.Now().UTC().Truncate(time.Second).Add(options.MinimumRetention)) {
		return errors.New("financial verifier: immutable Object Lock evidence is missing, expired, or changed")
	}

	if manifest.BatchSequence == 1 {
		if manifest.PreviousBatchID != "" || manifest.PreviousMerkleRoot != GenesisDigest || options.ExpectedPreviousAnchorID != "" {
			return errors.New("financial verifier: invalid genesis batch linkage")
		}
	} else {
		if options.ExpectedPreviousAnchorID == "" || options.ExpectedPreviousRoot == "" || manifest.PreviousMerkleRoot != options.ExpectedPreviousRoot {
			return errors.New("financial verifier: previous batch/root evidence is missing or wrong")
		}
	}
	expectedAnchor, err := BuildManagedAnchor(rebuilt, bundle.Signature, options.ExpectedPreviousAnchorID)
	if err != nil {
		return err
	}
	if anchorReceipt.Anchor != expectedAnchor || !anchorReceipt.Finalized || anchorReceipt.FinalizedCheckpoint == 0 || anchorReceipt.NetworkID != options.NetworkID {
		return errors.New("financial verifier: retained anchor is changed, unfinalized, or wrong-network")
	}
	payloadDigest, err := AnchorPayloadDigest(expectedAnchor)
	if err != nil || payloadDigest != anchorReceipt.PayloadDigest {
		return errors.New("financial verifier: anchor payload digest mismatch")
	}
	if options.Resolver == nil {
		return errors.New("financial verifier: independent live anchor resolver is required")
	}
	live, found, err := options.Resolver.ResolveManagedFinancialAnchor(ctx, expectedAnchor)
	if err != nil || !found || live.Anchor != expectedAnchor || live.PayloadDigest != anchorReceipt.PayloadDigest ||
		live.NetworkReferenceID != anchorReceipt.NetworkReferenceID || live.NetworkID != anchorReceipt.NetworkID ||
		!live.Finalized || live.FinalizedCheckpoint < anchorReceipt.FinalizedCheckpoint {
		return errors.New("financial verifier: finalized TOS anchor does not resolve independently")
	}
	return nil
}

func verifyBundleLedgerEvidence(commitments []Commitment, evidence LedgerChainEvidence) error {
	if err := verifyLedgerChainLinks(evidence); err != nil {
		return fmt.Errorf("financial verifier: Blnk chain: %w", err)
	}
	if len(evidence.Transactions) != len(commitments) {
		return errors.New("financial verifier: unexpected or missing Blnk ledger transaction")
	}
	expected := make(map[string]Commitment, len(commitments))
	for _, commitment := range commitments {
		if len(commitment.LedgerTransactionIDs) != 1 {
			return errors.New("financial verifier: commitment has invalid Blnk transaction identity")
		}
		expected[commitment.LedgerTransactionIDs[0]] = commitment
	}
	seen := make(map[string]struct{}, len(commitments))
	for _, row := range evidence.Transactions {
		commitment, ok := expected[row.Transaction.TransactionID]
		if !ok || len(commitment.Postings) != 2 || commitment.Postings[0].Direction != "debit" || commitment.Postings[1].Direction != "credit" {
			return errors.New("financial verifier: commitment has invalid ledger postings")
		}
		expectedSource, sourceErr := AccountIndicator(commitment.GatewayID, commitment.NetworkID,
			commitment.Postings[0].AccountCode, commitment.Postings[0].AccountOwnerID, commitment.Asset)
		expectedDestination, destinationErr := AccountIndicator(commitment.GatewayID, commitment.NetworkID,
			commitment.Postings[1].AccountCode, commitment.Postings[1].AccountOwnerID, commitment.Asset)
		if sourceErr != nil || destinationErr != nil || row.ChainVersion != blnkChainVersionCBORV3 ||
			row.Transaction.Source == "" || row.Transaction.Destination == "" || row.Transaction.Source == row.Transaction.Destination ||
			row.Transaction.SourceIndicator != expectedSource || row.Transaction.DestinationIndicator != expectedDestination ||
			row.Transaction.Reference != commitment.LedgerReference || row.Transaction.PreciseAmount.String() != commitment.AtomicAmount ||
			row.Transaction.Currency != commitment.Asset || row.Transaction.Description != "atos-financial-v1:"+commitmentDigest(commitment) ||
			row.Transaction.Status != "APPLIED" {
			return errors.New("financial verifier: Blnk transaction does not match its financial commitment")
		}
		if _, duplicate := seen[row.Transaction.TransactionID]; duplicate {
			return errors.New("financial verifier: duplicate Blnk transaction")
		}
		seen[row.Transaction.TransactionID] = struct{}{}
	}
	return nil
}

func commitmentDigest(commitment Commitment) string {
	digest, _ := codec.Digest(CommitmentDomain, commitment)
	return digest
}
