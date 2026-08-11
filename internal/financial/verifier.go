package financial

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/tosnetwork/tos-protocol/pkg/codec"
)

type VerifyOptions struct {
	GatewayID                  string
	NetworkID                  string
	TrustedPublicKeys          map[string]string
	ExpectedPreviousAnchorID   string
	ExpectedPreviousRoot       string
	ExpectedPreviousCommitment string
	Resolver                   AnchorPublisher
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
	if bundle.Version != "atos_financial_evidence_bundle_v1" || options.GatewayID == "" || options.NetworkID == "" {
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
	rebuilt, err := makeBatchManifest(manifest.GatewayID, manifest.NetworkID, manifest.BatchSequence, manifest.PreviousBatchID, manifest.PreviousMerkleRoot, bundle.Commitments, time.UnixMilli(manifest.CreatedUnixMillis))
	if err != nil || rebuilt.Manifest.BatchID != manifest.BatchID || rebuilt.ManifestDigest != manifestDigest {
		return errors.New("financial verifier: batch identity mismatch")
	}
	if bundle.Signature.BatchID != manifest.BatchID || bundle.Signature.GatewayID != options.GatewayID || bundle.Signature.NetworkID != options.NetworkID {
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
	if err != nil || !found || live != anchorReceipt {
		return errors.New("financial verifier: finalized TOS anchor does not resolve independently")
	}
	return nil
}
