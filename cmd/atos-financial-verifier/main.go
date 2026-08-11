// Command atos-financial-verifier independently verifies retained Managed
// financial evidence and resolves its finalized TOS anchor without reading the
// mutable ATOS database.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/financial"
	atostosv1 "github.com/tosnetwork/tos-protocol/gen/atos/tos/v1"
	"github.com/tosnetwork/tos-protocol/gen/atos/tos/v1/atostosv1connect"
)

type resolver struct {
	client         atostosv1connect.FinancialIntegrityServiceClient
	token, gateway string
}

func digest(value *atostosv1.Digest) string {
	if value == nil {
		return ""
	}
	return value.Algorithm + ":" + hex.EncodeToString(value.Value)
}
func fromProto(value *atostosv1.ManagedFinancialAnchorInput) financial.ManagedAnchor {
	if value == nil {
		return financial.ManagedAnchor{}
	}
	return financial.ManagedAnchor{
		Version: value.Version, AnchorID: value.AnchorId, BatchID: value.BatchId,
		BatchSequence: value.BatchSequence, FirstSequence: value.FirstSequence,
		LastSequence: value.LastSequence, CommitmentCount: value.CommitmentCount,
		PreviousAnchorID: value.PreviousAnchorId, PreviousMerkleRoot: digest(value.PreviousMerkleRoot),
		MerkleRoot: digest(value.MerkleRoot), ManifestDigest: digest(value.ManifestDigest),
		SignatureDigest: digest(value.SignatureDigest), SigningKeyID: value.SigningKeyId,
		Canonicalization: value.Canonicalization, GatewayID: value.GatewayId, NetworkID: value.NetworkId,
	}
}
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func (r resolver) ResolveManagedFinancialAnchor(ctx context.Context, anchor financial.ManagedAnchor) (financial.AnchorReceipt, bool, error) {
	now := time.Now()
	request := connect.NewRequest(&atostosv1.ResolveManagedFinancialAnchorRequest{Context: &atostosv1.RequestContext{RequestId: randomHex(16), TraceId: randomHex(16), CallerId: r.gateway, DeadlineUnixMillis: now.Add(30 * time.Second).UnixMilli()}, AnchorId: anchor.AnchorID, NetworkId: anchor.NetworkID})
	request.Header().Set("Authorization", "Bearer "+r.token)
	response, err := r.client.ResolveManagedFinancialAnchor(ctx, request)
	if err != nil {
		return financial.AnchorReceipt{}, false, err
	}
	if response.Msg == nil || !response.Msg.Found {
		return financial.AnchorReceipt{}, false, nil
	}
	msg := response.Msg
	receipt := financial.AnchorReceipt{Anchor: fromProto(msg.Anchor), PayloadDigest: digest(msg.PayloadDigest), Finalized: msg.Finalized, FinalizedCheckpoint: msg.FinalizedCheckpoint}
	if msg.AnchorRef != nil {
		receipt.NetworkReferenceID = msg.AnchorRef.Reference
		receipt.NetworkID = msg.AnchorRef.Network
		if msg.AnchorRef.Finalized {
			receipt.Finalized = true
		}
		if msg.AnchorRef.FinalizedCheckpoint > receipt.FinalizedCheckpoint {
			receipt.FinalizedCheckpoint = msg.AnchorRef.FinalizedCheckpoint
		}
	}
	return receipt, true, nil
}
func (r resolver) PublishManagedFinancialAnchor(context.Context, financial.ManagedAnchor) (financial.AnchorReceipt, error) {
	return financial.AnchorReceipt{}, fmt.Errorf("verifier is read-only")
}

func readJSON(path string, target any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func main() {
	evidencePath := flag.String("evidence", "", "retained evidence bundle JSON")
	anchorPath := flag.String("anchor", "", "retained anchor receipt JSON")
	keysPath := flag.String("trusted-keys", "", "JSON map of signing key ID to base64 public key")
	tosURL := flag.String("tos-url", "", "tos-protocol ConnectRPC base URL")
	gateway := flag.String("gateway", "", "expected gateway ID")
	network := flag.String("network", "", "expected TOS network ID")
	previousAnchor := flag.String("previous-anchor", "", "expected previous anchor ID for non-genesis batch")
	previousRoot := flag.String("previous-root", "", "expected previous Merkle root for non-genesis batch")
	previousCommitment := flag.String("previous-commitment", "", "expected prior commitment digest for non-genesis batch")
	previousLedgerHead := flag.String("previous-ledger-head", "", "expected prior Blnk chain head for non-genesis batch")
	previousLedgerSequence := flag.Int64("previous-ledger-sequence", 0, "expected prior Blnk chain sequence for non-genesis batch")
	retentionURL := flag.String("retention-url", "", "independent immutable-retention verification endpoint")
	retainedVersion := flag.String("retained-version", "", "exact immutable object version ID")
	minimumRetention := flag.Duration("minimum-retention", 365*24*time.Hour, "minimum remaining COMPLIANCE retention")
	flag.Parse()
	if *evidencePath == "" || *anchorPath == "" || *keysPath == "" || *tosURL == "" || *gateway == "" || *network == "" || *retentionURL == "" || *retainedVersion == "" {
		fmt.Fprintln(os.Stderr, "all evidence, anchor, trusted-keys, tos-url, gateway, network, retention-url and retained-version flags are required")
		os.Exit(2)
	}
	token := os.Getenv("ATOS_VERIFIER_TOS_TOKEN")
	if token == "" {
		fmt.Fprintln(os.Stderr, "ATOS_VERIFIER_TOS_TOKEN is required")
		os.Exit(2)
	}
	retentionKey := os.Getenv("ATOS_VERIFIER_RETENTION_HMAC_KEY")
	retentionResolver, err := financial.NewHTTPRetainer(*retentionURL, retentionKey, 30*time.Second, *minimumRetention)
	if err != nil {
		fail(err)
	}
	evidenceBytes, err := os.ReadFile(*evidencePath)
	if err != nil {
		fail(err)
	}
	bundle, err := financial.DecodeEvidenceBundle(evidenceBytes)
	if err != nil {
		fail(err)
	}
	var anchor financial.AnchorReceipt
	if err = readJSON(*anchorPath, &anchor); err != nil {
		fail(err)
	}
	keys := map[string]string{}
	if err = readJSON(*keysPath, &keys); err != nil {
		fail(err)
	}
	client := atostosv1connect.NewFinancialIntegrityServiceClient(http.DefaultClient, *tosURL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = financial.VerifyEvidence(ctx, bundle, anchor, financial.VerifyOptions{GatewayID: *gateway, NetworkID: *network, TrustedPublicKeys: keys, ExpectedPreviousAnchorID: *previousAnchor, ExpectedPreviousRoot: *previousRoot, ExpectedPreviousCommitment: *previousCommitment, ExpectedPreviousLedgerHead: *previousLedgerHead, ExpectedPreviousLedgerSequence: *previousLedgerSequence, Resolver: resolver{client: client, token: token, gateway: *gateway}, RetentionResolver: retentionResolver, RetainedVersionID: *retainedVersion, MinimumRetention: *minimumRetention})
	if err != nil {
		fail(err)
	}
	fmt.Printf("verified batch %s commitments=%d anchor=%s finalized_checkpoint=%d\n", bundle.Manifest.BatchID, bundle.Manifest.CommitmentCount, anchor.Anchor.AnchorID, anchor.FinalizedCheckpoint)
}
func fail(err error) { fmt.Fprintln(os.Stderr, "verification failed:", err); os.Exit(1) }
