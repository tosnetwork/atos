package financial

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"github.com/tosnetwork/tos-protocol/pkg/codec"
)

func TestFinancialCommitmentV1NormativeVector(t *testing.T) {
	commitment := Commitment{
		Version: CommitmentVersion, Canonicalization: Canonicalization,
		GatewayID: "gateway.example", NetworkID: "tos-localnet-1", Sequence: 1,
		PreviousCommitment: GenesisDigest, EventID: "fevt_01", EventType: EventReserve,
		IdempotencyIdentity: "gateway.example:job:job_01:reserve:v1", OccurredUnixMillis: 1786420800000,
		LedgerReference: "atos-fevt-fevt_01", LedgerTransactionIDs: []string{"txn_01"},
		Asset: "USD", AtomicAmount: "125",
		Identities: Identities{PrincipalID: "principal_01", ProviderID: "provider_01", JobID: "job_01", QuoteID: "quote_01", CapabilityID: "capability_01", CapabilityVersion: "1.0.0"},
		Postings:   []Posting{{0, PrincipalAvailable, "principal_01", "debit", "125"}, {1, PrincipalReserved, "principal_01", "credit", "125"}},
	}
	digest, err := codec.Digest(CommitmentDomain, commitment)
	if err != nil {
		t.Fatal(err)
	}
	if digest != "sha256:6358a74c3fe5ed5ed6e7a4e204b2833b6f121056b177c8b3c416345aeca18020" {
		t.Fatalf("digest changed: %s", digest)
	}
	root, err := MerkleRoot([]string{digest})
	if err != nil {
		t.Fatal(err)
	}
	if root != "sha256:5a3e794d34f0333eb5df7b4e727b54f30ad8daef357ae357b298935e9687b2e8" {
		t.Fatalf("root changed: %s", root)
	}
}

func TestTransferRequestRejectsPostingOwnerIdentitySubstitution(t *testing.T) {
	request := TransferRequest{
		EventType: EventReserve, IdempotencyIdentity: "job:j1:reserve:v1",
		Identities: Identities{PrincipalID: "principal-1", ProviderID: "provider-1", JobID: "job-1", QuoteID: "quote-1", CapabilityID: "cap-1", CapabilityVersion: "1"},
		Asset:      "USD", Decimals: 2, AtomicAmount: "100",
		SourceCode: PrincipalAvailable, SourceOwnerID: "other-principal",
		DestinationCode: PrincipalReserved, DestinationOwnerID: "principal-1",
	}
	if err := request.Validate(); err == nil {
		t.Fatal("posting owner substitution was accepted")
	}
}

type fixedResolver struct{ receipt AnchorReceipt }

func (r fixedResolver) PublishManagedFinancialAnchor(context.Context, ManagedAnchor) (AnchorReceipt, error) {
	return r.receipt, nil
}
func (r fixedResolver) ResolveManagedFinancialAnchor(context.Context, ManagedAnchor) (AnchorReceipt, bool, error) {
	return r.receipt, true, nil
}

func testEvidence(t *testing.T) (EvidenceBundle, AnchorReceipt, VerifyOptions) {
	t.Helper()
	at := time.UnixMilli(1786420800000)
	r1 := TransferRequest{EventType: EventReserve, IdempotencyIdentity: "job:j1:reserve:v1", Identities: Identities{PrincipalID: "p1", ProviderID: "v1", JobID: "j1", QuoteID: "q1", CapabilityID: "c1", CapabilityVersion: "1"}, Asset: "USD", Decimals: 2, AtomicAmount: "125", SourceCode: PrincipalAvailable, SourceOwnerID: "p1", DestinationCode: PrincipalReserved, DestinationOwnerID: "p1", OccurredAt: at}
	e1, err := BuildCommitment("gateway.example", "tos-localnet-1", 1, GenesisDigest, at.UnixMilli(), r1)
	if err != nil {
		t.Fatal(err)
	}
	r2 := r1
	r2.EventType, r2.IdempotencyIdentity = EventEscrowFund, "job:j1:escrow-fund:v1"
	r2.SourceCode, r2.DestinationCode, r2.DestinationOwnerID = PrincipalReserved, ManagedEscrow, "j1"
	e2, err := BuildCommitment("gateway.example", "tos-localnet-1", 2, e1.Digest, at.Add(time.Second).UnixMilli(), r2)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := makeBatchManifest("gateway.example", "tos-localnet-1", 1, "", GenesisDigest, []Commitment{e1.Commitment, e2.Commitment}, at)
	if err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signing, _ := signingDigest(batch.Manifest)
	raw, _ := DigestBytes(signing)
	envelope := SignatureEnvelope{Version: "atos_financial_batch_signature_v1", BatchID: batch.Manifest.BatchID, ManifestDigest: batch.ManifestDigest, SigningDigest: signing, GatewayID: batch.Manifest.GatewayID, NetworkID: batch.Manifest.NetworkID, SigningKeyID: "kms-key-1", SigningAlgorithm: "ed25519", Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(private, raw)), PublicKey: base64.StdEncoding.EncodeToString(public), SignedUnixMillis: at.UnixMilli()}
	anchor, err := BuildManagedAnchor(batch, envelope, "")
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := AnchorPayloadDigest(anchor)
	receipt := AnchorReceipt{Anchor: anchor, PayloadDigest: payload, NetworkReferenceID: "tos:anchor:1", NetworkID: anchor.NetworkID, Finalized: true, FinalizedCheckpoint: 42}
	bundle := EvidenceBundle{"atos_financial_evidence_bundle_v1", batch.Manifest, base64.StdEncoding.EncodeToString(batch.ManifestCBOR), batch.Commitments, envelope}
	options := VerifyOptions{GatewayID: anchor.GatewayID, NetworkID: anchor.NetworkID, TrustedPublicKeys: map[string]string{"kms-key-1": envelope.PublicKey}, Resolver: fixedResolver{receipt}}
	return bundle, receipt, options
}

func TestIndependentVerifierRejectsTampering(t *testing.T) {
	bundle, receipt, options := testEvidence(t)
	if err := VerifyEvidence(context.Background(), bundle, receipt, options); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*EvidenceBundle, *AnchorReceipt, *VerifyOptions){
		"amount": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) { b.Commitments[0].AtomicAmount = "126" },
		"identity": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) {
			b.Commitments[0].Identities.JobID = "other-job"
		},
		"deletion": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) { b.Commitments = b.Commitments[1:] },
		"insertion": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) {
			b.Commitments = append(b.Commitments, b.Commitments[1])
		},
		"reordering": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) {
			b.Commitments[0], b.Commitments[1] = b.Commitments[1], b.Commitments[0]
		},
		"root": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) { b.Manifest.MerkleRoot = GenesisDigest },
		"previous_root": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) {
			b.Manifest.PreviousMerkleRoot = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
		},
		"key": func(_ *EvidenceBundle, _ *AnchorReceipt, o *VerifyOptions) {
			o.TrustedPublicKeys = map[string]string{"other": "bad"}
		},
		"network_domain": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) { b.Manifest.NetworkID = "wrong" },
		"gateway_domain": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) { b.Manifest.GatewayID = "wrong" },
		"canonicalization": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) {
			b.Commitments[0].Canonicalization = "unknown"
		},
		"sequence": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) { b.Commitments[1].Sequence = 4 },
		"previous_commitment": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) {
			b.Commitments[1].PreviousCommitment = GenesisDigest
		},
		"anchor": func(_ *EvidenceBundle, a *AnchorReceipt, _ *VerifyOptions) { a.PayloadDigest = GenesisDigest },
		"anchor_network": func(_ *EvidenceBundle, a *AnchorReceipt, _ *VerifyOptions) {
			a.NetworkID = "wrong"
		},
		"unfinalized": func(_ *EvidenceBundle, a *AnchorReceipt, _ *VerifyOptions) { a.Finalized = false },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			b, a, o := testEvidence(t)
			mutate(&b, &a, &o)
			if err := VerifyEvidence(context.Background(), b, a, o); err == nil {
				t.Fatal("tampering accepted")
			}
		})
	}
}
