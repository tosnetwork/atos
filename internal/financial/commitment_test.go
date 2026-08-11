package financial

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
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
type fixedRetentionResolver struct{ proof RetentionProof }

func (r fixedRetentionResolver) ResolveRetention(_ context.Context, key, version, digest string) (RetentionProof, error) {
	if key != r.proof.ObjectKey || version != r.proof.VersionID || digest != r.proof.Digest {
		return RetentionProof{}, ErrIdempotencyConflict
	}
	return r.proof, nil
}

func (r fixedResolver) PublishManagedFinancialAnchor(context.Context, ManagedAnchor) (AnchorReceipt, error) {
	return r.receipt, nil
}
func (r fixedResolver) ResolveManagedFinancialAnchor(context.Context, ManagedAnchor) (AnchorReceipt, bool, error) {
	receipt := r.receipt
	receipt.FinalizedCheckpoint++
	return receipt, true, nil
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
	source1, _ := AccountIndicator(e1.GatewayID, e1.NetworkID, e1.Postings[0].AccountCode, e1.Postings[0].AccountOwnerID, e1.Asset)
	destination1, _ := AccountIndicator(e1.GatewayID, e1.NetworkID, e1.Postings[1].AccountCode, e1.Postings[1].AccountOwnerID, e1.Asset)
	source2, _ := AccountIndicator(e2.GatewayID, e2.NetworkID, e2.Postings[0].AccountCode, e2.Postings[0].AccountOwnerID, e2.Asset)
	destination2, _ := AccountIndicator(e2.GatewayID, e2.NetworkID, e2.Postings[1].AccountCode, e2.Postings[1].AccountOwnerID, e2.Asset)
	ledgerRows := []LedgerChainRow{
		{Transaction: LedgerTransaction{TransactionID: e1.LedgerTransactionIDs[0], Source: "bln_source_1", Destination: "bln_destination_1", SourceIndicator: source1, DestinationIndicator: destination1, Reference: e1.LedgerReference, PreciseAmount: json.Number(e1.AtomicAmount), Currency: e1.Asset, Description: "atos-financial-v1:" + e1.Digest, Status: "APPLIED", CreatedAt: at}, Amount: "1.25", ChainVersion: blnkChainVersionCBORV3, ChainSequence: 1},
		{Transaction: LedgerTransaction{TransactionID: e2.LedgerTransactionIDs[0], Source: "bln_source_2", Destination: "bln_destination_2", SourceIndicator: source2, DestinationIndicator: destination2, Reference: e2.LedgerReference, PreciseAmount: json.Number(e2.AtomicAmount), Currency: e2.Asset, Description: "atos-financial-v1:" + e2.Digest, Status: "APPLIED", CreatedAt: at.Add(time.Second)}, Amount: "1.25", ChainVersion: blnkChainVersionCBORV3, ChainSequence: 2},
	}
	chainHead := strings.Repeat("0", 64)
	for index := range ledgerRows {
		ledgerRows[index].ChainPreviousHash = chainHead
		chainHead, err = ledgerChainHash(chainHead, ledgerRows[index])
		if err != nil {
			t.Fatal(err)
		}
		ledgerRows[index].ChainHash = chainHead
	}
	ledgerEvidence := LedgerChainEvidence{State: LedgerChainState{ChainKey: "global", FirstSequence: 1, LastSequence: 2, PreviousHash: strings.Repeat("0", 64), HeadHash: chainHead, GenesisHash: strings.Repeat("0", 64)}, Transactions: ledgerRows}
	ledgerDigest, err := codec.Digest("tos.atos.financial.blnk-evidence.v1", ledgerEvidence)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := makeBatchManifest("gateway.example", "tos-localnet-1", 1, "", GenesisDigest, ledgerDigest, []Commitment{e1.Commitment, e2.Commitment}, at)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Manifest.BatchID != "fbat_0ebac55bb30866d2a5326ef36f21aac90c3b9060928af43862cdc72bb79e4d7b" ||
		batch.ManifestDigest != "sha256:1e3984bf5c3c54608e486202be5a21d3c826123d39f7fca72cd7e300385af3b0" ||
		base64.StdEncoding.EncodeToString(batch.ManifestCBOR) != "r2d2ZXJzaW9ud2F0b3NfZmluYW5jaWFsX2JhdGNoX3YyaGJhdGNoX2lkeEVmYmF0XzBlYmFjNTViYjMwODY2ZDJhNTMyNmVmMzZmMjFhYWM5MGMzYjkwNjA5MjhhZjQzODYyY2RjNzJiYjc5ZTRkN2JqZ2F0ZXdheV9pZG9nYXRld2F5LmV4YW1wbGVqbmV0d29ya19pZG50b3MtbG9jYWxuZXQtMWttZXJrbGVfcm9vdHhHc2hhMjU2OjJjMjljY2QxZDM4Y2VmZjliNDFhMmIyODAzMTUzYzIwNTE1MDczNzkzODM1Njk3MWQ5MDQ3MTJkMjVmZGJkZWFtbGFzdF9zZXF1ZW5jZQJuYmF0Y2hfc2VxdWVuY2UBbmZpcnN0X3NlcXVlbmNlAXBjYW5vbmljYWxpemF0aW9ueB9yZmM4OTQ5X2NvcmVfZGV0ZXJtaW5pc3RpY19jYm9ycGNvbW1pdG1lbnRfY291bnQCcXByZXZpb3VzX2JhdGNoX2lkYHJjb21taXRtZW50X2RpZ2VzdHOCeEdzaGEyNTY6NmVhMWMwZmMwMzg4YzdjZjM1Y2MzNjc0MDlmMjUyY2I0NDc3M2E1ZDc4ODNhNjJiZmM1ZDdkYzhjOWI5MGVjOHhHc2hhMjU2Ojc5ZjNjZGVkNTY5OGM5N2VjMjY4YThiOGU5NmFiMzNmODYwOWZhNmI0ZWM0Nzc1YjlhM2RmZWFlNzM2NWM1MjZzY3JlYXRlZF91bml4X21pbGxpcxsAAAGf7voqAHRwcmV2aW91c19tZXJrbGVfcm9vdHhHc2hhMjU2OjAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDB2bGVkZ2VyX2V2aWRlbmNlX2RpZ2VzdHhHc2hhMjU2OjIzMzhjN2M2YTE4YmM1NDZhZDU3N2UxY2FiOTQzNTJkNjIxN2U5YTNhOWUxNjUyZDlkNDU2N2ExYWQxOTU1MTQ=" {
		t.Fatalf("financial batch V2 deterministic test vector changed: batch=%s digest=%s cbor=%s ledger=%s", batch.Manifest.BatchID, batch.ManifestDigest, base64.StdEncoding.EncodeToString(batch.ManifestCBOR), ledgerDigest)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signing, _ := signingDigest(batch.Manifest)
	raw, _ := DigestBytes(signing)
	envelope := SignatureEnvelope{Version: "atos_financial_batch_signature_v2", BatchID: batch.Manifest.BatchID, ManifestDigest: batch.ManifestDigest, SigningDigest: signing, GatewayID: batch.Manifest.GatewayID, NetworkID: batch.Manifest.NetworkID, SigningKeyID: "kms-key-1", SigningAlgorithm: "ed25519", Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(private, raw)), PublicKey: base64.StdEncoding.EncodeToString(public), SignedUnixMillis: at.UnixMilli()}
	anchor, err := BuildManagedAnchor(batch, envelope, "")
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := AnchorPayloadDigest(anchor)
	receipt := AnchorReceipt{Anchor: anchor, PayloadDigest: payload, NetworkReferenceID: "tos:anchor:1", NetworkID: anchor.NetworkID, Finalized: true, FinalizedCheckpoint: 42}
	bundle := EvidenceBundle{Version: "atos_financial_evidence_bundle_v2", Manifest: batch.Manifest, ManifestCBOR: base64.StdEncoding.EncodeToString(batch.ManifestCBOR), Commitments: batch.Commitments, LedgerEvidence: ledgerEvidence, Signature: envelope}
	bundleBytes, _ := json.Marshal(bundle)
	bundleHash := sha256.Sum256(bundleBytes)
	bundleDigest := "sha256:" + hex.EncodeToString(bundleHash[:])
	objectKey := "atos-financial/v1/gateway.example/tos-localnet-1/1-" + batch.Manifest.BatchID + ".json"
	retention := RetentionProof{ObjectKey: objectKey, VersionID: "locked-version-1", Digest: bundleDigest, LockMode: "COMPLIANCE", RetainUntil: time.Now().UTC().Add(24 * time.Hour)}
	options := VerifyOptions{GatewayID: anchor.GatewayID, NetworkID: anchor.NetworkID, TrustedPublicKeys: map[string]string{"kms-key-1": envelope.PublicKey}, Resolver: fixedResolver{receipt}, RetentionResolver: fixedRetentionResolver{retention}, RetainedVersionID: retention.VersionID, MinimumRetention: time.Hour}
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
		"ledger_deletion": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) {
			b.LedgerEvidence.Transactions = b.LedgerEvidence.Transactions[1:]
		},
		"ledger_mutation": func(b *EvidenceBundle, _ *AnchorReceipt, _ *VerifyOptions) {
			b.LedgerEvidence.Transactions[0].Transaction.PreciseAmount = "999"
		},
		"retention_version": func(_ *EvidenceBundle, _ *AnchorReceipt, o *VerifyOptions) { o.RetainedVersionID = "substituted" },
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

func TestIndependentVerifierBindsLedgerAccountIndicators(t *testing.T) {
	bundle, _, _ := testEvidence(t)
	wrongSource, err := AccountIndicator(bundle.Commitments[0].GatewayID, bundle.Commitments[0].NetworkID,
		ProviderPayable, "substituted-provider", bundle.Commitments[0].Asset)
	if err != nil {
		t.Fatal(err)
	}
	bundle.LedgerEvidence.Transactions[0].Transaction.SourceIndicator = wrongSource
	previous := bundle.LedgerEvidence.State.GenesisHash
	for index := range bundle.LedgerEvidence.Transactions {
		bundle.LedgerEvidence.Transactions[index].ChainPreviousHash = previous
		previous, err = ledgerChainHash(previous, bundle.LedgerEvidence.Transactions[index])
		if err != nil {
			t.Fatal(err)
		}
		bundle.LedgerEvidence.Transactions[index].ChainHash = previous
	}
	bundle.LedgerEvidence.State.HeadHash = previous
	if err := verifyBundleLedgerEvidence(bundle.Commitments, bundle.LedgerEvidence); err == nil {
		t.Fatal("verifier accepted a valid hash chain whose ledger source did not match the signed debit posting")
	}
}
