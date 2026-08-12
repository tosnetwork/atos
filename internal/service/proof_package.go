package service

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/tos-protocol/pkg/verifiedproof"
	"strings"
	"time"
)

type PortableProofService struct {
	store store.Store
	core  toscore.Core
	now   func() time.Time
}

func NewPortableProofService(s store.Store, c toscore.Core) *PortableProofService {
	return &PortableProofService{s, c, func() time.Time { return time.Now().UTC() }}
}

type PortableProof struct {
	PackageID     string                        `json:"package_id"`
	Version       string                        `json:"version"`
	Digest        string                        `json:"package_digest"`
	CanonicalCBOR string                        `json:"canonical_cbor_base64"`
	Checkpoint    domain.ProofPackageCheckpoint `json:"operation_checkpoint"`
}

func atomicTOS(v domain.Money) (string, error) {
	if v.Currency != "TOS" {
		return "", fmt.Errorf("asset %q is not TOS", v.Currency)
	}
	a, e := money.Parse(v.Amount, "TOS", 9)
	if e != nil {
		return "", e
	}
	return a.Minor.String(), nil
}
func vpRef(network, reference string, checkpoint uint64) verifiedproof.Reference {
	return verifiedproof.Reference{Network: network, Reference: reference, FinalizedCheckpoint: checkpoint}
}
func semanticID(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}
func (s *PortableProofService) Get(ctx context.Context, receiptID, principalID string) (PortableProof, error) {
	op, e := s.store.ProofPackageOperationByReceipt(ctx, receiptID)
	if e != nil {
		if errors.Is(e, store.ErrNotFound) {
			return PortableProof{}, domain.NewError(domain.ErrNotFound, "portable proof not found", false)
		}
		return PortableProof{}, e
	}
	if op.PrincipalID != principalID {
		return PortableProof{}, domain.NewError(domain.ErrPermissionDenied, "not the proof owner", false)
	}
	if op.Checkpoint != domain.ProofPackageCompleted {
		return PortableProof{}, domain.NewError(domain.ErrNetworkUnavailable, "portable proof is still reconciling", true)
	}
	return publicProof(op), nil
}
func publicProof(op domain.ProofPackageOperation) PortableProof {
	id, _ := func() (string, error) {
		p, e := verifiedproof.Parse(op.CanonicalCBOR)
		if e != nil {
			return "", e
		}
		return verifiedproof.PackageID(p)
	}()
	return PortableProof{id, verifiedproof.Version, op.PackageDigest, base64.StdEncoding.EncodeToString(op.CanonicalCBOR), op.Checkpoint}
}
func (s *PortableProofService) Create(ctx context.Context, receiptID, principalID string) (PortableProof, error) {
	r, e := s.store.GetReceipt(ctx, receiptID)
	if e != nil {
		return PortableProof{}, e
	}
	if r.PrincipalID != principalID {
		return PortableProof{}, domain.NewError(domain.ErrPermissionDenied, "not the receipt owner", false)
	}
	if r.TrustMode != domain.TrustModeVerified || r.ProofProfile != domain.ProofProfileTOSVerifiedV1 {
		return PortableProof{}, domain.NewError(domain.ErrProofProfileUnavailable, "portable proof requires tos_verified_v1", false)
	}
	j, e := s.store.GetJob(ctx, r.JobID)
	if e != nil {
		return PortableProof{}, e
	}
	q, e := s.store.GetQuote(ctx, r.QuoteID)
	if e != nil {
		return PortableProof{}, e
	}
	esc, e := s.store.GetEscrow(ctx, r.EscrowID)
	if e != nil {
		return PortableProof{}, e
	}
	sem := semanticID(s.core.Network(), q.CommitmentDomain, q.ID, j.ID, esc.ID, r.ID, r.NetworkProofRef)
	now := s.now()
	op := domain.ProofPackageOperation{ID: "proofop_" + strings.TrimPrefix(sem, "sha256:")[:32], ReceiptID: r.ID, JobID: j.ID, QuoteID: q.ID, EscrowID: esc.ID, PrincipalID: principalID, SemanticDigest: sem, Checkpoint: domain.ProofPackageIntentPersisted, CreatedAt: now, UpdatedAt: now}
	op, _, e = s.store.OpenProofPackageOperation(ctx, op)
	if e != nil {
		return PortableProof{}, e
	}
	if op.Checkpoint == domain.ProofPackageCompleted {
		return publicProof(op), nil
	}
	return s.reconcile(ctx, op, q, j, esc, r)
}

func (s *PortableProofService) ReconcileStaleOperations(ctx context.Context, cutoff time.Time, limit int) error {
	ops, e := s.store.StaleProofPackageOperations(ctx, cutoff, limit)
	if e != nil {
		return e
	}
	var joined error
	for _, op := range ops {
		if _, e := s.Create(ctx, op.ReceiptID, op.PrincipalID); e != nil {
			joined = errors.Join(joined, e)
		}
	}
	return joined
}
func (s *PortableProofService) RunReconciler(ctx context.Context, interval, staleAfter time.Duration, limit int, report func(error)) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	if staleAfter <= 0 {
		staleAfter = 30 * time.Second
	}
	if limit <= 0 {
		limit = 100
	}
	sweep := func() {
		if e := s.ReconcileStaleOperations(ctx, s.now().Add(-staleAfter), limit); e != nil && report != nil {
			report(e)
		}
	}
	sweep()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}
func (s *PortableProofService) reconcile(ctx context.Context, op domain.ProofPackageOperation, q domain.Quote, j domain.Job, esc domain.Escrow, r domain.Receipt) (PortableProof, error) {
	fail := func(err error) (PortableProof, error) {
		_, _ = s.store.UpdateProofPackageOperation(ctx, op.ID, func(x domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
			if x.Checkpoint.Terminal() {
				return x, nil
			}
			x.Checkpoint = domain.ProofPackageReconciling
			x.LastError = err.Error()
			x.UpdatedAt = s.now()
			return x, nil
		})
		return PortableProof{}, err
	}
	if q.Commitment == nil || !q.Commitment.Finalized || q.Commitment.FinalizedCheckpoint == 0 {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "finalized Quote commitment required", true))
	}
	liveQ, found, e := s.core.GetQuoteCommitment(ctx, q)
	if e != nil || !found {
		return fail(domain.NewError(domain.ErrNetworkUnavailable, "canonical Quote unavailable", true))
	}
	if liveQ.Digest != q.Commitment.Digest || liveQ.Reference != q.Commitment.Reference || !liveQ.Finalized || liveQ.FinalizedCheckpoint < q.Commitment.FinalizedCheckpoint {
		return fail(domain.NewError(domain.ErrQuoteMismatch, "canonical Quote mismatch", false))
	}
	liveE, found, e := s.core.GetEscrow(ctx, toscore.GetEscrowRequest{Quote: q, JobID: j.ID, EscrowID: esc.ID, ExpectedReservationDigest: esc.ReservationDigest, ExpectedEscrowRef: esc.NetworkProofRef})
	if e != nil || !found {
		return fail(domain.NewError(domain.ErrNetworkUnavailable, "canonical TaskEscrow unavailable", true))
	}
	if !liveE.Finalized || liveE.FinalizedCheckpoint == 0 || liveE.ID != esc.ID || liveE.ReservationDigest != esc.ReservationDigest || liveE.Status != esc.Status {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "canonical TaskEscrow mismatch", false))
	}
	requester, bound, revoked, _, e := s.core.ResolvePrincipalBindingStatus(ctx, q.PrincipalID)
	if e != nil || !bound || revoked || requester.FinalizedCheckpoint == 0 {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "requester identity binding unavailable", true))
	}
	provider, bound, revoked, _, e := s.core.ResolvePrincipalBindingStatus(ctx, q.ProviderID)
	if e != nil || !bound || revoked || provider.FinalizedCheckpoint == 0 {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "provider identity binding unavailable", true))
	}
	own, found, e := s.core.ResolveCapabilityOwnershipEvidence(ctx, q.CapabilityID, q.ProviderID, q.CapabilityVersion, q.ManifestCommitment)
	if e != nil || !found {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "Capability ownership unavailable", true))
	}
	if j.ExecutionReceipt == nil {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "execution Receipt unavailable", false))
	}
	er := *j.ExecutionReceipt
	auth, found, e := s.core.ResolveExecutionSignerAuthorization(ctx, q.ProviderID, q.CapabilityID, q.CapabilityVersion, er.ExecutionSignerID, er.CompletedAt)
	if e != nil || !found || auth.Revoked || len(auth.SignerPublicKey) != 32 || auth.FinalizedCheckpoint == 0 {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "execution signer authorization unavailable", true))
	}
	receiptWire, e := s.core.PortableReceiptEvidence(ctx, er)
	if e != nil {
		return fail(e)
	}
	receiptLive, found, e := s.core.ResolveExecutionReceiptEvidence(ctx, er)
	if e != nil || !found || !receiptLive.Finalized || receiptLive.FinalizedCheckpoint == 0 || receiptLive.Digest != receiptWire.Digest {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "canonical Receipt unavailable", true))
	}
	pos, found, e := s.core.ReadProofOfServiceEvidence(ctx, er)
	if e != nil || !found || !pos.Finalized || pos.FinalizedCheckpoint == 0 {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "Proof-of-Service unavailable", true))
	}
	reserve, e := atomicTOS(esc.Reserved)
	if e != nil {
		return fail(e)
	}
	subtotal, e := atomicTOS(domain.Money{Amount: q.Price.Subtotal, Currency: q.Price.Currency})
	if e != nil {
		return fail(e)
	}
	fees, e := atomicTOS(domain.Money{Amount: q.Price.Fees, Currency: q.Price.Currency})
	if e != nil {
		return fail(e)
	}
	total, e := atomicTOS(domain.Money{Amount: q.Price.TotalMax, Currency: q.Price.Currency})
	if e != nil {
		return fail(e)
	}
	charged, e := atomicTOS(r.Charged)
	if e != nil {
		return fail(e)
	}
	refunded, e := atomicTOS(r.Refunded)
	if e != nil {
		return fail(e)
	}
	sig, e := base64.StdEncoding.DecodeString(er.Signature)
	if e != nil {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "Receipt signature encoding invalid", false))
	}
	kind := "provider_settlement"
	if r.Status == domain.ReceiptReleased {
		kind = "requester_release"
	} else if r.Status == domain.ReceiptSettledAfterDispute || r.Status == domain.ReceiptReleasedAfterDispute {
		kind = "dispute_resolution"
	}
	if r.NetworkProofRef == "" || r.FinalizedCheckpoint == 0 {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "terminal outcome finality unavailable", true))
	}
	signerProof := &verifiedproof.SignerAuthorization{AuthorizationID: auth.AuthorizationID, ExecutionSignerID: auth.ExecutionSignerID, AuthorizationRef: vpRef(s.core.Network(), auth.AuthorizationRef, auth.FinalizedCheckpoint), SignatureAlgorithm: auth.SignatureAlgorithm, SignerPublicKey: auth.SignerPublicKey, ValidFromUnixNanos: auth.ValidFrom.UnixNano(), ValidUntilUnixNanos: auth.ValidUntil.UnixNano()}
	receiptProof := &verifiedproof.Receipt{ReceiptID: er.ID, ReceiptDigest: receiptWire.Digest, ReceiptRef: vpRef(receiptLive.Network, receiptLive.Reference, receiptLive.FinalizedCheckpoint), Result: string(er.Result), InputCommitment: er.InputHash, OutputCommitment: er.OutputHash, UsageCommitment: er.UsageCommitment, StartedUnixNanos: er.StartedAt.UnixNano(), CompletedUnixNanos: er.CompletedAt.UnixNano(), ChargedAtomic: charged, SignatureAlgorithm: er.SignatureAlgorithm, Signature: sig, CanonicalCBOR: receiptWire.CanonicalCBOR}
	posProof := &verifiedproof.ProofOfService{EvidenceID: pos.EvidenceID, EvidenceDigest: pos.Digest, EvidenceRef: vpRef(pos.Network, pos.Reference, pos.FinalizedCheckpoint), ContentDigest: pos.ContentDigest}
	p := verifiedproof.Package{Version: verifiedproof.Version, Canonicalization: verifiedproof.Canonicalization, NetworkID: s.core.Network(), GatewayDomain: q.CommitmentDomain, PrincipalID: q.PrincipalID, RequesterAgentID: requester.AgentID, RequesterIdentityRef: vpRef(requester.Network, requester.BindingRef, requester.FinalizedCheckpoint), ProviderID: q.ProviderID, ProviderIdentityRef: vpRef(provider.Network, provider.BindingRef, provider.FinalizedCheckpoint), Capability: verifiedproof.Capability{CapabilityID: q.CapabilityID, CapabilityVersion: q.CapabilityVersion, ManifestDigest: q.ManifestCommitment, OwnershipRef: vpRef(own.Network, own.Reference, own.FinalizedCheckpoint)}, Quote: verifiedproof.Quote{QuoteID: q.ID, CommitmentDigest: liveQ.Digest, CommitmentRef: vpRef(liveQ.Network, liveQ.Reference, liveQ.FinalizedCheckpoint), TermsDigest: q.TermsHash, TrustMode: "verified", ProofProfile: "tos_verified_v1", SettlementBackend: "tos", SettlementAsset: "TOS", AssetDecimals: q.AssetDecimals, SubtotalAtomic: subtotal, FeesAtomic: fees, TotalMaxAtomic: total, AcceptanceDeadlineUnixNanos: q.ExpiresAt.UnixNano(), QuoteExpiryUnixNanos: q.ExpiresAt.UnixNano(), ExecutionDeadlineUnixNanos: q.ExecutionDeadline.UnixNano(), UnderlyingServiceQuoteRef: q.UnderlyingServiceQuoteRef, DisputePolicyDigest: q.DisputePolicyHash}, Escrow: verifiedproof.Escrow{EscrowID: esc.ID, JobID: j.ID, ContractRef: vpRef(s.core.Network(), esc.NetworkProofRef, liveE.FinalizedCheckpoint), ContractCodeHash: esc.ContractCodeHash, ReservationDigest: esc.ReservationDigest, ReservationRef: vpRef(s.core.Network(), esc.NetworkProofRef, liveE.FinalizedCheckpoint), ReservedAtomic: reserve, EscrowDeadlineUnixNanos: esc.ExpiresAt.UnixNano(), FundingModel: string(q.Settlement.FundingModel)}, SignerAuthorization: signerProof, Receipt: receiptProof, Outcome: verifiedproof.Outcome{Kind: kind, OutcomeRef: vpRef(s.core.Network(), r.NetworkProofRef, r.FinalizedCheckpoint), ChargedAtomic: charged, RefundedAtomic: refunded, ReleaseDigest: esc.ReleaseDigest, ReasonCode: esc.ReleaseReason}, ProofOfService: posProof}
	check := verifiedproof.Verifier{Observer: capturedProofObserver{auth: auth}, Network: s.core.Network(), GatewayDomain: q.CommitmentDomain}.Verify(ctx, p)
	if !check.Valid {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, fmt.Sprintf("portable proof self-verification failed: %+v", check.Failures), false))
	}
	bytes, e := verifiedproof.Marshal(p)
	if e != nil {
		return fail(e)
	}
	digest, e := verifiedproof.Digest(p)
	if e != nil {
		return fail(e)
	}
	op, e = s.store.UpdateProofPackageOperation(ctx, op.ID, func(x domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
		x.Checkpoint = domain.ProofPackageCanonicalObserved
		x.CanonicalCBOR = bytes
		x.PackageDigest = digest
		x.LastError = ""
		x.UpdatedAt = s.now()
		return x, nil
	})
	if e != nil {
		return PortableProof{}, e
	}
	op, e = s.store.UpdateProofPackageOperation(ctx, op.ID, func(x domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
		x.Checkpoint = domain.ProofPackageProjectionPersisted
		x.UpdatedAt = s.now()
		return x, nil
	})
	if e != nil {
		return PortableProof{}, e
	}
	op, e = s.store.UpdateProofPackageOperation(ctx, op.ID, func(x domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
		x.Checkpoint = domain.ProofPackageCompleted
		now := s.now()
		x.UpdatedAt = now
		x.CompletedAt = &now
		return x, nil
	})
	if e != nil {
		return PortableProof{}, e
	}
	return publicProof(op), nil
}

type capturedProofObserver struct {
	auth toscore.ExecutionSignerAuthorization
}

func (c capturedProofObserver) Observe(_ context.Context, r verifiedproof.EvidenceRequest) (verifiedproof.EvidenceObservation, error) {
	return verifiedproof.EvidenceObservation{Found: true, Network: r.Reference.Network, Kind: r.Kind, ObjectID: r.ObjectID, Digest: r.Digest, Reference: r.Reference.Reference, Finalized: true, FinalizedCheckpoint: r.Reference.FinalizedCheckpoint}, nil
}
func (c capturedProofObserver) ResolveSigner(_ context.Context, p verifiedproof.Package) (verifiedproof.SignerObservation, error) {
	return verifiedproof.SignerObservation{Found: true, Network: p.NetworkID, AuthorizationID: c.auth.AuthorizationID, ProviderID: c.auth.ProviderID, CapabilityID: c.auth.CapabilityID, CapabilityVersion: c.auth.CapabilityVersion, SignerID: c.auth.ExecutionSignerID, Reference: c.auth.AuthorizationRef, SignatureAlgorithm: c.auth.SignatureAlgorithm, PublicKey: c.auth.SignerPublicKey, ValidFromUnixNanos: c.auth.ValidFrom.UnixNano(), ValidUntilUnixNanos: c.auth.ValidUntil.UnixNano(), FinalizedCheckpoint: c.auth.FinalizedCheckpoint}, nil
}
