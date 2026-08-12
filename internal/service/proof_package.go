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
	atostosv1 "github.com/tosnetwork/tos-protocol/gen/atos/tos/v1"
	"github.com/tosnetwork/tos-protocol/pkg/disputecommitment"
	"github.com/tosnetwork/tos-protocol/pkg/verifiedproof"
	"reflect"
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
	// A completed local operation is only a projection. Retrieval must traverse
	// Create's live canonical reconciliation so cached bytes cannot bypass a
	// reorganization, finality regression, revoked signer, or unavailable
	// authority.
	return s.Create(ctx, receiptID, principalID)
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
	quoteWire, e := s.core.PortableQuoteEvidence(ctx, q)
	if e != nil || quoteWire.Digest != liveQ.Digest {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "canonical Quote bytes mismatch", false))
	}
	kind := "provider_settlement"
	if r.Status == domain.ReceiptReleased {
		kind = "requester_release"
	} else if r.Status == domain.ReceiptSettledAfterDispute || r.Status == domain.ReceiptReleasedAfterDispute {
		kind = "dispute_resolution"
	}
	var dispute domain.Dispute
	getEscrow := toscore.GetEscrowRequest{Quote: q, JobID: j.ID, EscrowID: esc.ID, ExpectedReservationDigest: esc.ReservationDigest, ExpectedEscrowRef: esc.NetworkProofRef, ExpectedTerminalRef: r.NetworkProofRef, ExpectedReleaseDigest: esc.ReleaseDigest, ExpectedReleaseReasonCode: esc.ReleaseReason}
	if kind == "dispute_resolution" {
		dispute, e = s.store.DisputeByJob(ctx, j.ID)
		if e != nil || j.ExecutionReceipt == nil || dispute.TrustMode != domain.TrustModeVerified || dispute.EconomicState != domain.DisputeEconomicVerifiedResolved || dispute.DisputeDigest == "" || dispute.DisputeRef == "" || dispute.ResolutionDigest == "" {
			return fail(domain.NewError(domain.ErrProofVerificationFailed, "canonical Verified dispute projection unavailable or inconsistent", true))
		}
		getEscrow.ExpectedDisputeDigest = dispute.DisputeDigest
		getEscrow.ExpectedDisputeRef = dispute.DisputeRef
		getEscrow.ExpectedDisputePayout = r.Charged
		getEscrow.ExpectedResolutionDigest = dispute.ResolutionDigest
		getEscrow.ExpectedResolutionRef = dispute.ResolutionRef
		getEscrow.ExpectedDisputeOutcome = string(dispute.Outcome)
		getEscrow.ExpectedDisputeID = dispute.ID
		getEscrow.ExpectedReviewerID = dispute.ReviewerID
		if dispute.ResolvedAt != nil {
			getEscrow.ExpectedResolvedAt = *dispute.ResolvedAt
		}
		getEscrow.ExpectedReceipt = j.ExecutionReceipt
		getEscrow.ExpectedReceiptRef = j.ExecutionReceipt.NetworkProofRef
		getEscrow.ExpectedSettlementCharge = r.Charged
	} else if kind == "provider_settlement" {
		getEscrow.ExpectedReceipt = j.ExecutionReceipt
		if j.ExecutionReceipt != nil {
			getEscrow.ExpectedReceiptRef = j.ExecutionReceipt.NetworkProofRef
		}
		getEscrow.ExpectedSettlementCharge = r.Charged
	}
	requester, bound, revoked, _, e := s.core.ResolvePrincipalBindingStatus(ctx, q.PrincipalID)
	if e != nil || !bound || revoked || requester.FinalizedCheckpoint == 0 {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "requester identity binding unavailable", true))
	}
	provider, bound, revoked, _, e := s.core.ResolvePrincipalBindingStatus(ctx, q.ProviderID)
	if e != nil || !bound || revoked || provider.FinalizedCheckpoint == 0 {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "provider identity binding unavailable", true))
	}
	identityResolver, ok := s.core.(toscore.AgentIdentityEvidenceResolver)
	if !ok {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "canonical Agent identity resolver unavailable", true))
	}
	requesterIdentity, found, e := identityResolver.ResolveAgentIdentityEvidence(ctx, requester.AgentID)
	if e != nil || !found || requesterIdentity.FinalizedCheckpoint == 0 || len(requesterIdentity.Controllers) != 1 {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "requester Agent identity unavailable", true))
	}
	providerIdentity, found, e := identityResolver.ResolveAgentIdentityEvidence(ctx, provider.AgentID)
	if e != nil || !found || providerIdentity.FinalizedCheckpoint == 0 || len(providerIdentity.Controllers) != 1 {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "provider Agent identity unavailable", true))
	}
	getEscrow.ExpectedCreatorAddress = requesterIdentity.Controllers[0]
	getEscrow.ExpectedAgentAddress = providerIdentity.Controllers[0]
	liveE, found, e := s.core.GetEscrow(ctx, getEscrow)
	if e != nil || !found {
		return fail(domain.NewError(domain.ErrNetworkUnavailable, "canonical TaskEscrow unavailable", true))
	}
	if !liveE.Finalized || liveE.FinalizedCheckpoint == 0 || liveE.ID != esc.ID || liveE.ReservationDigest != esc.ReservationDigest || liveE.Status != esc.Status {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "canonical TaskEscrow mismatch", false))
	}
	if liveE.TerminalProofRef == "" || r.NetworkProofRef != liveE.TerminalProofRef {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "terminal outcome reference mismatch", false))
	}
	escrowWire, e := s.core.PortableEscrowEvidence(ctx, q, j.ID)
	if e != nil || escrowWire.Digest != esc.ReservationDigest {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "canonical reservation bytes mismatch", false))
	}
	own, found, e := s.core.ResolveCapabilityOwnershipEvidence(ctx, q.CapabilityID, q.ProviderID, q.CapabilityVersion, q.ManifestCommitment)
	if e != nil || !found {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "Capability ownership unavailable", true))
	}
	hasExecution := j.ExecutionReceipt != nil
	if !hasExecution && r.Status != domain.ReceiptReleased {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "execution Receipt unavailable", false))
	}
	var er domain.ExecutionReceipt
	var auth toscore.ExecutionSignerAuthorization
	var signerProof *verifiedproof.SignerAuthorization
	var receiptProof *verifiedproof.Receipt
	var posProof *verifiedproof.ProofOfService
	if hasExecution {
		er = *j.ExecutionReceipt
		auth, found, e = s.core.ResolveExecutionSignerAuthorization(ctx, q.ProviderID, q.CapabilityID, q.CapabilityVersion, er.ExecutionSignerID, er.CompletedAt)
		if e != nil || !found || auth.Revoked || len(auth.SignerPublicKey) != 32 || auth.FinalizedCheckpoint == 0 {
			return fail(domain.NewError(domain.ErrProofVerificationFailed, "execution signer authorization unavailable", true))
		}
		receiptWire, wireErr := s.core.PortableReceiptEvidence(ctx, er)
		if wireErr != nil {
			return fail(wireErr)
		}
		receiptLive, receiptFound, receiptErr := s.core.ResolveExecutionReceiptEvidence(ctx, er)
		if receiptErr != nil || !receiptFound || !receiptLive.Finalized || receiptLive.FinalizedCheckpoint == 0 || receiptLive.Digest != receiptWire.Digest {
			return fail(domain.NewError(domain.ErrProofVerificationFailed, "canonical Receipt unavailable", true))
		}
		pos, posFound, posErr := s.core.ReadProofOfServiceEvidence(ctx, er)
		if posErr != nil || !posFound || !pos.Finalized || pos.FinalizedCheckpoint == 0 {
			return fail(domain.NewError(domain.ErrProofVerificationFailed, "Proof-of-Service unavailable", true))
		}
		sig, sigErr := base64.StdEncoding.DecodeString(er.Signature)
		if sigErr != nil {
			return fail(domain.NewError(domain.ErrProofVerificationFailed, "Receipt signature encoding invalid", false))
		}
		signerProof = &verifiedproof.SignerAuthorization{AuthorizationID: auth.AuthorizationID, ExecutionSignerID: auth.ExecutionSignerID, AuthorizationRef: vpRef(s.core.Network(), auth.AuthorizationRef, auth.FinalizedCheckpoint), SignatureAlgorithm: auth.SignatureAlgorithm, SignerPublicKey: auth.SignerPublicKey, ValidFromUnixNanos: auth.ValidFrom.UnixNano(), ValidUntilUnixNanos: auth.ValidUntil.UnixNano()}
		receiptProof = &verifiedproof.Receipt{ReceiptID: er.ID, ReceiptDigest: receiptWire.Digest, ReceiptRef: vpRef(receiptLive.Network, receiptLive.Reference, receiptLive.FinalizedCheckpoint), Result: string(er.Result), InputCommitment: er.InputHash, OutputCommitment: er.OutputHash, UsageCommitment: er.UsageCommitment, StartedUnixNanos: er.StartedAt.UnixNano(), CompletedUnixNanos: er.CompletedAt.UnixNano(), ChargedAtomic: "", SignatureAlgorithm: er.SignatureAlgorithm, Signature: sig, CanonicalCBOR: receiptWire.CanonicalCBOR}
		posProof = &verifiedproof.ProofOfService{EvidenceID: pos.EvidenceID, EvidenceDigest: pos.Digest, EvidenceRef: vpRef(pos.Network, pos.Reference, pos.FinalizedCheckpoint), ContentDigest: pos.ContentDigest, CanonicalCBOR: pos.CanonicalCBOR}
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
	var disputeDigest, resolutionDigest, disputeOutcome string
	var resolutionCBOR []byte
	var disputeRef, resolutionRef verifiedproof.Reference
	if kind == "dispute_resolution" {
		if dispute.ResolutionRef != r.NetworkProofRef || dispute.ResolutionCheckpoint == 0 || r.FinalizedCheckpoint < dispute.ResolutionCheckpoint {
			return fail(domain.NewError(domain.ErrProofVerificationFailed, "canonical Verified dispute projection unavailable or inconsistent", true))
		}
		disputeDigest, resolutionDigest, disputeOutcome = dispute.DisputeDigest, dispute.ResolutionDigest, string(dispute.Outcome)
		disputeRef = vpRef(s.core.Network(), dispute.DisputeRef, dispute.DisputeCheckpoint)
		resolutionRef = vpRef(s.core.Network(), dispute.ResolutionRef, dispute.ResolutionCheckpoint)
		if dispute.ResolvedAt == nil || dispute.ReviewerID == "" {
			return fail(domain.NewError(domain.ErrProofVerificationFailed, "complete Verified dispute resolution tuple unavailable", true))
		}
		resolution := &atostosv1.VerifiedDisputeResolution{Version: "atos_verified_dispute_resolution_v1", NetworkId: s.core.Network(), GatewayDomain: q.CommitmentDomain, DisputeId: dispute.ID, EscrowId: esc.ID, JobId: j.ID, QuoteId: q.ID, ReceiptId: r.ID, DisputeDigest: dispute.DisputeDigest, Outcome: string(dispute.Outcome), ReviewerPrincipalId: dispute.ReviewerID, Reserved: &atostosv1.NetworkAmount{Asset: "TOS", AtomicAmount: reserve}, ProviderPayout: &atostosv1.NetworkAmount{Asset: "TOS", AtomicAmount: charged}, RequesterRefund: &atostosv1.NetworkAmount{Asset: "TOS", AtomicAmount: refunded}, ResolvedUnixMillis: dispute.ResolvedAt.UnixMilli()}
		var resolutionErr error
		resolutionCBOR, resolutionErr = disputecommitment.ResolutionBytes(resolution)
		if resolutionErr != nil {
			return fail(domain.NewError(domain.ErrProofVerificationFailed, "canonical Verified dispute resolution encoding failed", false))
		}
		if digest, digestErr := disputecommitment.ResolutionDigest(resolution); digestErr != nil || digest != resolutionDigest {
			return fail(domain.NewError(domain.ErrProofVerificationFailed, "canonical Verified dispute resolution digest mismatch", false))
		}
	}
	if r.NetworkProofRef == "" || r.FinalizedCheckpoint == 0 {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, "terminal outcome finality unavailable", true))
	}
	if receiptProof != nil {
		receiptCharge, receiptChargeErr := atomicTOS(er.Cost)
		if receiptChargeErr != nil {
			return fail(receiptChargeErr)
		}
		receiptProof.ChargedAtomic = receiptCharge
	}
	p := verifiedproof.Package{Version: verifiedproof.Version, Canonicalization: verifiedproof.Canonicalization, NetworkID: s.core.Network(), GatewayDomain: q.CommitmentDomain, PrincipalID: q.PrincipalID, RequesterAgentID: requester.AgentID, RequesterIdentityRef: vpRef(requester.Network, requester.BindingRef, requester.FinalizedCheckpoint), ProviderID: q.ProviderID, ProviderIdentityRef: vpRef(provider.Network, provider.BindingRef, provider.FinalizedCheckpoint), Capability: verifiedproof.Capability{CapabilityID: q.CapabilityID, CapabilityVersion: q.CapabilityVersion, ManifestDigest: q.ManifestCommitment, OwnershipDigest: own.Digest, OwnershipRef: vpRef(own.Network, own.Reference, own.FinalizedCheckpoint)}, Quote: verifiedproof.Quote{QuoteID: q.ID, CommitmentDigest: liveQ.Digest, CommitmentRef: vpRef(liveQ.Network, liveQ.Reference, liveQ.FinalizedCheckpoint), TermsDigest: q.TermsHash, TrustMode: "verified", ProofProfile: "tos_verified_v1", SettlementBackend: "tos", SettlementAsset: "TOS", AssetDecimals: q.AssetDecimals, SubtotalAtomic: subtotal, FeesAtomic: fees, TotalMaxAtomic: total, AcceptanceDeadlineUnixNanos: q.ExpiresAt.UnixNano(), QuoteExpiryUnixNanos: q.ExpiresAt.UnixNano(), ExecutionDeadlineUnixNanos: q.ExecutionDeadline.UnixNano(), UnderlyingServiceQuoteRef: q.UnderlyingServiceQuoteRef, DisputePolicyDigest: q.DisputePolicyHash, CanonicalCBOR: quoteWire.CanonicalCBOR}, Escrow: verifiedproof.Escrow{EscrowID: esc.ID, JobID: j.ID, ContractRef: vpRef(s.core.Network(), esc.NetworkProofRef, liveE.FinalizedCheckpoint), ContractCodeHash: esc.ContractCodeHash, ReservationDigest: esc.ReservationDigest, ReservationRef: vpRef(s.core.Network(), esc.NetworkProofRef, liveE.FinalizedCheckpoint), ReservedAtomic: reserve, EscrowDeadlineUnixNanos: esc.ExpiresAt.UnixNano(), FundingModel: string(q.Settlement.FundingModel), CanonicalCBOR: escrowWire.CanonicalCBOR}, SignerAuthorization: signerProof, Receipt: receiptProof, Outcome: verifiedproof.Outcome{Kind: kind, OutcomeRef: vpRef(s.core.Network(), r.NetworkProofRef, r.FinalizedCheckpoint), ChargedAtomic: charged, RefundedAtomic: refunded, ReleaseDigest: esc.ReleaseDigest, ReasonCode: esc.ReleaseReason, DisputeDigest: disputeDigest, DisputeRef: disputeRef, ResolutionDigest: resolutionDigest, ResolutionRef: resolutionRef, DisputeOutcome: disputeOutcome, ResolutionCBOR: resolutionCBOR}, ProofOfService: posProof}
	p.RequesterIdentity = verifiedproof.Identity{AgentID: requesterIdentity.AgentID, CanonicalURI: requesterIdentity.CanonicalURI, Controllers: requesterIdentity.Controllers, Assurance: requesterIdentity.Assurance, IdentityRef: vpRef(requesterIdentity.Network, requesterIdentity.Reference, requesterIdentity.FinalizedCheckpoint)}
	p.ProviderIdentity = verifiedproof.Identity{AgentID: providerIdentity.AgentID, CanonicalURI: providerIdentity.CanonicalURI, Controllers: providerIdentity.Controllers, Assurance: providerIdentity.Assurance, IdentityRef: vpRef(providerIdentity.Network, providerIdentity.Reference, providerIdentity.FinalizedCheckpoint)}
	p.ProviderAgentID = provider.AgentID
	check := verifiedproof.Verifier{Observer: capturedProofObserver{auth: auth}, Network: s.core.Network(), GatewayDomain: q.CommitmentDomain}.Verify(ctx, p)
	if !check.Valid {
		return fail(domain.NewError(domain.ErrProofVerificationFailed, fmt.Sprintf("portable proof self-verification failed: %+v", check.Failures), false))
	}
	// The first canonical package is immutable. Live finality checkpoints can
	// advance forever, so replay must validate that the frozen references are
	// still canonical at an equal-or-higher live checkpoint without re-encoding
	// those newer observations into a second semantic package.
	if op.PackageDigest != "" || len(op.CanonicalCBOR) != 0 {
		stored, parseErr := verifiedproof.Parse(op.CanonicalCBOR)
		storedDigest, digestErr := verifiedproof.Digest(stored)
		if parseErr != nil || digestErr != nil || storedDigest != op.PackageDigest || !sameProofSemantics(stored, p) {
			return fail(domain.NewError(domain.ErrProofVerificationFailed, "durable portable proof differs from live canonical tuple", false))
		}
		storedCheck := verifiedproof.Verifier{Observer: capturedProofObserver{auth: auth}, Network: s.core.Network(), GatewayDomain: q.CommitmentDomain}.Verify(ctx, stored)
		if !storedCheck.Valid {
			return fail(domain.NewError(domain.ErrProofVerificationFailed, fmt.Sprintf("durable portable proof revalidation failed: %+v", storedCheck.Failures), false))
		}
		if op.Checkpoint == domain.ProofPackageCanonicalObserved {
			op, e = s.advanceProofCheckpoint(ctx, op, domain.ProofPackageProjectionPersisted)
			if e != nil {
				return PortableProof{}, e
			}
		}
		if op.Checkpoint == domain.ProofPackageProjectionPersisted {
			op, e = s.completeProofOperation(ctx, op)
			if e != nil {
				return PortableProof{}, e
			}
		}
		return publicProof(op), nil
	}
	bytes, e := verifiedproof.Marshal(p)
	if e != nil {
		return fail(e)
	}
	digest, e := verifiedproof.Digest(p)
	if e != nil {
		return fail(e)
	}
	op, e = s.persistCanonicalProof(ctx, op, p, bytes, digest)
	if e != nil {
		return PortableProof{}, e
	}
	op, e = s.advanceProofCheckpoint(ctx, op, domain.ProofPackageProjectionPersisted)
	if e != nil {
		return PortableProof{}, e
	}
	op, e = s.completeProofOperation(ctx, op)
	if e != nil {
		return PortableProof{}, e
	}
	return publicProof(op), nil
}

func sameProofSemantics(stored, live verifiedproof.Package) bool {
	refsStored := proofReferences(&stored)
	refsLive := proofReferences(&live)
	if len(refsStored) != len(refsLive) {
		return false
	}
	storedCheckpoints := make([]uint64, len(refsStored))
	liveCheckpoints := make([]uint64, len(refsLive))
	for i := range refsStored {
		if refsStored[i].Network != refsLive[i].Network || refsStored[i].Reference != refsLive[i].Reference || refsStored[i].FinalizedCheckpoint == 0 || refsStored[i].FinalizedCheckpoint > refsLive[i].FinalizedCheckpoint {
			return false
		}
		storedCheckpoints[i], liveCheckpoints[i] = refsStored[i].FinalizedCheckpoint, refsLive[i].FinalizedCheckpoint
		refsStored[i].FinalizedCheckpoint = 0
		refsLive[i].FinalizedCheckpoint = 0
	}
	equal := reflect.DeepEqual(stored, live)
	for i := range refsStored {
		refsStored[i].FinalizedCheckpoint, refsLive[i].FinalizedCheckpoint = storedCheckpoints[i], liveCheckpoints[i]
	}
	return equal
}

func proofReferences(p *verifiedproof.Package) []*verifiedproof.Reference {
	candidates := []*verifiedproof.Reference{&p.RequesterIdentityRef, &p.RequesterIdentity.IdentityRef, &p.ProviderIdentityRef, &p.ProviderIdentity.IdentityRef, &p.Capability.OwnershipRef, &p.Quote.CommitmentRef, &p.Escrow.ContractRef, &p.Escrow.ReservationRef, &p.Outcome.OutcomeRef, &p.Outcome.DisputeRef, &p.Outcome.ResolutionRef}
	refs := make([]*verifiedproof.Reference, 0, len(candidates)+3)
	for _, ref := range candidates {
		if ref.Reference != "" {
			refs = append(refs, ref)
		}
	}
	if p.SignerAuthorization != nil {
		refs = append(refs, &p.SignerAuthorization.AuthorizationRef)
	}
	if p.Receipt != nil {
		refs = append(refs, &p.Receipt.ReceiptRef)
	}
	if p.ProofOfService != nil {
		refs = append(refs, &p.ProofOfService.EvidenceRef)
	}
	return refs
}

func (s *PortableProofService) persistCanonicalProof(ctx context.Context, op domain.ProofPackageOperation, p verifiedproof.Package, canonical []byte, digest string) (domain.ProofPackageOperation, error) {
	return s.store.UpdateProofPackageOperation(ctx, op.ID, func(x domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
		if x.PackageDigest != "" || len(x.CanonicalCBOR) != 0 {
			stored, parseErr := verifiedproof.Parse(x.CanonicalCBOR)
			storedDigest, digestErr := verifiedproof.Digest(stored)
			if parseErr != nil || digestErr != nil || storedDigest != x.PackageDigest || !sameProofSemantics(stored, p) {
				return x, domain.NewError(domain.ErrProofVerificationFailed, "concurrent portable proof differs from canonical tuple", false)
			}
			return x, nil
		}
		x.Checkpoint = domain.ProofPackageCanonicalObserved
		x.CanonicalCBOR = canonical
		x.PackageDigest = digest
		x.LastError = ""
		x.UpdatedAt = s.now()
		return x, nil
	})
}

func proofCheckpointReached(current, target domain.ProofPackageCheckpoint) bool {
	order := map[domain.ProofPackageCheckpoint]int{
		domain.ProofPackageIntentPersisted:     1,
		domain.ProofPackageReconciling:         2,
		domain.ProofPackageCanonicalObserved:   3,
		domain.ProofPackageProjectionPersisted: 4,
		domain.ProofPackageCompleted:           5,
	}
	return order[current] != 0 && order[current] >= order[target]
}

func (s *PortableProofService) advanceProofCheckpoint(ctx context.Context, op domain.ProofPackageOperation, checkpoint domain.ProofPackageCheckpoint) (domain.ProofPackageOperation, error) {
	return s.store.UpdateProofPackageOperation(ctx, op.ID, func(x domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
		if proofCheckpointReached(x.Checkpoint, checkpoint) {
			return x, nil
		}
		x.Checkpoint = checkpoint
		x.UpdatedAt = s.now()
		return x, nil
	})
}

func (s *PortableProofService) completeProofOperation(ctx context.Context, op domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
	return s.store.UpdateProofPackageOperation(ctx, op.ID, func(x domain.ProofPackageOperation) (domain.ProofPackageOperation, error) {
		if x.Checkpoint == domain.ProofPackageCompleted {
			return x, nil
		}
		x.Checkpoint = domain.ProofPackageCompleted
		now := s.now()
		x.UpdatedAt = now
		x.CompletedAt = &now
		return x, nil
	})
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
