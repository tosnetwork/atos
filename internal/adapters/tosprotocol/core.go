package toprotocol

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/big"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
	atostosv1 "github.com/tosnetwork/tos-protocol/gen/atos/tos/v1"
	"github.com/tosnetwork/tos-protocol/pkg/disputecommitment"
	"github.com/tosnetwork/tos-protocol/pkg/escrowcommitment"
	"github.com/tosnetwork/tos-protocol/pkg/poscommitment"
	"github.com/tosnetwork/tos-protocol/pkg/quotecommitment"
	"github.com/tosnetwork/tos-protocol/pkg/receiptcommitment"
)

// Network returns this client's configured TOS network identity ("" if
// unconfigured -- see Config.Network's doc comment).
func (c *Client) Network() string {
	return c.network
}

// CommitCapabilityManifest is the public toscore.Core entry point for the
// existing commitCapability RPC call (used internally by RegisterProvider/
// QuoteExecution) -- exposed directly so CapabilityService can anchor a
// capability's manifest/ownership commitment as part of ordinary
// registration, not only as a side effect of third-party execution
// dispatch. Idempotent: see commitCapability's own CommitCapabilityManifest
// RPC semantics (identical provider_id+manifest digest replays the same
// commitment; a conflicting replay under the same capability_id+version
// errors).
func (c *Client) CommitCapabilityManifest(ctx context.Context, capability domain.Capability) (string, error) {
	// capability identity is validated inside commitCapability below.
	identity, err := c.commitCapability(ctx, capability)
	if err != nil {
		return "", err
	}
	return reference(identity.GetOwnershipRef()), nil
}

func (c *Client) ResolveAgent(ctx context.Context, principalID string) (string, error) {
	if strings.TrimSpace(principalID) == "" {
		return "", domain.NewError(domain.ErrValidationFailed, "principal_id is required", false)
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ResolvePrincipalBindingRequest{
		Context:     c.requestContext(ctx, principalID, "", time.Time{}),
		PrincipalId: principalID,
	})
	decorateRequest(c, ctx, request)
	response, err := c.identity.ResolvePrincipalBinding(callCtx, request)
	if err != nil {
		return "", rpcError(err)
	}
	if response.Msg != nil && response.Msg.Bound && response.Msg.Identity != nil {
		return response.Msg.Identity.AgentId, nil
	}
	// Managed callers do not need a wallet or pre-existing TOS identity. Keep
	// the gateway principal stable rather than inventing an attested identity.
	return principalID, nil
}

// ResolvePrincipalBindingStatus is tos-protocol's full
// ResolvePrincipalBinding answer -- unlike ResolveAgent (which silently
// falls back to treating principalID itself as the agent identity for
// Managed-compatible callers), this exposes the real bound/revoked facts a
// Phase 4A authority decision needs and must never paper over. revoked is
// true only when a binding EXISTED and was explicitly revoked (distinct
// from "never bound" -- see atos-spec docs/TOS_RPC.md §10).
func (c *Client) ResolvePrincipalBindingStatus(ctx context.Context, principalID string) (binding domain.PrincipalIdentityBinding, bound, revoked bool, revocationReasonCode string, err error) {
	if strings.TrimSpace(principalID) == "" {
		return domain.PrincipalIdentityBinding{}, false, false, "", domain.NewError(domain.ErrValidationFailed, "principal_id is required", false)
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ResolvePrincipalBindingRequest{
		Context: c.requestContext(ctx, "atos-gateway", "", time.Time{}), PrincipalId: principalID,
	})
	decorateRequest(c, ctx, request)
	response, err := c.identity.ResolvePrincipalBinding(callCtx, request)
	if err != nil {
		return domain.PrincipalIdentityBinding{}, false, false, "", rpcError(err)
	}
	if response.Msg == nil {
		return domain.PrincipalIdentityBinding{}, false, false, "", nil
	}
	revoked = response.Msg.Status == atostosv1.PrincipalBindingStatus_PRINCIPAL_BINDING_STATUS_REVOKED
	if !response.Msg.Bound || response.Msg.Identity == nil || response.Msg.BindingRef == nil {
		return domain.PrincipalIdentityBinding{}, false, revoked, response.Msg.RevocationReasonCode, nil
	}
	binding = domain.PrincipalIdentityBinding{
		PrincipalID: principalID, AgentID: response.Msg.Identity.AgentId,
		Network: response.Msg.BindingRef.Network, BindingRef: response.Msg.BindingRef.Reference,
		FinalizedCheckpoint: response.Msg.BindingRef.FinalizedCheckpoint,
	}
	// revoked (computed above from Status) is threaded through here too,
	// not hardcoded false: Bound and Status are independent response
	// fields with no wire-level guarantee a conforming server can never
	// return Bound=true with Status=REVOKED -- hardcoding false here would
	// let that combination silently discard the revocation fact at the
	// source, the same discard-instead-of-thread bug class already fixed
	// once for RevokePrincipalBinding's revoked bool.
	return binding, true, revoked, response.Msg.RevocationReasonCode, nil
}

func (c *Client) ResolveAgentIdentityEvidence(ctx context.Context, agentID string) (toscore.AgentIdentityEvidence, bool, error) {
	if strings.TrimSpace(agentID) == "" {
		return toscore.AgentIdentityEvidence{}, false, domain.NewError(domain.ErrValidationFailed, "agent_id is required", false)
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ResolveAgentIdentityRequest{Context: c.requestContext(ctx, "atos-gateway", "", time.Time{}), AgentId: agentID})
	decorateRequest(c, ctx, request)
	response, err := c.identity.ResolveAgentIdentity(callCtx, request)
	if err != nil {
		return toscore.AgentIdentityEvidence{}, false, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Found || response.Msg.Identity == nil || response.Msg.Identity.IdentityRef == nil {
		return toscore.AgentIdentityEvidence{}, false, nil
	}
	i := response.Msg.Identity
	liveRequest := connect.NewRequest(&atostosv1.ResolveAgentIdentityRequest{Context: c.requestContext(ctx, "atos-gateway", "", time.Time{}), AgentId: i.AgentId, CanonicalUri: i.CanonicalUri, ExpectedIdentity: &atostosv1.AgentIdentity{AgentId: i.AgentId, CanonicalUri: i.CanonicalUri, Controllers: append([]string(nil), i.Controllers...), Assurance: i.Assurance}, ExpectedIdentityRef: i.IdentityRef})
	decorateRequest(c, ctx, liveRequest)
	liveResponse, liveErr := c.identity.ResolveAgentIdentity(callCtx, liveRequest)
	if liveErr != nil || liveResponse.Msg == nil || !liveResponse.Msg.Found || liveResponse.Msg.Identity == nil {
		if liveErr != nil {
			return toscore.AgentIdentityEvidence{}, false, rpcError(liveErr)
		}
		return toscore.AgentIdentityEvidence{}, false, domain.NewError(domain.ErrNetworkUnavailable, "canonical identity unavailable", true)
	}
	i = liveResponse.Msg.Identity
	if !i.IdentityRef.Finalized || i.IdentityRef.FinalizedCheckpoint == 0 || i.IdentityRef.Network != c.network {
		return toscore.AgentIdentityEvidence{}, false, domain.NewError(domain.ErrProofVerificationFailed, "identity finality mismatch", false)
	}
	return toscore.AgentIdentityEvidence{AgentID: i.AgentId, CanonicalURI: i.CanonicalUri, Controllers: append([]string(nil), i.Controllers...), Assurance: i.Assurance, Network: i.IdentityRef.Network, Reference: i.IdentityRef.Reference, FinalizedCheckpoint: i.IdentityRef.FinalizedCheckpoint}, true, nil
}

// CreatePrincipalBinding anchors a durable binding from principalID to
// agentID through tos-protocol's IdentityService.CreatePrincipalBinding
// (Phase 4A). callerID scopes the underlying RPC's idempotency namespace --
// this gateway's own trusted identity (see atos-spec docs/TOS_RPC.md §10:
// context.caller_id identifies the operating ATOS backend, never the
// principal the binding is FOR), never principalID itself.
func (c *Client) CreatePrincipalBinding(ctx context.Context, callerID, idempotencyKey, principalID, agentID string) (domain.PrincipalIdentityBinding, bool, error) {
	if strings.TrimSpace(principalID) == "" || strings.TrimSpace(agentID) == "" {
		return domain.PrincipalIdentityBinding{}, false, domain.NewError(domain.ErrValidationFailed, "principal_id and agent_id are required", false)
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.CreatePrincipalBindingRequest{
		Context:     c.requestContext(ctx, callerID, idempotencyKey, time.Time{}),
		PrincipalId: principalID, AgentId: agentID,
	})
	decorateRequest(c, ctx, request)
	response, err := c.identity.CreatePrincipalBinding(callCtx, request)
	if err != nil {
		return domain.PrincipalIdentityBinding{}, false, rpcError(err)
	}
	if response.Msg == nil || response.Msg.BindingRef == nil {
		return domain.PrincipalIdentityBinding{}, false, domain.NewError(domain.ErrProviderFailed, "tos-protocol returned an incomplete principal binding", true)
	}
	return domain.PrincipalIdentityBinding{
		PrincipalID: principalID, AgentID: agentID,
		Network: response.Msg.BindingRef.Network, BindingRef: response.Msg.BindingRef.Reference,
	}, response.Msg.Created, nil
}

// RevokePrincipalBinding revokes principalID's current binding through
// tos-protocol's IdentityService.RevokePrincipalBinding. revoked=false with
// a nil error means there was nothing to revoke -- not an error, mirroring
// tos-protocol's own convention.
func (c *Client) RevokePrincipalBinding(ctx context.Context, callerID, idempotencyKey, principalID, reasonCode string) (revoked bool, revocationNetwork, revocationRef string, err error) {
	if strings.TrimSpace(principalID) == "" {
		return false, "", "", domain.NewError(domain.ErrValidationFailed, "principal_id is required", false)
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.RevokePrincipalBindingRequest{
		Context:     c.requestContext(ctx, callerID, idempotencyKey, time.Time{}),
		PrincipalId: principalID, ReasonCode: reasonCode,
	})
	decorateRequest(c, ctx, request)
	response, err := c.identity.RevokePrincipalBinding(callCtx, request)
	if err != nil {
		return false, "", "", rpcError(err)
	}
	if response.Msg == nil {
		return false, "", "", nil
	}
	if response.Msg.RevocationRef != nil {
		revocationNetwork, revocationRef = response.Msg.RevocationRef.Network, response.Msg.RevocationRef.Reference
	}
	return response.Msg.Revoked, revocationNetwork, revocationRef, nil
}

func (c *Client) ResolveCapability(ctx context.Context, capabilityID string) (domain.Trust, error) {
	if strings.TrimSpace(capabilityID) == "" {
		return domain.Trust{}, domain.NewError(domain.ErrValidationFailed, "capability_id is required", false)
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ResolveCapabilityRequest{
		Context:      c.requestContext(ctx, "atos-gateway", "", time.Time{}),
		CapabilityId: capabilityID,
	})
	decorateRequest(c, ctx, request)
	response, err := c.capability.ResolveCapability(callCtx, request)
	if err != nil {
		return domain.Trust{}, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Found || response.Msg.Capability == nil {
		return domain.Trust{}, domain.NewError(domain.ErrNotFound, "tos-protocol capability not found", false)
	}
	trust := domain.Trust{Level: domain.TrustSelfAsserted}
	if reputation, err := c.readReputation(ctx, response.Msg.Capability.ProviderId, capabilityID); err == nil {
		trust = reputation
	}
	return trust, nil
}

func (c *Client) ReadReputation(ctx context.Context, providerID string) (domain.Trust, error) {
	return c.readReputation(ctx, providerID, "")
}

func (c *Client) readReputation(ctx context.Context, providerID, capabilityID string) (domain.Trust, error) {
	if strings.TrimSpace(providerID) == "" {
		return domain.Trust{}, domain.NewError(domain.ErrValidationFailed, "provider_id is required", false)
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ReadReputationRequest{
		Context:    c.requestContext(ctx, "atos-gateway", "", time.Time{}),
		ProviderId: providerID, CapabilityId: capabilityID,
	})
	decorateRequest(c, ctx, request)
	response, err := c.proof.ReadReputation(callCtx, request)
	if err != nil {
		return domain.Trust{}, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Found || response.Msg.Reputation == nil {
		return domain.Trust{Level: domain.TrustSelfAsserted}, nil
	}
	updated := time.UnixMilli(response.Msg.Reputation.UpdatedUnixMillis).UTC()
	return domain.Trust{
		Score: response.Msg.Reputation.Score, Level: domain.TrustSelfAsserted,
		ProofOfServiceCount: response.Msg.Reputation.VerifiedExecutions,
		LastUpdatedAt:       &updated,
	}, nil
}

func (c *Client) VerifyCapabilityOwnership(ctx context.Context, capabilityID, providerID, version, expectedManifestDigest string) (bool, string, error) {
	if capabilityID == "" || providerID == "" {
		return false, "", domain.NewError(domain.ErrValidationFailed, "capability_id and provider_id are required", false)
	}
	req := &atostosv1.VerifyCapabilityOwnershipRequest{
		Context:      c.requestContext(ctx, providerID, "", time.Time{}),
		CapabilityId: capabilityID, ProviderId: providerID, Version: version,
	}
	if expectedManifestDigest != "" {
		d, err := digest(expectedManifestDigest)
		if err != nil {
			return false, "", err
		}
		req.ExpectedManifestDigest = d
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(req)
	decorateRequest(c, ctx, request)
	response, err := c.capability.VerifyCapabilityOwnership(callCtx, request)
	if err != nil {
		return false, "", rpcError(err)
	}
	if response.Msg == nil {
		return false, "NOT_FOUND", nil
	}
	return response.Msg.Verified, response.Msg.ReasonCode, nil
}

func (c *Client) ResolveCapabilityOwnershipEvidence(ctx context.Context, capabilityID, providerID, version, expectedManifestDigest string) (toscore.CanonicalEvidence, bool, error) {
	if capabilityID == "" || providerID == "" || version == "" || expectedManifestDigest == "" {
		return toscore.CanonicalEvidence{}, false, domain.NewError(domain.ErrValidationFailed, "complete capability ownership tuple is required", false)
	}
	d, err := digest(expectedManifestDigest)
	if err != nil {
		return toscore.CanonicalEvidence{}, false, err
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.VerifyCapabilityOwnershipRequest{Context: c.requestContext(ctx, providerID, "", time.Time{}), CapabilityId: capabilityID, ProviderId: providerID, Version: version, ExpectedManifestDigest: d})
	decorateRequest(c, ctx, request)
	response, err := c.capability.VerifyCapabilityOwnership(callCtx, request)
	if err != nil {
		return toscore.CanonicalEvidence{}, false, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Verified || response.Msg.OwnershipRef == nil {
		return toscore.CanonicalEvidence{}, false, nil
	}
	r := response.Msg.OwnershipRef
	if r.Network != c.network || !r.Finalized || r.FinalizedCheckpoint == 0 {
		return toscore.CanonicalEvidence{}, false, domain.NewError(domain.ErrNetworkUnavailable, "capability ownership is not finalized on configured network", true)
	}
	if response.Msg.OwnershipDigest == "" {
		return toscore.CanonicalEvidence{}, false, domain.NewError(domain.ErrProofVerificationFailed, "capability ownership authority digest is missing", false)
	}
	return toscore.CanonicalEvidence{Network: r.Network, Reference: r.Reference, Digest: response.Msg.OwnershipDigest, Finalized: r.Finalized, FinalizedCheckpoint: r.FinalizedCheckpoint}, true, nil
}

func (c *Client) UpdateReputationEvidence(context.Context, string, string) error {
	return domain.NewError(domain.ErrValidationFailed, "direct reputation evidence is unsupported; commit verified Proof-of-Service evidence instead", false)
}

func (c *Client) CommitQuote(ctx context.Context, quote domain.Quote) (toscore.QuoteCommitment, error) {
	if quote.PrincipalID == "" {
		return toscore.QuoteCommitment{}, domain.NewError(domain.ErrQuoteMismatch, "quote principal_id is required by the tos-protocol backend", false)
	}
	// tos-protocol's SubmitJob binds a Job to its Quote by requiring
	// underlying_service_quote_ref to equal the service_quote_id presented
	// later, so this must carry the raw service quote ID rather than
	// quote.UnderlyingServiceQuoteRef (the distinct commitment reference
	// tos-protocol returns from QuoteExecution).
	callCtx, cancel := c.callContext(ctx, quote.ExpiresAt)
	defer cancel()
	request := connect.NewRequest(&atostosv1.CommitQuoteRequest{
		Context: c.requestContext(ctx, quote.PrincipalID, quote.ID, quote.ExpiresAt),
		Quote:   quoteCommitmentInput(quote),
	})
	decorateRequest(c, ctx, request)
	response, err := c.trust.CommitQuote(callCtx, request)
	if err != nil {
		return toscore.QuoteCommitment{}, rpcError(err)
	}
	if response.Msg == nil || response.Msg.Quote == nil {
		return toscore.QuoteCommitment{}, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned an empty quote commitment", true)
	}
	expectedDigest := digestString(response.Msg.Quote.CommitmentDigest)
	if quote.TrustMode == domain.TrustModeVerified {
		var digestErr error
		expectedDigest, digestErr = quotecommitment.Digest(request.Msg.Quote)
		if digestErr != nil {
			return toscore.QuoteCommitment{}, domain.NewError(domain.ErrValidationFailed, "verified Quote cannot be canonicalized", false)
		}
		if digestString(response.Msg.Quote.CommitmentDigest) != expectedDigest {
			return toscore.QuoteCommitment{}, domain.NewError(domain.ErrQuoteMismatch, "tos-protocol returned a mismatched commitment digest", false)
		}
	}
	ref := reference(response.Msg.Quote.CommitmentRef)
	if ref != "" {
		c.proofRefs.Store(quote.ID, ref)
	}
	return mapQuoteCommitment(response.Msg.Quote, quote, expectedDigest), nil
}

func (c *Client) GetQuoteCommitment(ctx context.Context, quote domain.Quote) (toscore.QuoteCommitment, bool, error) {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	var expectedRef *atostosv1.NetworkReference
	if quote.Commitment != nil {
		expectedRef = &atostosv1.NetworkReference{Network: quote.Commitment.Network, Reference: quote.Commitment.Reference, Finalized: quote.Commitment.Finalized, FinalizedCheckpoint: quote.Commitment.FinalizedCheckpoint}
	}
	request := connect.NewRequest(&atostosv1.GetQuoteCommitmentRequest{Context: c.requestContext(ctx, "atos-gateway", "", time.Time{}), QuoteId: quote.ID, ExpectedQuote: quoteCommitmentInput(quote), ExpectedCommitmentRef: expectedRef})
	decorateRequest(c, ctx, request)
	response, err := c.trust.GetQuoteCommitment(callCtx, request)
	if err != nil {
		return toscore.QuoteCommitment{}, false, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Found || response.Msg.Quote == nil {
		return toscore.QuoteCommitment{}, false, nil
	}
	expectedDigest := digestString(response.Msg.Quote.CommitmentDigest)
	if quote.TrustMode == domain.TrustModeVerified {
		var digestErr error
		expectedDigest, digestErr = quotecommitment.Digest(request.Msg.ExpectedQuote)
		if digestErr != nil {
			return toscore.QuoteCommitment{}, false, domain.NewError(domain.ErrValidationFailed, "verified Quote cannot be canonicalized", false)
		}
		if digestString(response.Msg.Quote.CommitmentDigest) != expectedDigest {
			return toscore.QuoteCommitment{}, false, domain.NewError(domain.ErrQuoteMismatch, "tos-protocol returned a mismatched commitment digest", false)
		}
	}
	return mapQuoteCommitment(response.Msg.Quote, domain.Quote{}, expectedDigest), true, nil
}

func mapQuoteCommitment(value *atostosv1.QuoteCommitment, fallback domain.Quote, expectedDigest string) toscore.QuoteCommitment {
	ref := value.GetCommitmentRef()
	digestValue := value.GetCommitmentDigest()
	digestText := digestString(digestValue)
	if q := value.GetValue(); q != nil {
		fallback.ID = q.QuoteId
		fallback.PrincipalID = q.PrincipalId
		fallback.ProviderID = q.ProviderId
		fallback.CapabilityID = q.CapabilityId
		fallback.CapabilityVersion = q.CapabilityVersion
		fallback.NetworkID = q.NetworkId
		fallback.CommitmentDomain = q.Domain
		fallback.CommitmentVersion = q.Version
		fallback.CommitmentCanonicalization = q.Canonicalization
		fallback.RequesterAgentID = q.RequesterAgentId
		fallback.ManifestCommitment = digestString(q.ManifestDigest)
		fallback.OwnershipRef = reference(q.OwnershipRef)
		fallback.SignerAuthorizationID = q.SignerAuthorizationId
		fallback.SignerAuthorizationRef = reference(q.SignerAuthorizationRef)
		fallback.TermsHash = digestString(q.TermsDigest)
		fallback.DisputePolicyHash = digestString(q.DisputePolicyDigest)
		fallback.ExpiresAt = time.UnixMilli(q.ExpiresUnixMillis)
		fallback.ExecutionDeadline = time.UnixMilli(q.ExecutionDeadlineUnixMillis)
		fallback.TrustMode = domainTrustMode(q.TrustMode)
		fallback.ProofProfile = domainProofProfile(q.ProofProfile)
		if q.TotalMax != nil {
			fallback.Price.TotalMax = q.TotalMax.Amount
			fallback.Price.Currency = q.TotalMax.Currency
		}
		if q.Subtotal != nil {
			fallback.Price.Subtotal = q.Subtotal.Amount
		}
		if q.Fees != nil {
			fallback.Price.Fees = q.Fees.Amount
		}
		fallback.AssetDecimals = q.AssetDecimals
		fallback.Settlement.Backend = domain.SettlementBackend(q.SettlementBackend)
		fallback.Settlement.ProviderAsset = q.SettlementAsset
		fallback.ServiceQuoteID = q.UnderlyingServiceQuoteRef
	}
	return toscore.QuoteCommitment{Quote: fallback, Network: ref.GetNetwork(), Reference: ref.GetReference(), Digest: digestText, ExpectedDigest: expectedDigest, Finalized: ref.GetFinalized(), FinalizedCheckpoint: ref.GetFinalizedCheckpoint()}
}
func mustDigest(value string) *atostosv1.Digest { d, _ := digest(value); return d }
func quoteCommitmentInput(q domain.Quote) *atostosv1.QuoteCommitmentInput {
	asset := q.Settlement.ProviderAsset
	if asset == "" {
		asset = q.Settlement.ClientAsset
	}
	if asset == "" {
		asset = q.Price.Currency
	}
	return &atostosv1.QuoteCommitmentInput{QuoteId: q.ID, PrincipalId: q.PrincipalID, ProviderId: q.ProviderID, CapabilityId: q.CapabilityID, CapabilityVersion: q.CapabilityVersion, TrustMode: trustMode(q.TrustMode), ProofProfile: proofProfile(q.ProofProfile), TotalMax: &atostosv1.Money{Amount: q.Price.TotalMax, Currency: q.Price.Currency}, TermsDigest: mustDigest(q.TermsHash), DisputePolicyDigest: mustDigest(q.DisputePolicyHash), ExpiresUnixMillis: q.ExpiresAt.UnixMilli(), SettlementBackend: string(q.Settlement.Backend), SettlementAsset: asset, UnderlyingServiceQuoteRef: q.ServiceQuoteID, Version: quotecommitment.Version, Canonicalization: quotecommitment.Canonicalization, NetworkId: q.NetworkID, Domain: q.CommitmentDomain, RequesterAgentId: q.RequesterAgentID, ManifestDigest: mustDigest(q.ManifestCommitment), OwnershipRef: parseReference(q.NetworkID, q.OwnershipRef), Subtotal: &atostosv1.Money{Amount: q.Price.Subtotal, Currency: q.Price.Currency}, Fees: &atostosv1.Money{Amount: q.Price.Fees, Currency: q.Price.Currency}, AssetDecimals: q.AssetDecimals, AcceptanceDeadlineUnixMillis: q.ExpiresAt.UnixMilli(), ExecutionDeadlineUnixMillis: q.ExecutionDeadline.UnixMilli(), SignerAuthorizationId: q.SignerAuthorizationID, SignerAuthorizationRef: parseReference(q.NetworkID, q.SignerAuthorizationRef)}
}
func parseReference(network, value string) *atostosv1.NetworkReference {
	return &atostosv1.NetworkReference{Network: network, Reference: strings.TrimPrefix(value, network+":")}
}

func finalizedReference(network, value string, checkpoint uint64) *atostosv1.NetworkReference {
	ref := parseReference(network, value)
	ref.Finalized = checkpoint > 0
	ref.FinalizedCheckpoint = checkpoint
	return ref
}

func (c *Client) ResolveExecutionSignerAuthorization(
	ctx context.Context,
	providerID, capabilityID, capabilityVersion, signerID string,
	at time.Time,
) (toscore.ExecutionSignerAuthorization, bool, error) {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ResolveExecutionSignerAuthorizationRequest{
		Context:    c.requestContext(ctx, providerID, "", time.Time{}),
		ProviderId: providerID, CapabilityId: capabilityID,
		CapabilityVersion: capabilityVersion, ExecutionSignerId: signerID,
		AtUnixMillis: at.UnixMilli(),
	})
	decorateRequest(c, ctx, request)
	response, err := c.trust.ResolveExecutionSignerAuthorization(callCtx, request)
	if err != nil {
		return toscore.ExecutionSignerAuthorization{}, false, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Authorized || response.Msg.Authorization == nil || response.Msg.Authorization.Value == nil {
		return toscore.ExecutionSignerAuthorization{}, false, nil
	}
	value := response.Msg.Authorization.Value
	return toscore.ExecutionSignerAuthorization{
		AuthorizationID: value.AuthorizationId, ProviderID: value.ProviderId,
		CapabilityID: value.CapabilityId, CapabilityVersion: value.CapabilityVersion,
		ExecutionSignerID: value.ExecutionSignerId,
		ValidFrom:         time.UnixMilli(value.ValidFromUnixMillis).UTC(),
		ValidUntil:        time.UnixMilli(value.ValidUntilUnixMillis).UTC(),
		AuthorizationRef:  reference(response.Msg.Authorization.AuthorizationRef),
		Revoked:           response.Msg.Authorization.Revoked,
		SignerPublicKey:   append([]byte(nil), value.SignerPublicKey...), SignatureAlgorithm: value.SignatureAlgorithm,
		FinalizedCheckpoint: response.Msg.Authorization.AuthorizationRef.GetFinalizedCheckpoint(),
	}, true, nil
}

func toExecutionSignerAuthorization(msg *atostosv1.ExecutionSignerAuthorization) toscore.ExecutionSignerAuthorization {
	if msg == nil || msg.Value == nil {
		return toscore.ExecutionSignerAuthorization{}
	}
	value := msg.Value
	return toscore.ExecutionSignerAuthorization{
		AuthorizationID: value.AuthorizationId, ProviderID: value.ProviderId,
		CapabilityID: value.CapabilityId, CapabilityVersion: value.CapabilityVersion,
		ExecutionSignerID: value.ExecutionSignerId,
		ValidFrom:         time.UnixMilli(value.ValidFromUnixMillis).UTC(),
		ValidUntil:        time.UnixMilli(value.ValidUntilUnixMillis).UTC(),
		AuthorizationRef:  reference(msg.AuthorizationRef),
		Revoked:           msg.Revoked,
		RevocationRef:     reference(msg.RevocationRef),
		SignerPublicKey:   append([]byte(nil), value.SignerPublicKey...), SignatureAlgorithm: value.SignatureAlgorithm,
		FinalizedCheckpoint: msg.AuthorizationRef.GetFinalizedCheckpoint(),
	}
}

// AuthorizeExecutionSigner calls tos-protocol's TrustService RPC of the same
// name unchanged (atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.2.2 --
// reused, not replaced). req.AuthorizationID is the caller's durably
// persisted idempotency identity; tos-protocol's own atomicMutation
// machinery makes a retry with the same AuthorizationID and identical
// fields return the same result, and a changed field return
// IDEMPOTENCY_CONFLICT (surfaced here as domain.ErrIdempotencyConflict via
// rpcError).
func (c *Client) AuthorizeExecutionSigner(ctx context.Context, req toscore.AuthorizeExecutionSignerRequest) (toscore.ExecutionSignerAuthorization, bool, error) {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.AuthorizeExecutionSignerRequest{
		Context: c.requestContext(ctx, req.ProviderID, req.AuthorizationID, time.Time{}),
		Authorization: &atostosv1.ExecutionSignerAuthorizationInput{
			AuthorizationId: req.AuthorizationID, ProviderId: req.ProviderID,
			CapabilityId: req.CapabilityID, CapabilityVersion: req.CapabilityVersion,
			ExecutionSignerId: req.ExecutionSignerID, SignerPublicKey: req.SignerPublicKey,
			SignatureAlgorithm:  req.SignatureAlgorithm,
			ValidFromUnixMillis: req.ValidFrom.UnixMilli(), ValidUntilUnixMillis: req.ValidUntil.UnixMilli(),
		},
	})
	decorateRequest(c, ctx, request)
	response, err := c.trust.AuthorizeExecutionSigner(callCtx, request)
	if err != nil {
		return toscore.ExecutionSignerAuthorization{}, false, rpcError(err)
	}
	if response.Msg == nil || response.Msg.Authorization == nil {
		return toscore.ExecutionSignerAuthorization{}, false, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned an empty execution signer authorization", true)
	}
	return toExecutionSignerAuthorization(response.Msg.Authorization), response.Msg.Created, nil
}

// RevokeExecutionSigner calls tos-protocol's TrustService RPC of the same
// name unchanged. req.AuthorizationID identifies the existing authorization
// to revoke (not a new idempotency key of its own -- the underlying RPC's
// own idempotency is keyed by the request's canonical digest, matching
// AuthorizeExecutionSigner).
func (c *Client) RevokeExecutionSigner(ctx context.Context, req toscore.RevokeExecutionSignerRequest) (toscore.ExecutionSignerAuthorization, bool, error) {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.RevokeExecutionSignerRequest{
		Context:         c.requestContext(ctx, req.ProviderID, req.AuthorizationID, time.Time{}),
		AuthorizationId: req.AuthorizationID, ReasonCode: req.ReasonCode,
	})
	decorateRequest(c, ctx, request)
	response, err := c.trust.RevokeExecutionSigner(callCtx, request)
	if err != nil {
		return toscore.ExecutionSignerAuthorization{}, false, rpcError(err)
	}
	if response.Msg == nil || response.Msg.Authorization == nil {
		return toscore.ExecutionSignerAuthorization{}, false, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned an empty execution signer authorization", true)
	}
	return toExecutionSignerAuthorization(response.Msg.Authorization), response.Msg.Revoked, nil
}

func (c *Client) CreateEscrow(ctx context.Context, req toscore.CreateEscrowRequest) (domain.Escrow, error) {
	if req.Quote.ID == "" {
		req.Quote = domain.Quote{ID: req.QuoteID, CapabilityID: req.CapabilityID, CapabilityVersion: req.CapabilityVersion, ProviderID: req.ProviderID, PrincipalID: req.PrincipalID, TrustMode: req.TrustMode, ProofProfile: req.ProofProfile, Settlement: req.Settlement, Price: domain.Price{TotalMax: req.Reserved.Amount, Currency: req.Reserved.Currency}, ExpiresAt: time.Now().UTC().Add(c.retention)}
	}
	if req.Quote.TrustMode != domain.TrustModeVerified {
		return c.createManagedEscrow(ctx, req)
	}
	terms, digestValue, err := verifiedEscrowTerms(req.Quote, req.JobID)
	if err != nil {
		return domain.Escrow{}, err
	}
	if existing, found, getErr := c.GetEscrow(ctx, toscore.GetEscrowRequest{Quote: req.Quote, JobID: req.JobID, ExpectedReservationDigest: digestValue}); getErr != nil {
		return domain.Escrow{}, getErr
	} else if found {
		return existing, nil
	}
	expires := req.Quote.ExecutionDeadline
	if expires.IsZero() {
		expires = req.Quote.ExpiresAt
	}
	if !expires.After(time.Now().UTC()) {
		return domain.Escrow{}, domain.NewError(domain.ErrQuoteExpired, "verified escrow deadline has passed", false)
	}
	if req.Quote.TrustMode != domain.TrustModeVerified {
		return domain.Escrow{}, domain.NewError(domain.ErrValidationFailed, "TaskEscrow authority requires verified trust mode", false)
	}
	callCtx, cancel := c.callContext(ctx, expires)
	defer cancel()
	request := connect.NewRequest(&atostosv1.CreateEscrowRequest{
		Context: c.requestContext(ctx, req.Quote.PrincipalID, "create-escrow:"+req.Quote.ID+":"+req.JobID, expires),
		QuoteId: req.Quote.ID, PrincipalId: req.Quote.PrincipalID,
		ProviderId: req.Quote.ProviderID, CapabilityId: req.Quote.CapabilityID,
		TrustMode: trustMode(req.Quote.TrustMode), ProofProfile: proofProfile(req.Quote.ProofProfile),
		Reserve: terms.Reserve, FundingModel: string(req.Quote.Settlement.FundingModel),
		ExpiresUnixMillis: expires.UnixMilli(), VerifiedTerms: terms,
	})
	decorateRequest(c, ctx, request)
	response, err := c.settlement.CreateEscrow(callCtx, request)
	if err != nil {
		return domain.Escrow{}, rpcError(err)
	}
	if response.Msg == nil || response.Msg.Escrow == nil {
		return domain.Escrow{}, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned an empty escrow", true)
	}
	mapped := domainEscrow(response.Msg.Escrow, req.JobID, req.Quote.CapabilityVersion, req.Quote.Settlement)
	if err := validateCanonicalEscrow(req.Quote, req.JobID, digestValue, mapped); err != nil {
		return domain.Escrow{}, err
	}
	if ref := reference(response.Msg.Escrow.EscrowRef); ref != "" {
		c.proofRefs.Store(mapped.ID, ref)
	}
	return mapped, nil
}

func (c *Client) createManagedEscrow(ctx context.Context, req toscore.CreateEscrowRequest) (domain.Escrow, error) {
	if existing, err := c.store.EscrowByJob(ctx, req.JobID); err == nil {
		return existing, nil
	} else if err != store.ErrNotFound {
		return domain.Escrow{}, err
	}
	reserve, err := networkAmount(req.Reserved)
	if err != nil {
		return domain.Escrow{}, err
	}
	expires := req.Quote.ExpiresAt
	callCtx, cancel := c.callContext(ctx, expires)
	defer cancel()
	request := connect.NewRequest(&atostosv1.CreateEscrowRequest{Context: c.requestContext(ctx, req.PrincipalID, "create-escrow:"+req.QuoteID+":"+req.JobID, expires), QuoteId: req.QuoteID, PrincipalId: req.PrincipalID, ProviderId: req.ProviderID, CapabilityId: req.CapabilityID, TrustMode: trustMode(req.TrustMode), ProofProfile: proofProfile(req.ProofProfile), Reserve: reserve, FundingModel: string(req.Settlement.FundingModel), ExpiresUnixMillis: expires.UnixMilli()})
	decorateRequest(c, ctx, request)
	response, err := c.settlement.CreateEscrow(callCtx, request)
	if err != nil {
		return domain.Escrow{}, rpcError(err)
	}
	if response.Msg == nil || response.Msg.Escrow == nil {
		return domain.Escrow{}, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned an empty escrow", true)
	}
	mapped := domainEscrow(response.Msg.Escrow, req.JobID, req.CapabilityVersion, req.Settlement)
	mapped.NetworkProofRef = ""
	if err = c.store.PutEscrow(ctx, mapped); err != nil {
		return domain.Escrow{}, err
	}
	return mapped, nil
}

func (c *Client) GetEscrow(ctx context.Context, req toscore.GetEscrowRequest) (domain.Escrow, bool, error) {
	if req.Quote.TrustMode != domain.TrustModeVerified {
		e, err := c.store.EscrowByJob(ctx, req.JobID)
		if err == store.ErrNotFound {
			return domain.Escrow{}, false, nil
		}
		return e, err == nil, err
	}
	terms, digestValue, err := verifiedEscrowTerms(req.Quote, req.JobID)
	if err != nil {
		return domain.Escrow{}, false, err
	}
	if req.ExpectedReservationDigest != "" && req.ExpectedReservationDigest != digestValue {
		return domain.Escrow{}, false, domain.NewError(domain.ErrQuoteMismatch, "reservation digest assertion mismatched", false)
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.GetEscrowRequest{Context: c.requestContext(ctx, req.Quote.PrincipalID, "", time.Time{}), EscrowId: req.EscrowID, QuoteId: req.Quote.ID, ExpectedTerms: terms, ExpectedReservationDigest: digestValue})
	request.Msg.ExpectedCreatorAddress = req.ExpectedCreatorAddress
	request.Msg.ExpectedAgentAddress = req.ExpectedAgentAddress
	if req.ExpectedEscrowRef != "" {
		request.Msg.ExpectedEscrowRef = parseReference(req.Quote.NetworkID, req.ExpectedEscrowRef)
	}
	if req.ExpectedTerminalRef != "" {
		request.Msg.ExpectedTerminalRef = parseReference(req.Quote.NetworkID, req.ExpectedTerminalRef)
	}
	request.Msg.ExpectedReleaseDigest = req.ExpectedReleaseDigest
	request.Msg.ExpectedReleaseReasonCode = req.ExpectedReleaseReasonCode
	request.Msg.ExpectedDisputeDigest = req.ExpectedDisputeDigest
	if req.ExpectedDisputeRef != "" {
		request.Msg.ExpectedDisputeRef = parseReference(req.Quote.NetworkID, req.ExpectedDisputeRef)
	}
	if req.ExpectedDisputePayout.Amount != "" {
		payout, amountErr := networkAmount(req.ExpectedDisputePayout)
		if amountErr != nil {
			return domain.Escrow{}, false, amountErr
		}
		request.Msg.ExpectedDisputePayout = payout
	}
	if req.ExpectedResolutionDigest != "" {
		request.Msg.ExpectedDisputeResolutionDigest = req.ExpectedResolutionDigest
		request.Msg.ExpectedDisputeOutcome = req.ExpectedDisputeOutcome
		request.Msg.ExpectedDisputeResolutionRef = parseReference(req.Quote.NetworkID, req.ExpectedResolutionRef)
		refund, amountErr := networkAmount(domain.Money{Amount: req.Quote.Price.TotalMax, Currency: req.Quote.Price.Currency})
		if amountErr != nil {
			return domain.Escrow{}, false, amountErr
		}
		payout := request.Msg.ExpectedDisputePayout
		if payout == nil {
			return domain.Escrow{}, false, domain.NewError(domain.ErrValidationFailed, "dispute payout is required", false)
		}
		reserved, _ := new(big.Int).SetString(refund.AtomicAmount, 10)
		paid, _ := new(big.Int).SetString(payout.AtomicAmount, 10)
		if reserved == nil || paid == nil || paid.Sign() < 0 || paid.Cmp(reserved) > 0 {
			return domain.Escrow{}, false, domain.NewError(domain.ErrValidationFailed, "invalid dispute payout", false)
		}
		request.Msg.ExpectedDisputeResolution = &atostosv1.VerifiedDisputeResolution{
			Version: "atos_verified_dispute_resolution_v1", NetworkId: req.Quote.NetworkID,
			GatewayDomain: req.Quote.CommitmentDomain, DisputeId: req.ExpectedDisputeID,
			EscrowId: req.EscrowID, JobId: req.JobID, QuoteId: req.Quote.ID,
			ReceiptId: req.ExpectedReceipt.ID, DisputeDigest: req.ExpectedDisputeDigest,
			Outcome: req.ExpectedDisputeOutcome, ReviewerPrincipalId: req.ExpectedReviewerID,
			Reserved: refund, ProviderPayout: payout,
			RequesterRefund:    &atostosv1.NetworkAmount{Asset: "TOS", AtomicAmount: new(big.Int).Sub(reserved, paid).String()},
			ResolvedUnixMillis: req.ExpectedResolvedAt.UnixMilli(),
		}
	}
	if req.ExpectedReceipt != nil {
		envelope, envelopeErr := c.executionEnvelope(ctx, *req.ExpectedReceipt)
		if envelopeErr != nil {
			return domain.Escrow{}, false, envelopeErr
		}
		request.Msg.ExpectedReceipt = envelope
		request.Msg.ExpectedReceiptRef = parseReference(req.Quote.NetworkID, req.ExpectedReceiptRef)
		charge, amountErr := networkAmount(req.ExpectedSettlementCharge)
		if amountErr != nil {
			return domain.Escrow{}, false, amountErr
		}
		request.Msg.ExpectedSettlementCharge = charge
	}
	decorateRequest(c, ctx, request)
	response, err := c.settlement.GetEscrow(callCtx, request)
	if err != nil {
		return domain.Escrow{}, false, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Found || response.Msg.Escrow == nil {
		return domain.Escrow{}, false, nil
	}
	mapped := domainEscrow(response.Msg.Escrow, req.JobID, req.Quote.CapabilityVersion, req.Quote.Settlement)
	if err = validateCanonicalEscrow(req.Quote, req.JobID, digestValue, mapped); err != nil {
		return domain.Escrow{}, false, err
	}
	return mapped, true, nil
}

func (c *Client) ReleaseEscrow(ctx context.Context, req toscore.ReleaseEscrowRequest) (toscore.ReleaseEscrowResult, error) {
	if req.Quote.TrustMode != domain.TrustModeVerified {
		return c.releaseManagedEscrow(ctx, req)
	}
	local, found, err := c.GetEscrow(ctx, toscore.GetEscrowRequest{Quote: req.Quote, JobID: req.JobID, EscrowID: req.EscrowID})
	if err != nil {
		return toscore.ReleaseEscrowResult{}, err
	}
	if !found {
		return toscore.ReleaseEscrowResult{}, domain.NewError(domain.ErrSettlementFailed, "canonical escrow not found", true)
	}
	terms, digestValue, err := verifiedEscrowTerms(req.Quote, req.JobID)
	if err != nil {
		return toscore.ReleaseEscrowResult{}, err
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ReleaseEscrowRequest{
		Context:  c.requestContext(ctx, local.PrincipalID, "release-escrow:"+local.ID, time.Time{}),
		EscrowId: local.ID, QuoteId: local.QuoteID, JobId: local.JobID,
		ReasonCode: req.ReasonCode, ExpectedTerms: terms, ExpectedReservationDigest: digestValue, ExpectedEscrowRef: parseReference(req.Quote.NetworkID, local.NetworkProofRef),
	})
	decorateRequest(c, ctx, request)
	response, err := c.settlement.ReleaseEscrow(callCtx, request)
	if err != nil {
		return toscore.ReleaseEscrowResult{}, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Released || response.Msg.Escrow == nil {
		return toscore.ReleaseEscrowResult{}, domain.NewError(domain.ErrSettlementFailed, "tos-protocol did not release escrow", true)
	}
	released := domainEscrow(response.Msg.Escrow, req.JobID, req.Quote.CapabilityVersion, req.Quote.Settlement)
	if err = validateCanonicalEscrow(req.Quote, req.JobID, digestValue, released); err != nil {
		return toscore.ReleaseEscrowResult{}, err
	}
	if released.Status != domain.EscrowReleased || released.ReleaseRef == "" || released.ReleaseDigest == "" || released.ReleaseActionID == "" || released.ReleaseReason != req.ReasonCode {
		return toscore.ReleaseEscrowResult{}, domain.NewError(domain.ErrSettlementFailed, "canonical release transition is incomplete or mismatched", false)
	}
	now := time.Now().UTC()
	local.Status = domain.EscrowReleased
	local.SettledAt = &now
	if local.TrustMode != domain.TrustModeManaged {
		local.NetworkProofRef = reference(response.Msg.ReleaseRef)
	}
	proofStatus := domain.ProofNotRequired
	if local.TrustMode != domain.TrustModeManaged {
		proofStatus = domain.ProofReleased
	}
	receipt := domain.Receipt{
		ID: "rcpt_release_" + local.ID, QuoteID: local.QuoteID, EscrowID: local.ID,
		JobID: local.JobID, PrincipalID: local.PrincipalID,
		TrustMode: local.TrustMode, ProofProfile: local.ProofProfile,
		Charged:  domain.Money{Amount: "0.00", Currency: local.Reserved.Currency},
		Refunded: local.Reserved, Status: domain.ReceiptReleased,
		ProofStatus: proofStatus, CreatedAt: now,
	}
	if local.TrustMode != domain.TrustModeManaged {
		receipt.NetworkProofRef = reference(response.Msg.ReleaseRef)
		receipt.NetworkProofCheckpoint = response.Msg.ReleaseRef.GetFinalizedCheckpoint()
		receipt.Finalized = response.Msg.ReleaseRef.GetFinalized()
		receipt.FinalizedCheckpoint = response.Msg.ReleaseRef.GetFinalizedCheckpoint()
	}
	if receipt.NetworkProofRef != "" {
		c.proofRefs.Store(receipt.ID, receipt.NetworkProofRef)
	}
	return toscore.ReleaseEscrowResult{Escrow: released, Receipt: receipt}, nil
}

func (c *Client) PrepareVerifiedResult(ctx context.Context, req toscore.PrepareVerifiedResultRequest) (domain.Escrow, error) {
	terms, reservationDigest, err := verifiedEscrowTerms(req.Quote, req.Job.ID)
	if err != nil {
		return domain.Escrow{}, err
	}
	receipt, err := c.executionEnvelope(ctx, req.Receipt)
	if err != nil {
		return domain.Escrow{}, err
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	r := connect.NewRequest(&atostosv1.PrepareVerifiedResultRequest{Context: c.requestContext(ctx, req.Job.PrincipalID, "prepare-result:"+req.Job.ID, time.Time{}), EscrowId: req.Escrow.ID, QuoteId: req.Quote.ID, JobId: req.Job.ID, ReceiptId: req.Receipt.ID, ExpectedTerms: terms, ExpectedEscrowRef: parseReference(req.Quote.NetworkID, req.Escrow.NetworkProofRef), ExpectedReservationDigest: reservationDigest, ExpectedReceipt: receipt, ExpectedReceiptRef: finalizedReference(req.Quote.NetworkID, req.Receipt.NetworkProofRef, req.Receipt.NetworkProofCheckpoint)})
	decorateRequest(c, ctx, r)
	resp, e := c.settlement.PrepareVerifiedResult(callCtx, r)
	if e != nil {
		return domain.Escrow{}, rpcError(e)
	}
	if resp.Msg == nil || !resp.Msg.Prepared || resp.Msg.Escrow == nil {
		return domain.Escrow{}, domain.NewError(domain.ErrSettlementFailed, "TOS result review was not prepared", true)
	}
	escrow := domainEscrow(resp.Msg.Escrow, req.Job.ID, req.Job.CapabilityVersion, req.Quote.Settlement)
	if escrow.ResultRef == "" || escrow.ReviewDeadline == nil || escrow.FinalizedCheckpoint == 0 {
		return domain.Escrow{}, domain.NewError(domain.ErrSettlementFailed, "finalized result review evidence missing", true)
	}
	return escrow, nil
}

func (c *Client) OpenVerifiedDispute(ctx context.Context, req toscore.VerifiedDisputeOpenRequest) (toscore.VerifiedDisputeResult, error) {
	terms, reservationDigest, e := verifiedEscrowTerms(req.Quote, req.Job.ID)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, e
	}
	if req.Job.ExecutionReceipt == nil {
		return toscore.VerifiedDisputeResult{}, domain.NewError(domain.ErrDisputeNotEligible, "execution Receipt unavailable", false)
	}
	wire, e := c.PortableReceiptEvidence(ctx, *req.Job.ExecutionReceipt)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, e
	}
	receiptEnvelope, e := c.executionEnvelope(ctx, *req.Job.ExecutionReceipt)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, e
	}
	digests := make([]*atostosv1.Digest, 0, len(req.EvidenceDigests))
	for _, d := range req.EvidenceDigests {
		digests = append(digests, mustDigest(d))
	}
	d := &atostosv1.VerifiedDisputeOpen{Version: "atos_verified_dispute_open_v1", NetworkId: req.Quote.NetworkID, GatewayDomain: req.Quote.CommitmentDomain, DisputeId: req.DisputeID, EscrowId: req.Escrow.ID, JobId: req.Job.ID, QuoteId: req.Quote.ID, ReceiptId: req.Job.ExecutionReceipt.ID, PrincipalId: req.Job.PrincipalID, ProviderId: req.Job.ProviderID, CapabilityId: req.Job.CapabilityID, CapabilityVersion: req.Job.CapabilityVersion, QuoteCommitmentDigest: req.Quote.Commitment.Digest, ReservationDigest: req.Escrow.ReservationDigest, ReceiptDigest: wire.Digest, DisputePolicyDigest: mustDigest(req.Quote.DisputePolicyHash), ReasonCode: req.ReasonCode, EvidenceDigests: digests, OpenedUnixMillis: req.OpenedAt.UnixMilli()}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	r := connect.NewRequest(&atostosv1.OpenVerifiedDisputeRequest{Context: c.requestContext(ctx, req.Job.PrincipalID, "open-dispute:"+req.DisputeID, time.Time{}), Dispute: d, ExpectedTerms: terms, ExpectedEscrowRef: parseReference(req.Quote.NetworkID, req.Escrow.NetworkProofRef), ExpectedReservationDigest: reservationDigest, ExpectedResultHash: req.Escrow.ResultDigest, ExpectedEvidenceHash: req.Escrow.ResultEvidenceDigest, ExpectedResultRef: parseReference(req.Quote.NetworkID, req.Escrow.ResultRef), ExpectedReceipt: receiptEnvelope, ExpectedReceiptRef: finalizedReference(req.Quote.NetworkID, req.Job.ExecutionReceipt.NetworkProofRef, req.Job.ExecutionReceipt.NetworkProofCheckpoint)})
	decorateRequest(c, ctx, r)
	expectedDigest, e := disputecommitment.OpenDigest(d)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, domain.NewError(domain.ErrValidationFailed, e.Error(), false)
	}
	resp, e := c.settlement.OpenVerifiedDispute(callCtx, r)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, rpcError(e)
	}
	if resp.Msg == nil || !resp.Msg.Opened || resp.Msg.Escrow == nil || resp.Msg.DisputeRef == nil {
		return toscore.VerifiedDisputeResult{}, domain.NewError(domain.ErrSettlementFailed, "TOS dispute was not finalized", true)
	}
	escrow := domainEscrow(resp.Msg.Escrow, req.Job.ID, req.Job.CapabilityVersion, req.Quote.Settlement)
	if resp.Msg.DisputeDigest != expectedDigest || escrow.Status != domain.EscrowDisputed || escrow.DisputeDigest != expectedDigest || reference(resp.Msg.DisputeRef) != escrow.DisputeRef || resp.Msg.DisputeRef.Network != req.Quote.NetworkID || !resp.Msg.DisputeRef.Finalized || resp.Msg.DisputeRef.FinalizedCheckpoint == 0 {
		return toscore.VerifiedDisputeResult{}, domain.NewError(domain.ErrSettlementFailed, "TOS dispute response tuple mismatch", false)
	}
	return toscore.VerifiedDisputeResult{Escrow: escrow, ReceiptDigest: wire.Digest, DisputeDigest: resp.Msg.DisputeDigest, DisputeRef: reference(resp.Msg.DisputeRef), FinalizedCheckpoint: resp.Msg.DisputeRef.FinalizedCheckpoint}, nil
}

func (c *Client) ResolveVerifiedDispute(ctx context.Context, req toscore.VerifiedDisputeResolutionRequest) (toscore.VerifiedDisputeResult, error) {
	terms, reservationDigest, e := verifiedEscrowTerms(req.Quote, req.Job.ID)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, e
	}
	receiptEnvelope, e := c.executionEnvelope(ctx, *req.Job.ExecutionReceipt)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, e
	}
	reserved, e := networkAmount(req.Escrow.Reserved)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, e
	}
	payout, e := networkAmount(req.ProviderPayout)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, e
	}
	refund, e := networkAmount(req.RequesterRefund)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, e
	}
	rv := &atostosv1.VerifiedDisputeResolution{Version: "atos_verified_dispute_resolution_v1", NetworkId: req.Quote.NetworkID, GatewayDomain: req.Quote.CommitmentDomain, DisputeId: req.DisputeID, EscrowId: req.Escrow.ID, JobId: req.Job.ID, QuoteId: req.Quote.ID, ReceiptId: req.Job.ExecutionReceipt.ID, DisputeDigest: req.DisputeDigest, Outcome: req.Outcome, ReviewerPrincipalId: req.ReviewerID, Reserved: reserved, ProviderPayout: payout, RequesterRefund: refund, ResolvedUnixMillis: req.ResolvedAt.UnixMilli()}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	r := connect.NewRequest(&atostosv1.ResolveVerifiedDisputeRequest{Context: c.requestContext(ctx, req.ReviewerID, "resolve-dispute:"+req.DisputeID, time.Time{}), Resolution: rv, ExpectedTerms: terms, ExpectedEscrowRef: parseReference(req.Quote.NetworkID, req.Escrow.NetworkProofRef), ExpectedReservationDigest: reservationDigest, ExpectedDisputeRef: parseReference(req.Quote.NetworkID, req.DisputeRef), ExpectedReceipt: receiptEnvelope, ExpectedReceiptRef: finalizedReference(req.Quote.NetworkID, req.Job.ExecutionReceipt.NetworkProofRef, req.Job.ExecutionReceipt.NetworkProofCheckpoint)})
	decorateRequest(c, ctx, r)
	expectedDigest, e := disputecommitment.ResolutionDigest(rv)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, domain.NewError(domain.ErrValidationFailed, e.Error(), false)
	}
	resp, e := c.settlement.ResolveVerifiedDispute(callCtx, r)
	if e != nil {
		return toscore.VerifiedDisputeResult{}, rpcError(e)
	}
	if resp.Msg == nil || !resp.Msg.Resolved || resp.Msg.Escrow == nil || resp.Msg.ResolutionRef == nil || resp.Msg.Settlement == nil {
		return toscore.VerifiedDisputeResult{}, domain.NewError(domain.ErrSettlementFailed, "TOS dispute resolution was not finalized", true)
	}
	escrow := domainEscrow(resp.Msg.Escrow, req.Job.ID, req.Job.CapabilityVersion, req.Quote.Settlement)
	if resp.Msg.ResolutionDigest != expectedDigest || escrow.Status != domain.EscrowSettled || escrow.DisputeDigest != req.DisputeDigest || escrow.DisputeResolutionDigest != expectedDigest || reference(resp.Msg.ResolutionRef) != escrow.TerminalProofRef || resp.Msg.ResolutionRef.Network != req.Quote.NetworkID || !resp.Msg.ResolutionRef.Finalized || resp.Msg.ResolutionRef.FinalizedCheckpoint == 0 || resp.Msg.Settlement.State != atostosv1.SettlementState_SETTLEMENT_STATE_SETTLED || resp.Msg.Settlement.JobId != req.Job.ID || resp.Msg.Settlement.QuoteId != req.Quote.ID || resp.Msg.Settlement.EscrowId != req.Escrow.ID || resp.Msg.Settlement.ReceiptId != req.Job.ExecutionReceipt.ID || domainMoney(resp.Msg.Settlement.Charged) != req.ProviderPayout || domainMoney(resp.Msg.Settlement.Refunded) != req.RequesterRefund {
		return toscore.VerifiedDisputeResult{}, domain.NewError(domain.ErrSettlementFailed, "TOS dispute resolution response tuple mismatch", false)
	}
	receiptStatus := domain.ReceiptSettledAfterDispute
	if req.Outcome == string(domain.DisputeOutcomePrincipal) {
		receiptStatus = domain.ReceiptReleasedAfterDispute
	}
	receipt := domain.Receipt{ID: "rcpt_dispute_" + req.DisputeID, QuoteID: req.Quote.ID, EscrowID: req.Escrow.ID, JobID: req.Job.ID, PrincipalID: req.Job.PrincipalID, TrustMode: domain.TrustModeVerified, ProofProfile: domain.ProofProfileTOSVerifiedV1, Charged: req.ProviderPayout, Refunded: req.RequesterRefund, Status: receiptStatus, ProofStatus: domain.ProofSettled, NetworkProofRef: reference(resp.Msg.ResolutionRef), NetworkProofCheckpoint: resp.Msg.ResolutionRef.FinalizedCheckpoint, Finalized: true, FinalizedCheckpoint: resp.Msg.ResolutionRef.FinalizedCheckpoint, CreatedAt: req.ResolvedAt}
	return toscore.VerifiedDisputeResult{Escrow: escrow, Receipt: receipt, DisputeDigest: req.DisputeDigest, DisputeRef: req.DisputeRef, ResolutionDigest: resp.Msg.ResolutionDigest, ResolutionRef: reference(resp.Msg.ResolutionRef), FinalizedCheckpoint: resp.Msg.ResolutionRef.FinalizedCheckpoint}, nil
}

func (c *Client) releaseManagedEscrow(ctx context.Context, req toscore.ReleaseEscrowRequest) (toscore.ReleaseEscrowResult, error) {
	local, err := c.store.GetEscrow(ctx, req.EscrowID)
	if err != nil {
		return toscore.ReleaseEscrowResult{}, err
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ReleaseEscrowRequest{Context: c.requestContext(ctx, local.PrincipalID, "release-escrow:"+local.ID, time.Time{}), EscrowId: local.ID, QuoteId: local.QuoteID, JobId: local.JobID, ReasonCode: req.ReasonCode})
	decorateRequest(c, ctx, request)
	response, err := c.settlement.ReleaseEscrow(callCtx, request)
	if err != nil {
		return toscore.ReleaseEscrowResult{}, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Released {
		return toscore.ReleaseEscrowResult{}, domain.NewError(domain.ErrSettlementFailed, "tos-protocol did not release escrow", true)
	}
	now := time.Now().UTC()
	local.Status = domain.EscrowReleased
	local.SettledAt = &now
	if err = c.store.PutEscrow(ctx, local); err != nil {
		return toscore.ReleaseEscrowResult{}, err
	}
	receipt := domain.Receipt{ID: "rcpt_release_" + local.ID, QuoteID: local.QuoteID, EscrowID: local.ID, JobID: local.JobID, PrincipalID: local.PrincipalID, TrustMode: local.TrustMode, ProofProfile: local.ProofProfile, Charged: domain.Money{Amount: "0.00", Currency: local.Reserved.Currency}, Refunded: local.Reserved, Status: domain.ReceiptReleased, ProofStatus: domain.ProofNotRequired, CreatedAt: now}
	if err = c.store.PutReceipt(ctx, receipt); err != nil {
		return toscore.ReleaseEscrowResult{}, err
	}
	return toscore.ReleaseEscrowResult{Escrow: local, Receipt: receipt}, nil
}

func (c *Client) CommitExecutionReceipt(ctx context.Context, receipt domain.ExecutionReceipt) (string, error) {
	envelope, err := c.executionEnvelope(ctx, receipt)
	if err != nil {
		return "", err
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.CommitExecutionReceiptRequest{
		Context: c.requestContext(ctx, receipt.PrincipalID, "commit-receipt:"+receipt.ID, time.Time{}),
		Receipt: envelope,
	})
	decorateRequest(c, ctx, request)
	response, err := c.proof.CommitExecutionReceipt(callCtx, request)
	if err != nil {
		return "", rpcError(err)
	}
	if response.Msg == nil || response.Msg.Receipt == nil {
		return "", domain.NewError(domain.ErrSettlementFailed, "tos-protocol returned an empty committed receipt", true)
	}
	ref := reference(response.Msg.Receipt.ReceiptRef)
	if ref != "" {
		c.proofRefs.Store(receipt.ID, ref)
	}
	if receipt.TrustMode == domain.TrustModeManaged {
		return "", nil
	}
	return ref, nil
}

func (c *Client) VerifyExecutionReceipt(ctx context.Context, escrowID string, receipt domain.ExecutionReceipt) (toscore.VerifyExecutionReceiptResult, error) {
	if receipt.EscrowID == "" || receipt.EscrowID != escrowID {
		return toscore.VerifyExecutionReceiptResult{Valid: false, Reason: "escrow mismatch"}, nil
	}
	envelope, err := c.executionEnvelope(ctx, receipt)
	if err != nil {
		return toscore.VerifyExecutionReceiptResult{}, err
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.VerifyExecutionReceiptRequest{
		Context: c.requestContext(ctx, receipt.PrincipalID, "", time.Time{}),
		Receipt: envelope, ExpectedQuoteId: receipt.QuoteID,
		ExpectedJobId: receipt.JobID, RequiredProfile: proofProfile(receipt.ProofProfile),
	})
	decorateRequest(c, ctx, request)
	response, err := c.proof.VerifyExecutionReceipt(callCtx, request)
	if err != nil {
		return toscore.VerifyExecutionReceiptResult{}, rpcError(err)
	}
	if response.Msg == nil {
		return toscore.VerifyExecutionReceiptResult{}, domain.NewError(domain.ErrSettlementFailed, "tos-protocol returned an empty receipt verification", true)
	}
	ref := reference(response.Msg.ProofRef)
	if ref != "" {
		c.proofRefs.Store(receipt.ID, ref)
	}
	if receipt.TrustMode == domain.TrustModeManaged {
		ref = ""
	}
	return toscore.VerifyExecutionReceiptResult{
		Valid: response.Msg.Verified, Reason: response.Msg.ReasonCode, ProofRef: ref,
		Network: response.Msg.ProofRef.GetNetwork(), Finalized: response.Msg.ProofRef.GetFinalized(), FinalizedCheckpoint: response.Msg.ProofRef.GetFinalizedCheckpoint(),
	}, nil
}

func (c *Client) PortableReceiptEvidence(ctx context.Context, receipt domain.ExecutionReceipt) (toscore.PortableReceiptEvidence, error) {
	envelope, err := c.executionEnvelope(ctx, receipt)
	if err != nil {
		return toscore.PortableReceiptEvidence{}, err
	}
	b, err := receiptcommitment.Bytes(envelope)
	if err != nil {
		return toscore.PortableReceiptEvidence{}, err
	}
	d, err := receiptcommitment.Digest(envelope)
	if err != nil {
		return toscore.PortableReceiptEvidence{}, err
	}
	return toscore.PortableReceiptEvidence{CanonicalCBOR: b, Digest: d}, nil
}
func (c *Client) PortableQuoteEvidence(_ context.Context, q domain.Quote) (toscore.PortableReceiptEvidence, error) {
	in := quoteCommitmentInput(q)
	b, err := quotecommitment.Bytes(in)
	if err != nil {
		return toscore.PortableReceiptEvidence{}, err
	}
	d, err := quotecommitment.Digest(in)
	return toscore.PortableReceiptEvidence{CanonicalCBOR: b, Digest: d}, err
}
func (c *Client) PortableEscrowEvidence(_ context.Context, q domain.Quote, jobID string) (toscore.PortableReceiptEvidence, error) {
	in, _, err := verifiedEscrowTerms(q, jobID)
	if err != nil {
		return toscore.PortableReceiptEvidence{}, err
	}
	b, err := escrowcommitment.Bytes(in)
	if err != nil {
		return toscore.PortableReceiptEvidence{}, err
	}
	d, err := escrowcommitment.Digest(in)
	return toscore.PortableReceiptEvidence{CanonicalCBOR: b, Digest: d}, err
}

func (c *Client) ResolveExecutionReceiptEvidence(ctx context.Context, receipt domain.ExecutionReceipt) (toscore.CanonicalEvidence, bool, error) {
	envelope, err := c.executionEnvelope(ctx, receipt)
	if err != nil {
		return toscore.CanonicalEvidence{}, false, err
	}
	var expected *atostosv1.NetworkReference
	if receipt.NetworkProofRef != "" {
		expected = &atostosv1.NetworkReference{Network: c.network, Reference: receipt.NetworkProofRef, Finalized: true, FinalizedCheckpoint: receipt.NetworkProofCheckpoint}
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ResolveExecutionReceiptRequest{Context: c.requestContext(ctx, receipt.PrincipalID, "", time.Time{}), Receipt: envelope, ExpectedReceiptRef: expected})
	decorateRequest(c, ctx, request)
	response, err := c.proof.ResolveExecutionReceipt(callCtx, request)
	if err != nil {
		return toscore.CanonicalEvidence{}, false, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Found || response.Msg.ReceiptRef == nil {
		return toscore.CanonicalEvidence{}, false, nil
	}
	r := response.Msg.ReceiptRef
	return toscore.CanonicalEvidence{Network: r.Network, Reference: r.Reference, Digest: digestString(response.Msg.ReceiptDigest), Finalized: r.Finalized, FinalizedCheckpoint: r.FinalizedCheckpoint}, true, nil
}

func (c *Client) SettleJob(ctx context.Context, req toscore.SettleJobRequest) (toscore.SettleJobResult, error) {
	local, err := c.store.GetEscrow(ctx, req.EscrowID)
	if err != nil {
		return toscore.SettleJobResult{}, err
	}
	if local.Status == domain.EscrowSettled && local.TrustMode == domain.TrustModeManaged {
		if receipt, replayErr := c.store.ReceiptByJob(ctx, req.JobID); replayErr == nil && receipt.EscrowID == local.ID && receipt.Status == domain.ReceiptSettled {
			return toscore.SettleJobResult{Receipt: receipt}, nil
		}
	}
	charge, err := networkAmount(req.ActualCost)
	if err != nil {
		return toscore.SettleJobResult{}, err
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.SettleJobRequest{
		Context:  c.requestContext(ctx, local.PrincipalID, "settle-job:"+req.JobID+":"+req.ReceiptID, time.Time{}),
		EscrowId: req.EscrowID, QuoteId: local.QuoteID,
		JobId: req.JobID, ReceiptId: req.ReceiptID, RequestedCharge: charge,
	})
	if local.TrustMode == domain.TrustModeVerified {
		terms, digestValue, termsErr := verifiedEscrowTerms(req.Quote, req.JobID)
		if termsErr != nil {
			return toscore.SettleJobResult{}, termsErr
		}
		if err := validateCanonicalEscrow(req.Quote, req.JobID, digestValue, local); err != nil {
			return toscore.SettleJobResult{}, err
		}
		request.Msg.ExpectedTerms = terms
		request.Msg.ExpectedEscrowRef = parseReference(req.Quote.NetworkID, local.NetworkProofRef)
		request.Msg.ExpectedEscrowRef.Finalized = local.Finalized
		request.Msg.ExpectedEscrowRef.FinalizedCheckpoint = local.FinalizedCheckpoint
		request.Msg.ExpectedReservationDigest = digestValue
	}
	decorateRequest(c, ctx, request)
	response, err := c.settlement.SettleJob(callCtx, request)
	if err != nil {
		return toscore.SettleJobResult{}, rpcError(err)
	}
	if response.Msg == nil || response.Msg.Settlement == nil || response.Msg.Escrow == nil {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrSettlementFailed, "tos-protocol returned an incomplete settlement", true)
	}
	settlement := response.Msg.Settlement
	remoteEscrow := response.Msg.Escrow
	if err := c.validateSettlementResponse(req, local, charge, settlement, remoteEscrow); err != nil {
		return toscore.SettleJobResult{}, err
	}
	now := time.UnixMilli(response.Msg.Settlement.SettledUnixMillis).UTC()
	local.Status = domain.EscrowSettled
	local.SettledAt = &now
	if local.TrustMode != domain.TrustModeManaged {
		local.Finalized = settlement.SettlementRef.Finalized
		local.FinalizedCheckpoint = settlement.SettlementRef.FinalizedCheckpoint
	}
	if err := c.store.PutEscrow(ctx, local); err != nil {
		return toscore.SettleJobResult{}, err
	}
	proofStatus := domain.ProofNotRequired
	if local.TrustMode != domain.TrustModeManaged {
		proofStatus = domain.ProofVerified
	}
	receipt := domain.Receipt{
		ID:      "rcpt_settle_" + settlement.SettlementId,
		QuoteID: settlement.QuoteId, EscrowID: settlement.EscrowId,
		JobID: settlement.JobId, PrincipalID: local.PrincipalID,
		TrustMode: local.TrustMode, ProofProfile: local.ProofProfile,
		Charged: domainMoney(settlement.Charged), Refunded: domainMoney(settlement.Refunded),
		Status: domain.ReceiptSettled, ProofStatus: proofStatus, CreatedAt: now,
	}
	if stored, found := c.receipts.Load(req.ReceiptID); found {
		execution := domainReceipt(stored.(*atostosv1.ExecutionReceiptEnvelope))
		receipt.ExecutionSignerID = execution.ExecutionSignerID
		receipt.SignerAuthorizationRef = execution.SignerAuthorizationRef
		receipt.InputCommitment = execution.InputHash
		receipt.OutputCommitment = execution.OutputHash
		receipt.UsageCommitment = execution.UsageCommitment
	}
	if local.TrustMode != domain.TrustModeManaged {
		receipt.NetworkProofRef = reference(settlement.SettlementRef)
		receipt.Finalized = settlement.SettlementRef.Finalized
		receipt.FinalizedCheckpoint = settlement.SettlementRef.FinalizedCheckpoint
	}
	if err := c.store.PutReceipt(ctx, receipt); err != nil {
		return toscore.SettleJobResult{}, err
	}
	if local.TrustMode != domain.TrustModeManaged {
		if ref := reference(settlement.SettlementRef); ref != "" {
			c.proofRefs.Store(receipt.ID, ref)
		}
	}
	return toscore.SettleJobResult{Receipt: receipt}, nil
}

func (c *Client) validateSettlementResponse(
	req toscore.SettleJobRequest,
	local domain.Escrow,
	requested *atostosv1.NetworkAmount,
	settlement *atostosv1.Settlement,
	escrow *atostosv1.Escrow,
) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrSettlementFailed, message, false)
	}
	if settlement.SettlementId == "" || settlement.EscrowId != req.EscrowID ||
		settlement.EscrowId != local.ID || settlement.QuoteId != local.QuoteID ||
		settlement.JobId != req.JobID || settlement.JobId != local.JobID ||
		settlement.ReceiptId != req.ReceiptID ||
		settlement.State != atostosv1.SettlementState_SETTLEMENT_STATE_SETTLED ||
		settlement.SettledUnixMillis <= 0 {
		return invalid("tos-protocol settlement tuple does not match the local request")
	}
	if escrow.EscrowId != local.ID || escrow.QuoteId != local.QuoteID ||
		escrow.TrustMode != trustMode(local.TrustMode) || escrow.State != atostosv1.EscrowState_ESCROW_STATE_SETTLED ||
		!sameNetworkAmount(escrow.Reserved, mustNetworkAmount(local.Reserved)) {
		return invalid("tos-protocol settled escrow does not match the canonical local escrow")
	}
	if !sameNetworkAmount(settlement.Charged, requested) || settlement.Refunded == nil ||
		settlement.Charged.Asset != escrow.Reserved.Asset || settlement.Refunded.Asset != escrow.Reserved.Asset {
		return invalid("tos-protocol settlement amount or asset does not match the request")
	}
	reserved, reserveOK := new(big.Int).SetString(escrow.Reserved.AtomicAmount, 10)
	charged, chargeOK := new(big.Int).SetString(settlement.Charged.AtomicAmount, 10)
	refunded, refundOK := new(big.Int).SetString(settlement.Refunded.AtomicAmount, 10)
	if !reserveOK || !chargeOK || !refundOK || reserved.Sign() < 0 || charged.Sign() < 0 || refunded.Sign() < 0 ||
		new(big.Int).Add(charged, refunded).Cmp(reserved) != 0 {
		return invalid("tos-protocol settlement violates monetary conservation")
	}
	if local.TrustMode == domain.TrustModeVerified {
		ref := settlement.SettlementRef
		if escrow.JobId != local.JobID || escrow.PrincipalId != local.PrincipalID ||
			escrow.ProviderId != local.ProviderID || escrow.CapabilityId != local.CapabilityID ||
			escrow.CapabilityVersion != local.CapabilityVersion || escrow.ProofProfile != proofProfile(local.ProofProfile) ||
			escrow.EscrowRef == nil || escrow.EscrowRef.Network != c.network ||
			escrow.EscrowRef.Reference != local.NetworkProofRef ||
			escrow.ReservationDigest != local.ReservationDigest ||
			escrow.QuoteCommitmentDigest != local.QuoteCommitmentDigest ||
			reference(escrow.QuoteCommitmentRef) != local.QuoteCommitmentRef ||
			escrow.ContractCodeHash != local.ContractCodeHash ||
			ref == nil || ref.Network != c.network || strings.TrimSpace(ref.Reference) == "" ||
			!ref.Finalized || ref.FinalizedCheckpoint == 0 || !escrow.Finalized ||
			escrow.FinalizedCheckpoint == 0 || ref.FinalizedCheckpoint < escrow.FinalizedCheckpoint {
			return invalid("verified settlement lacks matching finalized TOS evidence")
		}
	}
	return nil
}

func sameNetworkAmount(left, right *atostosv1.NetworkAmount) bool {
	return left != nil && right != nil && left.Asset == right.Asset && left.AtomicAmount == right.AtomicAmount
}

func mustNetworkAmount(value domain.Money) *atostosv1.NetworkAmount {
	amount, _ := networkAmount(value)
	return amount
}

func (c *Client) CommitProofOfServiceEvidence(ctx context.Context, receipt domain.ExecutionReceipt) (string, error) {
	if receipt.ID == "" {
		return "", domain.NewError(domain.ErrValidationFailed, "receipt_id is required for Proof-of-Service", false)
	}
	volume, err := networkAmount(receipt.Cost)
	if err != nil {
		return "", err
	}
	latency := uint64(0)
	if !receipt.StartedAt.IsZero() && !receipt.CompletedAt.Before(receipt.StartedAt) {
		latency = uint64(receipt.CompletedAt.Sub(receipt.StartedAt) / time.Millisecond)
	}
	evidenceID := "pos_" + receipt.ID
	evidenceDigest := textDigest(
		evidenceID, receipt.ID, receipt.ProviderID, receipt.CapabilityID,
		receipt.CapabilityVersion, string(receipt.Result), fmt.Sprintf("%d", latency),
		volume.Asset, volume.AtomicAmount,
	)
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.CommitProofOfServiceEvidenceRequest{
		Context: c.requestContext(ctx, receipt.PrincipalID, "commit-pos:"+receipt.ID, time.Time{}),
		Evidence: &atostosv1.ProofOfServiceEvidenceInput{
			EvidenceId: evidenceID, ReceiptId: receipt.ID,
			ProviderId: receipt.ProviderID, CapabilityId: receipt.CapabilityID,
			CapabilityVersion: receipt.CapabilityVersion,
			Result:            protoExecutionResult(receipt.Result), LatencyMillis: latency,
			SettlementVolume: volume, EvidenceDigest: evidenceDigest,
			ObservedUnixMillis: receipt.CompletedAt.UnixMilli(),
		},
	})
	decorateRequest(c, ctx, request)
	response, err := c.proof.CommitProofOfServiceEvidence(callCtx, request)
	if err != nil {
		return "", rpcError(err)
	}
	if response.Msg == nil || response.Msg.Evidence == nil {
		return "", domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned empty Proof-of-Service evidence", true)
	}
	ref := reference(response.Msg.Evidence.EvidenceRef)
	if ref != "" {
		c.proofRefs.Store(evidenceID, ref)
	}
	if receipt.TrustMode == domain.TrustModeManaged {
		return "", nil
	}
	return ref, nil
}

func proofOfServiceInput(receipt domain.ExecutionReceipt) (*atostosv1.ProofOfServiceEvidenceInput, error) {
	volume, err := networkAmount(receipt.Cost)
	if err != nil {
		return nil, err
	}
	latency := uint64(0)
	if !receipt.StartedAt.IsZero() && !receipt.CompletedAt.Before(receipt.StartedAt) {
		latency = uint64(receipt.CompletedAt.Sub(receipt.StartedAt) / time.Millisecond)
	}
	evidenceID := "pos_" + receipt.ID
	return &atostosv1.ProofOfServiceEvidenceInput{EvidenceId: evidenceID, ReceiptId: receipt.ID, ProviderId: receipt.ProviderID, CapabilityId: receipt.CapabilityID, CapabilityVersion: receipt.CapabilityVersion, Result: protoExecutionResult(receipt.Result), LatencyMillis: latency, SettlementVolume: volume, EvidenceDigest: textDigest(evidenceID, receipt.ID, receipt.ProviderID, receipt.CapabilityID, receipt.CapabilityVersion, string(receipt.Result), fmt.Sprintf("%d", latency), volume.Asset, volume.AtomicAmount), ObservedUnixMillis: receipt.CompletedAt.UnixMilli()}, nil
}

func (c *Client) ReadProofOfServiceEvidence(ctx context.Context, receipt domain.ExecutionReceipt) (toscore.ProofOfServiceEvidence, bool, error) {
	if receipt.ID == "" || receipt.ProviderID == "" || receipt.CapabilityID == "" {
		return toscore.ProofOfServiceEvidence{}, false, domain.NewError(domain.ErrValidationFailed, "complete receipt tuple is required", false)
	}
	input, err := proofOfServiceInput(receipt)
	if err != nil {
		return toscore.ProofOfServiceEvidence{}, false, err
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ResolveProofOfServiceEvidenceRequest{Context: c.requestContext(ctx, receipt.PrincipalID, "", time.Time{}), Evidence: input})
	decorateRequest(c, ctx, request)
	response, err := c.proof.ResolveProofOfServiceEvidence(callCtx, request)
	if err != nil {
		return toscore.ProofOfServiceEvidence{}, false, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Found {
		return toscore.ProofOfServiceEvidence{}, false, nil
	}
	r := response.Msg.EvidenceRef
	if r == nil || r.Network != c.network || !r.Finalized || r.FinalizedCheckpoint == 0 {
		return toscore.ProofOfServiceEvidence{}, false, domain.NewError(domain.ErrProofVerificationFailed, "Proof-of-Service tuple/finality mismatch", false)
	}
	d := digestString(response.Msg.EvidenceDigest)
	canonical, canonicalErr := poscommitment.Bytes(input)
	if canonicalErr != nil {
		return toscore.ProofOfServiceEvidence{}, false, domain.NewError(domain.ErrProofVerificationFailed, "Proof-of-Service canonical encoding failed", false)
	}
	return toscore.ProofOfServiceEvidence{EvidenceID: input.EvidenceId, ReceiptID: input.ReceiptId, ProviderID: input.ProviderId, CapabilityID: input.CapabilityId, CapabilityVersion: input.CapabilityVersion, ContentDigest: digestString(input.EvidenceDigest), CanonicalCBOR: canonical, CanonicalEvidence: toscore.CanonicalEvidence{Network: r.Network, Reference: r.Reference, Digest: d, Finalized: r.Finalized, FinalizedCheckpoint: r.FinalizedCheckpoint}}, true, nil
}

func (c *Client) ReadSettlementStatus(ctx context.Context, escrowID string) (domain.EscrowStatus, error) {
	if local, localErr := c.store.GetEscrow(ctx, escrowID); localErr == nil && local.TrustMode == domain.TrustModeVerified {
		quote, quoteErr := c.store.GetQuote(ctx, local.QuoteID)
		if quoteErr != nil {
			return "", quoteErr
		}
		live, found, liveErr := c.GetEscrow(ctx, toscore.GetEscrowRequest{Quote: quote, JobID: local.JobID, EscrowID: local.ID, ExpectedEscrowRef: local.NetworkProofRef, ExpectedReservationDigest: local.ReservationDigest})
		if liveErr != nil {
			return "", liveErr
		}
		if !found {
			return "", domain.NewError(domain.ErrNetworkUnavailable, "canonical verified escrow is unavailable", true)
		}
		return live.Status, nil
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.GetEscrowRequest{
		Context: c.requestContext(ctx, "atos-gateway", "", time.Time{}), EscrowId: escrowID,
	})
	decorateRequest(c, ctx, request)
	response, err := c.settlement.GetEscrow(callCtx, request)
	if err != nil {
		return "", rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Found || response.Msg.Escrow == nil {
		return "", domain.NewError(domain.ErrNotFound, "tos-protocol escrow not found", false)
	}
	return domainEscrowStatus(response.Msg.Escrow.State), nil
}

func (c *Client) ReadProof(ctx context.Context, receiptID string) (map[string]any, error) {
	local, err := c.store.GetReceipt(ctx, receiptID)
	if err != nil && err != store.ErrNotFound {
		return nil, err
	}
	if err == nil && local.TrustMode == domain.TrustModeManaged {
		return map[string]any{
			"receipt_id": receiptID, "trust_mode": local.TrustMode,
			"proof_profile": local.ProofProfile, "proof_status": local.ProofStatus,
			"attested": false,
			"note":     "Managed execution has no TOS network proof",
		}, nil
	}
	var proofRef string
	if value, found := c.proofRefs.Load(receiptID); found {
		proofRef, _ = value.(string)
	}
	if proofRef == "" && err == nil {
		proofRef = local.NetworkProofRef
	}
	if proofRef == "" {
		if err == store.ErrNotFound {
			return nil, domain.NewError(domain.ErrNotFound, "proof reference not found", false)
		}
		return map[string]any{
			"receipt_id": receiptID, "trust_mode": local.TrustMode,
			"proof_profile": local.ProofProfile, "proof_status": local.ProofStatus,
			"attested": false,
			"note":     "Managed execution has no TOS network proof",
		}, nil
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ReadProofRequest{
		Context: c.requestContext(ctx, "atos-gateway", "", time.Time{}), ProofRef: proofRef,
	})
	decorateRequest(c, ctx, request)
	response, err := c.proof.ReadProof(callCtx, request)
	if err != nil {
		return nil, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Found {
		return nil, domain.NewError(domain.ErrNotFound, "tos-protocol proof not found", false)
	}
	return map[string]any{
		"proof_ref": response.Msg.ProofRef, "proof_type": response.Msg.ProofType,
		"proof_bytes":  base64.StdEncoding.EncodeToString(response.Msg.ProofBytes),
		"proof_digest": digestString(response.Msg.ProofDigest),
		"network": func() string {
			if response.Msg.NetworkRef == nil {
				return ""
			}
			return response.Msg.NetworkRef.Network
		}(),
		"network_ref": reference(response.Msg.NetworkRef), "attested": true,
	}, nil
}

func domainEscrow(value *atostosv1.Escrow, jobID, capabilityVersion string, settlement domain.SettlementDescriptor) domain.Escrow {
	if value == nil {
		return domain.Escrow{}
	}
	var reviewDeadline *time.Time
	if value.ReviewDeadlineUnixMillis > 0 {
		v := time.UnixMilli(value.ReviewDeadlineUnixMillis).UTC()
		reviewDeadline = &v
	}
	return domain.Escrow{
		ID: value.EscrowId, QuoteID: value.QuoteId, JobID: jobID,
		PrincipalID: value.PrincipalId, ProviderID: value.ProviderId,
		CapabilityID: value.CapabilityId, CapabilityVersion: capabilityVersion,
		TrustMode: domainTrustMode(value.TrustMode), ProofProfile: domainProofProfile(value.ProofProfile),
		Settlement: settlement, Reserved: domainMoney(value.Reserved),
		Status: domainEscrowStatus(value.State), NetworkProofRef: reference(value.EscrowRef), TerminalProofRef: reference(value.TerminalRef),
		QuoteCommitmentDigest: value.QuoteCommitmentDigest, QuoteCommitmentRef: reference(value.QuoteCommitmentRef),
		ReservationDigest: value.ReservationDigest, ReservationActionID: value.ReservationActionId,
		ContractCodeHash: value.ContractCodeHash, Finalized: value.Finalized, FinalizedCheckpoint: value.FinalizedCheckpoint,
		ReleaseReason: value.ReleaseReasonCode, ReleaseDigest: value.ReleaseDigest, ReleaseActionID: value.ReleaseActionId, ReleaseRef: reference(value.ReleaseRef),
		ResultRef: reference(value.ResultRef), ResultDigest: value.ResultDigest, ResultEvidenceDigest: value.ResultEvidenceDigest, ReviewDeadline: reviewDeadline, DisputeDigest: value.DisputeDigest, DisputeRef: reference(value.DisputeRef), DisputeResolutionDigest: value.DisputeResolutionDigest, DisputeOutcome: value.DisputeOutcome,
		CreatedAt: time.UnixMilli(value.CreatedUnixMillis).UTC(),
		ExpiresAt: time.UnixMilli(value.ExpiresUnixMillis).UTC(),
	}
}

func verifiedEscrowTerms(q domain.Quote, jobID string) (*atostosv1.VerifiedEscrowTerms, string, error) {
	if q.TrustMode != domain.TrustModeVerified || q.ProofProfile != domain.ProofProfileTOSVerifiedV1 || q.Commitment == nil || !q.Commitment.Finalized || q.Commitment.FinalizedCheckpoint == 0 {
		return nil, "", domain.NewError(domain.ErrQuoteMismatch, "verified quote has no finalized canonical commitment", false)
	}
	reserve, err := networkAmount(domain.Money{Amount: q.Price.TotalMax, Currency: q.Price.Currency})
	if err != nil {
		return nil, "", err
	}
	subtotal, err := networkAmount(domain.Money{Amount: q.Price.Subtotal, Currency: q.Price.Currency})
	if err != nil {
		return nil, "", err
	}
	fees, err := networkAmount(domain.Money{Amount: q.Price.Fees, Currency: q.Price.Currency})
	if err != nil {
		return nil, "", err
	}
	if reserve.Asset != "TOS" || q.AssetDecimals != 9 || q.Settlement.Backend != domain.SettlementTOS || q.Settlement.ProviderAsset != "TOS" {
		return nil, "", domain.NewError(domain.ErrValidationFailed, "verified escrow requires TOS with 9 atomic decimals", false)
	}
	t := &atostosv1.VerifiedEscrowTerms{Version: escrowcommitment.Version, Canonicalization: escrowcommitment.Canonicalization, NetworkId: q.NetworkID, Domain: q.CommitmentDomain, JobId: jobID, QuoteId: q.ID, QuoteCommitmentDigest: q.Commitment.Digest, QuoteCommitmentRef: parseReference(q.NetworkID, q.Commitment.Reference), PrincipalId: q.PrincipalID, RequesterAgentId: q.RequesterAgentID, ProviderId: q.ProviderID, CapabilityId: q.CapabilityID, CapabilityVersion: q.CapabilityVersion, ManifestDigest: mustDigest(q.ManifestCommitment), OwnershipRef: parseReference(q.NetworkID, q.OwnershipRef), TrustMode: trustMode(q.TrustMode), ProofProfile: proofProfile(q.ProofProfile), Reserve: reserve, AssetDecimals: q.AssetDecimals, SettlementBackend: string(q.Settlement.Backend), SettlementAsset: q.Settlement.ProviderAsset, FundingModel: string(q.Settlement.FundingModel), AcceptanceDeadlineUnixMillis: q.ExpiresAt.UnixMilli(), ExecutionDeadlineUnixMillis: q.ExecutionDeadline.UnixMilli(), EscrowDeadlineUnixMillis: q.ExecutionDeadline.UnixMilli(), UnderlyingServiceQuoteRef: q.ServiceQuoteID, DisputePolicyDigest: mustDigest(q.DisputePolicyHash), SignerAuthorizationId: q.SignerAuthorizationID, SignerAuthorizationRef: parseReference(q.NetworkID, q.SignerAuthorizationRef), Subtotal: subtotal, Fees: fees, TermsDigest: mustDigest(q.TermsHash)}
	t.QuoteCommitmentRef.Finalized = q.Commitment.Finalized
	t.QuoteCommitmentRef.FinalizedCheckpoint = q.Commitment.FinalizedCheckpoint
	t.EscrowId = escrowcommitment.EscrowID(t.NetworkId, t.Domain, t.QuoteId, t.JobId)
	d, err := escrowcommitment.Digest(t)
	if err != nil {
		return nil, "", domain.NewError(domain.ErrValidationFailed, "verified escrow terms cannot be canonicalized", false)
	}
	return t, d, nil
}

func validateCanonicalEscrow(q domain.Quote, jobID, digestValue string, e domain.Escrow) error {
	expectedID := escrowcommitment.EscrowID(q.NetworkID, q.CommitmentDomain, q.ID, jobID)
	legalState := e.Status == domain.EscrowReserved || e.Status == domain.EscrowReleased || e.Status == domain.EscrowSettled
	if e.ID != expectedID || e.JobID != jobID || e.QuoteID != q.ID || e.PrincipalID != q.PrincipalID || e.ProviderID != q.ProviderID || e.CapabilityID != q.CapabilityID || e.CapabilityVersion != q.CapabilityVersion || e.TrustMode != q.TrustMode || e.ProofProfile != q.ProofProfile || e.Reserved != (domain.Money{Amount: q.Price.TotalMax, Currency: q.Price.Currency}) || e.QuoteCommitmentDigest != q.Commitment.Digest || e.QuoteCommitmentRef != q.Commitment.Reference || e.ReservationDigest != digestValue || e.NetworkProofRef == "" || e.ContractCodeHash == "" || !e.Finalized || e.FinalizedCheckpoint == 0 || !legalState {
		return domain.NewError(domain.ErrSettlementFailed, "canonical TaskEscrow does not match the frozen verified Job/Quote", false)
	}
	return nil
}

func domainEscrowStatus(value atostosv1.EscrowState) domain.EscrowStatus {
	switch value {
	case atostosv1.EscrowState_ESCROW_STATE_RESERVED:
		return domain.EscrowReserved
	case atostosv1.EscrowState_ESCROW_STATE_SETTLED:
		return domain.EscrowSettled
	case atostosv1.EscrowState_ESCROW_STATE_RELEASED:
		return domain.EscrowReleased
	case atostosv1.EscrowState_ESCROW_STATE_DISPUTED:
		return domain.EscrowDisputed
	default:
		return ""
	}
}

func domainMoney(value *atostosv1.NetworkAmount) domain.Money {
	if value == nil {
		return domain.Money{}
	}
	decimals := 2
	if value.Asset == "TOS" {
		decimals = 9
	}
	return domain.Money{Amount: atomicToAmount(value.AtomicAmount, decimals), Currency: value.Asset}
}

func textDigest(parts ...string) *atostosv1.Digest {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return &atostosv1.Digest{Algorithm: "sha256", Value: sum[:]}
}
