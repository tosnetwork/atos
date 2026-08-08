package toprotocol

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
	atostosv1 "github.com/tosnetwork/tos-protocol/gen/atos/tos/v1"
)

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

func (c *Client) VerifyCapabilityOwnership(ctx context.Context, capabilityID, providerID string) (bool, error) {
	if capabilityID == "" || providerID == "" {
		return false, domain.NewError(domain.ErrValidationFailed, "capability_id and provider_id are required", false)
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.VerifyCapabilityOwnershipRequest{
		Context:      c.requestContext(ctx, providerID, "", time.Time{}),
		CapabilityId: capabilityID, ProviderId: providerID,
	})
	decorateRequest(c, ctx, request)
	response, err := c.capability.VerifyCapabilityOwnership(callCtx, request)
	if err != nil {
		return false, rpcError(err)
	}
	return response.Msg != nil && response.Msg.Verified, nil
}

func (c *Client) UpdateReputationEvidence(context.Context, string, string) error {
	return domain.NewError(domain.ErrValidationFailed, "direct reputation evidence is unsupported; commit verified Proof-of-Service evidence instead", false)
}

func (c *Client) CommitQuote(ctx context.Context, quote domain.Quote) (string, error) {
	if quote.PrincipalID == "" {
		return "", domain.NewError(domain.ErrQuoteMismatch, "quote principal_id is required by the tos-protocol backend", false)
	}
	terms, err := digest(quote.TermsHash)
	if err != nil {
		return "", err
	}
	var dispute *atostosv1.Digest
	if quote.DisputePolicyHash != "" {
		dispute, err = digest(quote.DisputePolicyHash)
		if err != nil {
			return "", err
		}
	}
	underlying := quote.UnderlyingServiceQuoteRef
	if underlying == "" {
		underlying = quote.ServiceQuoteID
	}
	settlementAsset := quote.Settlement.ProviderAsset
	if settlementAsset == "" {
		settlementAsset = quote.Settlement.ClientAsset
	}
	if settlementAsset == "" {
		settlementAsset = quote.Price.Currency
	}
	callCtx, cancel := c.callContext(ctx, quote.ExpiresAt)
	defer cancel()
	request := connect.NewRequest(&atostosv1.CommitQuoteRequest{
		Context: c.requestContext(ctx, quote.PrincipalID, "commit-quote:"+quote.ID, quote.ExpiresAt),
		Quote: &atostosv1.QuoteCommitmentInput{
			QuoteId: quote.ID, PrincipalId: quote.PrincipalID,
			ProviderId: quote.ProviderID, CapabilityId: quote.CapabilityID,
			CapabilityVersion: quote.CapabilityVersion,
			TrustMode:         trustMode(quote.TrustMode), ProofProfile: proofProfile(quote.ProofProfile),
			TotalMax:    &atostosv1.Money{Amount: quote.Price.TotalMax, Currency: quote.Price.Currency},
			TermsDigest: terms, DisputePolicyDigest: dispute,
			ExpiresUnixMillis: quote.ExpiresAt.UnixMilli(),
			SettlementBackend: string(quote.Settlement.Backend), SettlementAsset: settlementAsset,
			UnderlyingServiceQuoteRef: underlying,
		},
	})
	decorateRequest(c, ctx, request)
	response, err := c.trust.CommitQuote(callCtx, request)
	if err != nil {
		return "", rpcError(err)
	}
	if response.Msg == nil || response.Msg.Quote == nil {
		return "", domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned an empty quote commitment", true)
	}
	ref := reference(response.Msg.Quote.CommitmentRef)
	if ref != "" {
		c.proofRefs.Store(quote.ID, ref)
	}
	if quote.TrustMode == domain.TrustModeManaged {
		return "", nil
	}
	return ref, nil
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
	}, true, nil
}

func (c *Client) CreateEscrow(ctx context.Context, req toscore.CreateEscrowRequest) (domain.Escrow, error) {
	reserve, err := networkAmount(req.Reserved)
	if err != nil {
		return domain.Escrow{}, err
	}
	expires := time.Now().UTC().Add(c.retention)
	if quote, qerr := c.store.GetQuote(ctx, req.QuoteID); qerr == nil && quote.ExpiresAt.After(time.Now().UTC()) {
		expires = quote.ExpiresAt
	}
	callCtx, cancel := c.callContext(ctx, expires)
	defer cancel()
	request := connect.NewRequest(&atostosv1.CreateEscrowRequest{
		Context: c.requestContext(ctx, req.PrincipalID, "create-escrow:"+req.QuoteID+":"+req.JobID, expires),
		QuoteId: req.QuoteID, PrincipalId: req.PrincipalID,
		ProviderId: req.ProviderID, CapabilityId: req.CapabilityID,
		TrustMode: trustMode(req.TrustMode), ProofProfile: proofProfile(req.ProofProfile),
		Reserve: reserve, FundingModel: string(req.Settlement.FundingModel),
		ExpiresUnixMillis: expires.UnixMilli(),
	})
	decorateRequest(c, ctx, request)
	response, err := c.settlement.CreateEscrow(callCtx, request)
	if err != nil {
		return domain.Escrow{}, rpcError(err)
	}
	if response.Msg == nil || response.Msg.Escrow == nil {
		return domain.Escrow{}, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned an empty escrow", true)
	}
	mapped := domainEscrow(response.Msg.Escrow, req.JobID, req.CapabilityVersion, req.Settlement)
	if mapped.TrustMode == domain.TrustModeManaged {
		mapped.NetworkProofRef = ""
	}
	if err := c.store.PutEscrow(ctx, mapped); err != nil {
		return domain.Escrow{}, err
	}
	if ref := reference(response.Msg.Escrow.EscrowRef); ref != "" {
		c.proofRefs.Store(mapped.ID, ref)
	}
	return mapped, nil
}

func (c *Client) ReleaseEscrow(ctx context.Context, escrowID string) (domain.Receipt, error) {
	local, err := c.store.GetEscrow(ctx, escrowID)
	if err != nil {
		return domain.Receipt{}, err
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ReleaseEscrowRequest{
		Context:  c.requestContext(ctx, local.PrincipalID, "release-escrow:"+escrowID, time.Time{}),
		EscrowId: escrowID, QuoteId: local.QuoteID, JobId: local.JobID,
		ReasonCode: "ATOS_RELEASE",
	})
	decorateRequest(c, ctx, request)
	response, err := c.settlement.ReleaseEscrow(callCtx, request)
	if err != nil {
		return domain.Receipt{}, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Released || response.Msg.Escrow == nil {
		return domain.Receipt{}, domain.NewError(domain.ErrSettlementFailed, "tos-protocol did not release escrow", true)
	}
	now := time.Now().UTC()
	local.Status = domain.EscrowReleased
	local.SettledAt = &now
	if local.TrustMode != domain.TrustModeManaged {
		local.NetworkProofRef = reference(response.Msg.ReleaseRef)
	}
	if err := c.store.PutEscrow(ctx, local); err != nil {
		return domain.Receipt{}, err
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
	}
	if err := c.store.PutReceipt(ctx, receipt); err != nil {
		return domain.Receipt{}, err
	}
	if receipt.NetworkProofRef != "" {
		c.proofRefs.Store(receipt.ID, receipt.NetworkProofRef)
	}
	return receipt, nil
}

func (c *Client) CommitExecutionReceipt(ctx context.Context, receipt domain.ExecutionReceipt) (string, error) {
	envelope, err := c.executionEnvelope(receipt)
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
	envelope, err := c.executionEnvelope(receipt)
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
	}, nil
}

func (c *Client) SettleJob(ctx context.Context, req toscore.SettleJobRequest) (toscore.SettleJobResult, error) {
	local, err := c.store.GetEscrow(ctx, req.EscrowID)
	if err != nil {
		return toscore.SettleJobResult{}, err
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
	decorateRequest(c, ctx, request)
	response, err := c.settlement.SettleJob(callCtx, request)
	if err != nil {
		return toscore.SettleJobResult{}, rpcError(err)
	}
	if response.Msg == nil || response.Msg.Settlement == nil || response.Msg.Escrow == nil {
		return toscore.SettleJobResult{}, domain.NewError(domain.ErrSettlementFailed, "tos-protocol returned an incomplete settlement", true)
	}
	now := time.UnixMilli(response.Msg.Settlement.SettledUnixMillis).UTC()
	local.Status = domain.EscrowSettled
	local.SettledAt = &now
	if local.TrustMode != domain.TrustModeManaged {
		local.NetworkProofRef = reference(response.Msg.Settlement.SettlementRef)
	}
	if err := c.store.PutEscrow(ctx, local); err != nil {
		return toscore.SettleJobResult{}, err
	}
	proofStatus := domain.ProofNotRequired
	if local.TrustMode != domain.TrustModeManaged {
		proofStatus = domain.ProofVerified
	}
	settlement := response.Msg.Settlement
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

func (c *Client) ReadSettlementStatus(ctx context.Context, escrowID string) (domain.EscrowStatus, error) {
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
	return domain.Escrow{
		ID: value.EscrowId, QuoteID: value.QuoteId, JobID: jobID,
		PrincipalID: value.PrincipalId, ProviderID: value.ProviderId,
		CapabilityID: value.CapabilityId, CapabilityVersion: capabilityVersion,
		TrustMode: domainTrustMode(value.TrustMode), ProofProfile: domainProofProfile(value.ProofProfile),
		Settlement: settlement, Reserved: domainMoney(value.Reserved),
		Status: domainEscrowStatus(value.State), NetworkProofRef: reference(value.EscrowRef),
		CreatedAt: time.UnixMilli(value.CreatedUnixMillis).UTC(),
		ExpiresAt: time.UnixMilli(value.ExpiresUnixMillis).UTC(),
	}
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
	return domain.Money{Amount: atomicToAmount(value.AtomicAmount), Currency: value.Asset}
}

func textDigest(parts ...string) *atostosv1.Digest {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return &atostosv1.Digest{Algorithm: "sha256", Value: sum[:]}
}
