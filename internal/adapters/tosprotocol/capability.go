package toprotocol

import (
	"context"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/domain"
	atostosv1 "github.com/tosnetwork/tos-protocol/gen/atos/tos/v1"
)

func (c *Client) RegisterProvider(ctx context.Context, providerID string, capability domain.Capability) error {
	if providerID == "" || providerID != capability.ProviderID {
		return domain.NewError(domain.ErrValidationFailed, "provider_id does not match capability owner", false)
	}
	_, err := c.commitCapability(ctx, capability)
	return err
}

func (c *Client) commitCapability(ctx context.Context, capability domain.Capability) (*atostosv1.CapabilityIdentity, error) {
	manifest, err := digest(capability.ManifestCommitment)
	if err != nil {
		return nil, err
	}
	modes := make([]atostosv1.TrustMode, 0, len(capability.RequestedTrustModes))
	for _, mode := range capability.RequestedTrustModes {
		mapped := trustMode(mode)
		if mapped != atostosv1.TrustMode_TRUST_MODE_UNSPECIFIED {
			modes = append(modes, mapped)
		}
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.CommitCapabilityManifestRequest{
		Context:      c.requestContext(ctx, capability.ProviderID, "capability-manifest:"+capability.ID+":"+capability.Version, time.Time{}),
		CapabilityId: capability.ID, ProviderId: capability.ProviderID,
		Version: capability.Version, ManifestDigest: manifest, RequestedTrustModes: modes,
	})
	decorateRequest(c, ctx, request)
	response, err := c.capability.CommitCapabilityManifest(callCtx, request)
	if err != nil {
		return nil, rpcError(err)
	}
	if response.Msg == nil || response.Msg.Capability == nil {
		return nil, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned an empty capability commitment", true)
	}
	return response.Msg.Capability, nil
}

func (c *Client) QuoteExecution(ctx context.Context, req tosai.QuoteExecutionRequest) (tosai.ServiceExecutionQuote, error) {
	if req.Capability.ID == "" || req.Capability.ProviderID == "" || req.Capability.Version == "" {
		return tosai.ServiceExecutionQuote{}, domain.NewError(domain.ErrValidationFailed, "capability identity is incomplete", false)
	}
	if err := requireFuture(req.ExecutionDeadline, "execution_deadline"); err != nil {
		return tosai.ServiceExecutionQuote{}, err
	}
	identity, err := c.commitCapability(ctx, req.Capability)
	if err != nil {
		return tosai.ServiceExecutionQuote{}, err
	}
	requestedMode := trustMode(req.TrustMode)
	active := false
	for _, mode := range identity.ActiveTrustModes {
		if mode == requestedMode {
			active = true
			break
		}
	}
	if !active {
		return tosai.ServiceExecutionQuote{}, domain.NewError(domain.ErrTrustModeUnavailable, fmt.Sprintf("tos-protocol has not activated %s for capability %s", req.TrustMode, req.Capability.ID), true)
	}
	inputCommitment, err := digest(req.InputCommitment)
	if err != nil {
		return tosai.ServiceExecutionQuote{}, err
	}
	// The binding selected here is what QuoteExecution asks tos-protocol to
	// anchor a ServiceExecutionQuote against; JobService.submit freezes its
	// own independent domain.SelectBinding result onto the committed Job
	// (see domain.Job.Binding's doc comment) rather than re-deriving it
	// from this quote. If a live Capability update lands in between and the
	// two disagree, tos-protocol's SubmitJob rejects the mismatch
	// (QUOTE_MISMATCH) rather than silently executing against a binding
	// nobody actually committed to.
	selectedBinding, hasBinding := domain.SelectBinding(req.Capability.Bindings, req.TrustMode)
	var thirdPartyBinding *atostosv1.ThirdPartyBinding
	if hasBinding {
		thirdPartyBinding, err = thirdPartyBindingProto(&selectedBinding)
		if err != nil {
			return tosai.ServiceExecutionQuote{}, err
		}
	}
	callCtx, cancel := c.callContext(ctx, req.ExecutionDeadline)
	defer cancel()
	request := connect.NewRequest(&atostosv1.QuoteExecutionRequest{
		Context:    c.requestContext(ctx, req.Capability.ProviderID, "", req.ExecutionDeadline),
		ProviderId: req.Capability.ProviderID, CapabilityId: req.Capability.ID,
		CapabilityVersion: req.Capability.Version, InputCommitment: inputCommitment,
		InputBytes: req.InputBytes, MaxOutputBytes: req.MaxOutputBytes,
		ExecutionDeadlineUnixMillis: req.ExecutionDeadline.UnixMilli(),
		IntendedTrustMode:           requestedMode, IntendedProofProfile: proofProfile(req.ProofProfile),
		ThirdPartyBinding: thirdPartyBinding,
	})
	decorateRequest(c, ctx, request)
	response, err := c.execution.QuoteExecution(callCtx, request)
	if err != nil {
		return tosai.ServiceExecutionQuote{}, rpcError(err)
	}
	if response.Msg == nil || response.Msg.Quote == nil {
		return tosai.ServiceExecutionQuote{}, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned an empty service execution quote", true)
	}
	quote := response.Msg.Quote
	providerPrice := domain.Money{}
	if quote.ProviderPrice != nil {
		providerPrice = domain.Money{Amount: quote.ProviderPrice.AtomicAmount, Currency: quote.ProviderPrice.Asset}
	}
	return tosai.ServiceExecutionQuote{
		ID: quote.ServiceQuoteId, Reference: reference(quote.QuoteRef),
		ProviderPrice:     providerPrice,
		ExpiresAt:         time.UnixMilli(quote.ExpiresUnixMillis).UTC(),
		ExecutionDeadline: time.UnixMilli(quote.ExecutionDeadlineUnixMillis).UTC(),
		CapacityRevision:  quote.CapacityRevision, RuntimeRevision: quote.RuntimeRevision,
		ModelRevision: quote.ModelRevision, SignedDigest: digestString(quote.SignedQuoteDigest),
	}, nil
}

func (c *Client) GetProviderStatus(ctx context.Context, providerID string) (bool, error) {
	// The current Provider interface predates capability-scoped readiness. A
	// health probe is the only non-lossy answer at provider-only granularity;
	// QuoteExecution performs the authoritative capability/route check.
	if providerID == "" {
		return false, domain.NewError(domain.ErrValidationFailed, "provider_id is required", false)
	}
	if err := c.CheckReady(ctx); err != nil {
		return false, err
	}
	return true, nil
}
