package toprotocol

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/domain"
	atostosv1 "github.com/tosnetwork/tos-protocol/gen/atos/tos/v1"
	"google.golang.org/protobuf/proto"
)

func (c *Client) SubmitJob(ctx context.Context, req tosai.SubmitJobRequest) (tosai.SubmitJobResult, error) {
	if req.ServiceQuoteID == "" {
		return tosai.SubmitJobResult{}, domain.NewError(domain.ErrQuoteMismatch, "service_quote_id is required by the tos-protocol backend", false)
	}
	if req.ExecutionDeadline.IsZero() {
		return tosai.SubmitJobResult{}, domain.NewError(domain.ErrQuoteMismatch, "execution_deadline is missing from the Quote", false)
	}
	input, err := json.Marshal(req.Input)
	if err != nil {
		return tosai.SubmitJobResult{}, domain.NewError(domain.ErrValidationFailed, "job input is not serializable", false)
	}
	inputDigest, err := digest(req.InputCommitment)
	if err != nil {
		return tosai.SubmitJobResult{}, err
	}
	maxOutput := req.MaxOutputBytes
	if maxOutput == 0 {
		maxOutput = defaultExecutionMaxOutputBytes
	}
	retainUntil := req.RetainUntil
	if retainUntil.IsZero() {
		retainUntil = time.Now().UTC().Add(c.retention)
	}
	if !retainUntil.After(req.ExecutionDeadline) {
		return tosai.SubmitJobResult{}, domain.NewError(domain.ErrQuoteMismatch, "retain_until must be after the execution deadline", false)
	}
	// Binding is the Job's own frozen transport (see
	// tosai.SubmitJobRequest.Binding's doc comment) -- it is threaded
	// through unconditionally, never re-selected here, so tos-protocol
	// executes exactly the binding this Job already committed to, per
	// atos-spec docs/THIRD_PARTY_EXECUTION_PLANE.md.
	thirdPartyBinding, err := thirdPartyBindingProto(req.Binding)
	if err != nil {
		return tosai.SubmitJobResult{}, err
	}
	callCtx, cancel := c.callContext(ctx, req.ExecutionDeadline)
	defer cancel()
	request := connect.NewRequest(&atostosv1.SubmitJobRequest{
		Context: c.requestContext(ctx, req.PrincipalID, "submit-job:"+req.JobID, req.ExecutionDeadline),
		JobId:   req.JobID, InvocationId: req.InvocationID, PrincipalId: req.PrincipalID,
		ProviderId: req.ProviderID, CapabilityId: req.CapabilityID,
		CapabilityVersion: req.CapabilityVersion, QuoteId: req.QuoteID,
		ServiceQuoteId: req.ServiceQuoteID, EscrowId: req.EscrowID,
		TrustMode: trustMode(req.TrustMode), ProofProfile: proofProfile(req.ProofProfile),
		Input: input, InputCommitment: inputDigest, MaxOutputBytes: maxOutput,
		ExecutionDeadlineUnixMillis: req.ExecutionDeadline.UnixMilli(),
		RetainUntilUnixMillis:       retainUntil.UnixMilli(),
		ThirdPartyBinding:           thirdPartyBinding,
	})
	decorateRequest(c, ctx, request)
	response, err := c.execution.SubmitJob(callCtx, request)
	if err != nil {
		return tosai.SubmitJobResult{}, rpcError(err)
	}
	if response.Msg == nil {
		return tosai.SubmitJobResult{}, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned an empty job response", true)
	}
	state := domainJobState(response.Msg.State)
	if state == domain.JobCompleted {
		return c.fetchCompletion(callCtx, ctx, req.JobID)
	}
	if state.Terminal() {
		return tosai.SubmitJobResult{}, c.terminalJobError(callCtx, ctx, req.JobID, state)
	}

	// The current Edge implementation normally returns a terminal response, but
	// polling preserves correctness for an asynchronous implementation without
	// resubmitting the Job.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-callCtx.Done():
			return tosai.SubmitJobResult{}, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol job did not reach a terminal state before the deadline", true)
		case <-ticker.C:
			job, err := c.getProtocolJob(callCtx, ctx, req.JobID)
			if err != nil {
				return tosai.SubmitJobResult{}, err
			}
			state = domainJobState(job.State)
			if state == domain.JobCompleted {
				return c.fetchCompletion(callCtx, ctx, req.JobID)
			}
			if state.Terminal() {
				return tosai.SubmitJobResult{}, c.terminalJobError(callCtx, ctx, req.JobID, state)
			}
		}
	}
}

func (c *Client) terminalJobError(callCtx, originalCtx context.Context, jobID string, state domain.JobState) error {
	job, err := c.getProtocolJob(callCtx, originalCtx, jobID)
	if err != nil {
		return domain.NewError(domain.ErrProviderFailed, fmt.Sprintf("tos-protocol job ended in %s and its terminal record could not be read: %v", state, err), true)
	}
	reason := job.ErrorCode
	if reason == "" {
		reason = "UNSPECIFIED_TERMINAL_FAILURE"
	}
	retryable := job.State == atostosv1.JobState_JOB_STATE_UNCERTAIN
	return domain.NewError(domain.ErrProviderFailed, fmt.Sprintf("tos-protocol job ended in %s: %s", state, reason), retryable)
}

func (c *Client) GetJob(ctx context.Context, jobID string) (tosai.SubmitJobResult, error) {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	job, err := c.getProtocolJob(callCtx, ctx, jobID)
	if err != nil {
		return tosai.SubmitJobResult{}, err
	}
	state := domainJobState(job.State)
	if state == domain.JobCompleted {
		return c.fetchCompletion(callCtx, ctx, jobID)
	}
	return tosai.SubmitJobResult{State: state}, nil
}

func (c *Client) getProtocolJob(callCtx, originalCtx context.Context, jobID string) (*atostosv1.JobRecord, error) {
	request := connect.NewRequest(&atostosv1.GetJobRequest{
		Context: c.requestContext(originalCtx, "atos-gateway", "", time.Time{}), JobId: jobID,
	})
	decorateRequest(c, originalCtx, request)
	response, err := c.execution.GetJob(callCtx, request)
	if err != nil {
		return nil, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Found || response.Msg.Job == nil {
		return nil, domain.NewError(domain.ErrNotFound, "tos-protocol job not found", false)
	}
	return response.Msg.Job, nil
}

func (c *Client) CancelJob(ctx context.Context, jobID, reason string) error {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.CancelJobRequest{
		Context: c.requestContext(ctx, "atos-gateway", "cancel-job:"+jobID, time.Time{}),
		JobId:   jobID, ReasonCode: reason,
	})
	decorateRequest(c, ctx, request)
	response, err := c.execution.CancelJob(callCtx, request)
	if err != nil {
		return rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Accepted {
		return domain.NewError(domain.ErrJobNotCancelable, "tos-protocol did not accept cancellation", false)
	}
	return nil
}

func (c *Client) FetchResult(ctx context.Context, jobID string) (map[string]any, error) {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	response, err := c.fetchResult(callCtx, ctx, jobID)
	if err != nil {
		return nil, err
	}
	return decodeJSONObject(response.Output)
}

func (c *Client) FetchReceipt(ctx context.Context, jobID string) (*domain.ExecutionReceipt, error) {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	return c.fetchReceipt(callCtx, ctx, jobID)
}

func (c *Client) fetchCompletion(callCtx, originalCtx context.Context, jobID string) (tosai.SubmitJobResult, error) {
	result, err := c.fetchResult(callCtx, originalCtx, jobID)
	if err != nil {
		return tosai.SubmitJobResult{}, err
	}
	output, err := decodeJSONObject(result.Output)
	if err != nil {
		return tosai.SubmitJobResult{}, err
	}
	receipt, err := c.fetchReceipt(callCtx, originalCtx, jobID)
	if err != nil {
		return tosai.SubmitJobResult{}, err
	}
	artifacts := make([]domain.Artifact, 0, len(result.Artifacts))
	for _, artifact := range result.Artifacts {
		if artifact == nil {
			continue
		}
		artifacts = append(artifacts, domain.Artifact{
			ID: artifact.ArtifactId, MimeType: artifact.ContentType,
			ContentCommitment: digestString(artifact.Digest),
		})
	}
	return tosai.SubmitJobResult{
		State: domainJobState(result.State), Output: output, Artifacts: artifacts,
		Usage: domainUsage(result.Usage), Receipt: receipt,
	}, nil
}

func (c *Client) fetchResult(callCtx, originalCtx context.Context, jobID string) (*atostosv1.FetchResultResponse, error) {
	request := connect.NewRequest(&atostosv1.FetchResultRequest{
		Context: c.requestContext(originalCtx, "atos-gateway", "", time.Time{}), JobId: jobID,
	})
	decorateRequest(c, originalCtx, request)
	response, err := c.execution.FetchResult(callCtx, request)
	if err != nil {
		return nil, rpcError(err)
	}
	if response.Msg == nil {
		return nil, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned an empty result response", true)
	}
	if domainJobState(response.Msg.State) != domain.JobCompleted {
		return nil, domain.NewError(domain.ErrProviderFailed, "tos-protocol job has no completed result", true)
	}
	return response.Msg, nil
}

func (c *Client) fetchReceipt(callCtx, originalCtx context.Context, jobID string) (*domain.ExecutionReceipt, error) {
	request := connect.NewRequest(&atostosv1.FetchExecutionReceiptRequest{
		Context: c.requestContext(originalCtx, "atos-gateway", "", time.Time{}), JobId: jobID,
	})
	decorateRequest(c, originalCtx, request)
	response, err := c.execution.FetchExecutionReceipt(callCtx, request)
	if err != nil {
		return nil, rpcError(err)
	}
	if response.Msg == nil || len(response.Msg.CanonicalReceipt) == 0 {
		return nil, domain.NewError(domain.ErrSettlementFailed, "tos-protocol returned no execution receipt", true)
	}
	envelope := new(atostosv1.ExecutionReceiptEnvelope)
	if err := proto.Unmarshal(response.Msg.CanonicalReceipt, envelope); err != nil {
		return nil, domain.NewError(domain.ErrSettlementFailed, "tos-protocol returned an invalid execution receipt", false)
	}
	if envelope.ReceiptId == "" || envelope.ReceiptId != response.Msg.ReceiptId || envelope.JobId != jobID {
		return nil, domain.NewError(domain.ErrSettlementFailed, "tos-protocol execution receipt binding mismatch", false)
	}
	c.receipts.Store(envelope.ReceiptId, proto.Clone(envelope).(*atostosv1.ExecutionReceiptEnvelope))
	if response.Msg.ReceiptRef != nil {
		c.proofRefs.Store(envelope.ReceiptId, response.Msg.ReceiptRef.Reference)
	}
	mapped := domainReceipt(envelope)
	mapped.NetworkProofRef = reference(response.Msg.ReceiptRef)
	return &mapped, nil
}

func decodeJSONObject(encoded []byte) (map[string]any, error) {
	if len(encoded) == 0 {
		return map[string]any{}, nil
	}
	var output map[string]any
	if err := json.Unmarshal(encoded, &output); err != nil {
		return nil, domain.NewError(domain.ErrProviderFailed, "provider output is not a JSON object; binary output must be returned as an Artifact", false)
	}
	if output == nil {
		output = map[string]any{}
	}
	return output, nil
}

func domainUsage(value *atostosv1.Usage) domain.Usage {
	if value == nil {
		return domain.Usage{}
	}
	return domain.Usage{
		InputBytes: value.InputBytes, OutputBytes: value.OutputBytes,
		InputTokens: value.InputTokens, OutputTokens: value.OutputTokens,
		ExecutionMillis: value.ExecutionMillis,
	}
}

func domainReceipt(value *atostosv1.ExecutionReceiptEnvelope) domain.ExecutionReceipt {
	if value == nil {
		return domain.ExecutionReceipt{}
	}
	completed := time.UnixMilli(value.CompletedUnixMillis).UTC()
	started := completed
	if value.Usage != nil && value.Usage.ExecutionMillis <= uint64((24*time.Hour)/time.Millisecond) {
		started = completed.Add(-time.Duration(value.Usage.ExecutionMillis) * time.Millisecond)
	}
	artifacts := make([]domain.Artifact, 0, len(value.Artifacts))
	for _, artifact := range value.Artifacts {
		if artifact == nil {
			continue
		}
		artifacts = append(artifacts, domain.Artifact{
			ID: artifact.ArtifactId, MimeType: artifact.ContentType,
			ContentCommitment: digestString(artifact.Digest),
		})
	}
	canonical, _ := (proto.MarshalOptions{Deterministic: true}).Marshal(value)
	return domain.ExecutionReceipt{
		ID: value.ReceiptId, QuoteID: value.QuoteId, EscrowID: value.EscrowId,
		JobID: value.JobId, PrincipalID: value.PrincipalId, ProviderID: value.ProviderId,
		CapabilityID: value.CapabilityId, CapabilityVersion: value.CapabilityVersion,
		TrustMode: domainTrustMode(value.TrustMode), ProofProfile: domainProofProfile(value.ProofProfile),
		Result: executionResult(value.Result), InputHash: digestString(value.InputCommitment),
		OutputHash: digestString(value.OutputCommitment), UsageCommitment: digestString(value.UsageCommitment),
		Artifacts: artifacts, Usage: domainUsage(value.Usage), StartedAt: started, CompletedAt: completed,
		Cost: money(value.ClientCharge), ExecutionSignerID: value.ExecutionSignerId,
		SignerAuthorizationID: value.SignerAuthorizationId,
		SignatureAlgorithm:    value.SignatureAlgorithm,
		Signature:             base64.StdEncoding.EncodeToString(value.Signature),
		CanonicalEnvelope:     base64.StdEncoding.EncodeToString(canonical), ErrorCode: domain.ErrorCode(value.ErrorCode),
	}
}

func (c *Client) executionEnvelope(ctx context.Context, receipt domain.ExecutionReceipt) (*atostosv1.ExecutionReceiptEnvelope, error) {
	if stored, found := c.receipts.Load(receipt.ID); found {
		return proto.Clone(stored.(*atostosv1.ExecutionReceiptEnvelope)).(*atostosv1.ExecutionReceiptEnvelope), nil
	}
	if receipt.CanonicalEnvelope != "" {
		canonical, err := base64.StdEncoding.DecodeString(receipt.CanonicalEnvelope)
		if err != nil {
			return nil, domain.NewError(domain.ErrSettlementFailed, "canonical execution receipt is not valid base64", false)
		}
		envelope := new(atostosv1.ExecutionReceiptEnvelope)
		if err := proto.Unmarshal(canonical, envelope); err != nil || envelope.ReceiptId != receipt.ID || envelope.JobId != receipt.JobID {
			return nil, domain.NewError(domain.ErrSettlementFailed, "canonical execution receipt recovery binding mismatch", false)
		}
		c.receipts.Store(envelope.ReceiptId, proto.Clone(envelope).(*atostosv1.ExecutionReceiptEnvelope))
		return envelope, nil
	}
	// The signed envelope is protocol authority data, not reconstructible from
	// ATOS's convenience projection. Recover it read-only after a restart or
	// cache loss before considering the legacy reconstruction path.
	if receipt.JobID != "" {
		callCtx, cancel := c.callContext(ctx, time.Time{})
		defer cancel()
		if recovered, err := c.fetchReceipt(callCtx, ctx, receipt.JobID); err == nil && recovered != nil {
			if stored, found := c.receipts.Load(recovered.ID); found && recovered.ID == receipt.ID {
				return proto.Clone(stored.(*atostosv1.ExecutionReceiptEnvelope)).(*atostosv1.ExecutionReceiptEnvelope), nil
			}
			return nil, domain.NewError(domain.ErrSettlementFailed, "canonical execution receipt identity mismatch during recovery", false)
		} else if receipt.TrustMode == domain.TrustModeVerified {
			if err != nil {
				return nil, err
			}
			return nil, domain.NewError(domain.ErrNetworkUnavailable, "canonical execution receipt unavailable during recovery", true)
		}
	}
	inputDigest, err := digest(receipt.InputHash)
	if err != nil {
		return nil, err
	}
	outputDigest, err := digest(receipt.OutputHash)
	if err != nil {
		return nil, err
	}
	usageDigest, err := digest(receipt.UsageCommitment)
	if err != nil {
		return nil, err
	}
	signature, err := base64.StdEncoding.DecodeString(receipt.Signature)
	if err != nil {
		return nil, domain.NewError(domain.ErrSettlementFailed, "execution receipt signature is not valid base64", false)
	}
	return &atostosv1.ExecutionReceiptEnvelope{
		ReceiptId: receipt.ID, QuoteId: receipt.QuoteID, EscrowId: receipt.EscrowID,
		JobId: receipt.JobID, PrincipalId: receipt.PrincipalID, ProviderId: receipt.ProviderID,
		CapabilityId: receipt.CapabilityID, CapabilityVersion: receipt.CapabilityVersion,
		TrustMode: trustMode(receipt.TrustMode), ProofProfile: proofProfile(receipt.ProofProfile),
		Result: protoExecutionResult(receipt.Result), InputCommitment: inputDigest,
		OutputCommitment: outputDigest, UsageCommitment: usageDigest,
		Usage: &atostosv1.Usage{
			InputBytes: receipt.Usage.InputBytes, OutputBytes: receipt.Usage.OutputBytes,
			InputTokens: receipt.Usage.InputTokens, OutputTokens: receipt.Usage.OutputTokens,
			ExecutionMillis: receipt.Usage.ExecutionMillis,
		},
		ExecutionSignerId:     receipt.ExecutionSignerID,
		SignerAuthorizationId: receipt.SignerAuthorizationID,
		Signature:             signature, SignatureAlgorithm: receipt.SignatureAlgorithm,
		CompletedUnixMillis: receipt.CompletedAt.UnixMilli(), ErrorCode: string(receipt.ErrorCode),
	}, nil
}
