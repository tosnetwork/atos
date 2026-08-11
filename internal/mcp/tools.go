package mcp

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type toolHandler func(ctx context.Context, principal auth.Principal, args map[string]any) (any, error)

func (s *Server) dispatch() map[string]toolHandler {
	return map[string]toolHandler{
		"atos_search":                      s.toolSearch,
		"atos_get_capability":              s.toolGetCapability,
		"atos_quote":                       s.toolQuote,
		"atos_invoke":                      s.toolInvoke,
		"atos_create_job":                  s.toolCreateJob,
		"atos_get_job":                     s.toolGetJob,
		"atos_cancel_job":                  s.toolCancelJob,
		"atos_account":                     s.toolAccount,
		"atos_artifact":                    s.toolArtifact,
		"atos_register_capability":         s.toolRegisterCapability,
		"atos_update_capability":           s.toolUpdateCapability,
		"atos_list_my_capabilities":        s.toolListMyCapabilities,
		"atos_pause_capability":            s.toolPauseCapability,
		"atos_provider_earnings":           s.toolProviderEarnings,
		"atos_provider_jobs":               s.toolProviderJobs,
		"atos_deliver_job":                 s.toolDeliverJob,
		"atos_request_settlement":          s.toolRequestSettlement,
		"atos_dispute_job":                 s.toolDisputeJob,
		"atos_authorize_execution_signer":  s.toolAuthorizeExecutionSigner,
		"atos_rotate_execution_signer":     s.toolRotateExecutionSigner,
		"atos_revoke_execution_signer":     s.toolRevokeExecutionSigner,
		"atos_get_execution_signer_status": s.toolGetExecutionSignerStatus,
		"atos_evaluate_activation":         s.toolEvaluateActivation,
		"atos_bind_identity":               s.toolBindIdentity,
		"atos_revoke_identity":             s.toolRevokeIdentity,
		"atos_identity_binding_status":     s.toolIdentityBindingStatus,
		"atos_open_certification":          s.toolOpenCertification,
		"atos_get_certification_status":    s.toolGetCertificationStatus,
		"atos_publish_open_task":           s.toolPublishOpenTask,
		"atos_search_open_tasks":           s.toolSearchOpenTasks,
		"atos_get_open_task":               s.toolGetOpenTask,
		"atos_apply_to_open_task":          s.toolApplyToOpenTask,
		"atos_list_open_task_proposals":    s.toolListOpenTaskProposals,
		"atos_withdraw_open_task_proposal": s.toolWithdrawOpenTaskProposal,
		"atos_accept_open_task_proposal":   s.toolAcceptOpenTaskProposal,
		"atos_cancel_open_task":            s.toolCancelOpenTask,
	}
}

func argString(args map[string]any, key string) string {
	value, _ := args[key].(string)
	return value
}

func argInt64(args map[string]any, key string) int64 {
	switch value := args[key].(type) {
	case float64:
		return int64(value)
	case int64:
		return value
	case json.Number:
		parsed, _ := value.Int64()
		return parsed
	default:
		return 0
	}
}

func argObject(args map[string]any, key string) map[string]any {
	value, _ := args[key].(map[string]any)
	return value
}

func decodeValue(value any, target any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, target)
}

func (s *Server) toolSearch(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	input := service.SearchInput{Query: argString(args, "query"), Limit: int(argInt64(args, "limit"))}
	if filters := argObject(args, "filters"); filters != nil {
		if raw, ok := filters["max_price"].(map[string]any); ok {
			amount, _ := raw["amount"].(string)
			currency, _ := raw["currency"].(string)
			if amount != "" {
				input.Filters.MaxPrice = &domain.Money{Amount: amount, Currency: currency}
			}
		}
		if raw, ok := filters["min_trust_score"].(float64); ok {
			input.Filters.MinTrustScore = &raw
		}
		if raw, ok := filters["max_latency_ms"].(float64); ok {
			value := int64(raw)
			input.Filters.MaxLatencyMS = &value
		}
		if raw, ok := filters["delivery_modes"].([]any); ok {
			for _, value := range raw {
				if mode, ok := value.(string); ok {
					input.Filters.DeliveryModes = append(input.Filters.DeliveryModes, domain.DeliveryMode(mode))
				}
			}
		}
		input.Filters.RequestedTrustMode = domain.RequestedTrustMode(argString(filters, "requested_trust_mode"))
		if raw := filters["proof_requirements"]; raw != nil {
			if err := decodeValue(raw, &input.Filters.ProofRequirements); err != nil {
				return nil, domain.NewError(domain.ErrValidationFailed, "invalid proof_requirements", false)
			}
		}
	}
	caps, err := s.Capabilities.Search(ctx, input)
	if err != nil {
		return nil, err
	}
	return map[string]any{"matches": caps, "next_cursor": nil}, nil
}

func (s *Server) toolGetCapability(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	return service.GetCapabilityWithReadiness(ctx, s.Capabilities, s.Health, s.ExecutionSigners, argString(args, "capability_id"))
}

func (s *Server) toolQuote(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	input := service.CreateQuoteInput{
		PrincipalID:        principal.ID,
		CapabilityID:       argString(args, "capability_id"),
		InputSummary:       argObject(args, "input_summary"),
		RequestedTrustMode: domain.RequestedTrustMode(argString(args, "requested_trust_mode")),
		IdempotencyKey:     argString(args, "idempotency_key"),
	}
	if raw := args["proof_requirements"]; raw != nil {
		if err := decodeValue(raw, &input.ProofRequirements); err != nil {
			return nil, domain.NewError(domain.ErrValidationFailed, "invalid proof_requirements", false)
		}
	}
	if constraints := argObject(args, "constraints"); constraints != nil {
		if maxTotal, ok := constraints["max_total"].(map[string]any); ok {
			amount, _ := maxTotal["amount"].(string)
			currency, _ := maxTotal["currency"].(string)
			if amount != "" {
				input.MaxTotal = &domain.Money{Amount: amount, Currency: currency}
			}
		}
	}
	quote, err := s.Quotes.Create(ctx, input)
	if err != nil {
		return nil, err
	}
	return quote.Public(), nil
}

func (s *Server) toolInvoke(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	result, err := s.Jobs.Invoke(ctx, service.SubmitInput{
		PrincipalID: principal.ID, CapabilityID: argString(args, "capability_id"),
		QuoteID: argString(args, "quote_id"), Input: argObject(args, "input"),
		IdempotencyKey: argString(args, "idempotency_key"), MaxWaitMS: argInt64(args, "max_wait_ms"),
	})
	if err != nil {
		return nil, err
	}
	var receipt any
	if result.Job.State.Terminal() {
		if found, receiptErr := s.Receipts.ByJob(ctx, result.Job.ID, principal.ID); receiptErr == nil {
			receipt = found
		}
	}
	response := map[string]any{
		"result_type":   result.Type,
		"invocation_id": nullableString(result.Job.InvocationID),
		"job_id":        nullableString(result.Job.ID),
		"quote_id":      result.Job.QuoteID,
		"trust_mode":    result.Job.TrustMode,
		"proof_profile": nullableString(string(result.Job.ProofProfile)),
		"output":        result.Job.Output,
		"artifacts":     result.Job.Artifacts,
		"receipt":       receipt,
	}
	if result.Job.Confirmation != nil {
		response["confirmation"] = result.Job.Confirmation
		response["confirmation_uri"] = s.confirmationURI(result.Job.Confirmation.UserCode)
	}
	return response, nil
}

func (s *Server) toolCreateJob(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	result, err := s.Jobs.CreateJob(ctx, service.SubmitInput{
		PrincipalID: principal.ID, CapabilityID: argString(args, "capability_id"),
		QuoteID: argString(args, "quote_id"), Input: argObject(args, "input"),
		IdempotencyKey: argString(args, "idempotency_key"),
	})
	if err != nil {
		return nil, err
	}
	response := mapFrom(result.Job)
	response["result_type"] = result.Type
	if result.Job.Confirmation != nil {
		response["confirmation_uri"] = s.confirmationURI(result.Job.Confirmation.UserCode)
	}
	if result.Job.ID != "" {
		response["stream_url"] = s.jobStreamURI(result.Job.ID)
	}
	return response, nil
}

func (s *Server) toolGetJob(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	job, err := s.Jobs.Get(ctx, argString(args, "job_id"))
	if err != nil {
		return nil, err
	}
	if job.PrincipalID != principal.ID {
		return nil, domain.NewError(domain.ErrPermissionDenied, "not the job's owning principal", false)
	}
	response := mapFrom(job)
	if receipt, receiptErr := s.Receipts.ByJob(ctx, job.ID, principal.ID); receiptErr == nil {
		response["receipt"] = receipt
	}
	response["stream_url"] = s.jobStreamURI(job.ID)
	return response, nil
}

func (s *Server) toolCancelJob(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	return s.Jobs.Cancel(ctx, argString(args, "job_id"), principal.ID, argString(args, "reason"), argString(args, "idempotency_key"))
}

func (s *Server) toolAccount(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	return s.Accounts.Get(ctx, principal.ID)
}

func (s *Server) toolRegisterCapability(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	var pricing domain.Pricing
	if err := decodeValue(args["pricing"], &pricing); err != nil {
		return nil, domain.NewError(domain.ErrValidationFailed, "invalid pricing", false)
	}
	var tags []string
	_ = decodeValue(args["tags"], &tags)
	var requested []domain.TrustMode
	if err := decodeValue(args["requested_trust_modes"], &requested); err != nil {
		return nil, domain.NewError(domain.ErrValidationFailed, "invalid requested_trust_modes", false)
	}
	var bindings []domain.CapabilityBinding
	if args["bindings"] != nil {
		if err := decodeValue(args["bindings"], &bindings); err != nil {
			return nil, domain.NewError(domain.ErrValidationFailed, "invalid bindings", false)
		}
	}
	return s.Capabilities.Register(ctx, service.RegisterCapabilityInput{
		ProviderID: principal.ID, Name: argString(args, "name"), Description: argString(args, "description"),
		DeliveryMode: domain.DeliveryMode(argString(args, "delivery_mode")), InputSchema: argObject(args, "input_schema"),
		OutputSchema: argObject(args, "output_schema"), Pricing: pricing, Tags: tags,
		RequestedTrustModes: requested, Bindings: bindings,
		IdempotencyKey: argString(args, "idempotency_key"),
	})
}

func (s *Server) toolUpdateCapability(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	patch := argObject(args, "patch")
	if patch == nil {
		return nil, domain.NewError(domain.ErrValidationFailed, "patch is required", false)
	}
	return s.Capabilities.Update(
		ctx, argString(args, "capability_id"), principal.ID, patch,
		argString(args, "idempotency_key"),
	)
}

func (s *Server) toolListMyCapabilities(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	caps, err := s.Capabilities.ListByProvider(ctx, principal.ID)
	if err != nil {
		return nil, err
	}
	return map[string]any{"capabilities": caps}, nil
}

func (s *Server) toolProviderEarnings(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	if id := argString(args, "earning_id"); id != "" {
		return s.Earnings.Get(ctx, id, principal.ID)
	}
	earnings, err := s.Earnings.ListByProvider(ctx, principal.ID)
	if err != nil {
		return nil, err
	}
	return map[string]any{"earnings": earnings}, nil
}

func (s *Server) toolProviderJobs(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	if id := argString(args, "job_id"); id != "" {
		job, err := s.Jobs.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if job.ProviderID != principal.ID {
			return nil, domain.NewError(domain.ErrPermissionDenied, "not the job's owning provider", false)
		}
		return job, nil
	}
	jobs, err := s.Jobs.ListByProvider(ctx, principal.ID)
	if err != nil {
		return nil, err
	}
	return map[string]any{"jobs": jobs}, nil
}

func (s *Server) toolDeliverJob(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	output, _ := args["output"].(map[string]any)
	return s.Jobs.DeliverResult(ctx, service.DeliverResultInput{
		JobID: argString(args, "job_id"), ProviderID: principal.ID,
		Output: output, IdempotencyKey: argString(args, "idempotency_key"),
	})
}

// toolRequestSettlement is a thin facade over JobService.ReconcileJob --
// the same durable-state-driven economic reconciliation entry point the
// reconciler itself uses -- never a second settlement engine. It resolves
// every economic fact from the Job's own already-durable record; the only
// caller-supplied value is which Job to reconcile.
func (s *Server) toolRequestSettlement(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	jobID := argString(args, "job_id")
	if jobID == "" {
		return nil, domain.NewError(domain.ErrValidationFailed, "job_id is required", false)
	}
	job, err := s.Jobs.Get(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if job.ProviderID != principal.ID {
		return nil, domain.NewError(domain.ErrPermissionDenied, "not the job's owning provider", false)
	}
	return s.Jobs.ReconcileJob(ctx, jobID)
}

// toolDisputeJob is a thin, operation-discriminated facade over the
// existing Phase 2C DisputeService -- no parallel dispute state machine.
// ReviewerID always comes from the authenticated principal, never request
// JSON, so every Phase 2C invariant (parties cannot review their own
// dispute, exclusive reviewer claim, honest clawback_required, atomic
// resolution, ...) is preserved automatically by delegating to the exact
// same methods the REST surface calls.
func (s *Server) toolDisputeJob(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	disputeID := argString(args, "dispute_id")
	if disputeID == "" {
		return nil, domain.NewError(domain.ErrValidationFailed, "dispute_id is required", false)
	}
	switch operation := argString(args, "operation"); operation {
	case "review":
		return s.Disputes.Review(ctx, disputeID, principal.ID)
	case "resolve":
		outcome := domain.DisputeOutcome(argString(args, "outcome"))
		switch outcome {
		case domain.DisputeOutcomePrincipal, domain.DisputeOutcomeProvider, domain.DisputeOutcomeRejected:
		default:
			return nil, domain.NewError(domain.ErrValidationFailed, "outcome must be principal, provider or rejected", false)
		}
		return s.Disputes.Resolve(ctx, service.ResolveDisputeInput{
			DisputeID: disputeID, ReviewerID: principal.ID, Outcome: outcome,
			ReasonRejected: argString(args, "reason_rejected"),
		})
	default:
		return nil, domain.NewError(domain.ErrValidationFailed, "operation must be review or resolve", false)
	}
}

func (s *Server) toolPauseCapability(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	return s.Capabilities.Pause(
		ctx, argString(args, "capability_id"), principal.ID,
		argString(args, "idempotency_key"),
	)
}

func (s *Server) toolArtifact(ctx context.Context, principal auth.Principal, args map[string]any) (any, error) {
	switch operation := argString(args, "operation"); operation {
	case "create_upload":
		purpose := argString(args, "purpose")
		switch purpose {
		case "job_input":
			if !principal.HasAny(auth.ScopeInvocationsCreate, auth.ScopeJobsCreate) {
				return nil, domain.NewError(domain.ErrPermissionDenied, "job_input upload requires invocations:create or jobs:create", false)
			}
		case "capability_asset":
			if !principal.Has(auth.ScopeCapabilitiesWrite) {
				return nil, domain.NewError(domain.ErrPermissionDenied, "capability_asset upload requires capabilities:write", false)
			}
		default:
			return nil, domain.NewError(domain.ErrValidationFailed, "purpose must be job_input or capability_asset", false)
		}
		target, err := s.Artifacts.CreateUpload(ctx, service.CreateUploadInput{
			PrincipalID: principal.ID, ContentType: argString(args, "content_type"),
			SizeBytes: argInt64(args, "size_bytes"), Purpose: purpose,
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"operation": operation, "upload_id": target.UploadID,
			"upload_url": target.UploadURL, "upload_method": target.UploadMethod,
			"expires_at": target.ExpiresAt,
		}, nil
	case "complete_upload":
		artifact, err := s.Artifacts.CompleteUpload(ctx, principal.ID, argString(args, "upload_id"))
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"operation": operation, "artifact_id": artifact.ID,
			"content_type": artifact.ContentType, "size_bytes": artifact.SizeBytes,
			"sha256": artifact.SHA256,
		}, nil
	case "get_download_url":
		target, err := s.Artifacts.GetDownloadURL(ctx, principal.ID, argString(args, "artifact_id"))
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"operation": operation, "download_url": target.DownloadURL,
			"expires_at": target.ExpiresAt, "content_type": target.ContentType,
			"size_bytes": target.SizeBytes,
		}, nil
	default:
		return nil, domain.NewError(domain.ErrValidationFailed, "unknown artifact operation", false)
	}
}

func mapFrom(value any) map[string]any {
	encoded, _ := json.Marshal(value)
	var out map[string]any
	_ = json.Unmarshal(encoded, &out)
	return out
}

func (s *Server) confirmationURI(code string) string {
	base := strings.TrimRight(s.PublicBaseURL, "/")
	if base == "" {
		base = "http://localhost:8080"
	}
	return base + "/confirm?code=" + url.QueryEscape(code)
}

func (s *Server) jobStreamURI(jobID string) string {
	base := strings.TrimRight(s.PublicBaseURL, "/")
	if base == "" {
		base = "http://localhost:8080"
	}
	return base + "/v1/jobs/" + url.PathEscape(jobID) + "/stream"
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
