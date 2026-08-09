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

// StreamJobEvents calls the real tos-protocol ExecutionGatewayService.StreamJob
// server-streaming RPC and forwards each JobEvent in order. It never falls
// back to polling GetJob: an RPC/stream failure is returned to the caller as
// a retryable network error rather than silently degrading to a different
// transport.
func (c *Client) StreamJobEvents(ctx context.Context, req tosai.StreamJobEventsRequest, onEvent func(domain.JobEvent) error) error {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()

	var expectedDigest *atostosv1.Digest
	if req.ExpectedStreamDigest != "" {
		parsed, err := digest(req.ExpectedStreamDigest)
		if err != nil {
			return err
		}
		expectedDigest = parsed
	}

	request := connect.NewRequest(&atostosv1.StreamJobRequest{
		Context: c.requestContext(ctx, "atos-gateway", "", time.Time{}),
		JobId:   req.JobID, NextSequence: req.NextSequence, NextOffset: req.NextOffset,
		MaxChunkBytes: req.MaxChunkBytes, ExpectedStreamDigest: expectedDigest,
	})
	decorateRequest(c, ctx, request)

	stream, err := c.execution.StreamJob(callCtx, request)
	if err != nil {
		return rpcError(err)
	}
	defer stream.Close() //nolint:errcheck

	for stream.Receive() {
		event := stream.Msg()
		if event == nil {
			continue
		}
		if event.JobId != req.JobID {
			return domain.NewError(domain.ErrStreamJobBindingMismatch, fmt.Sprintf("tos-protocol StreamJob response for job %q contained an event bound to job %q", req.JobID, event.JobId), false)
		}
		eventType, err := domainJobEventType(event.EventType)
		if err != nil {
			return err
		}
		mapped := domain.JobEvent{
			JobID: event.JobId, Sequence: event.Sequence, EventType: eventType,
			State: domainJobState(event.State), Chunk: append([]byte(nil), event.Chunk...),
			Offset: event.Offset, TotalOutputBytes: event.TotalOutputBytes,
			Terminal: event.Terminal, ErrorCode: domain.ErrorCode(event.ErrorCode),
			// event.StreamDigest is deliberately NOT forwarded as StreamDigest:
			// tos-protocol sets it to the digest of the complete retained
			// output on every event (STATE, each chunk, and TERMINAL alike)
			// rather than a true progressive cumulative digest per chunk, so
			// it cannot be trusted as ATOS's per-chunk stream_digest -- the
			// durable store independently (re)computes that instead. It IS,
			// however, a stable per-Job execution-identity value, so it is
			// carried separately as UpstreamRetainedDigest: EnsureIngested
			// persists it once and supplies it back as expected_stream_digest
			// on a resumed ingestion pull (see internal/service/stream.go).
			UpstreamRetainedDigest: digestString(event.StreamDigest),
		}
		if event.Usage != nil {
			usage := domainUsage(event.Usage)
			mapped.Usage = &usage
		}
		if event.ProofStatus != nil {
			proofStatus := domainProofStatus(event.ProofStatus)
			mapped.ProofStatus = &proofStatus
		}
		if err := onEvent(mapped); err != nil {
			return err
		}
	}
	if err := stream.Err(); err != nil {
		return rpcError(err)
	}
	return nil
}

// domainJobEventType fails closed on any value it does not recognize
// (including JOB_EVENT_TYPE_UNSPECIFIED and any future enum addition this
// adapter has not been updated for yet) rather than silently guessing it
// means STATE.
func domainJobEventType(value atostosv1.JobEventType) (domain.JobEventType, error) {
	switch value {
	case atostosv1.JobEventType_JOB_EVENT_TYPE_STATE:
		return domain.JobEventState, nil
	case atostosv1.JobEventType_JOB_EVENT_TYPE_OUTPUT_CHUNK:
		return domain.JobEventOutputChunk, nil
	case atostosv1.JobEventType_JOB_EVENT_TYPE_INPUT_REQUIRED:
		return domain.JobEventInputRequired, nil
	case atostosv1.JobEventType_JOB_EVENT_TYPE_PROOF_STATUS:
		return domain.JobEventProofStatus, nil
	case atostosv1.JobEventType_JOB_EVENT_TYPE_TERMINAL:
		return domain.JobEventTerminal, nil
	default:
		return "", domain.NewError(domain.ErrStreamEventTypeUnsupported, fmt.Sprintf("tos-protocol StreamJob returned an unrecognized event type %v", value), false)
	}
}

var _ tosai.Streamer = (*Client)(nil)
