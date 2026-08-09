package toprotocol

import (
	"context"
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
		mapped := domain.JobEvent{
			JobID: event.JobId, Sequence: event.Sequence, EventType: domainJobEventType(event.EventType),
			State: domainJobState(event.State), Chunk: append([]byte(nil), event.Chunk...),
			Offset: event.Offset, TotalOutputBytes: event.TotalOutputBytes,
			Terminal: event.Terminal, ErrorCode: domain.ErrorCode(event.ErrorCode),
			// event.StreamDigest is deliberately NOT forwarded here: the
			// current tos-protocol StreamJob implementation sets it to the
			// digest of the complete final output on every event rather than
			// a true progressive cumulative digest per chunk, so it cannot
			// be trusted as ATOS's per-chunk stream_digest. Leaving this
			// empty is safe -- the durable store independently (re)computes
			// the real cumulative digest from the chunk bytes themselves and
			// only cross-checks a caller-supplied digest when non-empty.
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

func domainJobEventType(value atostosv1.JobEventType) domain.JobEventType {
	switch value {
	case atostosv1.JobEventType_JOB_EVENT_TYPE_STATE:
		return domain.JobEventState
	case atostosv1.JobEventType_JOB_EVENT_TYPE_OUTPUT_CHUNK:
		return domain.JobEventOutputChunk
	case atostosv1.JobEventType_JOB_EVENT_TYPE_INPUT_REQUIRED:
		return domain.JobEventInputRequired
	case atostosv1.JobEventType_JOB_EVENT_TYPE_PROOF_STATUS:
		return domain.JobEventProofStatus
	case atostosv1.JobEventType_JOB_EVENT_TYPE_TERMINAL:
		return domain.JobEventTerminal
	default:
		return domain.JobEventState
	}
}

var _ tosai.Streamer = (*Client)(nil)
