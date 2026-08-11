package toprotocol

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/financial"
	atostosv1 "github.com/tosnetwork/tos-protocol/gen/atos/tos/v1"
)

func anchorProto(anchor financial.ManagedAnchor) (*atostosv1.ManagedFinancialAnchorInput, error) {
	previous, err := digest(anchor.PreviousMerkleRoot)
	if err != nil {
		return nil, err
	}
	root, err := digest(anchor.MerkleRoot)
	if err != nil {
		return nil, err
	}
	manifest, err := digest(anchor.ManifestDigest)
	if err != nil {
		return nil, err
	}
	signature, err := digest(anchor.SignatureDigest)
	if err != nil {
		return nil, err
	}
	return &atostosv1.ManagedFinancialAnchorInput{
		Version: anchor.Version, AnchorId: anchor.AnchorID, BatchId: anchor.BatchID,
		BatchSequence: anchor.BatchSequence, FirstSequence: anchor.FirstSequence,
		LastSequence: anchor.LastSequence, CommitmentCount: anchor.CommitmentCount,
		PreviousAnchorId: anchor.PreviousAnchorID, PreviousMerkleRoot: previous,
		MerkleRoot: root, ManifestDigest: manifest, SignatureDigest: signature,
		SigningKeyId: anchor.SigningKeyID, Canonicalization: anchor.Canonicalization,
		GatewayId: anchor.GatewayID, NetworkId: anchor.NetworkID,
	}, nil
}

func anchorFromProto(value *atostosv1.ManagedFinancialAnchorInput) financial.ManagedAnchor {
	if value == nil {
		return financial.ManagedAnchor{}
	}
	return financial.ManagedAnchor{
		Version: value.Version, AnchorID: value.AnchorId, BatchID: value.BatchId,
		BatchSequence: value.BatchSequence, FirstSequence: value.FirstSequence,
		LastSequence: value.LastSequence, CommitmentCount: value.CommitmentCount,
		PreviousAnchorID: value.PreviousAnchorId, PreviousMerkleRoot: digestString(value.PreviousMerkleRoot),
		MerkleRoot: digestString(value.MerkleRoot), ManifestDigest: digestString(value.ManifestDigest),
		SignatureDigest: digestString(value.SignatureDigest), SigningKeyID: value.SigningKeyId,
		Canonicalization: value.Canonicalization, GatewayID: value.GatewayId, NetworkID: value.NetworkId,
	}
}

func anchorReceipt(anchor *atostosv1.ManagedFinancialAnchorInput, payload *atostosv1.Digest, reference *atostosv1.NetworkReference, finalized bool, checkpoint uint64) financial.AnchorReceipt {
	receipt := financial.AnchorReceipt{Anchor: anchorFromProto(anchor), PayloadDigest: digestString(payload), Finalized: finalized, FinalizedCheckpoint: checkpoint}
	if reference != nil {
		receipt.NetworkReferenceID = reference.Reference
		receipt.NetworkID = reference.Network
		if reference.Finalized {
			receipt.Finalized = true
		}
		if reference.FinalizedCheckpoint > receipt.FinalizedCheckpoint {
			receipt.FinalizedCheckpoint = reference.FinalizedCheckpoint
		}
	}
	return receipt
}

func (c *Client) PublishManagedFinancialAnchor(ctx context.Context, anchor financial.ManagedAnchor) (financial.AnchorReceipt, error) {
	value, err := anchorProto(anchor)
	if err != nil {
		return financial.AnchorReceipt{}, err
	}
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.PublishManagedFinancialAnchorRequest{Context: c.requestContext(ctx, anchor.GatewayID, anchor.AnchorID, time.Time{}), Anchor: value})
	decorateRequest(c, ctx, request)
	response, err := c.financialIntegrity.PublishManagedFinancialAnchor(callCtx, request)
	if err != nil {
		return financial.AnchorReceipt{}, rpcError(err)
	}
	if response.Msg == nil || response.Msg.Anchor == nil || response.Msg.AnchorRef == nil {
		return financial.AnchorReceipt{}, domain.NewError(domain.ErrNetworkUnavailable, "tos-protocol returned empty managed financial anchor evidence", true)
	}
	return anchorReceipt(response.Msg.Anchor, response.Msg.PayloadDigest, response.Msg.AnchorRef, response.Msg.Finalized, response.Msg.FinalizedCheckpoint), nil
}

func (c *Client) ResolveManagedFinancialAnchor(ctx context.Context, anchor financial.ManagedAnchor) (financial.AnchorReceipt, bool, error) {
	callCtx, cancel := c.callContext(ctx, time.Time{})
	defer cancel()
	request := connect.NewRequest(&atostosv1.ResolveManagedFinancialAnchorRequest{Context: c.requestContext(ctx, anchor.GatewayID, "", time.Time{}), AnchorId: anchor.AnchorID, NetworkId: anchor.NetworkID})
	decorateRequest(c, ctx, request)
	response, err := c.financialIntegrity.ResolveManagedFinancialAnchor(callCtx, request)
	if err != nil {
		return financial.AnchorReceipt{}, false, rpcError(err)
	}
	if response.Msg == nil || !response.Msg.Found {
		return financial.AnchorReceipt{}, false, nil
	}
	return anchorReceipt(response.Msg.Anchor, response.Msg.PayloadDigest, response.Msg.AnchorRef, response.Msg.Finalized, response.Msg.FinalizedCheckpoint), true, nil
}
