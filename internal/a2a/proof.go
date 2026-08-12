package a2a

import (
	"context"
	"encoding/json"
	"github.com/tosnetwork/atos/internal/domain"
	"net/http"
)

type proofParams struct {
	ReceiptID string `json:"receipt_id"`
}

func (s *Server) handleProofCreate(ctx context.Context, w http.ResponseWriter, req rpcRequest, principal string) {
	var p proofParams
	if json.Unmarshal(req.Params, &p) != nil || p.ReceiptID == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "receipt_id is required", nil)
		return
	}
	v, e := s.ProofPackages.Create(ctx, p.ReceiptID, principal)
	if e != nil {
		writeRPCError(w, req.ID, codeInternalError, e.Error(), map[string]any{"code": domain.ErrProviderFailed})
		return
	}
	writeRPCResult(w, req.ID, v)
}
func (s *Server) handleProofGet(ctx context.Context, w http.ResponseWriter, req rpcRequest, principal string) {
	var p proofParams
	if json.Unmarshal(req.Params, &p) != nil || p.ReceiptID == "" {
		writeRPCError(w, req.ID, codeInvalidParams, "receipt_id is required", nil)
		return
	}
	v, e := s.ProofPackages.Get(ctx, p.ReceiptID, principal)
	if e != nil {
		writeRPCError(w, req.ID, codeInternalError, e.Error(), map[string]any{"code": domain.ErrProviderFailed})
		return
	}
	writeRPCResult(w, req.ID, v)
}
