package httpapi

import (
	"net/http"
	"sort"

	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

type openDisputeRequest struct {
	Reason      string                   `json:"reason"`
	Description string                   `json:"description,omitempty"`
	Evidence    []domain.DisputeEvidence `json:"evidence,omitempty"`
}

// handleOpenDispute never accepts caller-supplied economic values (amount,
// provider_id, settlement status, ...) -- only the job_id path parameter,
// reason, description and evidence references. Every economic fact on the
// resulting Dispute is resolved internally by service.DisputeService.Open
// from durable Job/Quote/BillingSnapshot/ProviderEarning state.
func (s *Server) handleOpenDispute(w http.ResponseWriter, r *http.Request) {
	var req openDisputeRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed dispute request: "+err.Error(), false)
		return
	}
	d, err := s.Disputes.Open(r.Context(), service.OpenDisputeInput{
		PrincipalID: principalFrom(r), JobID: r.PathValue("id"),
		Reason: req.Reason, Description: req.Description, Evidence: req.Evidence,
		IdempotencyKey: idempotencyKeyFrom(r),
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, d)
}

// handleGetDispute is visible to the dispute's own principal, its own
// provider, or a caller holding disputes:review -- never to any other
// caller, regardless of what dispute_id they guess.
func (s *Server) handleGetDispute(w http.ResponseWriter, r *http.Request) {
	d, err := s.Disputes.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	principal := principalFrom(r)
	if d.PrincipalID != principal && d.ProviderID != principal && !authFrom(r).Principal.Has(auth.ScopeDisputesReview) {
		writeError(w, http.StatusForbidden, domain.ErrPermissionDenied, "not a party to this dispute", false)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

// handleListDisputes returns the reviewer queue (disputes still awaiting a
// decision) for a caller holding disputes:review, or otherwise every
// dispute the caller is a party to (as principal or as provider) -- never
// another party's disputes.
func (s *Server) handleListDisputes(w http.ResponseWriter, r *http.Request) {
	principal := principalFrom(r)
	if authFrom(r).Principal.Has(auth.ScopeDisputesReview) {
		disputes, err := s.Disputes.ListUnderReview(r.Context(), 100)
		if err != nil {
			writeDomainErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"disputes": disputes})
		return
	}
	asPrincipal, err := s.Disputes.ListByPrincipal(r.Context(), principal)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	asProvider, err := s.Disputes.ListByProvider(r.Context(), principal)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"disputes": mergeDisputesByID(asPrincipal, asProvider)})
}

// handleReviewDispute claims a dispute for the authenticated reviewer.
// reviewerID always comes from the authenticated principal, never a
// caller-supplied field -- service.DisputeService.Review itself rejects a
// reviewer who is a party to the dispute.
func (s *Server) handleReviewDispute(w http.ResponseWriter, r *http.Request) {
	d, err := s.Disputes.Review(r.Context(), r.PathValue("id"), principalFrom(r))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

type resolveDisputeRequest struct {
	Outcome        string `json:"outcome"`
	ReasonRejected string `json:"reason_rejected,omitempty"`
}

func (s *Server) handleResolveDispute(w http.ResponseWriter, r *http.Request) {
	var req resolveDisputeRequest
	if err := decodeRequestJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrValidationFailed, "malformed resolve request: "+err.Error(), false)
		return
	}
	d, err := s.Disputes.Resolve(r.Context(), service.ResolveDisputeInput{
		DisputeID: r.PathValue("id"), ReviewerID: principalFrom(r),
		Outcome: domain.DisputeOutcome(req.Outcome), ReasonRejected: req.ReasonRejected,
	})
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func mergeDisputesByID(lists ...[]domain.Dispute) []domain.Dispute {
	seen := make(map[string]domain.Dispute)
	for _, list := range lists {
		for _, d := range list {
			seen[d.ID] = d
		}
	}
	out := make([]domain.Dispute, 0, len(seen))
	for _, d := range seen {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OpenedAt.Equal(out[j].OpenedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].OpenedAt.After(out[j].OpenedAt)
	})
	return out
}
