package httpapi

import (
	"net/http"

	"github.com/tosnetwork/atos/internal/domain"
)

// handleGetJobBilling exposes the durable, auditable billing calculation
// for one Job to its owning principal (the payer) or its owning provider --
// the two parties with a legitimate interest in exactly how the charge was
// derived. Internal recovery/checkpoint fields never leave
// domain.BillingSnapshot's JSON contract in the first place, so there is
// nothing further to redact here.
func (s *Server) handleGetJobBilling(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.Jobs.Get(r.Context(), id)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	principal := principalFrom(r)
	if job.PrincipalID != principal && job.ProviderID != principal {
		writeError(w, http.StatusForbidden, domain.ErrPermissionDenied, "not a party to this job", false)
		return
	}
	snap, err := s.Earnings.BillingSnapshotForJob(r.Context(), id)
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// handleListEarnings returns every earning owned by the calling provider.
// providerID is always the authenticated principal's own ID, so a caller
// can never list another provider's earnings by any parameter.
func (s *Server) handleListEarnings(w http.ResponseWriter, r *http.Request) {
	earnings, err := s.Earnings.ListByProvider(r.Context(), principalFrom(r))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"earnings": earnings})
}

func (s *Server) handleGetEarning(w http.ResponseWriter, r *http.Request) {
	earning, err := s.Earnings.Get(r.Context(), r.PathValue("id"), principalFrom(r))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, earning)
}
