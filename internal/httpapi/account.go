package httpapi

import "net/http"

func (s *Server) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	a, err := s.Accounts.Get(r.Context(), principalFrom(r))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) handleGetAccountUsage(w http.ResponseWriter, r *http.Request) {
	usage, err := s.Receipts.UsageSummary(r.Context(), principalFrom(r))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, usage)
}

func (s *Server) handleListReceipts(w http.ResponseWriter, r *http.Request) {
	receipts, err := s.Receipts.ListByPrincipal(r.Context(), principalFrom(r))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"receipts": receipts})
}

func (s *Server) handleGetReceipt(w http.ResponseWriter, r *http.Request) {
	receipt, err := s.Receipts.Get(r.Context(), r.PathValue("id"), principalFrom(r))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

func (s *Server) handleSettlementProof(w http.ResponseWriter, r *http.Request) {
	proof, err := s.Receipts.SettlementProof(r.Context(), r.PathValue("id"), principalFrom(r))
	if err != nil {
		writeDomainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, proof)
}

func (s *Server) handleCreateProofPackage(w http.ResponseWriter, r *http.Request) {
	p, e := s.ProofPackages.Create(r.Context(), r.PathValue("id"), principalFrom(r))
	if e != nil {
		writeDomainErr(w, e)
		return
	}
	writeJSON(w, http.StatusOK, p)
}
func (s *Server) handleGetProofPackage(w http.ResponseWriter, r *http.Request) {
	p, e := s.ProofPackages.Get(r.Context(), r.PathValue("id"), principalFrom(r))
	if e != nil {
		writeDomainErr(w, e)
		return
	}
	writeJSON(w, http.StatusOK, p)
}
