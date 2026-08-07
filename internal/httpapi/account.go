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
