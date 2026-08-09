package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"

	skilldoc "github.com/tosnetwork/atos/skills"
)

func (s *Server) handleSkill(w http.ResponseWriter, r *http.Request) {
	content := skilldoc.Content()
	sum := sha256.Sum256(content)
	etag := `"sha256-` + hex.EncodeToString(sum[:]) + `"`
	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300, stale-while-revalidate=3600")
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	_, _ = w.Write(content)
}
