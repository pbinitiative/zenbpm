package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

// handleClusterBackup streams the backup bundle. Errors after the first byte
// surface as a truncated tar (no manifest), which restore rejects.
func (s *Server) handleClusterBackup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="zenbpm-backup-%d.tar"`, time.Now().Unix()))
	if _, err := s.node.ClusterBackup(r.Context(), w); err != nil {
		log.Error("cluster backup failed: %s", err)
	}
}

func (s *Server) handleClusterRestore(w http.ResponseWriter, r *http.Request) {
	force := r.URL.Query().Get("force") == "true"
	report, err := s.node.ClusterRestore(r.Context(), r.Body, force)
	if err != nil {
		writeError(w, r, http.StatusConflict, public.Error{Message: err.Error(), Code: "RESTORE_FAILED"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(report)
}
