package rest

import (
	"fmt"
	"net/http"
	"time"

	"github.com/pbinitiative/zenbpm/internal/log"
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
