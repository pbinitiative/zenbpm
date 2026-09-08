package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/log"
)

// handleClusterBackup streams the backup bundle. Errors after the first byte
// surface as a truncated tar (no manifest), which restore rejects.
func (s *Server) handleClusterBackup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="zenbpm-backup-%d.tar"`, time.Now().Unix()))
	if _, err := s.node.ClusterBackup(r.Context(), w); err != nil {
		log.Error("cluster backup failed: %v", err)
	}
}

// restoreErrorResponse is the error body of the restore endpoints. It carries
// the operation id (when the restore got as far as owning the cluster) so the
// operator can look the failure up through the restore status endpoint.
type restoreErrorResponse struct {
	Message     string             `json:"message"`
	Code        string             `json:"code"`
	OperationID string             `json:"operationId,omitempty"`
	Phase       state.RestorePhase `json:"phase,omitempty"`
}

const (
	// restoreCodeRefused: the restore never took ownership of the cluster
	// (another restore in progress, invalid bundle, non-empty cluster).
	restoreCodeRefused = "RESTORE_REFUSED"
	// restoreCodeFailed: a phase of an owned restore failed; the operation is
	// recorded as FAILED in the cluster state.
	restoreCodeFailed = "RESTORE_FAILED"
	// restoreCodeNotLeader: restore state is written through raft; the request
	// has to be repeated on the cluster raft leader.
	restoreCodeNotLeader = "NOT_CLUSTER_LEADER"
)

// handleClusterRestore runs a restore synchronously. The request context
// cancels the restore when the client disconnects; the operation record in
// the cluster state stays durable either way.
func (s *Server) handleClusterRestore(w http.ResponseWriter, r *http.Request) {
	force := r.URL.Query().Get("force") == "true"
	// A stalled upload must not hold the request open: the body read deadline
	// mirrors the coordinator's ingest timeout.
	if ingest := s.node.RestoreIngestTimeout(); ingest > 0 {
		if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(ingest)); err != nil {
			log.Warn("failed to set restore upload read deadline: %v", err)
		}
	}
	report, err := s.node.ClusterRestore(r.Context(), r.Body, force)
	if err != nil {
		httpStatus, code := restoreErrorStatus(err)
		resp := restoreErrorResponse{Message: err.Error(), Code: code}
		if report != nil {
			resp.OperationID = report.OperationID
			resp.Phase = report.Phase
		}
		writeError(w, r, httpStatus, resp)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

// restoreErrorStatus maps a coordinator error onto an HTTP status and error
// code: refusals before ownership are conflicts, a phase failure is an
// internal error, a phase deadline is a gateway timeout.
func restoreErrorStatus(err error) (int, string) {
	switch {
	case errors.Is(err, zenerr.ErrNotLeader):
		return http.StatusServiceUnavailable, restoreCodeNotLeader
	case errors.Is(err, zenerr.ErrResourceLimit):
		return http.StatusRequestEntityTooLarge, restoreCodeRefused
	case errors.Is(err, backup.ErrRestoreInProgress), errors.Is(err, backup.ErrInvalidBundle), errors.Is(err, backup.ErrClusterNotEmpty):
		return http.StatusConflict, restoreCodeRefused
	case backup.IsDeadlineExceeded(err):
		return http.StatusGatewayTimeout, restoreCodeFailed
	default:
		return http.StatusInternalServerError, restoreCodeFailed
	}
}

// handleRestoreOperations lists the restore operations recorded in the
// cluster state (the current or most recent one).
func (s *Server) handleRestoreOperations(w http.ResponseWriter, r *http.Request) {
	operations := []restoreOperationView{}
	if op, ok := s.node.RestoreOperation(); ok {
		operations = append(operations, buildRestoreOperationView(op, time.Now()))
	}
	writeJSON(w, http.StatusOK, struct {
		Operations []restoreOperationView `json:"operations"`
	}{Operations: operations})
}

// handleRestoreOperation returns one restore operation by id.
func (s *Server) handleRestoreOperation(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "operationId")
	op, ok := s.node.RestoreOperation()
	if !ok || op.ID != id {
		writeError(w, r, http.StatusNotFound, restoreErrorResponse{
			Message: fmt.Sprintf("restore operation %s not found", id), Code: "NOT_FOUND",
		})
		return
	}
	writeJSON(w, http.StatusOK, buildRestoreOperationView(op, time.Now()))
}

// handleAbortRestoreOperation terminates a restore operation and lifts the
// cluster gate. The optional JSON body {"reason": "..."} is recorded on the
// operation.
func (s *Server) handleAbortRestoreOperation(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "operationId")
	var body struct {
		Reason string `json:"reason"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, r, http.StatusBadRequest, restoreErrorResponse{Message: "invalid abort request body: " + err.Error(), Code: "BAD_REQUEST"})
			return
		}
	}
	op, err := s.node.AbortClusterRestore(r.Context(), id, body.Reason)
	if err != nil {
		var rejected *state.RestoreRejectedError
		if errors.As(err, &rejected) {
			httpStatus := http.StatusConflict
			if !rejected.Current.Exists() || rejected.Current.ID != id {
				httpStatus = http.StatusNotFound
			}
			writeError(w, r, httpStatus, restoreErrorResponse{Message: err.Error(), Code: "RESTORE_ABORT_REFUSED", OperationID: id})
			return
		}
		httpStatus, code := restoreErrorStatus(err)
		writeError(w, r, httpStatus, restoreErrorResponse{
			Message: fmt.Sprintf("failed to abort restore operation: %s", err), Code: code, OperationID: id,
		})
		return
	}
	writeJSON(w, http.StatusOK, buildRestoreOperationView(op, time.Now()))
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// the status line is already written; the client sees a truncated body
		log.Error("failed to write response body: %v", err)
	}
}
