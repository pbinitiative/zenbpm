package rest

import (
	"errors"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
)

// TestSetProcessSuspensionErrorAnswersByCode shows that a failure to change the suspension is answered by its
// code: a transient condition of the cluster is a 502, not an internal error.
func TestSetProcessSuspensionErrorAnswersByCode(t *testing.T) {
	for _, tt := range []struct {
		name     string
		err      error
		expected public.SetProcessSuspensionResponseObject
	}{
		{"bad request", zenerr.BadRequest(errors.New("partition id must be set")), public.SetProcessSuspension400JSONResponse{}},
		{"not found", zenerr.NotFound(errors.New("process definition not found")), public.SetProcessSuspension404JSONResponse{}},
		{"cluster error", zenerr.ClusterError(errors.New("no leader available")), public.SetProcessSuspension502JSONResponse{}},
		{"unavailable", zenerr.Unavailable(errors.New("partition is fenced")), public.SetProcessSuspension502JSONResponse{}},
		{"technical error", zenerr.TechnicalError(errors.New("disk full")), public.SetProcessSuspension500JSONResponse{}},
		{"error without a code", errors.New("disk full"), public.SetProcessSuspension500JSONResponse{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.IsType(t, tt.expected, setProcessSuspensionError(t.Context(), tt.err))
		})
	}
}
