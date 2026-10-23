package rest

import (
	"errors"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
)

// TestDeleteProcessInstanceVariableErrorAnswersByCode shows a failure of the
// lookup or of the deletion is answered by its code. A deletion which meets an
// instance deleted since the lookup is not found, as the lookup itself is,
// not an internal error.
func TestDeleteProcessInstanceVariableErrorAnswersByCode(t *testing.T) {
	for _, tt := range []struct {
		name     string
		err      error
		expected public.DeleteProcessInstanceVariableResponseObject
	}{
		{"not found", zenerr.NotFound(errors.New("process instance 1 not found")), public.DeleteProcessInstanceVariable404JSONResponse{}},
		{"cluster error", zenerr.ClusterError(errors.New("no leader available")), public.DeleteProcessInstanceVariable502JSONResponse{}},
		{"technical error", zenerr.TechnicalError(errors.New("disk full")), public.DeleteProcessInstanceVariable500JSONResponse{}},
		{"error without a code", errors.New("disk full"), public.DeleteProcessInstanceVariable500JSONResponse{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response := deleteProcessInstanceVariableError(t.Context(), tt.err)

			assert.IsType(t, tt.expected, response)
		})
	}
}
