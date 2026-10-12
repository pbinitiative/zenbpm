package server

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetJobFailuresRefusesAPageOutsideTheBounds shows the partition checks
// the page it is asked for itself: SQLite reads a negative limit as no limit,
// and the RPC is reachable without the REST API in front of it.
func TestGetJobFailuresRefusesAPageOutsideTheBounds(t *testing.T) {
	for name, page := range map[string]struct{ page, size int32 }{
		"page zero":           {0, 10},
		"negative page":       {-1, 10},
		"size zero":           {1, 0},
		"negative size":       {1, -1},
		"size beyond the cap": {1, MaxJobFailuresPageSize + 1},
	} {
		t.Run(name, func(t *testing.T) {
			resp, err := (&Server{}).GetJobFailures(t.Context(), &proto.GetJobFailuresRequest{
				JobKey: new(int64(42)),
				Page:   new(page.page),
				Size:   new(page.size),
			})

			require.NoError(t, err)
			require.NotNil(t, resp.GetError(), "the page is refused before the partition is asked")
			assert.Equal(t, uint32(zenerr.BadRequestCode), resp.GetError().GetCode())
		})
	}
}
