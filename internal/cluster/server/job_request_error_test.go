package server

import (
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/stretchr/testify/assert"
)

// TestJobRequestErrorClassifiesWhatTheCallerCanActOn shows a request about a
// job answers the cluster error a stale leader cache calls for, and not an
// internal error, also on a node whose job server runs because it still leads
// another partition: the engine of the lost partition is missing there.
func TestJobRequestErrorClassifiesWhatTheCallerCanActOn(t *testing.T) {
	for name, tt := range map[string]struct {
		err      error
		expected zenerr.ZenErrorCode
	}{
		"a node which leads no partition": {
			err:      jobmanager.NodeIsNotALeader,
			expected: zenerr.ClusterErrorCode,
		},
		"a node which lost the job's partition but leads another one": {
			err:      fmt.Errorf("cannot fail job 42 on partition 2: %w", jobmanager.NodeIsNotALeader),
			expected: zenerr.ClusterErrorCode,
		},
		"a node which refuses mutations while the cluster is restored": {
			err:      fmt.Errorf("failed to fail job: %w", zenerr.ClusterError(fmt.Errorf("cluster restore in progress"))),
			expected: zenerr.ClusterErrorCode,
		},
		"an unknown job": {
			err:      fmt.Errorf("failed to find job: %w", storage.ErrNotFound),
			expected: zenerr.NotFoundCode,
		},
		"a refused request": {
			err:      fmt.Errorf("retries: %w", bpmn.ErrInvalidJobRequest),
			expected: zenerr.BadRequestCode,
		},
		"a job which no longer waits": {
			err:      fmt.Errorf("job 42: %w", bpmn.ErrJobInTerminalState),
			expected: zenerr.ConflictCode,
		},
		"anything else": {
			err:      fmt.Errorf("disk full"),
			expected: zenerr.TechnicalErrorCode,
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, jobRequestError(42, "fail", tt.err).Code)
		})
	}
}
