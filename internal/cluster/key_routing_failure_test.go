package cluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
)

// TestKeyRoutingFailureIsNotFoundOnlyForAnAbsentPartition shows a key whose
// partition the cluster does not have is not found, with the partition it
// names, while a partition without a leader at the moment stays a cluster
// failure to retry.
func TestKeyRoutingFailureIsNotFoundOnlyForAnAbsentPartition(t *testing.T) {
	absent := fmt.Errorf("failed to get client: %w", fmt.Errorf("failed to get leader address for cluster client: %w: partition 0", zenerr.ErrPartitionAbsent))
	failure := keyRoutingFailure("job", 1, absent)
	assert.Equal(t, zenerr.NotFoundCode, failure.Code, "a key of an absent partition can never name a job")
	assert.Equal(t, "job 1 not found: its key names partition 0, which the cluster does not have", failure.Error())

	leaderless := errors.New("failed to get client: failed to get leader address for cluster client: no leader available")
	assert.Equal(t, zenerr.ClusterErrorCode, keyRoutingFailure("job", 4096, leaderless).Code, "a partition without a leader at the moment is a cluster failure to retry")

	forming := errors.New("failed to get follower node to get job: partition not found")
	assert.Equal(t, zenerr.ClusterErrorCode, keyRoutingFailure("job", 4096, forming).Code, "a partition still forming is a cluster failure to retry")
}
