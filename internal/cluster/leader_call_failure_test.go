package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
)

func TestLeaderCallFailureIsAClusterErrorUnlessTheCallerIsGone(t *testing.T) {
	unreachable := errors.New("rpc error: code = Unavailable desc = connection refused")
	assert.Equal(t, zenerr.ClusterErrorCode, leaderCallFailure(t.Context(), unreachable).Code, "a leader out of reach is a cluster failure to retry")

	timedOut := errors.New("rpc error: code = DeadlineExceeded desc = context deadline exceeded")
	assert.Equal(t, zenerr.ClusterErrorCode, leaderCallFailure(t.Context(), timedOut).Code, "a leader too slow to answer is the same failure, not an internal error")

	gone, cancel := context.WithCancel(t.Context())
	cancel()
	assert.Equal(t, zenerr.TechnicalErrorCode, leaderCallFailure(gone, context.Canceled).Code, "a caller which went away is no cluster failure")
}
