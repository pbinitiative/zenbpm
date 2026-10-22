package jobmanager

import (
	"errors"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientTellsAnAbsentPartitionFromAPartitionWithoutALeader shows a job
// request whose key names a partition the formed cluster does not have is
// refused as an unknown job, which a worker does not retry, the way an
// unknown key of an existing partition is, and that a partition which only
// lacks a leader, during a leader change or while the cluster forms its
// partitions, still answers that the leader is unavailable.
func TestClientTellsAnAbsentPartitionFromAPartitionWithoutALeader(t *testing.T) {
	mux, nodeListener, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeListener.Close()) }()
	listener := network.NewZenBpmClusterListener(mux)
	serverStore := getTestStore(listener)
	serverStore.updateState(func(cluster *state.Cluster) {
		cluster.Config.DesiredPartitions = 1
	})
	_, _, leaderGRPC := createServerNodeWithGRPC(t, listener, serverStore)
	existingPartitionKey := gen.Generate().Int64()
	absentPartitions := map[string]int64{
		"partition 0, reserved for global resources": 1,
		"a partition past the formed ones":           int64(7) << zenflake.StepBits,
	}

	for name, jobKey := range absentPartitions {
		t.Run(name, func(t *testing.T) {
			clientManager := createClientNode(t, serverStore.forNode("node-2"))

			errs := requestJob(t, clientManager, jobKey)

			assert.ErrorIs(t, errs.complete, ErrJobNotFound)
			assert.ErrorIs(t, errs.fail, ErrJobNotFound)
			assert.ErrorIs(t, errs.extend, ErrLockNotHeld, "the lock of a job which does not exist is not held")
			for _, err := range []error{errs.complete, errs.fail, errs.extend} {
				assert.NotErrorIs(t, err, ErrLeaderUnavailable, "retrying would never pass")
			}
		})
	}

	t.Run("no job in an existing partition", func(t *testing.T) {
		clientManager := createClientNode(t, serverStore.forNode("node-2"))
		unknown := zenerr.NotFound(errors.New("job not found")).ToProtoError()
		leaderGRPC.completeJobResponse = &proto.CompleteJobResponse{Error: unknown}
		leaderGRPC.failJobResponse = &proto.FailJobResponse{Error: unknown}
		leaderGRPC.extendLockResponse = &proto.ExtendJobLockResponse{Error: unknown, Refusal: proto.LockRefusal_LOCK_REFUSAL_NOT_HELD.Enum()}
		defer func() {
			leaderGRPC.completeJobResponse = nil
			leaderGRPC.failJobResponse = nil
			leaderGRPC.extendLockResponse = nil
		}()

		errs := requestJob(t, clientManager, existingPartitionKey)

		assert.ErrorIs(t, errs.complete, ErrJobNotFound)
		assert.ErrorIs(t, errs.fail, ErrJobNotFound)
		assert.ErrorIs(t, errs.extend, ErrLockNotHeld)
	})

	t.Run("the partition has no leader at the moment", func(t *testing.T) {
		clientStore := serverStore.forNode("node-2")
		clientStore.updateState(func(cluster *state.Cluster) {
			cluster.Partitions[partition] = state.Partition{Id: partition}
		})
		clientManager := createClientNode(t, clientStore)

		errs := requestJob(t, clientManager, existingPartitionKey)

		assert.ErrorIs(t, errs.complete, ErrLeaderUnavailable)
		assert.ErrorIs(t, errs.fail, ErrLeaderUnavailable)
		assert.ErrorIs(t, errs.extend, ErrLeaderUnavailable)
	})

	t.Run("the cluster has not formed its partitions yet", func(t *testing.T) {
		clientStore := serverStore.forNode("node-2")
		clientStore.updateState(func(cluster *state.Cluster) {
			cluster.Partitions = map[uint32]state.Partition{}
		})
		clientManager := createClientNode(t, clientStore)

		for _, jobKey := range []int64{existingPartitionKey, absentPartitions["partition 0, reserved for global resources"]} {
			errs := requestJob(t, clientManager, jobKey)

			assert.ErrorIs(t, errs.complete, ErrLeaderUnavailable, "key %d", jobKey)
			assert.ErrorIs(t, errs.fail, ErrLeaderUnavailable, "key %d", jobKey)
			assert.ErrorIs(t, errs.extend, ErrLeaderUnavailable, "key %d", jobKey)
		}
	})
}

// jobRequestErrors are the errors a completion, a failure and a lock
// extension of the same job came back with.
type jobRequestErrors struct {
	complete, fail, extend error
}

// requestJob completes, fails and extends the lock of the job through the
// job client of clientManager.
func requestJob(t *testing.T, clientManager *JobManager, jobKey int64) jobRequestErrors {
	t.Helper()
	_, extendErr := clientManager.ExtendJobLockReq(t.Context(), "client-1", jobKey, 0, nil)
	return jobRequestErrors{
		complete: clientManager.CompleteJobReq(t.Context(), "client-1", jobKey, nil),
		fail:     clientManager.FailJobReq(t.Context(), "client-1", jobKey, "down", nil, nil, nil, nil, nil),
		extend:   extendErr,
	}
}
