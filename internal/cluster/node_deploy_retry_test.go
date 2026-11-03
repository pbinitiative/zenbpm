package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTransientDeployErrorUsesTypedErrors(t *testing.T) {
	assert.False(t, transientDeployError(nil))
	assert.True(t, transientDeployError(errTransientDeploy), "no leader elected yet")
	assert.True(t, transientDeployError(fmt.Errorf("wrapped: %w", errTransientDeploy)))
	assert.True(t, transientDeployError(zenerr.Unavailable(errors.New("no engine available on this node"))))
	assert.True(t, transientDeployError(zenerr.ToZenError(zenerr.Unavailable(errors.New("store not open")).ToProtoError())),
		"the UNAVAILABLE code survives the trip through the proto error")
	assert.True(t, transientDeployError(status.Error(codes.Unavailable, "connection refused")), "gRPC transport failure")

	// message content alone never makes an error transient
	assert.False(t, transientDeployError(zenerr.TechnicalError(errors.New("no engine available on this node"))))
	assert.False(t, transientDeployError(errors.New("store not open")))
	assert.False(t, transientDeployError(status.Error(codes.Internal, "no engines available")))
}

func TestRetryDeployRetriesTransientFailuresAndStopsOnOthers(t *testing.T) {
	node := &ZenNode{}
	attempts := 0
	err := node.retryDeploy(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return zenerr.Unavailable(errors.New("engine starting"))
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, attempts)

	attempts = 0
	permanent := zenerr.BadRequest(errors.New("invalid bpmn"))
	err = node.retryDeploy(context.Background(), func() error {
		attempts++
		return permanent
	})
	assert.ErrorIs(t, err, permanent)
	assert.Equal(t, 1, attempts, "a permanent failure is not retried")
}

func TestRetryDeployHonoursContextCancellation(t *testing.T) {
	node := &ZenNode{}
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	start := time.Now()
	err := node.retryDeploy(ctx, func() error {
		attempts++
		if attempts == 2 {
			cancel() // the client goes away while a retry is pending
		}
		return zenerr.Unavailable(errors.New("engine starting"))
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), deployRetryFor, "cancellation must not wait for the retry window")
	assert.LessOrEqual(t, attempts, 3)

	var zerr *zenerr.ZenError
	require.ErrorAs(t, err, &zerr)
	assert.Equal(t, zenerr.ClusterErrorCode, zerr.Code)
}

func TestRetryDeployReportsMissingLeaderAsClusterError(t *testing.T) {
	// shorten the retry window so the test reaches the exhausted-window
	// branch instead of a context deadline
	window, interval := deployRetryFor, deployRetryInterval
	deployRetryFor, deployRetryInterval = 200*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { deployRetryFor, deployRetryInterval = window, interval })

	node := &ZenNode{}
	attempts := 0
	err := node.retryDeploy(context.Background(), func() error {
		attempts++
		return errTransientDeploy
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "no leader available")
	assert.Greater(t, attempts, 1, "a missing leader is retried until the window expires")
	var zerr *zenerr.ZenError
	require.ErrorAs(t, err, &zerr)
	assert.Equal(t, zenerr.ClusterErrorCode, zerr.Code)
}

func TestZenErrorUnavailableRoundTrip(t *testing.T) {
	original := zenerr.Unavailable(errors.New("no engine available on this node"))
	wire := original.ToProtoError()
	assert.Equal(t, uint32(zenerr.UnavailableCode), wire.GetCode())
	back := zenerr.ToZenError(wire)
	assert.Equal(t, zenerr.UnavailableCode, back.Code)
	assert.Equal(t, "UNAVAILABLE", back.Code.ToString())
	var _ = proto.ErrorResult{}
}

func TestRetryLeaderCallNamesTheActionInWhatItGivesUpWith(t *testing.T) {
	node := &ZenNode{}

	t.Run("no leader within the retry window", func(t *testing.T) {
		defer func(window, interval time.Duration) { deployRetryFor, deployRetryInterval = window, interval }(deployRetryFor, deployRetryInterval)
		deployRetryFor, deployRetryInterval = 20*time.Millisecond, time.Millisecond

		err := node.retryLeaderCall(t.Context(), "process suspension", "change the suspension of a process", func() error {
			return fmt.Errorf("%w: partition 1 has no leader", errTransientDeploy)
		})

		require.Error(t, err)
		assert.ErrorContains(t, err, "no leader available to change the suspension of a process within")
		assert.NotContains(t, err.Error(), "available to deploy", "the wording of the deploy is not the one of a suspension")
	})

	t.Run("cancelled while retrying", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		err := node.retryLeaderCall(ctx, "process suspension", "change the suspension of a process", func() error {
			cancel()
			return zenerr.Unavailable(errors.New("engine starting"))
		})

		require.ErrorIs(t, err, context.Canceled)
		assert.ErrorContains(t, err, "process suspension cancelled")
	})

	t.Run("deploy keeps its wording", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		err := node.retryDeploy(ctx, func() error {
			cancel()
			return zenerr.Unavailable(errors.New("engine starting"))
		})
		assert.ErrorContains(t, err, "deploy cancelled while retrying a transient failure")

		defer func(window, interval time.Duration) { deployRetryFor, deployRetryInterval = window, interval }(deployRetryFor, deployRetryInterval)
		deployRetryFor, deployRetryInterval = 20*time.Millisecond, time.Millisecond
		err = node.retryDeploy(t.Context(), func() error {
			return fmt.Errorf("%w: partition 1 has no leader", errTransientDeploy)
		})
		assert.ErrorContains(t, err, "no leader available to deploy to within")
	})
}
