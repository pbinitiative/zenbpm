package inmemory_test

import (
	"context"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
)

func TestRunningTokenPages(t *testing.T) {
	t.Run("failed instances do not consume a page slot", func(t *testing.T) {
		store := inmemory.NewStorage()
		for _, instance := range []struct {
			key   int64
			state runtime.ActivityState
		}{
			{key: 1, state: runtime.ActivityStateFailed},
			{key: 2, state: runtime.ActivityStateActive},
			{key: 3, state: runtime.ActivityStateReady},
		} {
			require.NoError(t, store.SaveProcessInstance(t.Context(), &runtime.DefaultProcessInstance{
				ProcessInstanceData: runtime.ProcessInstanceData{Key: instance.key, State: instance.state},
			}))
		}
		for _, token := range []runtime.ExecutionToken{
			{Key: 10, ProcessInstanceKey: 1, State: runtime.TokenStateRunning, CreatedAt: time.Now().Add(-time.Hour)},
			{Key: 20, ProcessInstanceKey: 2, State: runtime.TokenStateRunning, CreatedAt: time.Now().Add(-time.Hour)},
			{Key: 30, ProcessInstanceKey: 3, State: runtime.TokenStateRunning, CreatedAt: time.Now().Add(-time.Hour)},
		} {
			require.NoError(t, store.SaveToken(t.Context(), token))
		}

		startupPage, err := store.FindRunningTokensAfter(t.Context(), 0, 1)
		require.NoError(t, err)
		require.Len(t, startupPage, 1)
		require.Equal(t, int64(20), startupPage[0].Key)

		periodicPage, err := store.FindRecoverableRunningTokens(t.Context(), 0, time.Now(), 1)
		require.NoError(t, err)
		require.Len(t, periodicPage, 1)
		require.Equal(t, int64(20), periodicPage[0].Key)

		periodicPage, err = store.FindRecoverableRunningTokens(t.Context(), 20, time.Now(), 1)
		require.NoError(t, err)
		require.Len(t, periodicPage, 1)
		require.Equal(t, int64(30), periodicPage[0].Key)
	})

	t.Run("canceled scans stop before returning a page", func(t *testing.T) {
		store := inmemory.NewStorage()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := store.FindRunningTokensAfter(ctx, 0, 1)
		require.ErrorIs(t, err, context.Canceled)
		_, err = store.FindRecoverableRunningTokens(ctx, 0, time.Now(), 1)
		require.ErrorIs(t, err, context.Canceled)
	})
}
