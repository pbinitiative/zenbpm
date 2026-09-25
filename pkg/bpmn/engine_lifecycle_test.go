package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestEngineLifecycle(t *testing.T) {
	t.Run("stop on a pre-start copy stops shared managers", func(t *testing.T) {
		defer goleak.VerifyNone(t, sharedEngineGoleakOptions()...)

		engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
		copied := engine
		t.Cleanup(engine.Stop)
		require.NoError(t, engine.Start(t.Context()))
		timerManager := engine.currentTimerManager()
		reconciliationManager := engine.currentReconciliationManager()
		require.NotNil(t, timerManager)
		require.NotNil(t, reconciliationManager)
		require.Same(t, timerManager, copied.currentTimerManager())
		require.Same(t, reconciliationManager, copied.currentReconciliationManager())

		copied.Stop()

		require.Error(t, timerManager.ctx.Err(), "shared timer manager must be stopped")
		require.Error(t, reconciliationManager.ctx.Err(), "shared reconciliation manager must be stopped")
		require.Nil(t, engine.currentReconciliationManager())
		require.Error(t, engine.context.Err(), "engine context must be cancelled")
	})

	t.Run("start after stop leaves no managers", func(t *testing.T) {
		defer goleak.VerifyNone(t, sharedEngineGoleakOptions()...)

		engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
		engine.Stop()

		require.Error(t, engine.Start(t.Context()), "stopped engine resources cannot be restarted")
		require.Nil(t, engine.currentTimerManager())
		require.Nil(t, engine.currentReconciliationManager())
	})

	t.Run("start and repeated stop release managers and owned pools", func(t *testing.T) {
		defer goleak.VerifyNone(t, sharedEngineGoleakOptions()...)

		tracker := newEngineConstructionTracker()
		engine := newEngine(tracker.factories(), EngineWithStorage(inmemory.NewStorage()))
		require.NoError(t, engine.Start(t.Context()))
		timerManager := engine.currentTimerManager()
		require.NotNil(t, timerManager)

		engine.Stop()
		engine.Stop()

		require.Error(t, timerManager.ctx.Err(), "timer manager must be stopped")
		require.Error(t, engine.context.Err(), "engine context must be cancelled")
		require.EqualValues(t, 1, tracker.feelRuntime.stopCalls.Load(), "owned FEEL runtime must be stopped exactly once")
		require.EqualValues(t, 1, tracker.jsRuntime.stopCalls.Load(), "owned JavaScript runtime must be stopped exactly once")
	})
}
