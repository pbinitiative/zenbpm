package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestEngineStopOnCopyDoesNotPreventStoppingOriginal guards against a value copy of an
// Engine (NewEngine returns Engine by value) consuming the shared stopOnce before the
// running engine is stopped. The copy has no timer manager, so stopping it must not
// prevent the original engine's timer manager goroutine from being stopped later.
func TestEngineStopOnCopyDoesNotPreventStoppingOriginal(t *testing.T) {
	defer goleak.VerifyNone(t, sharedEngineGoleakOptions()...)

	engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
	copied := engine // shares *stopOnce, copied.timerManager stays nil
	require.NoError(t, engine.Start(t.Context()))
	require.NotNil(t, engine.timerManager)
	require.Nil(t, copied.timerManager)

	copied.Stop()
	engine.Stop()

	require.Error(t, engine.timerManager.ctx.Err(), "running engine's timer manager must be stopped")
	require.Error(t, engine.context.Err(), "engine context must be cancelled")
}

// TestEngineRestartAfterStopStopsNewTimerManager guards against Stop becoming a permanent
// no-op after the first call: Start creates a fresh timer manager, and a subsequent Stop must
// terminate it rather than leak its goroutine.
func TestEngineRestartAfterStopStopsNewTimerManager(t *testing.T) {
	defer goleak.VerifyNone(t, sharedEngineGoleakOptions()...)

	engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
	engine.Stop()

	// Start creates and starts a new timer manager before touching persistence.
	_ = engine.Start(t.Context())
	require.NotNil(t, engine.timerManager)
	restartedTimerManager := engine.timerManager

	engine.Stop()

	require.Error(t, restartedTimerManager.ctx.Err(), "restarted timer manager must be stopped")
}

// TestEngineStartStopStopsTimerManagerAndOwnedPools verifies the full Start -> Stop lifecycle
// releases the timer manager, engine context and engine-owned script pools, and that repeated
// Stop calls remain safe.
func TestEngineStartStopStopsTimerManagerAndOwnedPools(t *testing.T) {
	defer goleak.VerifyNone(t, sharedEngineGoleakOptions()...)

	tracker := newEngineConstructionTracker()
	engine := newEngine(tracker.factories(), EngineWithStorage(inmemory.NewStorage()))
	require.NoError(t, engine.Start(t.Context()))
	require.NotNil(t, engine.timerManager)

	engine.Stop()
	engine.Stop()

	require.Error(t, engine.timerManager.ctx.Err(), "timer manager must be stopped")
	require.Error(t, engine.context.Err(), "engine context must be cancelled")
	require.EqualValues(t, 1, tracker.feelRuntime.stopCalls.Load(), "owned FEEL runtime must be stopped exactly once")
	require.EqualValues(t, 1, tracker.jsRuntime.stopCalls.Load(), "owned JavaScript runtime must be stopped exactly once")
}
