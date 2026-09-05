package bpmn

import (
	"sync/atomic"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/dmn"
	"github.com/pbinitiative/zenbpm/pkg/script"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestDefaultEngineOwnsAndStopsItsScriptPools(t *testing.T) {
	defer goleak.VerifyNone(t, sharedEngineGoleakOptions()...)

	tracker := newEngineConstructionTracker()
	engine := newEngine(tracker.factories())

	require.EqualValues(t, 1, tracker.feelConstructionCalls.Load(), "default construction must create exactly one FEEL runtime")
	require.EqualValues(t, 1, tracker.jsConstructionCalls.Load(), "default construction must create exactly one JavaScript runtime")
	require.EqualValues(t, 1, tracker.dmnConstructionCalls.Load(), "default construction must create exactly one DMN engine")
	require.True(t, engine.ownsFeelRuntime, "engine must own the default FEEL runtime")
	require.True(t, engine.ownsJsRuntime, "engine must own the default JS runtime")
	require.NotNil(t, engine.dmnEngine, "engine must create an embedded DMN engine")
	require.Same(t, engine.feelRuntime, tracker.dmnFeelRuntime, "DMN must reuse the BPMN engine's FEEL runtime")

	// Stop must release both engine-owned runtimes exactly once.
	engine.Stop()
	engine.Stop()
	require.EqualValues(t, 1, tracker.feelRuntime.stopCalls.Load(), "owned FEEL runtime must be stopped exactly once")
	require.EqualValues(t, 1, tracker.jsRuntime.stopCalls.Load(), "owned JavaScript runtime must be stopped exactly once")
}

func TestEngineWithStorageDoesNotCreateAdditionalFeelRuntime(t *testing.T) {
	defer goleak.VerifyNone(t, sharedEngineGoleakOptions()...)

	tracker := newEngineConstructionTracker()
	engine := newEngine(tracker.factories(), EngineWithStorage(inmemory.NewStorage()))

	require.EqualValues(t, 1, tracker.feelConstructionCalls.Load(), "storage-only construction must create exactly one FEEL runtime")
	require.EqualValues(t, 1, tracker.jsConstructionCalls.Load(), "storage-only construction must create exactly one JavaScript runtime")
	require.EqualValues(t, 1, tracker.dmnConstructionCalls.Load(), "storage-only construction must create exactly one DMN engine")
	require.True(t, engine.ownsFeelRuntime, "engine must own the default FEEL runtime")
	require.True(t, engine.ownsJsRuntime, "engine must own the default JS runtime")
	require.Same(t, engine.feelRuntime, tracker.dmnFeelRuntime, "DMN must reuse the BPMN engine's FEEL runtime")

	engine.Stop()
}

func TestEngineWithInjectedRuntimesDoesNotStopOrReplaceThem(t *testing.T) {
	defer goleak.VerifyNone(t, sharedEngineGoleakOptions()...)

	feelRuntime := &stopCountingFeelRuntime{}
	jsRuntime := &stopCountingJsRuntime{}
	tracker := newEngineConstructionTracker()

	engine := newEngine(
		tracker.factories(),
		EngineWithStorageAndFeel(inmemory.NewStorage(), feelRuntime),
		EngineWithJs(jsRuntime),
	)

	require.Zero(t, tracker.feelConstructionCalls.Load(), "injected construction must not create a default FEEL runtime")
	require.Zero(t, tracker.jsConstructionCalls.Load(), "injected construction must not create a default JavaScript runtime")
	require.EqualValues(t, 1, tracker.dmnConstructionCalls.Load(), "injected construction must create exactly one DMN engine")
	require.False(t, engine.ownsFeelRuntime, "injected FEEL runtime must stay caller-owned")
	require.False(t, engine.ownsJsRuntime, "injected JS runtime must stay caller-owned")
	require.Same(t, feelRuntime, engine.feelRuntime.(*stopCountingFeelRuntime), "injected FEEL runtime must be used as-is")
	require.Same(t, jsRuntime, engine.jsRuntime.(*stopCountingJsRuntime), "injected JS runtime must be used as-is")
	require.Same(t, feelRuntime, tracker.dmnFeelRuntime, "DMN must reuse the injected BPMN FEEL runtime")

	// Stopping the engine (and its embedded DMN engine) must never stop
	// caller-owned runtimes, no matter how many times Stop is called.
	engine.GetDmnEngine().Stop()
	engine.Stop()
	engine.Stop()

	require.Zero(t, feelRuntime.stopCalls.Load(), "caller-owned FEEL runtime must not be stopped by the engine")
	require.Zero(t, jsRuntime.stopCalls.Load(), "caller-owned JS runtime must not be stopped by the engine")

}

func TestEngineStopIsIdempotentWithOwnedRuntimes(t *testing.T) {
	defer goleak.VerifyNone(t, sharedEngineGoleakOptions()...)

	engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
	engine.Stop()
	// A second Stop must not panic, deadlock, or double-stop the pools.
	engine.Stop()
	require.False(t, engine.ownsFeelRuntime, "ownership must be cleared after Stop")
	require.False(t, engine.ownsJsRuntime, "ownership must be cleared after Stop")
}

// sharedEngineGoleakOptions returns the goleak options used by engine
// construction tests. The shared package-level bpmnEngine (started in TestMain)
// keeps polling timers in the background. Its timer manager may spawn transient
// timer-waiter goroutines while a test runs, after goleak.IgnoreCurrent() takes
// its snapshot. Those goroutines belong to the shared engine, not the engine
// under test, so they are ignored explicitly.
func sharedEngineGoleakOptions() []goleak.Option {
	return []goleak.Option{
		goleak.IgnoreCurrent(),
		goleak.IgnoreTopFunction("github.com/pbinitiative/zenbpm/pkg/bpmn.(*timerManager).addWaitingTimer.func1"),
	}
}

// stopCountingFeelRuntime is a caller-owned script.FeelRuntime stub that counts
// how many times the engine tried to stop it.
type stopCountingFeelRuntime struct {
	stopCalls atomic.Int64
}

func (r *stopCountingFeelRuntime) UnaryTest(string, map[string]any) (bool, error) {
	return true, nil
}

func (r *stopCountingFeelRuntime) Evaluate(string, map[string]any) (any, error) {
	return nil, nil
}

func (r *stopCountingFeelRuntime) Stop() {
	r.stopCalls.Add(1)
}

// stopCountingJsRuntime is a caller-owned script.JsRuntime stub that counts how
// many times the engine tried to stop it.
type stopCountingJsRuntime struct {
	stopCalls atomic.Int64
}

func (r *stopCountingJsRuntime) RunScript(string) (any, error) {
	return nil, nil
}

func (r *stopCountingJsRuntime) Stop() {
	r.stopCalls.Add(1)
}

type engineConstructionTracker struct {
	feelRuntime           *stopCountingFeelRuntime
	jsRuntime             *stopCountingJsRuntime
	feelConstructionCalls atomic.Int64
	jsConstructionCalls   atomic.Int64
	dmnConstructionCalls  atomic.Int64
	dmnFeelRuntime        script.FeelRuntime
}

func newEngineConstructionTracker() *engineConstructionTracker {
	return &engineConstructionTracker{
		feelRuntime: &stopCountingFeelRuntime{},
		jsRuntime:   &stopCountingJsRuntime{},
	}
}

func (tracker *engineConstructionTracker) factories() engineFactories {
	return engineFactories{
		newFeelRuntime: func() script.FeelRuntime {
			tracker.feelConstructionCalls.Add(1)
			return tracker.feelRuntime
		},
		newJsRuntime: func() script.JsRuntime {
			tracker.jsConstructionCalls.Add(1)
			return tracker.jsRuntime
		},
		newDmnEngine: func(persistence storage.Storage, feelRuntime script.FeelRuntime) *dmn.ZenDmnEngine {
			tracker.dmnConstructionCalls.Add(1)
			tracker.dmnFeelRuntime = feelRuntime
			return dmn.NewEngine(
				dmn.EngineWithStorage(persistence),
				dmn.EngineWithFeel(feelRuntime),
			)
		},
	}
}
