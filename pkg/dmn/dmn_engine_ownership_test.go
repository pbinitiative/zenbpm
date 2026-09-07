package dmn

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/script"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestDmnDefaultEngineOwnsAndStopsItsFeelRuntime(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	feelRuntime := &stopCountingDmnFeelRuntime{}
	var constructionCalls atomic.Int64
	engine := newEngine(func() script.FeelRuntime {
		constructionCalls.Add(1)
		return feelRuntime
	})

	require.EqualValues(t, 1, constructionCalls.Load(), "default construction must create exactly one FEEL runtime")
	require.True(t, engine.ownsFeelRuntime, "engine must own the default FEEL runtime")
	require.Same(t, feelRuntime, engine.feelRuntime, "engine must retain the runtime created by its factory")

	// Stop must release the engine-owned runtime exactly once.
	engine.Stop()
	engine.Stop()
	require.EqualValues(t, 1, feelRuntime.stopCalls.Load(), "owned FEEL runtime must be stopped exactly once")
}

func TestDmnEngineWithStorageOwnsAndStopsItsFeelRuntime(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	feelRuntime := &stopCountingDmnFeelRuntime{}
	var constructionCalls atomic.Int64
	engine := newEngine(func() script.FeelRuntime {
		constructionCalls.Add(1)
		return feelRuntime
	}, EngineWithStorage(inmemory.NewStorage()))

	require.EqualValues(t, 1, constructionCalls.Load(), "storage-only construction must create exactly one FEEL runtime")
	require.True(t, engine.ownsFeelRuntime, "engine must own the default FEEL runtime")

	engine.Stop()
	engine.Stop()
	require.EqualValues(t, 1, feelRuntime.stopCalls.Load(), "owned FEEL runtime must be stopped exactly once")
}

func TestDmnEngineWithInjectedRuntimeCreatesNoDefaultPoolAndNeverStopsIt(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	feelRuntime := &stopCountingDmnFeelRuntime{}
	var constructionCalls atomic.Int64
	engine := newEngine(func() script.FeelRuntime {
		constructionCalls.Add(1)
		return &stopCountingDmnFeelRuntime{}
	}, EngineWithFeel(feelRuntime))

	require.Zero(t, constructionCalls.Load(), "injected construction must not create a default FEEL runtime")
	require.False(t, engine.ownsFeelRuntime, "injected FEEL runtime must stay caller-owned")
	require.Same(t, feelRuntime, engine.feelRuntime.(*stopCountingDmnFeelRuntime), "injected FEEL runtime must be used as-is")

	engine.Stop()
	engine.Stop()

	require.Zero(t, feelRuntime.stopCalls.Load(), "caller-owned FEEL runtime must not be stopped by the engine")

}

func TestDmnEngineStopIsConcurrentAndExactlyOnce(t *testing.T) {
	feelRuntime := &stopCountingDmnFeelRuntime{}
	engine := &ZenDmnEngine{feelRuntime: feelRuntime, ownsFeelRuntime: true}
	start := make(chan struct{})
	var callers sync.WaitGroup

	for range 100 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			<-start
			engine.Stop()
		}()
	}

	close(start)
	callers.Wait()

	require.EqualValues(t, 1, feelRuntime.stopCalls.Load(), "concurrent shutdown must stop the owned FEEL runtime exactly once")
	require.False(t, engine.ownsFeelRuntime, "shutdown must clear runtime ownership")
}

// TestEngineWithFeelRejectsConstructedEngine verifies that applying EngineWithFeel to an already
// constructed engine panics instead of silently orphaning the engine-owned runtime, and that the
// owned runtime is still released by Stop afterwards.
func TestEngineWithFeelRejectsConstructedEngine(t *testing.T) {
	ownedRuntime := &stopCountingDmnFeelRuntime{}
	injectedRuntime := &stopCountingDmnFeelRuntime{}
	engine := newEngine(func() script.FeelRuntime { return ownedRuntime })
	require.True(t, engine.ownsFeelRuntime)

	require.PanicsWithValue(t,
		"dmn: EngineWithFeel must only be passed to NewEngine; applying it to a constructed engine is not supported",
		func() { EngineWithFeel(injectedRuntime)(engine) })

	require.Same(t, ownedRuntime, engine.feelRuntime.(*stopCountingDmnFeelRuntime), "rejected option must not replace the runtime")
	require.True(t, engine.ownsFeelRuntime, "rejected option must not clear ownership")

	engine.Stop()
	require.EqualValues(t, 1, ownedRuntime.stopCalls.Load(), "owned runtime must still be released by Stop")
	require.Zero(t, injectedRuntime.stopCalls.Load(), "runtime from the rejected option must never be touched")
}

// TestDmnEngineStopWaitsForInFlightShutdown verifies that a Stop call racing with another
// Stop call does not return before the owned runtime has actually finished shutting down.
func TestDmnEngineStopWaitsForInFlightShutdown(t *testing.T) {
	feelRuntime := &blockingStopDmnFeelRuntime{
		stopCountingDmnFeelRuntime: stopCountingDmnFeelRuntime{},
		entered:                    make(chan struct{}),
		release:                    make(chan struct{}),
	}
	engine := &ZenDmnEngine{feelRuntime: feelRuntime, ownsFeelRuntime: true}

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		engine.Stop()
	}()
	<-feelRuntime.entered // first caller is now inside feelRuntime.Stop()

	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		engine.Stop()
	}()

	select {
	case <-secondDone:
		t.Fatal("second Stop returned while the owned runtime shutdown was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(feelRuntime.release)
	require.Eventually(t, func() bool {
		select {
		case <-firstDone:
		default:
			return false
		}
		select {
		case <-secondDone:
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond, "both Stop callers must return once shutdown completes")
	require.True(t, feelRuntime.finished.Load(), "runtime shutdown must have completed before Stop returned")
	require.EqualValues(t, 1, feelRuntime.stopCalls.Load(), "owned FEEL runtime must be stopped exactly once")
}

// stopCountingDmnFeelRuntime is a caller-owned script.DmnFeelRuntime stub that
// counts how many times the engine tried to stop it.
type stopCountingDmnFeelRuntime struct {
	stopCalls atomic.Int64
}

func (r *stopCountingDmnFeelRuntime) UnaryTest(string, map[string]any) (bool, error) {
	return true, nil
}

func (r *stopCountingDmnFeelRuntime) UnaryTestStrict(string, map[string]any) (bool, error) {
	return true, nil
}

func (r *stopCountingDmnFeelRuntime) Evaluate(string, map[string]any) (any, error) {
	return nil, nil
}

func (r *stopCountingDmnFeelRuntime) ValidateExpression(string) error {
	return nil
}

func (r *stopCountingDmnFeelRuntime) ValidateUnaryTest(string) error {
	return nil
}

func (r *stopCountingDmnFeelRuntime) Stop() {
	r.stopCalls.Add(1)
}

// blockingStopDmnFeelRuntime is a script.DmnFeelRuntime stub whose Stop blocks until
// released, so tests can observe an in-flight shutdown.
type blockingStopDmnFeelRuntime struct {
	stopCountingDmnFeelRuntime
	entered  chan struct{}
	release  chan struct{}
	finished atomic.Bool
}

func (r *blockingStopDmnFeelRuntime) Stop() {
	r.stopCountingDmnFeelRuntime.Stop()
	close(r.entered)
	<-r.release
	r.finished.Store(true)
}
