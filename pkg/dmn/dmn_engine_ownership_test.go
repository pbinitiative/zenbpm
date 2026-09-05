package dmn

import (
	"sync"
	"sync/atomic"
	"testing"

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
