package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"go.uber.org/goleak"
)

func TestEngineWithStorageDoesNotCreateAdditionalFeelRuntime(t *testing.T) {
	// The shared package-level bpmnEngine (started in TestMain) keeps polling timers in the
	// background. Its timer manager may spawn transient timer-waiter goroutines while this
	// test runs, after goleak.IgnoreCurrent() takes its snapshot. Those goroutines belong to
	// the shared engine, not the engine under test, so ignore them explicitly.
	defer goleak.VerifyNone(t,
		goleak.IgnoreCurrent(),
		goleak.IgnoreTopFunction("github.com/pbinitiative/zenbpm/pkg/bpmn.(*timerManager).addWaitingTimer.func1"),
	)

	engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
	defer func() {
		engine.contextCancel()
		engine.feelRuntime.Stop()
		engine.jsRuntime.Stop()
	}()
}
