package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"go.uber.org/goleak"
)

func TestEngineWithStorageDoesNotCreateAdditionalFeelRuntime(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
	defer func() {
		engine.contextCancel()
		engine.feelRuntime.Stop()
		engine.jsRuntime.Stop()
	}()
}
