package js

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dop251/goja"
	"github.com/pbinitiative/zenbpm/pkg/script"
)

type JsRunnerFactory struct {
}

func (JsRunnerFactory) NewRunner() script.Runner {
	return newJsRunner()
}

type JsRuntime struct {
	pool *script.RunnerPool
}

func (r *JsRuntime) ScriptRuntime() {}

func NewJsRuntime(ctx context.Context, maxVmPoolSize int, minVmPoolSize int) *JsRuntime {
	return &JsRuntime{
		pool: script.NewRunnerPool(ctx, JsRunnerFactory{}, maxVmPoolSize, minVmPoolSize),
	}
}

func (r *JsRuntime) RunScript(script string) (any, error) {
	var runner = r.pool.GetRunnerFromPool()
	defer r.pool.ReturnRunnerToPool(runner)

	return runner.(*JsRunner).runScript(script)
}

type JsRunner struct {
	vm *goja.Runtime
}

func (r *JsRunner) Runner() {}

func newJsRunner() *JsRunner {
	r := JsRunner{vm: goja.New()}
	return &r
}

// TODO: we need to add a method to Goja to compile this without the global context
func (r *JsRunner) runScript(script string) (interface{}, error) {
	resp, err := r.vm.RunString(script)
	if err != nil {
		return resp, fmt.Errorf("error running script \"%s\" : %v", script, err)
	}
	return resp, nil
}
