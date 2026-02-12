package feel

import (
	"context"
	_ "embed"
	"github.com/dop251/goja"
	"github.com/pbinitiative/zenbpm/pkg/script"
)

type FeelinRunnerFactory struct {
}

func (factory FeelinRunnerFactory) NewRunner() script.Runner {
	return newFeelRunner()
}

type FeelinRuntime struct {
	pool *script.RunnerPool
}

func NewFeelinRuntime(ctx context.Context, maxVmPoolSize int, minVmPoolSize int) script.FeelRuntime {
	return &FeelinRuntime{
		pool: script.NewRunnerPool(ctx, FeelinRunnerFactory{}, maxVmPoolSize, minVmPoolSize),
	}
}

func (r *FeelinRuntime) UnaryTest(expression string, variableContext map[string]any) (bool, error) {
	var runner = r.pool.GetRunnerFromPool()
	defer r.pool.ReturnRunnerToPool(runner)

	return (*runner.(*FeelinRunner).unaryTest)(expression, variableContext)
}

func (r *FeelinRuntime) Evaluate(expression string, variableContext map[string]any) (any, error) {
	var runner = r.pool.GetRunnerFromPool()
	defer r.pool.ReturnRunnerToPool(runner)

	return (*runner.(*FeelinRunner).evalFunc)(expression, variableContext)
}

//go:embed feelin/index.esm.js
var feelinSource string

type FeelinRunner struct {
	vm        *goja.Runtime
	evalFunc  *func(expression string, variableContext map[string]any) (any, error)
	unaryTest *func(expression string, variableContext map[string]any) (bool, error)
}

func (r *FeelinRunner) Runner() {}

func newFeelRunner() *FeelinRunner {
	r := FeelinRunner{vm: goja.New()}
	//TODO: this can be optimized into using already compiled *Program to skip compilation step
	_, err := r.vm.RunString(feelinSource)
	if err != nil {
		panic(err)
	}
	r.exportEvaluate()
	r.exportUnaryTest()
	return &r
}

func (r *FeelinRunner) exportEvaluate() {
	var evalFunc func(expression string, variableContext map[string]any) (any, error)
	err := r.vm.ExportTo(r.vm.Get("evaluate"), &evalFunc)
	if err != nil {
		panic(err)
	}
	r.evalFunc = &evalFunc
}

func (r *FeelinRunner) exportUnaryTest() {
	var unaryTest func(expression string, variableContext map[string]any) (bool, error)
	err := r.vm.ExportTo(r.vm.Get("unaryTest"), &unaryTest)
	if err != nil {
		panic(err)
	}
	r.unaryTest = &unaryTest
}
