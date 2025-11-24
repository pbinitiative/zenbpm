package feel

import (
	_ "embed"
	"github.com/dop251/goja"
)

//go:embed feelin/index.esm.js
var feelinSource string

type FeelinRuntime struct {
	vmPool        chan *goja.Runtime
	evalFuncPool  chan *func(expression string, variableContext map[string]any) any
	unaryTestPool chan *func(expression string, variableContext map[string]any) bool
}

func NewFeelinRuntime() *FeelinRuntime {
	const vmPoolSize = 1
	runtime := FeelinRuntime{
		vmPool:        make(chan *goja.Runtime, vmPoolSize),
		evalFuncPool:  make(chan *func(expression string, variableContext map[string]any) any, vmPoolSize),
		unaryTestPool: make(chan *func(expression string, variableContext map[string]any) bool, vmPoolSize),
	}

	for i := 0; i < vmPoolSize; i++ {
		vm := goja.New()
		_, err := vm.RunString(feelinSource)
		if err != nil {
			panic(err)
		}
		evaluateFunc, err := exportEvaluate(vm)
		if err != nil {
			panic(err)
		}
		unaryTestFunc, err := exportUnaryTest(vm)
		if err != nil {
			panic(err)
		}

		runtime.vmPool <- vm
		runtime.evalFuncPool <- evaluateFunc
		runtime.unaryTestPool <- unaryTestFunc
	}

	return &runtime
}

func exportEvaluate(vm *goja.Runtime) (*func(expression string, variableContext map[string]any) any, error) {
	var evalFunc func(expression string, variableContext map[string]any) any
	err := vm.ExportTo(vm.Get("evaluate"), &evalFunc)
	if err != nil {
		return nil, err
	}
	return &evalFunc, nil
}

func exportUnaryTest(vm *goja.Runtime) (*func(expression string, variableContext map[string]any) bool, error) {
	var unaryTest func(expression string, variableContext map[string]any) bool
	err := vm.ExportTo(vm.Get("unaryTest"), &unaryTest)
	if err != nil {
		return nil, err
	}
	return &unaryTest, nil
}

func (r *FeelinRuntime) UnaryTest(expression string, variableContext map[string]any) bool {
	unaryTest := <-r.unaryTestPool
	defer func() { r.unaryTestPool <- unaryTest }()
	return (*unaryTest)(expression, variableContext)
}

func (r *FeelinRuntime) Evaluate(expression string, variableContext map[string]any) any {
	evalFunc := <-r.evalFuncPool
	defer func() { r.evalFuncPool <- evalFunc }()
	return (*evalFunc)(expression, variableContext)
}
