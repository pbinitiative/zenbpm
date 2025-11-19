package feel

import (
	_ "embed"
	"github.com/dop251/goja"
	"sync/atomic"
	"time"
)

//go:embed feelin/index.esm.js
var feelinSource string

type FeelinRuntime struct {
	vm        *goja.Runtime
	evalFunc  atomic.Pointer[func(expression string, variableContext map[string]any) any]
	unaryTest atomic.Pointer[func(expression string, variableContext map[string]any) bool]
}

func NewFeelinRuntime() *FeelinRuntime {
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

	runtime := FeelinRuntime{}
	runtime.unaryTest.Store(&unaryTestFunc)
	runtime.evalFunc.Store(&evaluateFunc)
	runtime.vm = vm

	go func() {
		for {
			time.Sleep(5 * time.Minute)
			vm = goja.New()
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
			runtime.unaryTest.Store(&unaryTestFunc)
			runtime.evalFunc.Store(&evaluateFunc)
			runtime.vm = vm
		}
	}()

	return &runtime
}

func exportEvaluate(vm *goja.Runtime) (func(expression string, variableContext map[string]any) any, error) {
	var evalFunc func(expression string, variableContext map[string]any) any
	err := vm.ExportTo(vm.Get("evaluate"), &evalFunc)
	if err != nil {
		return nil, err
	}
	return evalFunc, nil
}

func exportUnaryTest(vm *goja.Runtime) (func(expression string, variableContext map[string]any) bool, error) {
	var unaryTest func(expression string, variableContext map[string]any) bool
	err := vm.ExportTo(vm.Get("unaryTest"), &unaryTest)
	if err != nil {
		return nil, err
	}
	return unaryTest, nil
}

func (r *FeelinRuntime) UnaryTest(expression string, variableContext map[string]any) bool {
	unaryTest := *r.unaryTest.Load()
	return unaryTest(expression, variableContext)
}

func (r *FeelinRuntime) Evaluate(expression string, variableContext map[string]any) any {
	evalFunc := *r.evalFunc.Load()
	return evalFunc(expression, variableContext)
}
