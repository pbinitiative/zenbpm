package feel

import (
	_ "embed"
	"github.com/dop251/goja"
)

//go:embed feelin/index.esm.js
var feelinSource string

type Runner struct {
	vm        *goja.Runtime
	evalFunc  *func(expression string, variableContext map[string]any) (any, error)
	unaryTest *func(expression string, variableContext map[string]any) (bool, error)
}

func newRunner() *Runner {
	r := Runner{vm: goja.New()}
	_, err := r.vm.RunString(feelinSource)
	if err != nil {
		panic(err)
	}
	r.exportEvaluate()
	r.exportUnaryTest()
	return &r
}

func (r *Runner) exportEvaluate() {
	var evalFunc func(expression string, variableContext map[string]any) (any, error)
	err := r.vm.ExportTo(r.vm.Get("evaluate"), &evalFunc)
	if err != nil {
		panic(err)
	}
	r.evalFunc = &evalFunc
}

func (r *Runner) exportUnaryTest() {
	var unaryTest func(expression string, variableContext map[string]any) (bool, error)
	err := r.vm.ExportTo(r.vm.Get("unaryTest"), &unaryTest)
	if err != nil {
		panic(err)
	}
	r.unaryTest = &unaryTest
}
