package bpmn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestForkUncontrolledJoin(t *testing.T) {
	// setup
	cp := CallPath{}
	bpmnEngine.clearTaskHandlers()

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/fork-uncontrolled-join.bpmn")
	bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-a-2").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)

	// when
	_, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "id-a-1,id-a-2,id-b-1,id-b-1", cp.CallPath)
}

func TestForkControlledParallelJoin(t *testing.T) {
	// setup
	cp := CallPath{}
	bpmnEngine.clearTaskHandlers()

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/fork-controlled-parallel-join.bpmn")
	bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-a-2").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)

	// when
	_, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "id-a-1,id-a-2,id-b-1", cp.CallPath)
}

func TestForkControlledExclusiveJoin(t *testing.T) {
	// setup
	cp := CallPath{}
	bpmnEngine.clearTaskHandlers()

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/fork-controlled-exclusive-join.bpmn")
	bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-a-2").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)

	// when
	_, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "id-a-1,id-a-2,id-b-1,id-b-1", cp.CallPath)
}
