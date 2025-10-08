package bpmn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestForkUncontrolledJoin(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/fork-uncontrolled-join.bpmn")
	a1H := bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a1H)
	a2H := bpmnEngine.NewTaskHandler().Id("id-a-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a2H)
	b1H := bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b1H)

	// when
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "id-a-1,id-a-2,id-b-1,id-b-1", cp.CallPath)
}

func TestForkControlledParallelJoin(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/fork-controlled-parallel-join.bpmn")
	a1H := bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a1H)
	a2H := bpmnEngine.NewTaskHandler().Id("id-a-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a2H)
	b1H := bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b1H)

	// when
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "id-a-1,id-a-2,id-b-1", cp.CallPath)
}

func TestForkControlledExclusiveJoin(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/fork-controlled-exclusive-join.bpmn")
	a1H := bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a1H)
	a2H := bpmnEngine.NewTaskHandler().Id("id-a-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a2H)
	b1H := bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b1H)

	// when
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "id-a-1,id-a-2,id-b-1,id-b-1", cp.CallPath)
}
