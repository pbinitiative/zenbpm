package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func TestLinkEventsAreThrownAndCaughtAndFlowContinued(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-link-events.bpmn")
	h := bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil, nil)

	// then
	assert.Nil(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)
	assert.Equal(t, "Task-A,Task-B", cp.CallPath)
}

func TestMissingIntermediateLinkCatchEventStopsEngineWithError(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-link-event-broken.bpmn")
	h := bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil, nil)
	assert.Error(t, err)

	// then
	assert.ErrorContains(t, err, "failed to find link")
	assert.Equal(t, runtime.ActivityStateFailed, instance.State)
	assert.Equal(t, "", cp.CallPath)

	instanceDb, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, instanceDb.State)
}

func TestMissingIntermediateLinkVariablesMapped(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-link-event-output-variables.bpmn")
	h := bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil, nil)

	assert.Nil(t, err)
	assert.Equal(t, instance.State, runtime.ActivityStateCompleted)
	assert.Equal(t, "Task", cp.CallPath)

	// then
	assert.NotNil(t, instance.GetVariable("throw"))
	assert.Equal(t, "throw", instance.GetVariable("throw").(string))
	// then
	assert.NotNil(t, instance.GetVariable("catch"))
	assert.Equal(t, "catch", instance.GetVariable("catch").(string))
}
