package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func Test_Link_events_are_thrown_and_caught_and_flow_continued(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-link-events.bpmn")
	h := bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	// then
	assert.Nil(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)
	assert.Equal(t, "Task-A,Task-B", cp.CallPath)
}

func Test_missing_intermediate_link_catch_event_stops_engine_with_error(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-link-event-broken.bpmn")
	h := bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.Error(t, err)

	// then
	assert.ErrorContains(t, err, "failed to find link")
	assert.Equal(t, runtime.ActivityStateFailed, instance.State)
	assert.Equal(t, "", cp.CallPath)

	instanceDb, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, instanceDb.State)
}

func Test_missing_intermediate_link_variables_mapped(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-link-event-output-variables.bpmn")
	h := bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

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
