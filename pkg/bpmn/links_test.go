package bpmn

import (
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func Test_Link_events_are_thrown_and_caught_and_flow_continued(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-link-events.bpmn")
	bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	instance, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)

	// then
	assert.Nil(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)
	assert.Equal(t, "Task-A,Task-B", cp.CallPath)
}

func Test_missing_intermediate_link_catch_event_stops_engine_with_error(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-link-event-broken.bpmn")
	bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	instance, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)

	// then
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "missing link intermediate catch event with linkName="))
	assert.Equal(t, runtime.ActivityStateFailed, instance.State)
	assert.Equal(t, "", cp.CallPath)
}

func Test_missing_intermediate_link_variables_mapped(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-link-event-output-variables.bpmn")
	bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	instance, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)

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
