package bpmn

import (
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func Test_EventBasedGateway_selects_path_where_timer_occurs(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-timer-event.bpmn")
	mH := bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(mH)
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	instance, _ := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	// when
	bpmnEngine.PublishEventForInstance(t.Context(), instance.GetInstanceKey(), "message", nil)

	// then
	assert.Equal(t, "task-for-message", cp.CallPath)
}

func Test_InvalidTimer_will_stop_execution_and_return_err(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-invalid-timer-event.bpmn")
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	// then
	assert.Equal(t, runtime.ActivityStateFailed, instance.State)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "Error evaluating expression in intermediate timer cacht event element id="))
	assert.Equal(t, "", cp.CallPath)
}

func Test_EventBasedGateway_selects_path_where_message_received(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-timer-event.bpmn")
	mH := bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(mH)
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	// when
	time.Sleep((1 * time.Second) + (1 * time.Millisecond))

	// then
	assert.Equal(t, "task-for-timer", cp.CallPath)
}

func Test_EventBasedGateway_selects_just_one_path(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-timer-event.bpmn")
	mH := bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(mH)
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	instance, _ := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	// when
	time.Sleep((1 * time.Second) + (1 * time.Millisecond))
	err := bpmnEngine.PublishEventForInstance(t.Context(), instance.GetInstanceKey(), "message", nil)
	assert.Nil(t, err)

	// then
	assert.True(t, strings.HasPrefix(cp.CallPath, "task-for"))
	assert.False(t, strings.Contains(cp.CallPath, ","))
}
