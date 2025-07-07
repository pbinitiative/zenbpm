package bpmn

import (
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func TestEventBasedGatewaySelectsPathWhereTimerOccurs(t *testing.T) {
	cp := CallPath{}

	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-timer-event.bpmn")
	mH := bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(mH)
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	_, _ = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	time.Sleep(2 * time.Second)

	assert.Equal(t, "task-for-timer", cp.CallPath)
}

func TestInvalidTimerWillStopExecutionAndReturnErr(t *testing.T) {
	cp := CallPath{}

	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-invalid-timer-event.bpmn")
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	assert.Equal(t, runtime.ActivityStateFailed, instance.State)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Can't find 'timeDuration' value for INTERMEDIATE_CATCH_EVENT with id=TimerEventDefinition_0he1igl"))
	assert.Equal(t, "", cp.CallPath)
}

func TestEventBasedGatewaySelectsJustOnePath(t *testing.T) {
	cp := CallPath{}

	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-timer-event.bpmn")
	mH := bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(mH)
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	instance, _ := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	err := bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "message", nil)
	time.Sleep((2 * time.Second) + (1 * time.Millisecond))
	assert.Nil(t, err)

	assert.True(t, strings.HasPrefix(cp.CallPath, "task-for-message"))
	assert.NotContains(t, cp.CallPath, ",")

	cp.CallPath = ""
	instance, _ = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	time.Sleep((2 * time.Second) + (1 * time.Millisecond))
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "message", nil)
	assert.Nil(t, err)

	assert.True(t, strings.HasPrefix(cp.CallPath, "task-for-timer"))
	assert.NotContains(t, cp.CallPath, ",")
}
