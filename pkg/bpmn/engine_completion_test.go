package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessWithOnlyStartEventCompletes verifies that a process consisting of
// just a start event (no outgoing flows, no end event) completes immediately
// after being started, instead of remaining stuck in Active state.
func TestProcessWithOnlyStartEventCompletes(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/start-event-only.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
}

// TestLinearProcessStillCompletes is a regression test ensuring that the normal
// StartEvent → ServiceTask → EndEvent flow still completes correctly after the
// auto-completion logic is introduced.
func TestLinearProcessStillCompletes(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	require.NoError(t, err)

	handler := bpmnEngine.NewTaskHandler().Type("TestType").Handler(func(job ActivatedJob) {
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(handler)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
}

// TestProcessWaitingForMessageRemainsActive verifies that a process waiting on
// an intermediate message catch event is NOT incorrectly auto-completed by the
// new logic — it must remain Active until the message is published.
func TestProcessWaitingForMessageRemainsActive(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple-intermediate-message-catch-event.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().State)
}

// TestProcessWaitingForTimerRemainsActive verifies that a process blocked on a
// timer catch event is NOT incorrectly auto-completed — it must remain Active
// until the timer fires.
func TestProcessWaitingForTimerRemainsActive(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple-timer-catch-event.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().State)
}

// TestProcessWaitingForUserTaskRemainsActive verifies that a process blocked on
// a user task is NOT incorrectly auto-completed — it must remain Active until
// the task is completed by a user.
func TestProcessWaitingForUserTaskRemainsActive(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple-user-task.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().State)
}
