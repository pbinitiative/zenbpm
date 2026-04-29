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
	require.Error(t, err)

	assert.Equal(t, runtime.ActivityStateFailed, instance.ProcessInstance().State)
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

// TestJobCompletionCancelsEventSubprocessTimer verifies that when a service task job
// is completed and the process instance reaches its end event, any active timers
// belonging to event subprocess timer start events are transitioned to
// TimerStateCancelled.
func TestJobCompletionCancelsEventSubprocessTimer(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/timer-event-subprocess-interrupting.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	require.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().State)

	// Find the active job for the service task
	var jobKey int64
	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == instance.ProcessInstance().Key &&
			job.Type == "input-task-timer-event-subprocess-interrupting" &&
			job.State == runtime.ActivityStateActive {
			jobKey = job.Key
			break
		}
	}
	require.NotZero(t, jobKey, "expected to find an active job for input-task-timer-event-subprocess-interrupting")

	// Before completing the job the event subprocess timer should be in TimerStateCreated
	createdTimers, err := bpmnEngine.persistence.FindProcessInstanceTimers(t.Context(), instance.ProcessInstance().Key, runtime.TimerStateCreated)
	require.NoError(t, err)
	found := false
	for _, timer := range createdTimers {
		if timer.ElementId == "subProcessTimerEvent_12i3m6f" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected event subprocess timer to be in TimerStateCreated before job completion")

	// Complete the job
	err = bpmnEngine.JobCompleteByKey(t.Context(), jobKey, nil)
	require.NoError(t, err)

	// The process instance should now be completed
	updatedInstance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, updatedInstance.ProcessInstance().State)

	// The event subprocess timer start event timer should now be in TimerStateCancelled
	cancelledTimers, err := bpmnEngine.persistence.FindProcessInstanceTimers(t.Context(), instance.ProcessInstance().Key, runtime.TimerStateCancelled)
	require.NoError(t, err)
	found = false
	for _, timer := range cancelledTimers {
		if timer.ElementId == "subProcessTimerEvent_12i3m6f" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected event subprocess timer to be in TimerStateCancelled after job completion, got: %+v", cancelledTimers)
}
