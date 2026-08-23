package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/require"
)

func TestInterruptedActivityHistory(t *testing.T) {
	t.Run("closes a service task interrupted by an error boundary", func(t *testing.T) {
		processInstance := createProcessInstance(t, "service_task/service_task_with_error_boundary_event.bpmn")
		job := findJobForProcessInstance(processInstance.ProcessInstance().Key, "service-task-error-boundary")
		require.NotZero(t, job.Key, "expected the service task job to be created")

		errorCode := "42"
		require.NoError(t, bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, nil))
		waitForProcessInstanceState(t, bpmnEngine.persistence, processInstance.ProcessInstance().Key, runtime.ActivityStateCompleted)

		requireFlowElementHistoryCompleted(t, processInstance.ProcessInstance().Key, "service-task-error-boundary")
	})

	t.Run("closes a service task interrupted by a message boundary", func(t *testing.T) {
		process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-boundary-event-interrupting.bpmn")
		require.NoError(t, err)
		processInstance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
		require.NoError(t, err)

		correlationKey := "message-boundary-event-interruptingCorrelationKey"
		require.NoError(t, bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", &correlationKey, nil))
		waitForProcessInstanceState(t, bpmnEngine.persistence, processInstance.ProcessInstance().Key, runtime.ActivityStateCompleted)

		requireFlowElementHistoryCompleted(t, processInstance.ProcessInstance().Key, "service-task-id")
	})

	t.Run("closes a service task interrupted by a timer boundary", func(t *testing.T) {
		process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/timer-boundary-event-interrupting.bpmn")
		require.NoError(t, err)
		processInstance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
		require.NoError(t, err)

		timers, err := bpmnEngine.persistence.FindProcessInstanceTimers(t.Context(), processInstance.ProcessInstance().Key, runtime.TimerStateCreated)
		require.NoError(t, err)
		require.Len(t, timers, 1)
		instanceToResume, tokens, err := bpmnEngine.TriggerTimer(t.Context(), timers[0])
		require.NoError(t, err)
		require.NotNil(t, instanceToResume)
		require.NoError(t, bpmnEngine.RunProcessInstance(t.Context(), *instanceToResume, tokens))
		waitForProcessInstanceState(t, bpmnEngine.persistence, processInstance.ProcessInstance().Key, runtime.ActivityStateCompleted)

		requireFlowElementHistoryCompleted(t, processInstance.ProcessInstance().Key, "service-task-id")
	})

	t.Run("closes a service task interrupted by an error event subprocess", func(t *testing.T) {
		processInstance := createErrorEventSubprocessInstance(t, "error-event-subprocess-interrupting.bpmn")
		job := findJobForProcessInstance(processInstance.ProcessInstance().Key, "service-task-error-event-subprocess")
		require.NotZero(t, job.Key, "expected the service task job to be created")

		require.NoError(t, bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected event subprocess error", new("42"), nil))
		waitForProcessInstanceState(t, bpmnEngine.persistence, processInstance.ProcessInstance().Key, runtime.ActivityStateCompleted)

		requireFlowElementHistoryCompleted(t, processInstance.ProcessInstance().Key, "service-task-error-event-subprocess")
	})

	t.Run("closes a user task when its process instance is canceled", func(t *testing.T) {
		process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/user-tasks-with-assignments.bpmn")
		require.NoError(t, err)
		processInstance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
		require.NoError(t, err)

		require.NoError(t, bpmnEngine.CancelInstanceByKey(t.Context(), processInstance.ProcessInstance().Key))
		waitForProcessInstanceState(t, bpmnEngine.persistence, processInstance.ProcessInstance().Key, runtime.ActivityStateTerminated)

		requireFlowElementHistoryCompleted(t, processInstance.ProcessInstance().Key, "assignee-task")
	})

	t.Run("closes a user task terminated by process instance modification", func(t *testing.T) {
		process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/user-tasks-with-assignments.bpmn")
		require.NoError(t, err)
		processInstance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
		require.NoError(t, err)

		activeTokens, err := bpmnEngine.persistence.GetActiveTokensForProcessInstance(t.Context(), processInstance.ProcessInstance().Key)
		require.NoError(t, err)
		userTaskToken, found := findTokenByElementID(activeTokens, "assignee-task")
		require.True(t, found, "expected an active token on the user task")
		_, _, err = bpmnEngine.ModifyInstance(t.Context(), processInstance.ProcessInstance().Key, []int64{userTaskToken.ElementInstanceKey}, nil, nil)
		require.NoError(t, err)

		requireFlowElementHistoryCompleted(t, processInstance.ProcessInstance().Key, "assignee-task")
	})

	t.Run("closes an embedded subprocess caught by a parent error event subprocess", func(t *testing.T) {
		processInstance := createErrorEventSubprocessInstance(t, "error-event-subprocess-end-error-propagation.bpmn")
		waitForProcessInstanceState(t, bpmnEngine.persistence, processInstance.ProcessInstance().Key, runtime.ActivityStateCompleted)

		requireFlowElementHistoryCompleted(t, processInstance.ProcessInstance().Key, "embedded-sub")
	})

	t.Run("closes every call activity while an error end event propagates", func(t *testing.T) {
		_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_end_event/call_activity/call_activity_nested_with_error_end_event_leaf.bpmn")
		require.NoError(t, err)
		_, err = bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_end_event/call_activity/call_activity_nested_with_error_end_event_middle.bpmn")
		require.NoError(t, err)
		process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_end_event/call_activity/call_activity_nested_with_error_end_event_root_matching.bpmn")
		require.NoError(t, err)

		processInstance, zerr := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
		require.Nil(t, zerr)
		waitForProcessInstanceState(t, bpmnEngine.persistence, processInstance.ProcessInstance().Key, runtime.ActivityStateCompleted)

		requireFlowElementHistoryCompleted(t, processInstance.ProcessInstance().Key, "Activity_01fl3mz")
		middleInstance := findProcessInstanceByProcessIdAndParent(processInstance.ProcessInstance().Key, "nested_call_activity_error_end_event_middle")
		require.NotNil(t, middleInstance)
		requireFlowElementHistoryCompleted(t, middleInstance.ProcessInstance().Key, "call_activity")
	})
}

func requireFlowElementHistoryCompleted(t testing.TB, processInstanceKey int64, elementID string) {
	t.Helper()

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), processInstanceKey, true)
	require.NoError(t, err)

	for _, flowElement := range flowElements {
		if flowElement.ElementId == elementID {
			require.NotNil(t, flowElement.CompletedAt, "flow element %s should no longer be active", elementID)
			return
		}
	}

	require.Failf(t, "missing flow element history", "flow element %s was not recorded", elementID)
}
