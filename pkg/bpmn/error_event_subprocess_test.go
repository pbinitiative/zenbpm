package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJobErrorCaughtByMatchingErrorEventSubprocess verifies that a job error whose error code matches
// an interrupting error event subprocess defined in the same scope is caught: the failing job is
// terminated, the event subprocess runs and the parent process instance completes without incident.
func TestJobErrorCaughtByMatchingErrorEventSubprocess(t *testing.T) {
	instance := createErrorEventSubprocessInstance(t, "error-event-subprocess-interrupting.bpmn")

	job := findJobForProcessInstance(instance.ProcessInstance().Key, "service-task-error-event-subprocess")
	require.NotZero(t, job.Key, "expected to find the main service task job")

	require.NoError(t, bpmnEngine.JobFailByKey(t.Context(), job.Key, "boom", new("42"), nil))

	completed := waitForErrorEventSubprocessParentCompleted(t, instance.ProcessInstance().Key)
	assert.Equal(t, "error-caught", completed.ProcessInstance().GetVariable("subProcessResult"))
	assert.Equal(t, "errorStartEventValue", completed.ProcessInstance().GetVariable("errorStartEventVar"))
	assertJobState(t, job, runtime.ActivityStateTerminated)
	assertIncidentCount(t, completed, 0)
	assertTokenElementIds(t, completed, nil, []string{"should-not-happen-end"})
}

// TestJobErrorWithNonMatchingCodeIsNotCaughtByErrorEventSubprocess verifies that a job error whose
// error code does not match the error event subprocess falls through to incident handling and leaves
// the parent process instance active.
func TestJobErrorWithNonMatchingCodeIsNotCaughtByErrorEventSubprocess(t *testing.T) {
	instance := createErrorEventSubprocessInstance(t, "error-event-subprocess-interrupting.bpmn")

	job := findJobForProcessInstance(instance.ProcessInstance().Key, "service-task-error-event-subprocess")
	require.NotZero(t, job.Key, "expected to find the main service task job")

	require.NoError(t, bpmnEngine.JobFailByKey(t.Context(), job.Key, "boom", new("99"), nil))

	persisted, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateActive, persisted.ProcessInstance().State)

	assertJobState(t, job, runtime.ActivityStateFailed)
	assertIncidentCount(t, persisted, 1)

	_, hasChild := findChildProcessInstanceByParentAndType(instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess)
	assert.False(t, hasChild, "no error event subprocess instance should be started for a non-matching error code")
}

// TestJobErrorCaughtByCatchAllErrorEventSubprocess verifies that an error event subprocess whose start
// event has no errorRef (catch-all) catches an error of any code.
func TestJobErrorCaughtByCatchAllErrorEventSubprocess(t *testing.T) {
	instance := createErrorEventSubprocessInstance(t, "error-event-subprocess-catch-all.bpmn")

	job := findJobForProcessInstance(instance.ProcessInstance().Key, "service-task-error-event-subprocess")
	require.NotZero(t, job.Key, "expected to find the main service task job")

	require.NoError(t, bpmnEngine.JobFailByKey(t.Context(), job.Key, "boom", new("any-arbitrary-error"), nil))

	completed := waitForErrorEventSubprocessParentCompleted(t, instance.ProcessInstance().Key)
	assert.Equal(t, "catch-all-caught", completed.ProcessInstance().GetVariable("subProcessResult"))
	assertJobState(t, job, runtime.ActivityStateTerminated)
	assertIncidentCount(t, completed, 0)
}

// TestErrorBoundaryEventPrioritizedOverErrorEventSubprocess verifies that, when both an error boundary
// event attached to the throwing activity and an error event subprocess in the same scope match the
// thrown error code, the boundary event takes precedence and the event subprocess is not started.
func TestErrorBoundaryEventPrioritizedOverErrorEventSubprocess(t *testing.T) {
	instance := createErrorEventSubprocessInstance(t, "error-event-subprocess-boundary-priority.bpmn")

	job := findJobForProcessInstance(instance.ProcessInstance().Key, "service-task-error-boundary-priority")
	require.NotZero(t, job.Key, "expected to find the main service task job")

	require.NoError(t, bpmnEngine.JobFailByKey(t.Context(), job.Key, "boom", new("42"), nil))

	completed := waitForErrorEventSubprocessParentCompleted(t, instance.ProcessInstance().Key)
	assert.Equal(t, "boundary", completed.ProcessInstance().GetVariable("caughtBy"),
		"the error boundary event should have caught the error, not the event subprocess")
	assertJobState(t, job, runtime.ActivityStateTerminated)
	assertIncidentCount(t, completed, 0)
	assertTokenElementIds(t, completed, []string{"handled-end"}, []string{"should-not-happen-end", "event-subprocess-end"})

	_, hasChild := findChildProcessInstanceByParentAndType(instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess)
	assert.False(t, hasChild, "no error event subprocess instance should be started when a boundary event wins")
}

// TestSpecificErrorEventSubprocessPrioritizedOverCatchAll verifies that, within a single scope, an error
// event subprocess referencing a specific error code is prioritized over a catch-all error event subprocess.
func TestSpecificErrorEventSubprocessPrioritizedOverCatchAll(t *testing.T) {
	instance := createErrorEventSubprocessInstance(t, "error-event-subprocess-specific-over-catch-all.bpmn")

	job := findJobForProcessInstance(instance.ProcessInstance().Key, "service-task-error-event-subprocess")
	require.NotZero(t, job.Key, "expected to find the main service task job")

	require.NoError(t, bpmnEngine.JobFailByKey(t.Context(), job.Key, "boom", new("42"), nil))

	completed := waitForErrorEventSubprocessParentCompleted(t, instance.ProcessInstance().Key)
	assert.Equal(t, "specific", completed.ProcessInstance().GetVariable("caughtBy"),
		"the error code-specific event subprocess should have priority over the catch-all one")
	assertJobState(t, job, runtime.ActivityStateTerminated)
	assertIncidentCount(t, completed, 0)
}

// TestErrorEndEventCaughtByParentErrorEventSubprocess verifies that an error thrown by an error end event
// inside an embedded subprocess propagates up and is caught by an error event subprocess defined in the
// parent process scope.
func TestErrorEndEventCaughtByParentErrorEventSubprocess(t *testing.T) {
	instance := createErrorEventSubprocessInstance(t, "error-event-subprocess-end-error-propagation.bpmn")

	completed := waitForErrorEventSubprocessParentCompleted(t, instance.ProcessInstance().Key)
	assert.Equal(t, "end-error-caught", completed.ProcessInstance().GetVariable("subProcessResult"))
	assertIncidentCount(t, completed, 0)
	assertTokenElementIds(t, completed, nil, []string{"should-not-happen-end"})
}

// TestErrorEndEventCaughtBySameScopeErrorEventSubprocess verifies that an error thrown by an error
// end event is caught by an error event subprocess declared in that same scope before propagating to
// any parent scope or creating an unhandled-error incident.
func TestErrorEndEventCaughtBySameScopeErrorEventSubprocess(t *testing.T) {
	instance := createErrorEventSubprocessInstance(t, "error-event-subprocess-same-scope-end-error.bpmn")

	completed := waitForErrorEventSubprocessParentCompleted(t, instance.ProcessInstance().Key)
	assert.Equal(t, "same-scope-end-error-caught", completed.ProcessInstance().GetVariable("subProcessResult"))
	assertIncidentCount(t, completed, 0)
}

// TestNestedJobErrorCaughtByParentErrorEventSubprocess verifies that a job error thrown deep inside an
// embedded subprocess propagates up the scope hierarchy and is caught by an error event subprocess in
// the parent process scope.
func TestNestedJobErrorCaughtByParentErrorEventSubprocess(t *testing.T) {
	instance := createErrorEventSubprocessInstance(t, "error-event-subprocess-nested-job-error.bpmn")

	var childInstance runtime.ProcessInstance
	require.Eventually(t, func() bool {
		var found bool
		childInstance, found = findChildProcessInstanceByParentAndType(instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess)
		return found
	}, time.Second, 20*time.Millisecond, "expected the embedded subprocess child instance to be created")

	var job runtime.Job
	require.Eventually(t, func() bool {
		job = findJobForProcessInstance(childInstance.ProcessInstance().Key, "nested-service-task")
		return job.Key != 0
	}, time.Second, 20*time.Millisecond, "expected to find the nested service task job")

	require.NoError(t, bpmnEngine.JobFailByKey(t.Context(), job.Key, "boom", new("42"), nil))

	completed := waitForErrorEventSubprocessParentCompleted(t, instance.ProcessInstance().Key)
	assert.Equal(t, "nested-job-error-caught", completed.ProcessInstance().GetVariable("subProcessResult"))
	assertJobState(t, job, runtime.ActivityStateTerminated)
	assertIncidentCount(t, completed, 0)

	embeddedChild, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), childInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, embeddedChild.ProcessInstance().State,
		"the embedded subprocess that threw the error should be terminated")
}

func createErrorEventSubprocessInstance(t *testing.T, filename string) runtime.ProcessInstance {
	t.Helper()

	processDefinition, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_event_subprocess/"+filename)
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), processDefinition.Key, nil)
	require.NoError(t, err)

	return instance
}

func waitForErrorEventSubprocessParentCompleted(t *testing.T, processInstanceKey int64) runtime.ProcessInstance {
	t.Helper()

	var persisted runtime.ProcessInstance
	require.Eventually(t, func() bool {
		var err error
		persisted, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), processInstanceKey)
		require.NoError(t, err)
		return persisted.ProcessInstance().State == runtime.ActivityStateCompleted
	}, 2*time.Second, 20*time.Millisecond, "the parent process instance should complete after the error event subprocess runs")

	return persisted
}
