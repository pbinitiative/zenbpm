package bpmn

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobFailOnServiceTaskWithMatchingErrorBoundaryIsCaught(t *testing.T) {

	createdProcessInstance := createProcessInstance(t, "service_task/service_task_with_error_boundary_event.bpmn")

	job := findJobForProcessInstance(createdProcessInstance.ProcessInstance().Key, "service-task-error-boundary")
	require.NotZero(t, job.Key, fmt.Sprintf("expected to find %s job created for process instance", "service-task-error-boundary"))

	errorCode := "42"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, nil)
	assert.NoError(t, err)

	assertProcessInstanceWithoutIncident(t, createdProcessInstance, job)
}

func TestJobFailOnServiceTaskWithMismatchingErrorBoundaryCreatesIncident(t *testing.T) {
	createdProcessInstance := createProcessInstance(t, "service_task/service_task_with_error_boundary_event.bpmn")

	job := findJobForProcessInstance(createdProcessInstance.ProcessInstance().Key, "service-task-error-boundary")
	require.NotZero(t, job.Key, "expected to find service-task-error-boundary job created for process instance")

	errorCode := "99"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected incident", &errorCode, nil)
	assert.NoError(t, err)

	assertProcessInstanceWithIncident(t, createdProcessInstance, job)
}

func TestJobFailOnServiceTaskWithoutErrorCodeCreatesIncident(t *testing.T) {
	createdProcessInstance := createProcessInstance(t, "service_task/service_task_with_error_boundary_event.bpmn")

	job := findJobForProcessInstance(createdProcessInstance.ProcessInstance().Key, "service-task-error-boundary")
	require.NotZero(t, job.Key, "expected to find service-task-error-boundary job created for process instance")

	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "missing error code", nil, nil)
	assert.NoError(t, err)

	assertProcessInstanceWithIncident(t, createdProcessInstance, job)
}

func TestJobFailOnServiceTaskWithoutErrorRefInBoundaryEventShouldCatchAll(t *testing.T) {
	createdProcessInstance := createProcessInstance(t, "service_task/service_task_with_catch_all_error_boundary_event.bpmn")

	job := findJobForProcessInstance(createdProcessInstance.ProcessInstance().Key, "service-task-error-boundary")
	require.NotZero(t, job.Key, "expected to find service-task-error-boundary job created for process instance")

	errorCode := "42"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, nil)
	assert.NoError(t, err)

	assertProcessInstanceWithoutIncident(t, createdProcessInstance, job)
}

func TestBusinessRuleTaskExternalWithoutErrorRefInBoundaryEventShouldCatchAll(t *testing.T) {
	createdProcessInstance := createProcessInstance(t, "business_rule_task/business_rule_task_external_with_catch_all_error_boundary_event.bpmn")

	job := findJobForProcessInstance(createdProcessInstance.ProcessInstance().Key, "boundary-error-business-rule-external")
	require.NotZero(t, job.Key, "expected to find boundary-error-business-rule-external job created for process instance")

	errorCode := "any-error"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, nil)
	assert.NoError(t, err)

	assertProcessInstanceWithoutIncident(t, createdProcessInstance, job)
}

func TestBusinessRuleTaskExternalWithMatchingErrorBoundaryIsCaught(t *testing.T) {
	createdProcessInstance := createProcessInstance(t, "business_rule_task/business_rule_task_external_with_error_boundary_event.bpmn")

	job := findJobForProcessInstance(createdProcessInstance.ProcessInstance().Key, "boundary-error-business-rule-external")
	require.NotZero(t, job.Key, "expected to find boundary-error-business-rule-external job created for process instance")

	errorCode := "42"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, nil)
	assert.NoError(t, err)

	assertProcessInstanceWithoutIncident(t, createdProcessInstance, job)
}

func TestBusinessRuleTaskExternalWithMismatchingErrorBoundaryCreatesIncident(t *testing.T) {
	createdProcessInstance := createProcessInstance(t, "business_rule_task/business_rule_task_external_with_error_boundary_event.bpmn")

	job := findJobForProcessInstance(createdProcessInstance.ProcessInstance().Key, "boundary-error-business-rule-external")
	require.NotZero(t, job.Key, "expected to find boundary-error-business-rule-external job created for process instance")

	errorCode := "99"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected incident", &errorCode, nil)
	assert.NoError(t, err)

	assertProcessInstanceWithIncident(t, createdProcessInstance, job)
}

func TestJobFailOnCallActivityWithCatchAllErrorBoundaryIsCaught(t *testing.T) {
	createdProcessInstance := createCallActivityProcessInstance(t, "call_activity_with_catch_all_error_boundary_event.bpmn")

	var childInstance runtime.CallActivityInstance
	require.Eventually(t, func() bool {
		var found bool
		childInstance, found = findCallActivityChildInstance(createdProcessInstance.ProcessInstance().Key)
		return found
	}, time.Second, 20*time.Millisecond)

	var job runtime.Job
	require.Eventually(t, func() bool {
		job = findJobForProcessInstance(childInstance.ProcessInstance().Key, "id")
		return job.Key != 0
	}, time.Second, 20*time.Millisecond, "expected to find child process job created for call activity")

	errorCode := "any-error"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, nil)
	assert.NoError(t, err)

	parentProcessInstance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), createdProcessInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, parentProcessInstance.ProcessInstance().State)

	childProcess, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), childInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, childProcess.ProcessInstance().State)

	assertJobState(t, job, runtime.ActivityStateTerminated)
	assertIncidentCount(t, parentProcessInstance, 0)

	childIncidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), childInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Len(t, childIncidents, 0)

	assertTokenElementIds(t, parentProcessInstance, []string{"handled-end"}, []string{"should-not-happen-end"})
}

func TestJobFailOnCallActivityWithMatchingErrorBoundaryIsCaught(t *testing.T) {
	createdProcessInstance := createCallActivityProcessInstance(t, "call_activity_with_error_boundary_event.bpmn")

	var childInstance runtime.CallActivityInstance
	require.Eventually(t, func() bool {
		var found bool
		childInstance, found = findCallActivityChildInstance(createdProcessInstance.ProcessInstance().Key)
		return found
	}, time.Second, 20*time.Millisecond)

	var job runtime.Job
	require.Eventually(t, func() bool {
		job = findJobForProcessInstance(childInstance.ProcessInstance().Key, "id")
		return job.Key != 0
	}, time.Second, 20*time.Millisecond, "expected to find child process job created for call activity")

	errorCode := "42"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, nil)
	assert.NoError(t, err)

	processInstance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), createdProcessInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, processInstance.ProcessInstance().State)

	childProcess, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), childInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, childProcess.ProcessInstance().State)

	assertJobState(t, job, runtime.ActivityStateTerminated)

	parentIncidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), processInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Len(t, parentIncidents, 0)

	childIncidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), childInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Len(t, childIncidents, 0)

	assertTokenElementIds(t, processInstance, []string{"handled-end"}, []string{"should-not-happen-end"})
}

func TestJobFailOnCallActivityWithMatchingErrorBoundaryPropagatesVariablesToCatchingScope(t *testing.T) {
	createdProcessInstance := createCallActivityProcessInstance(t, "call_activity_with_error_boundary_event_and_output_mapping.bpmn")

	var childInstance runtime.CallActivityInstance
	require.Eventually(t, func() bool {
		var found bool
		childInstance, found = findCallActivityChildInstance(createdProcessInstance.ProcessInstance().Key)
		return found
	}, time.Second, 20*time.Millisecond)

	var job runtime.Job
	require.Eventually(t, func() bool {
		job = findJobForProcessInstance(childInstance.ProcessInstance().Key, "id")
		return job.Key != 0
	}, time.Second, 20*time.Millisecond, "expected to find child process job created for call activity")

	errorCode := "42"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, map[string]interface{}{
		"variable_from_request": "request_variable",
	})
	assert.NoError(t, err)

	processInstance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), createdProcessInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, processInstance.ProcessInstance().State)
	assert.Equal(t, "request_variable", processInstance.ProcessInstance().GetVariable("boundary_variable"))
	assert.Nil(t, processInstance.ProcessInstance().GetVariable("variable_from_request"))

	childProcess, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), childInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, childProcess.ProcessInstance().State)
	assert.Nil(t, childProcess.ProcessInstance().GetVariable("boundary_variable"))
	assert.Nil(t, childProcess.ProcessInstance().GetVariable("variable_from_request"))

	assertJobState(t, job, runtime.ActivityStateTerminated)
	assertIncidentCount(t, processInstance, 0)
	assertTokenElementIds(t, processInstance, []string{"handled-end"}, []string{"should-not-happen-end"})
}

func TestJobFailOnCallActivityWithMatchingErrorBoundaryWithoutOutputMappingPropagatesAllVariablesToCatchingScope(t *testing.T) {
	createdProcessInstance := createCallActivityProcessInstance(t, "call_activity_with_error_boundary_event.bpmn")

	var childInstance runtime.CallActivityInstance
	require.Eventually(t, func() bool {
		var found bool
		childInstance, found = findCallActivityChildInstance(createdProcessInstance.ProcessInstance().Key)
		return found
	}, time.Second, 20*time.Millisecond)

	var job runtime.Job
	require.Eventually(t, func() bool {
		job = findJobForProcessInstance(childInstance.ProcessInstance().Key, "id")
		return job.Key != 0
	}, time.Second, 20*time.Millisecond, "expected to find child process job created for call activity")

	errorCode := "42"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, map[string]interface{}{
		"variable_from_request": "request_variable",
		"request_count":         7,
	})
	assert.NoError(t, err)

	processInstance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), createdProcessInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, processInstance.ProcessInstance().State)
	assert.Equal(t, "request_variable", processInstance.ProcessInstance().GetVariable("variable_from_request"))
	assert.Equal(t, 7, processInstance.ProcessInstance().GetVariable("request_count"))

	childProcess, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), childInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, childProcess.ProcessInstance().State)
	assert.Nil(t, childProcess.ProcessInstance().GetVariable("variable_from_request"))
	assert.Nil(t, childProcess.ProcessInstance().GetVariable("request_count"))

	assertJobState(t, job, runtime.ActivityStateTerminated)
	assertIncidentCount(t, processInstance, 0)
	assertTokenElementIds(t, processInstance, []string{"handled-end"}, []string{"should-not-happen-end"})
}

func TestJobFailOnCallActivityWithNonMatchingErrorBoundaryIsIncident(t *testing.T) {
	createdProcessInstance := createCallActivityProcessInstance(t, "call_activity_with_error_boundary_event.bpmn")

	var childInstance runtime.CallActivityInstance
	require.Eventually(t, func() bool {
		var found bool
		childInstance, found = findCallActivityChildInstance(createdProcessInstance.ProcessInstance().Key)
		return found
	}, time.Second, 20*time.Millisecond)

	var job runtime.Job
	require.Eventually(t, func() bool {
		job = findJobForProcessInstance(childInstance.ProcessInstance().Key, "id")
		return job.Key != 0
	}, time.Second, 25*time.Millisecond, "expected to find child process job created for call activity")

	errorCode := "422"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, nil)
	assert.NoError(t, err)

	parentProcessInstance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), createdProcessInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateActive, parentProcessInstance.ProcessInstance().State)

	childProcess, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), childInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, childProcess.ProcessInstance().State)

	assertJobState(t, job, runtime.ActivityStateFailed)
	assertIncidentCount(t, parentProcessInstance, 0)

	childIncidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), childInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Len(t, childIncidents, 1)

	parentTokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), parentProcessInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Contains(t, tokenElementIDs(parentTokens), "boundary-error-call-activity")
	assert.NotContains(t, tokenElementIDs(parentTokens), "handled-end")
	assert.NotContains(t, tokenElementIDs(parentTokens), "should-not-happen-end")

	parentToken, found := findTokenByElementID(parentTokens, "boundary-error-call-activity")
	require.True(t, found, "expected to find parent token waiting on call activity")
	assert.Equal(t, runtime.TokenStateWaiting, parentToken.State)

	childTokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), childInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Contains(t, tokenElementIDs(childTokens), "id")
	assert.NotContains(t, tokenElementIDs(childTokens), "End_Event")

	childToken, found := findTokenByElementID(childTokens, "id")
	require.True(t, found, "expected to find failed child service task token")
	assert.Equal(t, runtime.TokenStateFailed, childToken.State)
}

func TestJobFailOnNestedCallActivityBoundaryIsCaughtInAncestor(t *testing.T) {
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_events/call_activity/call_activity_nested_with_error_boundary_leaf.bpmn")
	require.NoError(t, err)
	_, err = bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_events/call_activity/call_activity_nested_with_error_boundary_parent_1.bpmn")
	require.NoError(t, err)
	_, err = bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_events/call_activity/call_activity_nested_with_error_boundary_parent_2.bpmn")
	require.NoError(t, err)
	rootDefinition, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_events/call_activity/call_activity_nested_with_error_boundary_parent_root.bpmn")
	require.NoError(t, err)

	createdProcessInstance, err := bpmnEngine.CreateInstanceByKey(t.Context(), rootDefinition.Key, map[string]interface{}{
		"variable_name": "oldVal",
	})
	require.NoError(t, err)

	var parentTwoInstance runtime.ProcessInstance
	require.Eventually(t, func() bool {
		var found bool
		parentTwoInstance, found = findChildProcessInstanceByParentAndProcessID(createdProcessInstance.ProcessInstance().Key, "nested_call_activity_error_parent_2")
		return found
	}, time.Second, 20*time.Millisecond)

	var parentOneInstance runtime.ProcessInstance
	require.Eventually(t, func() bool {
		var found bool
		parentOneInstance, found = findChildProcessInstanceByParentAndProcessID(parentTwoInstance.ProcessInstance().Key, "nested_call_activity_error_parent_1")
		return found
	}, time.Second, 20*time.Millisecond)

	var leafInstance runtime.ProcessInstance
	require.Eventually(t, func() bool {
		var found bool
		leafInstance, found = findChildProcessInstanceByParentAndProcessID(parentOneInstance.ProcessInstance().Key, "nested_call_activity_error_leaf")
		return found
	}, time.Second, 20*time.Millisecond)

	var job runtime.Job
	require.Eventually(t, func() bool {
		job = findJobForProcessInstance(leafInstance.ProcessInstance().Key, "id")
		return job.Key != 0
	}, time.Second, 20*time.Millisecond, "expected to find leaf process job created for nested call activity")

	errorCode := "42"
	err = bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected nested boundary error", &errorCode, nil)
	assert.NoError(t, err)

	var rootInstance runtime.ProcessInstance
	require.Eventually(t, func() bool {
		rootInstance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), createdProcessInstance.ProcessInstance().Key)
		require.NoError(t, err)
		return rootInstance.ProcessInstance().State == runtime.ActivityStateCompleted
	}, time.Second, 20*time.Millisecond)

	require.Eventually(t, func() bool {
		parentTwoInstance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), parentTwoInstance.ProcessInstance().Key)
		require.NoError(t, err)
		return parentTwoInstance.ProcessInstance().State == runtime.ActivityStateCompleted
	}, time.Second, 20*time.Millisecond)

	assertJobState(t, job, runtime.ActivityStateTerminated)

	leafInstance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), leafInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, leafInstance.ProcessInstance().State)

	assertIncidentCount(t, leafInstance, 0)
	assertErrorSubscriptionCount(t, leafInstance, runtime.ErrorStateCreated, 0)

	parentOneInstance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), parentOneInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, parentOneInstance.ProcessInstance().State)

	assertIncidentCount(t, parentOneInstance, 0)
	assertErrorSubscriptionCount(t, parentOneInstance, runtime.ErrorStateCreated, 0)

	assertErrorSubscriptionCount(t, parentTwoInstance, runtime.ErrorStateCreated, 0)
	assertErrorSubscriptionCount(t, parentTwoInstance, runtime.ErrorStateCancelled, 1)
	assertIncidentCount(t, parentTwoInstance, 0)
	assertTokenElementIds(t, parentTwoInstance, []string{"handled-end"}, []string{"should-not-happen-end"})
	assertAllDescendantProcessInstancesTerminated(t, parentTwoInstance, []string{
		"nested_call_activity_error_parent_1",
		"nested_call_activity_error_leaf",
	})

	assertErrorSubscriptionCount(t, rootInstance, runtime.ErrorStateCreated, 0)
	assertIncidentCount(t, rootInstance, 0)

	assertTokenElementIds(t, rootInstance, []string{"End"}, nil)
}

func createCallActivityProcessInstance(t *testing.T, callActivityFilename string) runtime.ProcessInstance {

	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_events/call_activity/call_activity_with_error_boundary_event_child_process.bpmn")
	assert.NoError(t, err)

	processDefinition, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_events/call_activity/"+callActivityFilename)
	require.NoError(t, err)

	variableContext := make(map[string]interface{}, 2)
	variableContext["variable_name"] = "oldVal"

	createdProcessInstance, err := bpmnEngine.CreateInstanceByKey(t.Context(), processDefinition.Key, variableContext)
	require.NoError(t, err)

	return createdProcessInstance
}

func TestJobFailOnUserTaskWithCatchAllErrorBoundaryIsCaught(t *testing.T) {
	createdProcessInstance := createProcessInstance(t, "user_task/user_task_with_error_boundary_event_catch_all.bpmn")

	job := findJobForProcessInstance(createdProcessInstance.ProcessInstance().Key, "user-task-error-boundary")
	require.NotZero(t, job.Key, "expected to find user-task-error-boundary job created for process instance")

	errorCode := "any-error"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, nil)
	assert.NoError(t, err)

	assertProcessInstanceWithoutIncident(t, createdProcessInstance, job)
}

func TestJobFailOnUserTaskWithMatchingErrorBoundaryIsCaught(t *testing.T) {
	createdProcessInstance := createProcessInstance(t, "user_task/user_task_with_error_boundary_event.bpmn")

	job := findJobForProcessInstance(createdProcessInstance.ProcessInstance().Key, "user-task-error-boundary")
	require.NotZero(t, job.Key, fmt.Sprintf("expected to find %s job created for process instance", "user-task-error-boundary"))

	errorCode := "42"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, nil)
	assert.NoError(t, err)

	assertProcessInstanceWithoutIncident(t, createdProcessInstance, job)
}

func TestJobFailOnUserTaskWithMismatchingErrorBoundaryCreatesIncident(t *testing.T) {
	createdProcessInstance := createProcessInstance(t, "user_task/user_task_with_error_boundary_event.bpmn")

	job := findJobForProcessInstance(createdProcessInstance.ProcessInstance().Key, "user-task-error-boundary")
	require.NotZero(t, job.Key, fmt.Sprintf("expected to find %s job created for process instance", "user-task-error-boundary"))

	errorCode := "99"
	err := bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected incident", &errorCode, nil)
	assert.NoError(t, err)

	assertProcessInstanceWithIncident(t, createdProcessInstance, job)
}

func createProcessInstance(t *testing.T, filename string) runtime.ProcessInstance {

	processDefinition, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_events/"+filename)
	require.NoError(t, err)

	createdProcessInstance, zerr := bpmnEngine.CreateInstanceByKey(t.Context(), processDefinition.Key, nil)
	require.Nil(t, zerr)

	return createdProcessInstance
}

func findJobForProcessInstance(processInstanceKey int64, elementID string) runtime.Job {
	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == processInstanceKey && job.ElementId == elementID {
			return job
		}
	}
	return runtime.Job{}
}

func findCallActivityChildInstance(parentProcessInstanceKey int64) (runtime.CallActivityInstance, bool) {
	for _, processInstance := range engineStorage.ProcessInstances {
		if processInstance.Type() != runtime.ProcessTypeCallActivity {
			continue
		}

		callActivityInstance := processInstance.(*runtime.CallActivityInstance)
		if callActivityInstance.ParentProcessExecutionToken.ProcessInstanceKey == parentProcessInstanceKey {
			return *callActivityInstance, true
		}
	}

	return runtime.CallActivityInstance{}, false
}

func findChildProcessInstanceByParentAndProcessID(parentProcessInstanceKey int64, processID string) (runtime.ProcessInstance, bool) {
	for _, processInstance := range engineStorage.ProcessInstances {
		if processInstance.GetParentProcessInstanceKey() == nil {
			continue
		}
		if *processInstance.GetParentProcessInstanceKey() != parentProcessInstanceKey {
			continue
		}
		if processInstance.ProcessInstance().Definition.BpmnProcessId != processID {
			continue
		}
		return processInstance, true
	}

	return nil, false
}

func findTokenByElementID(tokens []runtime.ExecutionToken, elementID string) (runtime.ExecutionToken, bool) {
	for _, token := range tokens {
		if token.ElementId == elementID {
			return token, true
		}
	}

	return runtime.ExecutionToken{}, false
}

func findDescendantProcessInstances(parentProcessInstanceKey int64) []runtime.ProcessInstance {
	descendants := make([]runtime.ProcessInstance, 0)
	queue := []int64{parentProcessInstanceKey}

	for len(queue) > 0 {
		currentParentKey := queue[0]
		queue = queue[1:]

		for _, processInstance := range engineStorage.ProcessInstances {
			parentKey := processInstance.GetParentProcessInstanceKey()
			if parentKey == nil || *parentKey != currentParentKey {
				continue
			}

			descendants = append(descendants, processInstance)
			queue = append(queue, processInstance.ProcessInstance().Key)
		}
	}

	return descendants
}

func tokenElementIDs(tokens []runtime.ExecutionToken) []string {
	ids := make([]string, 0, len(tokens))
	for _, token := range tokens {
		ids = append(ids, token.ElementId)
	}
	return ids
}

func assertProcessInstanceWithIncident(t testing.TB, createdProcessInstance runtime.ProcessInstance, job runtime.Job) {
	t.Helper()

	processInstance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), createdProcessInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, processInstance.ProcessInstance().State)

	assertJobState(t, job, runtime.ActivityStateFailed)
	assertIncidentCount(t, processInstance, 1)
	assertTokenElementIds(t, processInstance, nil, []string{"handled-end", "should-not-happen-end"})
}

func assertProcessInstanceWithoutIncident(t testing.TB, createdProcessInstance runtime.ProcessInstance, job runtime.Job) {
	t.Helper()

	processInstance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), createdProcessInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, processInstance.ProcessInstance().State)

	assertJobState(t, job, runtime.ActivityStateTerminated)
	assertIncidentCount(t, processInstance, 0)
	assertTokenElementIds(t, processInstance, []string{"handled-end"}, []string{"should-not-happen-end"})
}

func assertJobState(t testing.TB, job runtime.Job, activityState runtime.ActivityState) {
	t.Helper()

	job, err := bpmnEngine.persistence.FindJobByJobKey(t.Context(), job.Key)
	assert.NoError(t, err)
	assert.Equal(t, activityState, job.State)
}

func assertIncidentCount(t testing.TB, processInstance runtime.ProcessInstance, incidentCount int) {
	t.Helper()

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), processInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Len(t, incidents, incidentCount)
}

func assertErrorSubscriptionCount(t testing.TB, processInstance runtime.ProcessInstance, state runtime.ErrorState, count int) {
	t.Helper()

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceErrorSubscriptions(t.Context(), processInstance.ProcessInstance().Key, state)
	assert.NoError(t, err)
	assert.Len(t, subscriptions, count)
}

func assertTokenElementIds(t testing.TB, processInstance runtime.ProcessInstance, contains []string, notContains []string) {
	t.Helper()

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), processInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	for _, elementID := range contains {
		assert.Contains(t, tokenElementIDs(tokens), elementID)
	}
	for _, elementID := range notContains {
		assert.NotContains(t, tokenElementIDs(tokens), elementID)
	}
}

func assertAllDescendantProcessInstancesTerminated(t testing.TB, parentProcessInstance runtime.ProcessInstance, expectedProcessIDs []string) {
	t.Helper()

	descendants := findDescendantProcessInstances(parentProcessInstance.ProcessInstance().Key)
	require.Len(t, descendants, len(expectedProcessIDs))

	foundProcessIDs := make([]string, 0, len(descendants))
	for _, descendant := range descendants {
		storedInstance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), descendant.ProcessInstance().Key)
		require.NoError(t, err)

		foundProcessIDs = append(foundProcessIDs, storedInstance.ProcessInstance().Definition.BpmnProcessId)
		assert.Equal(t, runtime.ActivityStateTerminated, storedInstance.ProcessInstance().State)
	}

	assert.ElementsMatch(t, expectedProcessIDs, foundProcessIDs)
}
