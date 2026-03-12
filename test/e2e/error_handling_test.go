package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGrpcJobFailOnServiceTask(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definition, err := deployDefinitionWithJobType(t, "service_task_with_catch_all_error_boundary_event.bpmn", fmt.Sprintf("service-task-catch-all-%d", time.Now().UnixNano()), map[string]string{
			"TestType": fmt.Sprintf("service-task-catch-all-%d", time.Now().UnixNano()),
		})
		require.NoError(t, err)

		instance := createErrorBoundaryProcessInstance(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "service-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc service task catch all", ptr.To("any-error"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "service_task_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		instance := createErrorBoundaryProcessInstance(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "service-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc service task catch by error code", ptr.To("42"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "service_task_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		instance := createErrorBoundaryProcessInstance(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "service-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc service task incident", ptr.To("99"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceIncidentsLength(t, instance.Key, 1)
		assertProcessHistoryElements(t, instance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}

func TestRestJobFailOnServiceTask(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definition, err := deployDefinitionWithJobType(t, "service_task_with_catch_all_error_boundary_event.bpmn", fmt.Sprintf("service-task-catch-all-%d", time.Now().UnixNano()), map[string]string{
			"TestType": fmt.Sprintf("service-task-catch-all-%d", time.Now().UnixNano()),
		})
		require.NoError(t, err)

		instance := createErrorBoundaryProcessInstance(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "service-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To(""))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("error-boundary-rest-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "service_task_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		instance := createErrorBoundaryProcessInstance(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "service-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("error-boundary-rest-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "service_task_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		instance := createErrorBoundaryProcessInstance(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "service-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceIncidentsLength(t, instance.Key, 1)
		assertProcessHistoryElements(t, instance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}

func TestGrpcJobFailOnUserTask(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definition, err := deployGetUniqueDefinition(t, "user_task_with_error_boundary_event_catch_all.bpmn")
		require.NoError(t, err)
		require.NotZero(t, definition.Key)

		instance := createErrorBoundaryProcessInstance(t, definition.Key)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "user-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc user task catch all", ptr.To("any-error"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "user_task_with_error_boundary_event.bpmn", "user-task-with-error-boundary")

		instance := createErrorBoundaryProcessInstance(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "user-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc user task catch by error code", ptr.To("42"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "user_task_with_error_boundary_event.bpmn", "user-task-with-error-boundary")
		instance := createErrorBoundaryProcessInstance(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "user-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc user task incident", ptr.To("99"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceIncidentsLength(t, instance.Key, 1)
		assertProcessHistoryElements(t, instance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}

func TestRestJobFailOnUserTask(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definition, err := deployGetUniqueDefinition(t, "user_task_with_error_boundary_event_catch_all.bpmn")
		require.NoError(t, err)
		require.NotZero(t, definition.Key)

		instance := createErrorBoundaryProcessInstance(t, definition.Key)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "user-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "user_task_with_error_boundary_event.bpmn", "user-task-with-error-boundary")

		instance := createErrorBoundaryProcessInstance(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "user-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {

		definitionKey := deployProcessDefinitionKey(t, "user_task_with_error_boundary_event.bpmn", "user-task-with-error-boundary")

		instance := createErrorBoundaryProcessInstance(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "user-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceIncidentsLength(t, instance.Key, 1)
		assertProcessHistoryElements(t, instance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}

func TestGrpcJobFailOnCallActivity(t *testing.T) {

	t.Run("catch_all", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "call_activity_with_catch_all_error_boundary_event.bpmn", "call-activity-with-catch-all-error-boundary-event")

		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstance(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		job := waitForProcessInstanceJobByElementID(t, childInstance.Key, "id")
		callFailActiveJobViaGrpc(t, job, "grpc call activity catch all", ptr.To("any-error"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessHistoryElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")

		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstance(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		job := waitForProcessInstanceJobByElementID(t, childInstance.Key, "id")
		callFailActiveJobViaGrpc(t, job, "grpc call activity catch by error code", ptr.To("42"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessHistoryElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")
		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstance(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		job := waitForProcessInstanceJobByElementID(t, childInstance.Key, "id")
		callFailActiveJobViaGrpc(t, job, "grpc call activity incident", ptr.To("99"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateActive, childInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 1)
		assertProcessHistoryElements(t, parentInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}
func TestRestJobFailOnCallActivity(t *testing.T) {

	t.Run("catch_all", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "call_activity_with_catch_all_error_boundary_event.bpmn", "call-activity-with-catch-all-error-boundary-event")
		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstance(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		job := waitForProcessInstanceJobByElementID(t, childInstance.Key, "id")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessHistoryElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")
		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstance(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		job := waitForProcessInstanceJobByElementID(t, childInstance.Key, "id")
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessHistoryElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")

		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstance(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		job := waitForProcessInstanceJobByElementID(t, childInstance.Key, "id")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateActive, childInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 1)
		assertProcessHistoryElements(t, parentInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}

func TestGrpcJobFailOnBusinessRule(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "business_rule_task_external_with_catch_all_error_boundary_event.bpmn", "business-rule-task-external-catch-all-boundary")

		instance := createErrorBoundaryProcessInstance(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "boundary-error-business-rule-external")
		callFailActiveJobViaGrpc(t, job, "grpc business rule catch all", ptr.To("any-error"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "business_rule_task_external_with_error_boundary_event.bpmn", "business-rule-task-external-error-boundary")

		instance := createErrorBoundaryProcessInstance(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "boundary-error-business-rule-external")
		callFailActiveJobViaGrpc(t, job, "grpc business rule catch by error code", ptr.To("42"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {

		definitionKey := deployProcessDefinitionKey(t, "business_rule_task_external_with_error_boundary_event.bpmn", "business-rule-task-external-error-boundary")

		instance := createErrorBoundaryProcessInstance(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "boundary-error-business-rule-external")
		callFailActiveJobViaGrpc(t, job, "grpc business rule incident", ptr.To("99"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceIncidentsLength(t, instance.Key, 1)
		assertProcessHistoryElements(t, instance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}
func TestRestJobFailOnBusinessRule(t *testing.T) {

	t.Run("catch_all", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "business_rule_task_external_with_catch_all_error_boundary_event.bpmn", "business-rule-task-external-catch-all-boundary")

		instance := createErrorBoundaryProcessInstance(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "boundary-error-business-rule-external")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "business_rule_task_external_with_error_boundary_event.bpmn", "business-rule-task-external-error-boundary")

		instance := createErrorBoundaryProcessInstance(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "boundary-error-business-rule-external")
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "business_rule_task_external_with_error_boundary_event.bpmn", "business-rule-task-external-error-boundary")

		instance := createErrorBoundaryProcessInstance(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		job := waitForProcessInstanceJobByElementID(t, instance.Key, "boundary-error-business-rule-external")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceIncidentsLength(t, instance.Key, 1)
		assertProcessHistoryElements(t, instance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}

func deployProcessDefinitionKey(t *testing.T, filename string, processID string) int64 {
	t.Helper()

	definition, err := deployGetDefinition(t, filename, processID)
	require.NoError(t, err)
	require.NotZero(t, definition.Key)
	return definition.Key
}

func deployCallActivityChildProcessDefinition(t testing.TB) {
	t.Helper()

	_, err := deployDefinitionWithJobType(t, "call_activity_with_error_boundary_event_child_process.bpmn", "simple_task_for_call_activity_error_boundary_event", map[string]string{
		"TestType": fmt.Sprintf("call-activity-child-%d", time.Now().UnixNano()),
	})
	require.NoError(t, err)
}

func createErrorBoundaryProcessInstance(t testing.TB, definitionKey int64) zenclient.ProcessInstance {
	return createErrorBoundaryProcessInstanceWithVariables(t, definitionKey, map[string]any{
		"variable_name": "test-value",
	})
}

func createErrorBoundaryProcessInstanceWithVariables(t testing.TB, definitionKey int64, variables map[string]any) zenclient.ProcessInstance {
	t.Helper()

	instance, err := createProcessInstance(t, &definitionKey, variables)
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	return instance
}

func waitForProcessInstanceState(t testing.TB, processInstanceKey int64, expectedState zenclient.ProcessInstanceState) {
	t.Helper()

	assert.Eventually(t, func() bool {
		current, err := getProcessInstance(t, processInstanceKey)
		if err != nil {
			return false
		}
		return current.State == expectedState
	}, 10*time.Second, 100*time.Millisecond, "process instance %d should reach state %s", processInstanceKey, expectedState)
}

func waitForTwoProcessInstanceStates(t testing.TB, firstKey int64, firstExpected zenclient.ProcessInstanceState, secondKey int64, secondExpected zenclient.ProcessInstanceState) {
	t.Helper()

	assert.Eventually(t, func() bool {
		first, err := getProcessInstance(t, firstKey)
		if err != nil {
			return false
		}
		second, err := getProcessInstance(t, secondKey)
		if err != nil {
			return false
		}
		return first.State == firstExpected && second.State == secondExpected
	}, 10*time.Second, 100*time.Millisecond, "process instances %d and %d should reach states %s and %s", firstKey, secondKey, firstExpected, secondExpected)
}

func waitForProcessInstanceJobByElementID(t testing.TB, processInstanceKey int64, elementID string) public.Job {
	t.Helper()

	var foundJob public.Job
	require.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if err != nil {
			return false
		}
		for _, job := range jobs {
			if job.ElementId == elementID && job.State == public.JobStateActive {
				foundJob = job
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "process instance %d should expose active job for element %s", processInstanceKey, elementID)
	return foundJob
}

func waitForChildProcessInstance(t testing.TB, parentProcessInstanceKey int64) zenclient.ProcessInstance {
	t.Helper()

	var child zenclient.ProcessInstance
	require.Eventually(t, func() bool {
		page, err := getChildInstances(t, parentProcessInstanceKey)
		if err != nil {
			return false
		}
		if len(page.Partitions) == 0 || len(page.Partitions[0].Items) == 0 {
			return false
		}
		child = page.Partitions[0].Items[0]
		return true
	}, 10*time.Second, 100*time.Millisecond, "process instance %d should create a child process instance", parentProcessInstanceKey)
	return child
}

func callFailJobViaRest(t testing.TB, jobKey int64, errorCode *string) {
	t.Helper()

	body := zenclient.FailJobJSONRequestBody{}
	if errorCode != nil {
		body.ErrorCode = errorCode
	}

	response, err := app.restClient.FailJobWithResponse(t.Context(), jobKey, body)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode(), "unexpected fail job response: %s body: %s", response.Status(), string(response.Body))
	require.Nil(t, response.JSON400)
	require.Nil(t, response.JSON502)
}

func callFailActiveJobViaGrpc(t testing.TB, job public.Job, message string, errorCode *string) {
	t.Helper()

	require.NotNil(t, errorCode)

	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	client := zenclient.NewGrpc(conn)
	clientId := fmt.Sprintf("grpc-error-boundary-%d", time.Now().UnixNano())

	_, err = client.RegisterWorker(t.Context(), clientId, func(ctx context.Context, receivedJob *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, job.Type, receivedJob.GetType())
		return nil, &zenclient.WorkerError{
			Err:       errors.New(message),
			ErrorCode: *errorCode,
		}
	}, job.Type)
	require.NoError(t, err)
}

func assertProcessInstanceIncidentsLength(t testing.TB, processInstanceKey int64, expectedLen int) {
	t.Helper()

	incidents, err := getProcessInstanceIncidents(t, processInstanceKey)
	assert.NoError(t, err)
	assert.Len(t, incidents, expectedLen)
}

func assertProcessHistoryElements(t testing.TB, processInstanceKey int64, contains []string, notContains []string) {
	t.Helper()

	history, err := getProcessInstanceHistory(t, processInstanceKey)
	assert.NoError(t, err)
	for _, elementID := range contains {
		assert.Contains(t, history, elementID)
	}
	for _, elementID := range notContains {
		assert.NotContains(t, history, elementID)
	}
}

func getProcessInstanceHistory(t testing.TB, processInstanceKey int64) ([]string, error) {
	response, err := app.restClient.GetHistoryWithResponse(t.Context(), processInstanceKey, &zenclient.GetHistoryParams{})
	if err != nil {
		return nil, fmt.Errorf("failed to get history for process instance %d: %w", processInstanceKey, err)
	}
	if response.StatusCode() != 200 {
		return nil, fmt.Errorf("failed to get history for process instance %d: %s", processInstanceKey, response.Status())
	}
	if response.JSON200 == nil || response.JSON200.Items == nil {
		return []string{}, nil
	}

	elementIDs := make([]string, 0, len(*response.JSON200.Items))
	for _, item := range *response.JSON200.Items {
		elementIDs = append(elementIDs, item.ElementId)
	}
	return elementIDs, nil
}

func cleanupOwnedProcessInstance(t testing.TB, processInstanceKey int64) {
	t.Helper()

	response, err := app.restClient.CancelProcessInstanceWithResponse(context.Background(), processInstanceKey)
	assert.NoError(t, err)

	switch response.StatusCode() {
	case http.StatusNoContent, http.StatusConflict:
		return
	default:
		assert.Failf(t, "unexpected cleanup response", "process instance %d cleanup returned %s", processInstanceKey, response.Status())
	}
}
