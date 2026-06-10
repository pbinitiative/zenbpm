package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

var (
	callActivityParentErrorBoundaryHistoryBeforeFailure = []string{
		"StartEvent_1",
		"Flow_start_main",
		"boundary-error-call-activity",
	}
	callActivityParentErrorBoundaryHistoryAfterHandledFailure = []string{
		"StartEvent_1",
		"Flow_start_main",
		"boundary-error-call-activity",
		"Flow_boundary_handled",
		"handled-end",
	}
	callActivityChildErrorBoundaryHistoryBeforeFailure = []string{
		"StartEvent_1",
		"Flow_0xt1d7q",
		"id",
	}
	callActivityChildErrorBoundaryHistoryAfterHandledFailure = []string{
		"StartEvent_1",
		"Flow_0xt1d7q",
		"id",
		"boundary-error-main-task",
	}
)

func TestCallActivityErrorBoundaryFlow(t *testing.T) {
	t.Run("Matching error boundary cancels child token and completes handled parent path", func(t *testing.T) {
		processInstance, childInstance := createCallActivityErrorBoundaryFlowInstance(t, "testdata/call_activity/call_activity_with_error_boundary_event.bpmn")

		job := waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		assertCallActivityErrorBoundaryWaitingFlow(t, processInstance.Key, childInstance.Key, 1)

		failJob(t, job.Key, new("42"), nil)

		assertCallActivityErrorBoundaryHandledFlow(t, processInstance.Key, childInstance.Key, 1)
	})

	t.Run("Catch-all error boundary catches any code and completes handled parent path", func(t *testing.T) {
		processInstance, childInstance := createCallActivityErrorBoundaryFlowInstance(t, "testdata/call_activity/call_activity_with_catch_all_error_boundary_event.bpmn")

		job := waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		assertCallActivityErrorBoundaryWaitingFlow(t, processInstance.Key, childInstance.Key, 1)

		failJob(t, job.Key, new("any-error"), nil)

		assertCallActivityErrorBoundaryHandledFlow(t, processInstance.Key, childInstance.Key, 1)
	})

	t.Run("Non-matching error boundary keeps parent and child active and creates child incident", func(t *testing.T) {
		processInstance, childInstance := createCallActivityErrorBoundaryFlowInstance(t, "testdata/call_activity/call_activity_with_error_boundary_event.bpmn")

		job := waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		assertCallActivityErrorBoundaryWaitingFlow(t, processInstance.Key, childInstance.Key, 1)

		failJob(t, job.Key, new("99"), nil)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateActive, childInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", runtime.TokenStateWaiting)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)
		assertExactProcessInstanceHistory(t, childInstance.Key, callActivityChildErrorBoundaryHistoryBeforeFailure)

		assertProcessInstanceTokenState(t, processInstance.Key, "boundary-error-call-activity", runtime.TokenStateWaiting)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, callActivityParentErrorBoundaryHistoryBeforeFailure)
	})

	t.Run("Exact-match error boundary completes exact parent handler instead of catch-all path", func(t *testing.T) {
		processInstance, childInstance := createCallActivityErrorBoundaryFlowInstance(t, "testdata/call_activity/call_activity_with_error_boundary_and_catch_all.bpmn")

		job := waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		assertCallActivityErrorBoundaryWaitingFlow(t, processInstance.Key, childInstance.Key, 2)

		failJob(t, job.Key, new("42"), nil)

		assertCallActivityErrorBoundaryHandledFlow(t, processInstance.Key, childInstance.Key, 2)
	})

	t.Run("Nested matching error boundary completes nearest matching parent", func(t *testing.T) {
		deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_leaf.bpmn", "nested_call_activity_error_leaf")
		deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_parent_1.bpmn", "nested_call_activity_error_parent_1")
		deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_parent_2.bpmn", "nested_call_activity_error_parent_2")
		rootDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_parent_root.bpmn", "nested_call_activity_error_root")

		rootProcessInstance := createProcessInstanceWithDefaultVariables(t, rootDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, rootProcessInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, rootProcessInstance.Key)

		parentTwoInstance := waitForChildProcessInstance(t, rootProcessInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentTwoInstance.Key, 1, 0)

		parentOneInstance := waitForChildProcessInstance(t, parentTwoInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, parentOneInstance.Key)

		leafInstance := waitForChildProcessInstance(t, parentOneInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, leafInstance.Key)

		job := waitForProcessInstanceActiveJobByElementId(t, leafInstance.Key, "id")
		failJob(t, job.Key, new("42"), map[string]any{"variable_from_request": "request_variable"})

		waitForProcessInstanceState(t, leafInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, leafInstance.Key, "id", runtime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, leafInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, leafInstance.Key)
		assertProcessInstanceHistory(t, leafInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "id", "boundary-error-main-task"})

		waitForProcessInstanceState(t, parentOneInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, parentOneInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, parentOneInstance.Key)
		assertProcessInstanceHistory(t, parentOneInstance.Key, []string{"Flow_start_main", "StartEvent_1", "call_activity_parent_1"})

		waitForProcessInstanceState(t, parentTwoInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, parentTwoInstance.Key, "handled-end", runtime.TokenStateCompleted)
		assertProcessInstanceIncidentsLength(t, parentTwoInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentTwoInstance.Key, 0, 1)
		assertProcessInstanceHistory(t, parentTwoInstance.Key, []string{"Flow_start_main", "StartEvent_1", "call_activity_parent_2", "Flow_boundary_handled", "handled-end"})

		waitForProcessInstanceState(t, rootProcessInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, rootProcessInstance.Key, "End", runtime.TokenStateCompleted)
		assertProcessInstanceIncidentsLength(t, rootProcessInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, rootProcessInstance.Key)
		assertProcessInstanceHistory(t, rootProcessInstance.Key, []string{"Flow_start_main", "StartEvent_1", "call_activity_root", "Flow_main_success", "End"})
	})

	t.Run("Matching error boundary catches error from multi-instance call activity child", func(t *testing.T) {
		jobType := fmt.Sprintf("call-activity-child-%d", time.Now().UnixNano())
		deployMultiInstanceCallActivityLeaf(t, "nested_call_activity_error_parent_1", jobType)

		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_parent_2.bpmn", "nested_call_activity_error_parent_2")
		parentInstance := createProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForDirectChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		jobOwnerKey, job := waitForProcessTreeJobByElementId(t, childInstance.Key, "service_task")
		failJob(t, job.Key, new("42"), map[string]any{"variable_from_request": "request_variable"})

		waitForProcessInstanceState(t, jobOwnerKey, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, jobOwnerKey, "service_task", runtime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, jobOwnerKey, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, jobOwnerKey)

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)
		assertProcessInstanceHistory(t, childInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task"})

		assertProcessInstanceTokenState(t, parentInstance.Key, "handled-end", runtime.TokenStateCompleted)
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 0, 1)
		assertProcessInstanceHistory(t, parentInstance.Key, []string{"Flow_start_main", "StartEvent_1", "call_activity_parent_2", "Flow_boundary_handled", "handled-end"})
	})
}

func createCallActivityErrorBoundaryFlowInstance(t *testing.T, filename string) (zenclient.ProcessInstance, zenclient.ProcessInstancesSimple) {
	t.Helper()

	parentDefinitionKey := deployCallActivityErrorBoundaryTestDataProcessDefinitions(t, filename)
	processInstance := createProcessInstanceWithVariables(t, parentDefinitionKey, nil)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})
	childInstance := waitForChildProcessInstance(t, processInstance.Key, 0)
	return processInstance, childInstance
}

func assertCallActivityErrorBoundaryWaitingFlow(t testing.TB, parentKey int64, childKey int64, subscriptionCount int) {
	t.Helper()

	waitForTwoProcessInstanceStates(t, parentKey, zenclient.ProcessInstanceStateActive, childKey, zenclient.ProcessInstanceStateActive)
	assertProcessInstanceTokenState(t, parentKey, "boundary-error-call-activity", runtime.TokenStateWaiting)
	assertProcessInstanceErrorSubscriptionCount(t, parentKey, subscriptionCount, 0)
	assertExactProcessInstanceHistory(t, parentKey, callActivityParentErrorBoundaryHistoryBeforeFailure)

	assertProcessInstanceTokenState(t, childKey, "id", runtime.TokenStateWaiting)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, childKey)
	assertExactProcessInstanceHistory(t, childKey, callActivityChildErrorBoundaryHistoryBeforeFailure)
}

func assertCallActivityErrorBoundaryHandledFlow(t testing.TB, parentKey int64, childKey int64, subscriptionCount int) {
	t.Helper()

	waitForTwoProcessInstanceStates(t, parentKey, zenclient.ProcessInstanceStateCompleted, childKey, zenclient.ProcessInstanceStateTerminated)
	assertProcessInstanceTokenState(t, childKey, "id", runtime.TokenStateCanceled)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, childKey)
	assertExactProcessInstanceHistory(t, childKey, callActivityChildErrorBoundaryHistoryAfterHandledFailure)

	assertProcessInstanceIsCompleted(t, parentKey, "handled-end")
	assertProcessInstanceErrorSubscriptionCount(t, parentKey, 0, subscriptionCount)
	assertExactProcessInstanceHistory(t, parentKey, callActivityParentErrorBoundaryHistoryAfterHandledFailure)
}

func waitForDirectChildProcessInstance(t testing.TB, parentProcessInstanceKey int64) zenclient.ProcessInstancesSimple {
	t.Helper()

	var child zenclient.ProcessInstancesSimple
	require.Eventually(t, func() bool {
		page, err := getChildInstances(t, parentProcessInstanceKey)
		if err != nil || len(page.Partitions) == 0 {
			return false
		}
		for _, item := range page.Partitions[0].Items {
			if item.ParentProcessInstanceKey != nil && *item.ParentProcessInstanceKey == parentProcessInstanceKey {
				child = item
				return true
			}
		}
		return false
	}, 1*time.Second, 100*time.Millisecond, "process instance %d should create a direct child process instance", parentProcessInstanceKey)
	return child
}

func waitForProcessTreeJobByElementId(t testing.TB, processInstanceKey int64, elementId string) (int64, public.Job) {
	t.Helper()

	var ownerKey int64
	var foundJob public.Job
	require.Eventually(t, func() bool {
		candidateKeys := []int64{processInstanceKey}

		page, err := getChildInstances(t, processInstanceKey)
		if err != nil {
			return false
		}
		if len(page.Partitions) > 0 {
			for _, item := range page.Partitions[0].Items {
				candidateKeys = append(candidateKeys, item.Key)
			}
		}

		for _, key := range candidateKeys {
			jobs, err := getProcessInstanceJobs(t, key)
			if err != nil {
				continue
			}
			for _, job := range jobs {
				if job.ElementId == elementId && job.State == public.JobStateActive {
					ownerKey = key
					foundJob = job
					return true
				}
			}
		}

		return false
	}, 1*time.Second, 100*time.Millisecond, "process instance tree %d should expose active job for element %s", processInstanceKey, elementId)
	return ownerKey, foundJob
}

func deployMultiInstanceCallActivityLeaf(t testing.TB, processID string, jobType string) {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)

	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	loc := path.Join(wd, "pkg", "bpmn", "test-cases", "error_events/call_activity/call_activity_with_multi_instance_and_error_boundary_leaf.bpmn")

	file, err := os.ReadFile(loc)
	require.NoError(t, err)

	re, err := regexp.Compile("bpmn:process id=\"\\w*\"")
	require.NoError(t, err)
	file = re.ReplaceAll(file, fmt.Appendf([]byte{}, "bpmn:process id=\"%s\"", processID))
	file = bytes.ReplaceAll(file, []byte(`taskDefinition type="TestType"`), []byte(fmt.Sprintf(`taskDefinition type="%s"`, jobType)))
	file = bytes.ReplaceAll(file, []byte(`inputCollection="=testInputCollection"`), []byte(`inputCollection="=[1]"`))

	resp, err := app.NewRequest(t).
		WithPath("/v1/process-definitions").
		WithMethod("POST").
		WithMultipartBody(file, path.Base(loc)).
		DoOk()
	require.NoError(t, err)

	var result public.CreateProcessDefinition201JSONResponse
	require.NoError(t, json.Unmarshal(resp, &result))
}
