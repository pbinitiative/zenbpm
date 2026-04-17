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

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

func TestGrpcJobFailOnCallActivity(t *testing.T) {

	t.Run("catch_all", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_catch_all_error_boundary_event.bpmn", "call-activity-with-catch-all-error-boundary-event")

		deployCallActivityChildProcessDefinition(t)
		parentInstance := createProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailActiveJobViaGrpc(t, job, "grpc call activity catch all", ptr.To("any-error"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")

		deployCallActivityChildProcessDefinition(t)
		parentInstance := createProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailActiveJobViaGrpc(t, job, "grpc call activity catch by error code", ptr.To("42"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")
		deployCallActivityChildProcessDefinition(t)
		parentInstance := createProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailActiveJobViaGrpc(t, job, "grpc call activity incident", ptr.To("99"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateActive, childInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateFailed)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "boundary-error-call-activity", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, parentInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}

func TestRestJobFailOnCallActivity(t *testing.T) {

	t.Run("catch_all", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_catch_all_error_boundary_event.bpmn", "call-activity-with-catch-all-error-boundary-event")
		deployCallActivityChildProcessDefinition(t)
		parentInstance := createProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")
		deployCallActivityChildProcessDefinition(t)
		parentInstance := createProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_in_multi_instance_is_caught", func(t *testing.T) {
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
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForProcessInstanceState(t, jobOwnerKey, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, jobOwnerKey, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, jobOwnerKey, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, jobOwnerKey)
		assertProcessInstanceTokenElements(t, jobOwnerKey, []string{"service_task"}, []string{"end"})

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")

		deployCallActivityChildProcessDefinition(t)
		parentInstance := createProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateActive, childInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateFailed)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "boundary-error-call-activity", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, parentInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})

	t.Run("nested_matched_error_is_caught", func(t *testing.T) {
		deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_leaf.bpmn", "nested_call_activity_error_leaf")
		deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_parent_1.bpmn", "nested_call_activity_error_parent_1")
		deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_parent_2.bpmn", "nested_call_activity_error_parent_2")
		rootDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_parent_root.bpmn", "nested_call_activity_error_root")

		rootProcessInstance := createProcessInstanceWithDefaultVariables(t, rootDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, rootProcessInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, rootProcessInstance.Key)

		parentTwoInstance := waitForChildProcessInstance(t, rootProcessInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, parentTwoInstance.Key, 1, 0)

		parentOneInstance := waitForChildProcessInstance(t, parentTwoInstance.Key)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, parentOneInstance.Key)

		leafInstance := waitForChildProcessInstance(t, parentOneInstance.Key)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, leafInstance.Key)

		job := waitForProcessInstanceJobByElementId(t, leafInstance.Key, "id")
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForProcessInstanceState(t, leafInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, leafInstance.Key, "id", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, leafInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, leafInstance.Key)

		waitForProcessInstanceState(t, parentOneInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, parentOneInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, parentOneInstance.Key)

		waitForProcessInstanceState(t, parentTwoInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, parentTwoInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceIncidentsLength(t, parentTwoInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentTwoInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentTwoInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})

		waitForProcessInstanceState(t, rootProcessInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, rootProcessInstance.Key, "End", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceIncidentsLength(t, rootProcessInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, rootProcessInstance.Key)
		assertProcessInstanceTokenElements(t, rootProcessInstance.Key, []string{"End"}, nil)
	})
}

func deployCallActivityChildProcessDefinition(t testing.TB) {
	t.Helper()

	_, err := deployDefinitionWithJobType(t, "error_events/call_activity/call_activity_with_error_boundary_event_child_process.bpmn", "simple_task_for_call_activity_error_boundary_event", map[string]string{
		"TestType": fmt.Sprintf("call-activity-child-%d", time.Now().UnixNano()),
	})
	require.NoError(t, err)
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
	}, 1*time.Second, 100*time.Millisecond, "process instance %d should create a child process instance", parentProcessInstanceKey)
	return child
}

func waitForDirectChildProcessInstance(t testing.TB, parentProcessInstanceKey int64) zenclient.ProcessInstance {
	t.Helper()

	var child zenclient.ProcessInstance
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
