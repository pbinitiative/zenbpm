package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const callActivityBusinessKeyCalledProcessID = "business-key-called-process"

func TestCallActivityBusinessKey(t *testing.T) {

	t.Run("The called process should inherit the business key when the extension element is missing.", func(t *testing.T) {

		businessKey := fmt.Sprintf("business-key-%d", time.Now().UnixNano())

		definitionKey := deployCallActivityBusinessKeyProcessDefinitions(
			t,
			"testdata/call_activity/call_activity_with_business_key_no_override.bpmn",
		)
		processInstance := createProcessInstanceWithVariablesAndBusinessKey(t, definitionKey, &businessKey, map[string]any{
			"businessKey": "business-key-value",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		calledProcess := waitForDirectChildProcessInstance(t, processInstance.Key)
		require.Equal(t, zenclient.ProcessInstanceProcessTypeCallActivity, calledProcess.ProcessType)
		require.Equal(t, businessKey, requireCallActivityBusinessKey(t, calledProcess.BusinessKey))

		completeJobForElementId(t, calledProcess.Key, "called-service-task", nil)
		waitForProcessInstanceState(t, calledProcess.Key, zenclient.ProcessInstanceStateCompleted)

		persistedCalledProcess, err := getProcessInstance(t, calledProcess.Key)
		require.NoError(t, err)
		require.Equal(t, businessKey, requireCallActivityBusinessKey(t, persistedCalledProcess.BusinessKey))
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("The called process business key should be overridden when the extension element exists.", func(t *testing.T) {

		businessKey := fmt.Sprintf("business-key-%d", time.Now().UnixNano())
		businessKeyFromInputVariable := fmt.Sprintf("business-key-from-input-variable-%d", time.Now().UnixNano())

		definitionKey := deployCallActivityBusinessKeyProcessDefinitions(
			t,
			"testdata/call_activity/call_activity_with_business_key_override.bpmn",
		)
		processInstance := createProcessInstanceWithVariablesAndBusinessKey(t, definitionKey, &businessKey, map[string]any{
			"businessKeyFromInputVariable": businessKeyFromInputVariable,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		calledProcess := waitForDirectChildProcessInstance(t, processInstance.Key)
		require.Equal(t, zenclient.ProcessInstanceProcessTypeCallActivity, calledProcess.ProcessType)
		require.Equal(t, businessKeyFromInputVariable, requireCallActivityBusinessKey(t, calledProcess.BusinessKey))
		require.NotEqual(t, businessKey, requireCallActivityBusinessKey(t, calledProcess.BusinessKey))

		completeJobForElementId(t, calledProcess.Key, "called-service-task", nil)
		waitForProcessInstanceState(t, calledProcess.Key, zenclient.ProcessInstanceStateCompleted)

		persistedCalledProcess, err := getProcessInstance(t, calledProcess.Key)
		require.NoError(t, err)
		require.Equal(t, businessKeyFromInputVariable, requireCallActivityBusinessKey(t, persistedCalledProcess.BusinessKey))
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("The called process business key should be cleared when the override is empty.", func(t *testing.T) {

		businessKey := fmt.Sprintf("business-key-%d", time.Now().UnixNano())

		definitionKey := deployCallActivityBusinessKeyProcessDefinitions(
			t,
			"testdata/call_activity/call_activity_with_empty_business_key_override.bpmn",
		)
		processInstance := createProcessInstanceWithVariablesAndBusinessKey(t, definitionKey, &businessKey, map[string]any{})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		calledProcess := waitForDirectChildProcessInstance(t, processInstance.Key)
		require.Equal(t, zenclient.ProcessInstanceProcessTypeCallActivity, calledProcess.ProcessType)
		require.Empty(t, requireCallActivityBusinessKey(t, calledProcess.BusinessKey))

		completeJobForElementId(t, calledProcess.Key, "called-service-task", nil)
		waitForProcessInstanceState(t, calledProcess.Key, zenclient.ProcessInstanceStateCompleted)

		persistedCalledProcess, err := getProcessInstance(t, calledProcess.Key)
		require.NoError(t, err)
		require.Empty(t, requireCallActivityBusinessKey(t, persistedCalledProcess.BusinessKey))
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("A missing business key override value should create an incident.", func(t *testing.T) {

		businessKey := fmt.Sprintf("business-key-%d", time.Now().UnixNano())

		definitionKey := deployCallActivityBusinessKeyProcessDefinitions(
			t,
			"testdata/call_activity/call_activity_with_business_key_override.bpmn",
		)
		processInstance := createProcessInstanceWithVariablesAndBusinessKey(t, definitionKey, &businessKey, map[string]any{})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Equal(t, "service_task", incidents[0].ElementId)
		require.Contains(t, incidents[0].Message, "business key expression must evaluate to a string")

		childProcesses, err := getChildInstances(t, processInstance.Key)
		require.NoError(t, err)
		require.Zero(t, childProcesses.TotalCount)
	})
}

func deployCallActivityBusinessKeyProcessDefinitions(t testing.TB, parentFilename string) int64 {
	t.Helper()

	uniqueSuffix := time.Now().UnixNano()
	calledProcessID := fmt.Sprintf("call-activity-business-key-called-process-%d", uniqueSuffix)
	calledJobType := fmt.Sprintf("call-activity-business-key-called-task-%d", uniqueSuffix)

	_, err := deployTestDataDefinitionWithJobType(
		t,
		"testdata/call_activity/call_activity_with_business_key_child_process.bpmn",
		calledProcessID,
		map[string]string{
			"business-key-called-task": calledJobType,
		},
	)
	require.NoError(t, err)

	parentProcessID := fmt.Sprintf("call-activity-business-key-parent-%d", uniqueSuffix)
	parentDefinition, err := deployTestDataDefinition(
		t,
		parentFilename,
		parentProcessID,
		nil,
		map[string]string{
			callActivityBusinessKeyCalledProcessID: calledProcessID,
		},
	)
	require.NoError(t, err)
	require.NotZero(t, parentDefinition.ProcessDefinitionKey)
	return parentDefinition.ProcessDefinitionKey
}

func requireCallActivityBusinessKey(t testing.TB, businessKey *string) string {
	t.Helper()
	require.NotNil(t, businessKey)
	return *businessKey
}
