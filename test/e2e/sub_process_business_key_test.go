package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestSubProcessBusinessKey(t *testing.T) {

	t.Run("The business key should not be overridden when the extension element is missing.", func(t *testing.T) {

		businessKey := fmt.Sprintf("business-key-%d", time.Now().UnixNano())

		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/sub_process/subprocess_with_business_key_no_override.bpmn")
		processInstance := createProcessInstanceWithVariablesAndBusinessKey(t, definitionKey, &businessKey, map[string]any{
			"businessKey": "business-key-value",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)

		require.NotNil(t, innerProcess.BusinessKey)

		completeJobForElementId(t, innerProcess.Key, "service_task", nil)
		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateCompleted)

		persistedInnerProcessInstance, err := getProcessInstance(t, innerProcess.Key)

		require.NoError(t, err)
		require.Equal(t, requireBusinessKey(t, innerProcess.BusinessKey), requireBusinessKey(t, persistedInnerProcessInstance.BusinessKey))

		persistedSubProcessInstance := waitForDirectChildProcessInstance(t, processInstance.Key)
		require.Equal(t, zenclient.ProcessInstanceProcessTypeSubprocess, persistedSubProcessInstance.ProcessType)
		require.Equal(t, businessKey, requireBusinessKey(t, persistedSubProcessInstance.BusinessKey))
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("The business key should be overridden when the extension element exists.", func(t *testing.T) {

		businessKey := fmt.Sprintf("business-key-%d", time.Now().UnixNano())
		businessKeyFromInputVariable := fmt.Sprintf("business-key-from-input-variable-%d", time.Now().UnixNano())

		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/sub_process/subprocess_with_business_key_override.bpmn")
		processInstance := createProcessInstanceWithVariablesAndBusinessKey(t, definitionKey, &businessKey, map[string]any{
			"businessKeyFromInputVariable": businessKeyFromInputVariable,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)

		require.NotNil(t, innerProcess.BusinessKey)

		completeJobForElementId(t, innerProcess.Key, "service_task", nil)
		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateCompleted)

		persistedInnerProcessInstance, err := getProcessInstance(t, innerProcess.Key)

		require.NoError(t, err)
		require.Equal(t, requireBusinessKey(t, innerProcess.BusinessKey), requireBusinessKey(t, persistedInnerProcessInstance.BusinessKey))
		require.NotEqual(t, businessKey, requireBusinessKey(t, persistedInnerProcessInstance.BusinessKey))

		persistedSubProcessInstance := waitForDirectChildProcessInstance(t, processInstance.Key)
		require.Equal(t, zenclient.ProcessInstanceProcessTypeSubprocess, persistedSubProcessInstance.ProcessType)
		require.Equal(t, businessKeyFromInputVariable, requireBusinessKey(t, persistedSubProcessInstance.BusinessKey))
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	})
}

func requireBusinessKey(t testing.TB, businessKey *string) string {
	t.Helper()
	require.NotNil(t, businessKey)
	return *businessKey
}
