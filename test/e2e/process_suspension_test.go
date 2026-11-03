package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestApiProcessSuspension(t *testing.T) {
	// process definition:
	// (Message start event) -> (service task) -> (end event)
	//                           ^
	// (Plain start event) -----+
	definition := deployUniqueMessageStartEventDefinition(t, "suspension")

	// an instance which runs when the process is suspended
	running := createProcessInstanceWithVariables(t, definition.key, map[string]any{})
	waitForProcessInstanceActiveJobByElementId(t, running.Key, messageStartEventTaskID)

	t.Run("suspending an unknown process is not found", func(t *testing.T) {
		response := setProcessSuspension(t, "no-such-process-"+definition.processID, true)
		assert.Equal(t, http.StatusNotFound, response.StatusCode())
	})

	t.Run("suspend the process", func(t *testing.T) {
		assert.Equal(t, http.StatusNoContent, setProcessSuspension(t, definition.processID, true).StatusCode())
		assert.True(t, isProcessSuspended(t, definition.processID, definition.key))

		// the request is idempotent
		assert.Equal(t, http.StatusNoContent, setProcessSuspension(t, definition.processID, true).StatusCode())
		assert.True(t, isProcessSuspended(t, definition.processID, definition.key))
	})

	t.Run("a suspended process starts no instance through the api", func(t *testing.T) {
		byKey, err := app.restClient.CreateProcessInstanceWithResponse(t.Context(), zenclient.CreateProcessInstanceJSONRequestBody{
			ProcessDefinitionKey: &definition.key,
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusConflict, byKey.StatusCode())
		require.NotNil(t, byKey.JSON409)
		assert.Contains(t, byKey.JSON409.Message, definition.processID)
		assert.Contains(t, byKey.JSON409.Message, "suspended")

		byId, err := app.restClient.CreateProcessInstanceWithResponse(t.Context(), zenclient.CreateProcessInstanceJSONRequestBody{
			BpmnProcessId: &definition.processID,
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusConflict, byId.StatusCode())

		onElements, err := app.restClient.StartProcessInstanceOnElementsWithResponse(t.Context(), zenclient.StartProcessInstanceOnElementsJSONRequestBody{
			ProcessDefinitionKey: definition.key,
			StartingElementIds:   []string{messageStartEventTaskID},
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusConflict, onElements.StatusCode())
	})

	t.Run("a suspended process refuses the message of its message start event", func(t *testing.T) {
		response, err := app.restClient.PublishMessageWithResponse(t.Context(), zenclient.PublishMessageJSONRequestBody{
			MessageName: definition.messageName,
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusConflict, response.StatusCode())
	})

	t.Run("a suspended process refuses the message of its message start event sent with a correlation key", func(t *testing.T) {
		// without an instance waiting for the key the message falls back to the message start event
		correlationKey := "no-instance-waits-for-this-key"
		response, err := app.restClient.PublishMessageWithResponse(t.Context(), zenclient.PublishMessageJSONRequestBody{
			MessageName:    definition.messageName,
			CorrelationKey: &correlationKey,
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusConflict, response.StatusCode())
	})

	t.Run("no instance but the running one exists", func(t *testing.T) {
		instances := listParentInstances(t, definition.processID)
		require.Len(t, instances, 1)
		assert.Equal(t, running.Key, instances[0].Key)
	})

	t.Run("an instance which was running when the process was suspended runs to its end", func(t *testing.T) {
		completeJobForElementId(t, running.Key, messageStartEventTaskID, map[string]any{})
		require.Eventually(t, func() bool {
			instance, err := getProcessInstance(t, running.Key)
			return err == nil && instance.State == zenclient.ProcessInstanceStateCompleted
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("resume the process", func(t *testing.T) {
		assert.Equal(t, http.StatusNoContent, setProcessSuspension(t, definition.processID, false).StatusCode())
		assert.False(t, isProcessSuspended(t, definition.processID, definition.key))

		// the request is idempotent
		assert.Equal(t, http.StatusNoContent, setProcessSuspension(t, definition.processID, false).StatusCode())
	})

	t.Run("a resumed process starts instances again", func(t *testing.T) {
		instance := createProcessInstanceWithVariables(t, definition.key, map[string]any{})
		assert.NotEmpty(t, instance.Key)

		publishMessageStartEvent(t, definition.messageName, map[string]any{})
		require.Eventually(t, func() bool {
			return len(listParentInstances(t, definition.processID)) == 3
		}, 10*time.Second, 100*time.Millisecond, "the message must start an instance once the process is resumed")
	})
}

func TestRestApiProcessSuspensionRefusesTheMessageOfAnInstantiatingReceiveTask(t *testing.T) {
	bpmnData, err := os.ReadFile("../../pkg/bpmn/test-cases/receive_task/receive-task-boundary-message-interrupting-instantiating.bpmn")
	require.NoError(t, err)
	suffix := time.Now().UnixNano()
	processId := fmt.Sprintf("suspension-receive-task-%d", suffix)
	messageName := fmt.Sprintf("suspension-receive-task-message-%d", suffix)
	content := strings.NewReplacer(
		"Process_msg_i_instantiating", processId,
		"globalMsgRefMsgIInst", messageName,
	).Replace(string(bpmnData))
	deployBPMNTestCaseContent(t, []byte(content))

	require.Equal(t, http.StatusNoContent, setProcessSuspension(t, processId, true).StatusCode())
	refused, err := app.restClient.PublishMessageWithResponse(t.Context(), zenclient.PublishMessageJSONRequestBody{MessageName: messageName})
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, refused.StatusCode())
	assert.Empty(t, listParentInstances(t, processId))

	require.Equal(t, http.StatusNoContent, setProcessSuspension(t, processId, false).StatusCode())
	publishMessageStartEvent(t, messageName, map[string]any{})
	require.Eventually(t, func() bool {
		return len(listParentInstances(t, processId)) == 1
	}, 10*time.Second, 100*time.Millisecond, "the message must start an instance once the process is resumed")
}

func TestRestApiProcessSuspensionRejectsABodyWithoutSuspended(t *testing.T) {
	definition := deployUniqueMessageStartEventDefinition(t, "suspension-body")

	for _, body := range []string{`{}`, `{"suspend":true}`} {
		response, err := app.restClient.SetProcessSuspensionWithBodyWithResponse(t.Context(), definition.processID, "application/json", strings.NewReader(body))
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, response.StatusCode(), "body %s", body)
	}
	assert.False(t, isProcessSuspended(t, definition.processID, definition.key))
}

func TestRestApiProcessSuspensionAppliesToEveryVersion(t *testing.T) {
	bpmnData, err := os.ReadFile("../../pkg/bpmn/test-cases/simple_task.bpmn")
	require.NoError(t, err)
	processId := fmt.Sprintf("suspension-versions-%d", time.Now().UnixNano())
	versionOne := strings.ReplaceAll(string(bpmnData), "Simple_Task_Process", processId)
	versionTwo := strings.ReplaceAll(versionOne, `name="Test"`, `name="Test v2"`)

	keyOne := deployBPMNTestCaseContent(t, []byte(versionOne))
	require.Equal(t, http.StatusNoContent, setProcessSuspension(t, processId, true).StatusCode())

	// a version deployed while the process is suspended is suspended as well
	keyTwo := deployBPMNTestCaseContent(t, []byte(versionTwo))
	require.NotEqual(t, keyOne, keyTwo)
	assert.True(t, isProcessSuspended(t, processId, keyOne))
	assert.True(t, isProcessSuspended(t, processId, keyTwo))

	for _, key := range []int64{keyOne, keyTwo} {
		response, createErr := app.restClient.CreateProcessInstanceWithResponse(t.Context(), zenclient.CreateProcessInstanceJSONRequestBody{
			ProcessDefinitionKey: &key,
		})
		require.NoError(t, createErr)
		assert.Equal(t, http.StatusConflict, response.StatusCode(), "version with key %d must be suspended", key)
	}

	require.Equal(t, http.StatusNoContent, setProcessSuspension(t, processId, false).StatusCode())
	assert.False(t, isProcessSuspended(t, processId, keyOne))
	assert.False(t, isProcessSuspended(t, processId, keyTwo))
}

func TestRestApiProcessSuspensionStopsTimerStartEvent(t *testing.T) {
	// every second the timer start event creates an instance, until the process is suspended
	bpmnData, err := os.ReadFile(timerCycleSimpleBpmnPath)
	require.NoError(t, err)
	processId := fmt.Sprintf("timer-cycle-suspension-%d", time.Now().UnixNano())
	content := strings.NewReplacer(
		"__TIMER_CYCLE_PLACEHOLDER__", "* * * * * *",
		"timer-cycle-start-event-simple", processId,
	).Replace(string(bpmnData))
	definitionKey := deployBPMNTestCaseContent(t, []byte(content))
	// an open-ended cycle must not outlive the test; the context of the test is cancelled before cleanups run
	t.Cleanup(func() {
		response, cleanupErr := app.restClient.SetProcessSuspensionWithResponse(context.Background(), processId, zenclient.SetProcessSuspensionJSONRequestBody{Suspended: true})
		require.NoError(t, cleanupErr)
		assert.Equal(t, http.StatusNoContent, response.StatusCode())
	})

	require.Eventually(t, func() bool {
		return len(listParentInstances(t, processId)) >= 1
	}, 20*time.Second, 100*time.Millisecond, "the timer start event creates instances before the process is suspended")

	require.Equal(t, http.StatusNoContent, setProcessSuspension(t, processId, true).StatusCode())
	assert.True(t, isProcessSuspended(t, processId, definitionKey))
	// a start which had passed the check when the process was suspended may still create its instance: wait
	// until the count stops growing
	var suspendedCount, stablePolls int
	require.Eventually(t, func() bool {
		count := len(listParentInstances(t, processId))
		if count != suspendedCount {
			suspendedCount, stablePolls = count, 0
			return false
		}
		stablePolls++
		return stablePolls >= 8
	}, 30*time.Second, 200*time.Millisecond, "the instances of a suspended process stop growing")
	assert.Never(t, func() bool {
		return len(listParentInstances(t, processId)) > suspendedCount
	}, 3*time.Second, 200*time.Millisecond, "a suspended process must not start instances from its timer start event")

	require.Equal(t, http.StatusNoContent, setProcessSuspension(t, processId, false).StatusCode())
	require.Eventually(t, func() bool {
		return len(listParentInstances(t, processId)) > suspendedCount
	}, 20*time.Second, 100*time.Millisecond, "a resumed process starts instances from its timer start event again")
}

func setProcessSuspension(t testing.TB, bpmnProcessId string, suspended bool) *zenclient.SetProcessSuspensionResponse {
	t.Helper()
	response, err := app.restClient.SetProcessSuspensionWithResponse(t.Context(), bpmnProcessId, zenclient.SetProcessSuspensionJSONRequestBody{
		Suspended: suspended,
	})
	require.NoError(t, err)
	return response
}

// isProcessSuspended reads the flag where the api offers it: the list of the definitions of the process and the
// detail of one of its versions.
func isProcessSuspended(t testing.TB, bpmnProcessId string, definitionKey int64) bool {
	t.Helper()
	list, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
		BpmnProcessId: &bpmnProcessId,
	})
	require.NoError(t, err)
	require.NotNil(t, list.JSON200)
	require.NotEmpty(t, list.JSON200.Items)
	inList := list.JSON200.Items[0].Suspended
	require.NotNil(t, inList)
	for _, item := range list.JSON200.Items {
		require.NotNil(t, item.Suspended)
		assert.Equal(t, *inList, *item.Suspended, "every version of the process shares the flag")
	}

	detail, err := app.restClient.GetProcessDefinitionWithResponse(t.Context(), definitionKey)
	require.NoError(t, err)
	require.NotNil(t, detail.JSON200)
	require.NotNil(t, detail.JSON200.Suspended)
	assert.Equal(t, *inList, *detail.JSON200.Suspended, "the list and the detail agree")
	return *inList
}
