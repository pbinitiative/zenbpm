package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const startEventFixturePath = "start_event/start_event.bpmn"

func TestStartEventVariables(t *testing.T) {
	t.Run("Process start variables are available at the first flow node", func(t *testing.T) {
		initialVariables := map[string]any{
			"order_id":      "order-123",
			"customer_name": "Příliš žluťoučký kůň",
		}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/"+startEventFixturePath, initialVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "first_flow_node")
		require.Equal(t, initialVariables, job.InputVariables)
		assertProcessInstanceVariables(t, processInstance.Key, initialVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "start_event", nil)
		assertFlowElementOutputVariables(t, processInstance.Key, "start_event", nil)
	})

	t.Run("Output mappings initialize and overwrite process variables before the first flow node", func(t *testing.T) {
		definitionKey := deployStartEventDefinition(t, `<bpmn:extensionElements>
        <zenbpm:ioMapping>
          <zenbpm:output source="=source_value" target="mapped_value" />
          <zenbpm:output source="=&#34;initialized-at-start&#34;" target="status" />
        </zenbpm:ioMapping>
      </bpmn:extensionElements>`)

		processInstance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{
			"source_value": "source-from-api",
			"status":       "before-start",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		expectedVariables := map[string]any{
			"source_value": "source-from-api",
			"mapped_value": "source-from-api",
			"status":       "initialized-at-start",
		}
		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "first_flow_node")
		require.Equal(t, expectedVariables, job.InputVariables)
		assertProcessInstanceVariables(t, processInstance.Key, expectedVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "start_event", nil)
		assertFlowElementOutputVariables(t, processInstance.Key, "start_event", map[string]any{
			"mapped_value": "source-from-api",
			"status":       "initialized-at-start",
		})
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
	})
}

func deployStartEventDefinition(t testing.TB, startEventExtensionElements string) int64 {
	t.Helper()

	bpmnData, err := readE2ETestDataBPMN(startEventFixturePath)
	require.NoError(t, err)
	content := string(bpmnData)

	uniqueProcessID := fmt.Sprintf("start-event-process-%d", time.Now().UnixNano())
	content = strings.Replace(content, `bpmn:process id="start_event_process"`, fmt.Sprintf(`bpmn:process id="%s"`, uniqueProcessID), 1)
	require.NotEqual(t, string(bpmnData), content, "start event fixture process id must be replaced")

	if startEventExtensionElements != "" {
		const emptyExtensionElements = `<bpmn:extensionElements />`
		require.Contains(t, content, emptyExtensionElements)
		content = strings.Replace(content, emptyExtensionElements, startEventExtensionElements, 1)
	}

	return deployBPMNTestCaseContent(t, "start_event.bpmn", []byte(content))
}
