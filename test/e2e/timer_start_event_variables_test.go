package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const timerStartEventFixturePath = "timer_start_event/timer_start_event.bpmn"

func TestTimerStartEventVariables(t *testing.T) {
	t.Run("Output mappings initialize process variables before the first flow node", func(t *testing.T) {
		processID := deployTimerStartEventDefinition(t, `<bpmn:extensionElements>
        <zenbpm:ioMapping>
          <zenbpm:output source="=&#34;started-by-timer&#34;" target="trigger_type" />
          <zenbpm:output source="=42" target="attempt" />
        </zenbpm:ioMapping>
      </bpmn:extensionElements>`)

		instances := waitForProcessInstancesByBPMNProcessID(t, processID, 1)
		processInstance := instances[0]
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		expectedVariables := map[string]any{
			"trigger_type": "started-by-timer",
			"attempt":      float64(42),
		}
		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "first_flow_node")
		require.Equal(t, expectedVariables, job.InputVariables)
		assertProcessInstanceVariables(t, processInstance.Key, expectedVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "timer_start_event", nil)
		assertFlowElementOutputVariables(t, processInstance.Key, "timer_start_event", expectedVariables)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
	})

	t.Run("Invalid output mapping creates an incident on the timer start event", func(t *testing.T) {
		processID := deployTimerStartEventDefinition(t, `<bpmn:extensionElements>
        <zenbpm:ioMapping>
          <zenbpm:output source="=timer_value." target="mapped_value" />
        </zenbpm:ioMapping>
      </bpmn:extensionElements>`)

		instances := waitForProcessInstancesByBPMNProcessID(t, processID, 1)
		processInstance := instances[0]
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, processInstance.Key, "timer_start_event", runtime.TokenStateFailed)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "first_flow_node")
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{})

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Equal(t, "timer_start_event", incidents[0].ElementId)
		require.Contains(t, incidents[0].Message, "failed to evaluate StartEvent output mappings")
	})
}

func deployTimerStartEventDefinition(t testing.TB, startEventExtensionElements string) string {
	t.Helper()

	bpmnData, err := readE2ETestDataBPMN(timerStartEventFixturePath)
	require.NoError(t, err)
	content := string(bpmnData)

	uniqueProcessID := fmt.Sprintf("timer-start-event-%d", time.Now().UnixNano())
	content = strings.Replace(content, `bpmn:process id="timer_start_event_process"`, fmt.Sprintf(`bpmn:process id="%s"`, uniqueProcessID), 1)
	require.NotEqual(t, string(bpmnData), content, "timer start event fixture process id must be replaced")

	const emptyExtensionElements = `<bpmn:extensionElements />`
	require.Contains(t, content, emptyExtensionElements)
	content = strings.Replace(content, emptyExtensionElements, startEventExtensionElements, 1)

	deployBPMNTestCaseContent(t, "timer_start_event.bpmn", []byte(content))
	return uniqueProcessID
}
