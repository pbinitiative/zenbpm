package bpmn20

import (
	"encoding/xml"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorStartEventParsesFromBpmn(t *testing.T) {
	xmlData, err := os.ReadFile("../../test-cases/error_event_subprocess/error-event-subprocess-interrupting.bpmn")
	require.NoError(t, err)

	var definitions TDefinitions
	err = xml.Unmarshal(xmlData, &definitions)
	require.NoError(t, err)

	require.Equal(t, 1, len(definitions.Process.SubProcess))
	eventSubProcess := definitions.Process.SubProcess[0]
	assert.True(t, eventSubProcess.TriggeredByEvent, "subprocess should be triggered by event")

	require.Equal(t, 1, len(eventSubProcess.TProcess.StartEvents))
	subStart := eventSubProcess.TProcess.StartEvents[0]
	assert.True(t, subStart.IsInterrupting, "error start event should default to interrupting")
	require.Equal(t, 1, len(subStart.EventDefinitions), "error start event should have one event definition")

	errorDef, isError := subStart.EventDefinitions[0].(TErrorEventDefinition)
	require.True(t, isError, "event subprocess start event should be an error event definition")
	require.NotNil(t, errorDef.ErrorRef)
	assert.Equal(t, "Error_fail_test", *errorDef.ErrorRef)

	bpmnError, err := definitions.GetErrorByRef(*errorDef.ErrorRef)
	require.NoError(t, err)
	assert.Equal(t, "42", bpmnError.ErrorCode)
}

func TestErrorStartEventNonInterruptingXmlIsRejected(t *testing.T) {
	const xmlData = `
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="Definitions_error_start_non_interrupting_xml">
  <bpmn:process id="error-start-non-interrupting-xml" isExecutable="true">
    <bpmn:subProcess id="error-event-subprocess" triggeredByEvent="true">
      <bpmn:startEvent id="error-start-event" isInterrupting="false">
        <bpmn:errorEventDefinition id="ErrorEventDefinition_sub" errorRef="Error_fail_test" />
      </bpmn:startEvent>
    </bpmn:subProcess>
  </bpmn:process>
  <bpmn:error id="Error_fail_test" name="Fail test error" errorCode="42" />
</bpmn:definitions>`

	var definitions TDefinitions
	err := xml.Unmarshal([]byte(strings.TrimSpace(xmlData)), &definitions)
	require.Error(t, err, "a non-interrupting error start event must be rejected at parse/deployment time")
	assert.Contains(t, err.Error(), "non-interrupting error start event")
}
