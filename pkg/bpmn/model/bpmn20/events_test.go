package bpmn20

import (
	"encoding/xml"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoundaryEventParses(t *testing.T) {

	definitions, err := readBpnmFile(t, "message-boundary-event-interrupting.bpmn")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))

	be := definitions.Process.BoundaryEvent[0]
	assert.Equal(t, 1, len(be.Output))
	assert.Equal(t, "service-task-id", be.AttachedToRef)
	assert.Equal(t, true, be.CancellActivity)

	assert.Equal(t, ElementTypeBoundaryEvent, be.GetType())

	var m TMessageEventDefinition
	if messageEventDefinition, ok := be.EventDefinition.(TMessageEventDefinition); ok {
		m = messageEventDefinition
	} else {
		assert.Fail(t, "Expected TMessageEventDefinition")
	}

	assert.Equal(t, "simple-boundary-id", m.MessageRef)
}

func TestIntermediateMessageThrowEventParsesMappings(t *testing.T) {
	const processXML = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0">
  <bpmn:process id="message-throw" isExecutable="true">
    <bpmn:intermediateThrowEvent id="message_throw_event">
      <bpmn:extensionElements>
        <zenbpm:taskDefinition type="message-publication" />
        <zenbpm:ioMapping>
          <zenbpm:input source="=payload" target="messagePayload" />
          <zenbpm:output source="=deliveryId" target="publicationResult" />
        </zenbpm:ioMapping>
      </bpmn:extensionElements>
      <bpmn:messageEventDefinition id="message_definition" />
    </bpmn:intermediateThrowEvent>
  </bpmn:process>
</bpmn:definitions>`

	var definitions TDefinitions
	err := xml.Unmarshal([]byte(processXML), &definitions)
	assert.NoError(t, err)
	if assert.Len(t, definitions.Process.IntermediateThrowEvent, 1) {
		event := definitions.Process.IntermediateThrowEvent[0]
		assert.Equal(t, "message-publication", event.GetTaskType())
		if assert.Len(t, event.GetInputMapping(), 1) {
			assert.Equal(t, "=payload", event.GetInputMapping()[0].Source)
			assert.Equal(t, "messagePayload", event.GetInputMapping()[0].Target)
		}
		if assert.Len(t, event.GetOutputMapping(), 1) {
			assert.Equal(t, "=deliveryId", event.GetOutputMapping()[0].Source)
			assert.Equal(t, "publicationResult", event.GetOutputMapping()[0].Target)
		}
		_, ok := event.EventDefinition.(TMessageEventDefinition)
		assert.True(t, ok)
	}
}

func readBpnmFile(t testing.TB, filename string) (result TDefinitions, err error) {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		return result, err
	}

	wd = strings.ReplaceAll(wd, "/model/bpmn20", "")
	loc := path.Join(wd, "test-cases", filename)
	xmlData, err := os.ReadFile(loc)

	if err != nil {
		return result, fmt.Errorf("failed to read file: %w", err)
	}

	var definitions TDefinitions
	unmarshalError := xml.Unmarshal(xmlData, &definitions)
	if unmarshalError != nil {
		t.Fatalf("failed to unmarshal XML: %v", unmarshalError)
	}

	return definitions, nil
}
