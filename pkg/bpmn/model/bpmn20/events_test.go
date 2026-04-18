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
	err1 := xml.Unmarshal(xmlData, &definitions)
	if err1 != nil {
		t.Fatalf("failed to unmarshal XML: %v", err)
	}

	return definitions, nil
}
