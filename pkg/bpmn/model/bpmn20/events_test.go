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

func TestParse_BusinessRuleTaskWithErrorBoundaryEvent(t *testing.T) {

	definitions, err := readBpnmFile(t, "business_rule_task_with_error_boundary_event.bpmn")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))
	assert.Equal(t, 1, len(definitions.Errors))
	assert.NotNil(t, definitions.Process.TFlowElementsContainer.BusinessRuleTask)

	boundaryEvent := definitions.Process.BoundaryEvent[0]
	assertEventDefinition(t, definitions, boundaryEvent, "boundary-error-business-task", "Error_fail_test", "Fail test error", "42")
}

func TestParse_CallActivityWithErrorBoundaryEvent(t *testing.T) {

	definitions, err := readBpnmFile(t, "call_activity_with_error_boundary_event.bpmn")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))
	assert.Equal(t, 1, len(definitions.Errors))
	assert.NotNil(t, definitions.Process.TFlowElementsContainer.CallActivity)

	boundaryEvent := definitions.Process.BoundaryEvent[0]
	assertEventDefinition(t, definitions, boundaryEvent, "boundary-error-call-activity", "Error_fail_test", "Fail test error", "42")
}

func TestParse_SendTaskWithErrorBoundaryEvent(t *testing.T) {

	definitions, err := readBpnmFile(t, "send_task_with_error_boundary_event.bpmn")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))
	assert.Equal(t, 1, len(definitions.Errors))
	assert.NotNil(t, definitions.Process.TFlowElementsContainer.SendTask)

	boundaryEvent := definitions.Process.BoundaryEvent[0]
	assertEventDefinition(t, definitions, boundaryEvent, "boundary-error-send-task", "Error_fail_test", "Fail test error", "42")
}

func TestParse_ServiceTaskWithErrorBoundaryEvent(t *testing.T) {

	definitions, err := readBpnmFile(t, "service_task_with_error_boundary_event.bpmn")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))
	assert.Equal(t, 1, len(definitions.Errors))
	assert.NotNil(t, definitions.Process.TFlowElementsContainer.ServiceTasks)

	boundaryEvent := definitions.Process.BoundaryEvent[0]
	assertEventDefinition(t, definitions, boundaryEvent, "service-task-error-boundary", "Error_fail_test", "Fail test error", "42")
}

func TestParse_UserTaskWithErrorBoundaryEvent(t *testing.T) {

	definitions, err := readBpnmFile(t, "user_task_with_error_boundary_event.bpmn")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))
	assert.Equal(t, 1, len(definitions.Errors))
	assert.NotNil(t, definitions.Process.TFlowElementsContainer.UserTasks)

	boundaryEvent := definitions.Process.BoundaryEvent[0]
	assertEventDefinition(t, definitions, boundaryEvent, "user-task-error-boundary", "Error_fail_test", "Fail test error", "42")
}

func TestParse_UserTaskWithErrorBoundaryEventAndTwoErrors(t *testing.T) {

	definitions, err := readBpnmFile(t, "user_task_with_error_boundary_event_and_two_errors.bpmn")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))
	assert.Equal(t, 2, len(definitions.Errors))
	assert.NotNil(t, definitions.Process.TFlowElementsContainer.UserTasks)

	boundaryEvent := definitions.Process.BoundaryEvent[0]
	assertEventDefinition(t, definitions, boundaryEvent, "user-task-error-boundary", "Error_fail_test_1", "Fail test error first", "1")

	bpmnError, err := definitions.GetErrorByRef("Error_fail_test_2")
	assert.NoError(t, err)
	assert.Equal(t, "Fail test error second", bpmnError.Name)
	assert.Equal(t, "2", bpmnError.ErrorCode)
}

func TestParse_UserTaskWithTwoErrorBoundaryEvents(t *testing.T) {

	definitions, err := readBpnmFile(t, "user_task_with_two_error_boundary_events.bpmn")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	assert.Equal(t, 2, len(definitions.Process.BoundaryEvent))
	assert.Equal(t, 2, len(definitions.Errors))
	assert.NotNil(t, definitions.Process.TFlowElementsContainer.UserTasks)

	firstBoundaryEvent := definitions.Process.BoundaryEvent[0]
	assertEventDefinition(t, definitions, firstBoundaryEvent, "user-task-error-boundary", "Error_fail_test_1", "Fail test error first", "1")

	secondBoundaryEvent := definitions.Process.BoundaryEvent[1]
	assertEventDefinition(t, definitions, secondBoundaryEvent, "user-task-error-boundary", "Error_fail_test_2", "Fail test error second", "2")
}

func readBpnmFile(t testing.TB, filename string) (result TDefinitions, err error) {

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

func assertEventDefinition(t testing.TB, definitions TDefinitions, event TBoundaryEvent, expectedAttachedToRefName string, expectedErrorRefName string, expectedErrorName string, expectedErrorCode string) {
	assert.Equal(t, expectedAttachedToRefName, event.AttachedToRef)
	assert.Equal(t, true, event.CancellActivity)

	var errorEventDefinition TErrorEventDefinition
	if eventDefinition, ok := event.EventDefinition.(TErrorEventDefinition); ok {
		errorEventDefinition = eventDefinition
	} else {
		assert.Fail(t, "Expected TErrorEventDefinition")
	}

	assert.Equal(t, expectedErrorRefName, errorEventDefinition.ErrorRef)

	bpmnError, err := definitions.GetErrorByRef(errorEventDefinition.ErrorRef)
	assert.NoError(t, err)
	assert.Equal(t, expectedErrorName, bpmnError.Name)
	assert.Equal(t, expectedErrorCode, bpmnError.ErrorCode)
}
