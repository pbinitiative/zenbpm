package bpmn20

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/stretchr/testify/assert"
)

func TestParse_BusinessRuleTaskWithErrorBoundaryEvent(t *testing.T) {

	definitions, err := readBpnmFile(t, "error_events/business_rule_task/business_rule_task_external_with_error_boundary_event.bpmn")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))
	assert.Equal(t, 1, len(definitions.Errors))
	assert.NotNil(t, definitions.Process.TFlowElementsContainer.BusinessRuleTask)

	boundaryEvent := definitions.Process.BoundaryEvent[0]
	assertEventDefinition(t, definitions, boundaryEvent, "boundary-error-business-rule-external", "Error_fail_test", "Fail test error", "42")
}

func TestParse_BusinessRuleTaskWithCatchallErrorBoundaryEvent(t *testing.T) {

	definitions, err := readBpnmFile(t, "error_events/business_rule_task/business_rule_task_external_with_catch_all_error_boundary_event.bpmn")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))
	assert.Equal(t, 0, len(definitions.Errors))
	assert.NotNil(t, definitions.Process.TFlowElementsContainer.BusinessRuleTask)

	boundaryEvent := definitions.Process.BoundaryEvent[0]

	assert.Equal(t, "boundary-error-business-rule-external", boundaryEvent.AttachedToRef)
	assert.Equal(t, true, boundaryEvent.CancellActivity)

	var errorEventDefinition TErrorEventDefinition
	if eventDefinition, ok := boundaryEvent.EventDefinition.(TErrorEventDefinition); ok {
		errorEventDefinition = eventDefinition
	} else {
		assert.Fail(t, "Expected TErrorEventDefinition")
	}

	assert.Equal(t, "catch-all-errors", ptr.Deref(errorEventDefinition.Id, ""))
	assert.Nil(t, errorEventDefinition.ErrorRef)
	assert.Equal(t, "", errorEventDefinition.Name)
}

func TestParse_CallActivityWithErrorBoundaryEvent(t *testing.T) {

	definitions, err := readBpnmFile(t, "error_events/call_activity/call_activity_with_error_boundary_event.bpmn")
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

	definitions, err := readBpnmFile(t, "error_events/send_task/send_task_with_error_boundary_event.bpmn")
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

	definitions, err := readBpnmFile(t, "error_events/service_task/service_task_with_error_boundary_event.bpmn")
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

	definitions, err := readBpnmFile(t, "error_events/user_task/user_task_with_error_boundary_event.bpmn")
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

	definitions, err := readBpnmFile(t, "error_events/user_task/user_task_with_error_boundary_event_and_two_errors.bpmn")
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

	definitions, err := readBpnmFile(t, "error_events/user_task/user_task_with_two_error_boundary_events.bpmn")
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

func assertEventDefinition(t testing.TB, definitions TDefinitions, event TBoundaryEvent, expectedAttachedToRefName string, expectedErrorRefName string, expectedErrorName string, expectedErrorCode string) {
	t.Helper()

	assert.Equal(t, expectedAttachedToRefName, event.AttachedToRef)
	assert.Equal(t, true, event.CancellActivity)

	var errorEventDefinition TErrorEventDefinition
	if eventDefinition, ok := event.EventDefinition.(TErrorEventDefinition); ok {
		errorEventDefinition = eventDefinition
	} else {
		assert.Fail(t, "Expected TErrorEventDefinition")
	}

	assert.Equal(t, expectedErrorRefName, ptr.Deref(errorEventDefinition.ErrorRef, ""))

	bpmnError, err := definitions.GetErrorByRef(*errorEventDefinition.ErrorRef)
	assert.NoError(t, err)
	assert.Equal(t, expectedErrorName, bpmnError.Name)
	assert.Equal(t, expectedErrorCode, bpmnError.ErrorCode)
}
