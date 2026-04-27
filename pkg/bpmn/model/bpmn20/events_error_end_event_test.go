package bpmn20

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/stretchr/testify/assert"
)

func TestParse_SubprocessWithErrorEndEvent(t *testing.T) {

	t.Run("Catch_all", func(t *testing.T) {
		definitions, err := readBpnmFile(t, "error_end_event/sub_process/simple_sub_process_task_with_error_end_event_and_catch_all_error_boundary.bpmn")
		if err != nil {
			t.Fatalf("failed to read file: %v", err)
		}

		assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))
		assert.Equal(t, 0, len(definitions.Errors))
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.EndEvents)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.StartEvents)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.SequenceFlows)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.BoundaryEvent)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.SubProcess)

		assert.Equal(t, 1, len(definitions.Process.TFlowElementsContainer.StartEvents))
		assert.Equal(t, 2, len(definitions.Process.TFlowElementsContainer.EndEvents))
		assert.Equal(t, 3, len(definitions.Process.TFlowElementsContainer.SequenceFlows))
		assert.Equal(t, 1, len(definitions.Process.TFlowElementsContainer.BoundaryEvent))
		assert.Equal(t, 1, len(definitions.Process.TFlowElementsContainer.SubProcess))

		boundaryEvent := definitions.Process.BoundaryEvent[0]

		assert.Equal(t, "Subprocess", boundaryEvent.AttachedToRef)
		assert.Equal(t, true, boundaryEvent.CancellActivity)

		var errorEventDefinition TErrorEventDefinition
		if eventDefinition, ok := boundaryEvent.EventDefinition.(TErrorEventDefinition); ok {
			errorEventDefinition = eventDefinition
		} else {
			assert.Fail(t, "Expected TErrorEventDefinition")
		}

		assert.Nil(t, errorEventDefinition.ErrorRef)
		assert.Equal(t, "ErrorEventDefinition_07rs7zy", ptr.Deref(errorEventDefinition.Id, ""))
	})

	t.Run("Catch_by_code", func(t *testing.T) {
		definitions, err := readBpnmFile(t, "error_end_event/sub_process/simple_sub_process_task_with_error_end_event_and_matching_error_boundary.bpmn")
		if err != nil {
			t.Fatalf("failed to read file: %v", err)
		}

		assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))
		assert.Equal(t, 1, len(definitions.Errors))
		assert.Equal(t, "88", definitions.Errors[0].ErrorCode)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.EndEvents)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.StartEvents)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.SequenceFlows)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.BoundaryEvent)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.SubProcess)

		assert.Equal(t, 1, len(definitions.Process.TFlowElementsContainer.StartEvents))
		assert.Equal(t, 2, len(definitions.Process.TFlowElementsContainer.EndEvents))
		assert.Equal(t, 3, len(definitions.Process.TFlowElementsContainer.SequenceFlows))
		assert.Equal(t, 1, len(definitions.Process.TFlowElementsContainer.BoundaryEvent))
		assert.Equal(t, 1, len(definitions.Process.TFlowElementsContainer.SubProcess))

		boundaryEvent := definitions.Process.BoundaryEvent[0]
		assertEventDefinition(t, definitions, boundaryEvent, "Subprocess", "Error_fail_test", "Fail test error", "88")
	})

	t.Run("Uncatched error code", func(t *testing.T) {
		definitions, err := readBpnmFile(t, "error_end_event/sub_process/simple_sub_process_task_with_error_end_event_and_nonmatching_error_boundary.bpmn")
		if err != nil {
			t.Fatalf("failed to read file: %v", err)
		}

		assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))
		assert.Equal(t, 2, len(definitions.Errors))
		assert.Equal(t, "UNEXPECTED_FAILURE", definitions.Errors[0].ErrorCode)
		assert.Equal(t, "VALIDATION_ERROR", definitions.Errors[1].ErrorCode)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.EndEvents)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.StartEvents)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.SequenceFlows)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.BoundaryEvent)
		assert.NotNil(t, definitions.Process.TFlowElementsContainer.SubProcess)

		assert.Equal(t, 1, len(definitions.Process.TFlowElementsContainer.StartEvents))
		assert.Equal(t, 2, len(definitions.Process.TFlowElementsContainer.EndEvents))
		assert.Equal(t, 3, len(definitions.Process.TFlowElementsContainer.SequenceFlows))
		assert.Equal(t, 1, len(definitions.Process.TFlowElementsContainer.BoundaryEvent))
		assert.Equal(t, 1, len(definitions.Process.TFlowElementsContainer.SubProcess))

		boundaryEvent := definitions.Process.BoundaryEvent[0]
		assertEventDefinition(t, definitions, boundaryEvent, "Subprocess", "Error_expected", "Expected error", "VALIDATION_ERROR")
	})
}
