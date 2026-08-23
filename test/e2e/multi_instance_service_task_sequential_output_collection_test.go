package e2e

import "testing"

func TestSequentialMultiInstanceServiceTaskOutputCollection(t *testing.T) {
	t.Run("empty input collection does not create output variables", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/multi_instance/multi_instance_service_task_sequential_without_output_collection.bpmn", map[string]any{
			"items": []string{},
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"items": []interface{}{},
		})
		assertNoMultiInstanceOutputVariables(t, processInstance.Key, "service_task", 1)
	})

	t.Run("missing output collection and non-empty input collection does not create output variables", func(t *testing.T) {
		inputCollection := []string{"first", "second"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/multi_instance/multi_instance_service_task_sequential_without_output_collection.bpmn", map[string]any{
			"items": inputCollection,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		childProcessInstanceKey := completeMultiInstanceJobs(t, processInstance.Key, "service_task", len(inputCollection), false)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"items": []interface{}{"first", "second"},
		})
		assertNoMultiInstanceOutputVariables(t, processInstance.Key, "service_task", 1)
		assertNoMultiInstanceOutputVariables(t, childProcessInstanceKey, "service_task", len(inputCollection))
	})
}
