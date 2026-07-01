package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

// TestErrorEventSubProcessVariables verifies variable propagation when an interrupting error event
// subprocess catches a matching job error: the error start event and subprocess output mappings are
// propagated to the parent, job fail variables do not leak through the start event output mapping and
// the initial process variables are retained.
func TestErrorEventSubProcessVariables(t *testing.T) {

	t.Run("Event subprocess output mappings propagate to the parent when the error is caught", func(t *testing.T) {
		instance := setupErrorEventSubProcessInstance(t, nil)

		failJobForElementId(t, instance.Key, "service-task-error-event-subprocess", new("42"), nil)

		_ = requireChildEventSubProcessCompleted(t, instance.Key)
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)

		assertProcessInstanceVariables(t, instance.Key, map[string]any{
			"subProcessResult":   "error-caught",
			"errorStartEventVar": "errorStartEventValue",
		})
	})

	t.Run("Job fail variables do not leak into the parent through the error start event output mapping", func(t *testing.T) {
		instance := setupErrorEventSubProcessInstance(t, nil)

		failJobForElementId(t, instance.Key, "service-task-error-event-subprocess", new("42"), map[string]any{"failVar": "failValue"})

		_ = requireChildEventSubProcessCompleted(t, instance.Key)
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)

		assertProcessInstanceVariables(t, instance.Key, map[string]any{
			"subProcessResult":   "error-caught",
			"errorStartEventVar": "errorStartEventValue",
		})
	})

	t.Run("Initial process variables are retained alongside the event subprocess outputs", func(t *testing.T) {
		instance := setupErrorEventSubProcessInstance(t, map[string]any{"process_variable": "input_value"})

		failJobForElementId(t, instance.Key, "service-task-error-event-subprocess", new("42"), map[string]any{"failVar": "failValue"})

		_ = requireChildEventSubProcessCompleted(t, instance.Key)
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)

		assertProcessInstanceVariables(t, instance.Key, map[string]any{
			"process_variable":   "input_value",
			"subProcessResult":   "error-caught",
			"errorStartEventVar": "errorStartEventValue",
		})

	})
}
