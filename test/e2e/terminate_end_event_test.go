package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
)

func TestTerminateEndEvent(t *testing.T) {
	var instance public.ProcessInstance
	definition, err := deployGetDefinition(t, "parallel_flow_with_terminate_end_task.bpmn", "parallel_flow_with_terminate_end_task")

	t.Run("create process instance with parallel flow and terminate end task on 1 end", func(t *testing.T) {
		instance, err = createProcessInstance(t, definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})

	// The overall process instance state should be 'COMPLETED', even though the 2nd flow was waiting on the task
	// but the first flow hits the Terminate End Event and cancels all other active flows(tokens).
	// See the visualization of 'parallel_flow_with_terminate_end_task.bpmn'.
	t.Run("the process instance state should be COMPLETED", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, public.ProcessInstanceStateCompleted, fetchedInstance.State)
	})

}
