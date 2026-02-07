package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
)

func TestTerminateEndEvent(t *testing.T) {
	var instance public.ProcessInstance
	definition, err := deployGetDefinition(t, "parallel_flow_with_terminate_end_task.bpmn", "parallel_flow_with_terminate_end_task")

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})

	t.Run("read instance state", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, public.ProcessInstanceStateCompleted, fetchedInstance.State)
	})

}
