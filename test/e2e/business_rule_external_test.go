package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestBusinessRuleExternal(t *testing.T) {

	t.Run("external business rule happy path", func(t *testing.T) {
		uniqueSuffix := time.Now().UnixNano()
		processId := fmt.Sprintf("business-rule-external-happy-%d", uniqueSuffix)
		jobType := fmt.Sprintf("business-rule-external-happy-job-%d", uniqueSuffix)

		definition, err := deployDefinitionWithJobType(
			t,
			"business_rule/simple-business-rule-task-external.bpmn",
			processId,
			map[string]string{
				"test-business-rule-task-job": jobType,
			},
		)
		require.NoError(t, err)

		processInstance, err := createProcessInstance(t, &definition.ProcessDefinitionKey, map[string]any{"input": "value"})
		require.NoError(t, err)
		require.NotZero(t, processInstance.Key)

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "BusinessRuleTask1")
		require.Equal(t, jobType, job.Type)
		require.NoError(t, completeJob(t, job.Key, map[string]any{"worker_result": "ok"}))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"input": "value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	})
}
