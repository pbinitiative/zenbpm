package partition

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/require"
)

func TestCompleteProcessInstanceSnapshot(t *testing.T) {
	t.Run("find includes mutable fields supplied by refresh", func(t *testing.T) {
		partition, conf, clientMgr, testStore, server := prepareTestSetup(t, false)
		t.Cleanup(func() {
			require.NoError(t, partition.Stop())
			require.NoError(t, server.Close())
		})
		db := newTestDB(t, partition, conf, clientMgr, testStore, "test-complete-process-instance-snapshot")

		definitionKey := db.GenerateId()
		definition := runtime.ProcessDefinition{
			Key:           definitionKey,
			BpmnProcessId: fmt.Sprintf("complete-snapshot-%d", definitionKey),
			Version:       1,
			BpmnData:      `<bpmn:process xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="complete-snapshot" isExecutable="true"/>`,
			BpmnChecksum:  [16]byte{4},
		}
		require.NoError(t, db.SaveProcessDefinition(t.Context(), definition))

		key := db.GenerateId()
		require.NoError(t, db.SaveProcessInstance(t.Context(), &runtime.DefaultProcessInstance{
			ProcessInstanceData: runtime.ProcessInstanceData{
				Definition:     &definition,
				Key:            key,
				State:          runtime.ActivityStateActive,
				CreatedAt:      time.Now(),
				VariableHolder: runtime.NewVariableHolder(nil, map[string]any{"recovered": "yes"}),
			},
		}))
		require.NoError(t, db.IncrementFlowNodeCount(t.Context(), key))

		found, err := db.FindProcessInstanceByKey(t.Context(), key)
		require.NoError(t, err)
		refreshed := &runtime.DefaultProcessInstance{ProcessInstanceData: runtime.ProcessInstanceData{Key: key}}
		require.NoError(t, db.RefreshProcessInstance(t.Context(), refreshed))
		require.Equal(t, refreshed.ProcessInstance().State, found.ProcessInstance().State)
		require.Equal(t, refreshed.ProcessInstance().FlowNodeCount, found.ProcessInstance().FlowNodeCount)
		require.Equal(t, refreshed.ProcessInstance().VariableHolder.LocalVariables(), found.ProcessInstance().VariableHolder.LocalVariables())
		require.Equal(t, int64(1), found.ProcessInstance().FlowNodeCount)
	})
}
