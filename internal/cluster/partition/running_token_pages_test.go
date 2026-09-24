package partition

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/require"
)

func TestRunningTokenPages(t *testing.T) {
	t.Run("SQL pages skip failed instances and honor token history", func(t *testing.T) {
		partition, conf, clientMgr, testStore, server := prepareTestSetup(t, false)
		t.Cleanup(func() {
			require.NoError(t, partition.Stop())
			require.NoError(t, server.Close())
		})
		db := newTestDB(t, partition, conf, clientMgr, testStore, "test-running-token-pages")

		definitionKey := db.GenerateId()
		definition := runtime.ProcessDefinition{
			Key:           definitionKey,
			BpmnProcessId: fmt.Sprintf("running-token-pages-%d", definitionKey),
			Version:       1,
			BpmnData:      `<bpmn:process xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="running-token-pages" isExecutable="true"/>`,
			BpmnChecksum:  [16]byte{3},
		}
		require.NoError(t, db.SaveProcessDefinition(t.Context(), definition))

		instanceKeys := make([]int64, 3)
		for i, state := range []runtime.ActivityState{
			runtime.ActivityStateFailed,
			runtime.ActivityStateActive,
			runtime.ActivityStateReady,
		} {
			instanceKeys[i] = db.GenerateId()
			require.NoError(t, db.SaveProcessInstance(t.Context(), &runtime.DefaultProcessInstance{
				ProcessInstanceData: runtime.ProcessInstanceData{
					Definition:     &definition,
					Key:            instanceKeys[i],
					State:          state,
					CreatedAt:      time.Now(),
					VariableHolder: runtime.VariableHolder{},
				},
			}))
		}
		for i, key := range []int64{10, 20, 30} {
			require.NoError(t, db.SaveToken(t.Context(), runtime.ExecutionToken{
				Key:                key,
				ElementInstanceKey: key,
				ElementId:          "test-token",
				ProcessInstanceKey: instanceKeys[i],
				State:              runtime.TokenStateRunning,
				CreatedAt:          time.Now().Add(-time.Hour),
			}))
		}

		startupPage, err := db.FindRunningTokensAfter(t.Context(), 0, 1)
		require.NoError(t, err)
		require.Len(t, startupPage, 1)
		require.Equal(t, int64(20), startupPage[0].Key)

		periodicPage, err := db.FindRecoverableRunningTokens(t.Context(), 0, time.Now(), 1)
		require.NoError(t, err)
		require.Len(t, periodicPage, 1)
		require.Equal(t, int64(20), periodicPage[0].Key)

		// The rqlite writer assigns token timestamps at insert time. Put the new
		// history safely beyond the cutoff so the other token remains eligible.
		runningBefore := time.Now().Add(time.Second)
		recentActivity := runningBefore.Add(time.Second).Truncate(time.Millisecond)
		require.NoError(t, db.SaveFlowElementInstance(t.Context(), runtime.FlowElementInstance{
			Key:                db.GenerateId(),
			ProcessInstanceKey: instanceKeys[1],
			ElementId:          "recent-activity",
			CreatedAt:          recentActivity,
			ExecutionTokenKey:  20,
		}))
		periodicPage, err = db.FindRecoverableRunningTokens(t.Context(), 0, runningBefore, 1)
		require.NoError(t, err)
		require.Len(t, periodicPage, 1)
		require.Equal(t, int64(30), periodicPage[0].Key)

		periodicPage, err = db.FindRecoverableRunningTokens(t.Context(), 20, recentActivity.Add(time.Second), 1)
		require.NoError(t, err)
		require.Len(t, periodicPage, 1)
		require.Equal(t, int64(30), periodicPage[0].Key)
	})
}
