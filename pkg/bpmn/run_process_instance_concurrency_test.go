package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunProcessInstanceConcurrency(t *testing.T) {
	t.Run("reloads a supplied token after waiting for the instance lock", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		engine.reconciliationInterval = time.Hour
		require.NoError(t, engine.Start(t.Context()))
		t.Cleanup(engine.Stop)

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "value"})
		require.NoError(t, err)

		activeTokens, err := store.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Len(t, activeTokens, 1)
		persistedToken := activeTokens[0]
		require.Equal(t, runtime.TokenStateWaiting, persistedToken.State)

		// Model the gap between a foreground caller taking its Running snapshot and
		// acquiring the instance lock. Another runner parks the token while this call waits.
		persistedToken.State = runtime.TokenStateRunning
		require.NoError(t, store.SaveToken(t.Context(), persistedToken))
		staleToken := persistedToken
		flowNodeCountBefore, err := store.GetFlowNodeCount(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)

		instanceKey := instance.ProcessInstance().Key
		engine.runningInstances.lockInstance(instanceKey)
		lockHeld := true
		defer func() {
			if lockHeld {
				engine.runningInstances.unlockInstance(instanceKey)
			}
		}()

		runResult := make(chan error, 1)
		go func() {
			runResult <- engine.RunProcessInstance(t.Context(), instance, []runtime.ExecutionToken{staleToken})
		}()

		require.Eventually(t, func() bool {
			engine.runningInstances.mu.Lock()
			defer engine.runningInstances.mu.Unlock()
			runningInstance := engine.runningInstances.processInstances[instanceKey]
			return runningInstance != nil && runningInstance.waiters == 2
		}, time.Second, time.Millisecond)

		persistedToken.State = runtime.TokenStateWaiting
		require.NoError(t, store.SaveToken(t.Context(), persistedToken))
		engine.runningInstances.unlockInstance(instanceKey)
		lockHeld = false

		require.NoError(t, <-runResult)

		jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instanceKey)
		require.NoError(t, err)
		assert.Len(t, jobs, 1, "the stale token must not create a duplicate job")
		flowNodeCountAfter, err := store.GetFlowNodeCount(t.Context(), instanceKey)
		require.NoError(t, err)
		assert.Equal(t, flowNodeCountBefore, flowNodeCountAfter, "the stale token must not execute the flow node twice")
	})

	t.Run("uses the complete persisted token instead of only rechecking its state", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		engine.reconciliationInterval = time.Hour
		require.NoError(t, engine.Start(t.Context()))
		t.Cleanup(engine.Stop)

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "value"})
		require.NoError(t, err)

		activeTokens, err := store.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Len(t, activeTokens, 1)
		staleToken := activeTokens[0]
		staleToken.State = runtime.TokenStateRunning

		persistedToken := staleToken
		persistedToken.ElementId = "Event_1j4mcqg"
		persistedToken.ElementInstanceKey = engine.generateKey()
		require.NoError(t, store.SaveToken(t.Context(), persistedToken))
		jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		jobs[0].State = runtime.ActivityStateCompleted
		require.NoError(t, store.SaveJob(t.Context(), jobs[0]))

		require.NoError(t, engine.RunProcessInstance(t.Context(), instance, []runtime.ExecutionToken{staleToken}))

		persistedInstance, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		assert.Equal(t, runtime.ActivityStateCompleted, persistedInstance.ProcessInstance().State)
		persistedToken, err = store.GetTokenByKey(t.Context(), persistedToken.Key)
		require.NoError(t, err)
		assert.Equal(t, runtime.TokenStateCompleted, persistedToken.State)
		jobs, err = store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		assert.Empty(t, jobs, "the stale service-task snapshot must not create a duplicate job")
	})
}
