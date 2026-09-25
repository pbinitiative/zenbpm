package bpmn

import (
	"context"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
)

func TestDuplicateJobCompletion(t *testing.T) {
	t.Run("skips the instance lock when no running tokens remain", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
		require.NoError(t, err)
		instanceKey := instance.ProcessInstance().Key
		job := requireSinglePendingJob(t, store, instanceKey)
		require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

		engine.runningInstances.lockInstance(instanceKey)
		locked := true
		defer func() {
			if locked {
				engine.runningInstances.unlockInstance(instanceKey)
			}
		}()
		result := make(chan error, 1)
		go func() {
			result <- engine.JobCompleteByKey(context.Background(), job.Key, nil)
		}()

		select {
		case err := <-result:
			require.NoError(t, err)
		case <-time.After(time.Second):
			engine.runningInstances.unlockInstance(instanceKey)
			locked = false
			<-result
			t.Fatal("duplicate completion waited for the instance lock despite having no running tokens")
		}
	})

	t.Run("recovers a running sibling of the completed job", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/fork-controlled-parallel-join.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
		require.NoError(t, err)
		instanceKey := instance.ProcessInstance().Key
		jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instanceKey)
		require.NoError(t, err)
		require.Len(t, jobs, 2)

		completedJob, strandedJob := jobs[0], jobs[1]
		require.NoError(t, engine.JobCompleteByKey(t.Context(), completedJob.Key, nil))
		completedToken, err := store.GetTokenByKey(t.Context(), completedJob.Token.Key)
		require.NoError(t, err)
		require.NotEqual(t, runtime.TokenStateRunning, completedToken.State)
		strandedToken := persistJobCompletionWithoutContinuation(t, &engine, store, strandedJob)

		require.NoError(t, engine.JobCompleteByKey(t.Context(), completedJob.Key, nil))
		advancedToken, err := store.GetTokenByKey(t.Context(), strandedToken.Key)
		require.NoError(t, err)
		require.NotEqual(t, runtime.TokenStateRunning, advancedToken.State)
		joinedJob := requireSinglePendingJob(t, store, instanceKey)
		require.Equal(t, "id-b-1", joinedJob.ElementId)
	})
}
