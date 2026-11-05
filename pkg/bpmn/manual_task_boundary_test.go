package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManualTaskBoundary(t *testing.T) {
	t.Run("duration timer interrupts waiting iterations and cancels the message boundary", func(t *testing.T) {
		engine, store := newManualTaskTestEngine(t)
		definition := deployManualTaskProcess(t, engine, "manual-task-multi-instance-timer-boundary.bpmn")
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{
			"contracts": []any{"first", "second"},
		})
		require.NoError(t, err)
		processInstanceKey := instance.ProcessInstance().Key
		childKey := requireFailedManualTaskIterations(t, engine, store, processInstanceKey, 2)

		timers, err := store.FindProcessInstanceTimers(t.Context(), processInstanceKey, runtime.TimerStateCreated)
		require.NoError(t, err)
		require.Len(t, timers, 1)
		assert.Equal(t, "boundary-timer", timers[0].ElementId)
		assert.Equal(t, time.Second, timers[0].DueAt.Sub(timers[0].CreatedAt), "the boundary uses the fixture's one-second duration")
		subscriptions, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), processInstanceKey, runtime.ActivityStateActive)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1, "the sibling boundary is subscribed before interruption")

		// Drive the timer explicitly; the engine has no background timer manager.
		// This exercises the duration boundary without depending on wall-clock waits.
		continuedInstance, tokens, err := engine.TriggerTimer(t.Context(), timers[0])
		require.NoError(t, err)
		require.NotNil(t, continuedInstance)
		require.NotEmpty(t, tokens)
		require.NoError(t, engine.RunProcessInstance(t.Context(), *continuedInstance, tokens))

		waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateCompleted)
		requireCancelledManualTaskIterations(t, engine, store, childKey, 2)
		activeTimers, err := store.FindProcessInstanceTimers(t.Context(), processInstanceKey, runtime.TimerStateCreated)
		require.NoError(t, err)
		assert.Empty(t, activeTimers)
		terminatedSubscriptions, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), processInstanceKey, runtime.ActivityStateTerminated)
		require.NoError(t, err)
		require.Len(t, terminatedSubscriptions, 1)
		assert.Equal(t, "manual-task-unused-boundary-message", terminatedSubscriptions[0].MessageSubscription().Name)
		activeSubscriptions, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), processInstanceKey, runtime.ActivityStateActive)
		require.NoError(t, err)
		assert.Empty(t, activeSubscriptions)

		boundaryEvent := requireFlowElementInstance(t, engine, processInstanceKey, "boundary-timer")
		assert.NotNil(t, boundaryEvent.CompletedAt)
		endEvent := requireFlowElementInstance(t, engine, processInstanceKey, "end-after-timer")
		assert.NotNil(t, endEvent.CompletedAt, "the process continues along the timer boundary's outgoing flow")
		history, err := store.GetFlowElementInstancesByProcessInstanceKey(t.Context(), processInstanceKey, true)
		require.NoError(t, err)
		for _, element := range history {
			assert.NotEqual(t, "end", element.ElementId, "the normal outgoing path is not taken")
			assert.NotEqual(t, "boundary-unused-message", element.ElementId, "the cancelled sibling boundary is not taken")
		}
	})
}

func requireFailedManualTaskIterations(t *testing.T, engine *Engine, store *inmemory.Storage, parentKey int64, expected int) int64 {
	t.Helper()
	var childKey int64
	require.Eventually(t, func() bool {
		parentTokens, err := store.GetAllTokensForProcessInstance(t.Context(), parentKey)
		if err != nil || len(parentTokens) != 1 || parentTokens[0].State != runtime.TokenStateWaiting {
			return false
		}
		// The storage snapshot protects map enumeration. Read only immutable keys
		// before locking each candidate; child ancestry is refreshed during execution.
		for _, candidate := range store.ProcessInstancesSnapshot() {
			if candidate.Type() != runtime.ProcessTypeMultiInstance {
				continue
			}
			key := candidate.ProcessInstance().Key
			engine.runningInstances.lockInstance(key)
			child, err := store.FindProcessInstanceByKey(t.Context(), key)
			if err != nil {
				engine.runningInstances.unlockInstance(key)
				continue
			}
			multiInstance := child.(*runtime.MultiInstanceInstance)
			matches := multiInstance.ParentProcessExecutionToken.Key == parentTokens[0].Key && child.ProcessInstance().State == runtime.ActivityStateFailed
			tokens, tokenErr := store.GetAllTokensForProcessInstance(t.Context(), key)
			incidents, incidentErr := store.FindIncidentsByProcessInstanceKey(t.Context(), key)
			matches = matches && tokenErr == nil && incidentErr == nil && len(tokens) == expected && len(incidents) == expected
			for _, token := range tokens {
				matches = matches && token.State == runtime.TokenStateFailed
			}
			for _, incident := range incidents {
				matches = matches && incident.ResolvedAt == nil
			}
			engine.runningInstances.unlockInstance(key)
			if matches {
				childKey = key
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond, "every manual task iteration must finish failing before interruption")
	return childKey
}

func requireCancelledManualTaskIterations(t *testing.T, engine *Engine, store *inmemory.Storage, childKey int64, expected int) {
	t.Helper()
	engine.runningInstances.lockInstance(childKey)
	defer engine.runningInstances.unlockInstance(childKey)
	child, err := store.FindProcessInstanceByKey(t.Context(), childKey)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, child.ProcessInstance().State)
	tokens, err := store.GetAllTokensForProcessInstance(t.Context(), childKey)
	require.NoError(t, err)
	require.Len(t, tokens, expected)
	for _, token := range tokens {
		assert.Equal(t, runtime.TokenStateCanceled, token.State, "child token %d is cancelled", token.Key)
	}
	activeTokens, err := store.GetActiveTokensForProcessInstance(t.Context(), childKey)
	require.NoError(t, err)
	assert.Empty(t, activeTokens)
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), childKey)
	require.NoError(t, err)
	require.Len(t, incidents, expected, "cancellation retains incident history")
	for _, incident := range incidents {
		assert.NotNil(t, incident.ResolvedAt, "child incident %d is resolved by cancellation", incident.Key)
	}
}
