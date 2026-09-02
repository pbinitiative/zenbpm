package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishMessageOnTokenDoesNotConsumeTriggerForFailedInstance(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.contextCancel)
	instance := failedTestProcessInstance(store.GenerateId())
	require.NoError(t, store.SaveProcessInstance(t.Context(), instance))

	subscription := &runtime.TokenMessageSubscription{
		ProcessInstanceKey: instance.ProcessInstance().Key,
		Token: runtime.ExecutionToken{
			Key:                store.GenerateId(),
			ElementInstanceKey: store.GenerateId(),
			ElementId:          "message-element",
			ProcessInstanceKey: instance.ProcessInstance().Key,
			State:              runtime.TokenStateWaiting,
		},
		MessageSubscriptionData: runtime.MessageSubscriptionData{
			Key:       store.GenerateId(),
			ElementId: "message-element",
			Name:      "message",
			State:     runtime.ActivityStateActive,
			CreatedAt: time.Now(),
		},
	}
	require.NoError(t, store.SaveMessageSubscription(t.Context(), subscription))

	err := engine.PublishMessageOnToken(t.Context(), subscription, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve the instance's incidents first")
	persisted, findErr := store.FindMessageSubscriptionByKey(t.Context(), subscription.Key, runtime.ActivityStateActive)
	require.NoError(t, findErr)
	assert.Equal(t, runtime.ActivityStateActive, persisted.MessageSubscription().State)
}

func TestTriggerTimerDoesNotConsumeTriggerForFailedInstance(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.contextCancel)
	instance := failedTestProcessInstance(store.GenerateId())
	require.NoError(t, store.SaveProcessInstance(t.Context(), instance))

	token := runtime.ExecutionToken{
		Key:                store.GenerateId(),
		ElementInstanceKey: store.GenerateId(),
		ElementId:          "timer-element",
		ProcessInstanceKey: instance.ProcessInstance().Key,
		State:              runtime.TokenStateWaiting,
	}
	timer := runtime.Timer{
		ElementId:          token.ElementId,
		Key:                store.GenerateId(),
		ProcessInstanceKey: &instance.ProcessInstance().Key,
		TimerState:         runtime.TimerStateCreated,
		CreatedAt:          time.Now(),
		DueAt:              time.Now(),
		Token:              &token,
	}
	require.NoError(t, store.SaveTimer(t.Context(), timer))

	_, _, err := engine.TriggerTimer(t.Context(), timer)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve the instance's incidents first")
	persisted, findErr := store.GetTimer(t.Context(), timer.Key)
	require.NoError(t, findErr)
	assert.Equal(t, runtime.TimerStateCreated, persisted.TimerState)
}

func failedTestProcessInstance(key int64) runtime.ProcessInstance {
	return &runtime.DefaultProcessInstance{ProcessInstanceData: runtime.ProcessInstanceData{
		Definition:     &runtime.ProcessDefinition{},
		Key:            key,
		VariableHolder: runtime.NewVariableHolder(nil, nil),
		State:          runtime.ActivityStateFailed,
	}}
}
