package bpmn

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRegisterProcessDefinitionSubscriptionsIsIdempotent verifies that a
// repeated deployment (a retry, a concurrent identical deployment) does not
// schedule the definition-level start subscriptions a second time.
func TestRegisterProcessDefinitionSubscriptionsIsIdempotent(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	defer engine.Stop()
	ctx := t.Context()

	_, err := engine.DeployProcessDefinition(ctx, dateStartXML("date-start-process", time.Now().Add(time.Hour)), 100, 1)
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(ctx, 100))
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(ctx, 100))
	assert.Len(t, timersOf(t, store, 100), 1, "a date start event is scheduled once")

	_, err = engine.DeployProcessDefinition(ctx, messageStartXML("message-start-process"), 200, 1)
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(ctx, 200))
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(ctx, 200))
	assert.Len(t, definitionMessageSubscriptionsOf(t, store, 200), 1, "a message start event is subscribed once")
}

// TestRegisterProcessDefinitionSubscriptionsSkipsHistoricalVersions verifies
// that a version delivered after a newer one (an allocation that lost the
// race) is stored but never activated: only the latest version of a process
// owns the definition-level subscriptions.
func TestRegisterProcessDefinitionSubscriptionsSkipsHistoricalVersions(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	defer engine.Stop()
	ctx := t.Context()

	_, err := engine.DeployProcessDefinition(ctx, timerStartXML("late-process", "PT2H"), 200, 2)
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(ctx, 200))

	_, err = engine.DeployProcessDefinition(ctx, timerStartXML("late-process", "PT1H"), 100, 1)
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(ctx, 100))

	assert.Len(t, timersOf(t, store, 200), 1, "the latest version keeps its subscriptions")
	assert.Empty(t, timersOf(t, store, 100), "a historical version is never activated")
}

func dateStartXML(processID string, at time.Time) []byte {
	return []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="defs-%[1]s">
  <bpmn:process id="%[1]s" isExecutable="true">
    <bpmn:startEvent id="start">
      <bpmn:outgoing>to-end</bpmn:outgoing>
      <bpmn:timerEventDefinition><bpmn:timeDate>%[2]s</bpmn:timeDate></bpmn:timerEventDefinition>
    </bpmn:startEvent>
    <bpmn:endEvent id="end"><bpmn:incoming>to-end</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="to-end" sourceRef="start" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>`, processID, at.UTC().Format(time.RFC3339)))
}

func messageStartXML(processID string) []byte {
	return []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="defs-%[1]s">
  <bpmn:message id="msg-%[1]s" name="start-%[1]s" />
  <bpmn:process id="%[1]s" isExecutable="true">
    <bpmn:startEvent id="start">
      <bpmn:outgoing>to-end</bpmn:outgoing>
      <bpmn:messageEventDefinition messageRef="msg-%[1]s" />
    </bpmn:startEvent>
    <bpmn:endEvent id="end"><bpmn:incoming>to-end</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="to-end" sourceRef="start" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>`, processID))
}

func definitionMessageSubscriptionsOf(t *testing.T, store *inmemory.Storage, definitionKey int64) []runtime.MessageSubscription {
	t.Helper()
	var subscriptions []runtime.MessageSubscription
	for _, subscription := range store.MessageSubscriptions {
		if subscription.Type() == runtime.MessageSubscriptionTypeDefinition && subscription.MessageSubscription().ProcessDefinitionKey == definitionKey {
			subscriptions = append(subscriptions, subscription)
		}
	}
	return subscriptions
}
