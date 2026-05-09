package bpmn

import (
	"os"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMessageStartEventProcess_LoadAndParse mirrors TestTimerStartEventProcess_LoadAndParse
// for the message-start-event-process.bpmn fixture: it just verifies that the BPMN file
// loads and parses without error.
func TestMessageStartEventProcess_LoadAndParse(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)
	assert.NotNil(t, process)
	assert.Equal(t, "message-start-event-process-1", process.BpmnProcessId)
}

// TestLoadFromBytes_MessageStartEvent_IdenticalReloadKeepsExistingSubscription mirrors
// TestLoadFromBytes_TimerStartEvent_IdenticalReloadKeepsExistingTimer for the
// message-start-event variant: loading the same BPMN bytes twice must return the same
// process definition (key + version) and must NOT create duplicate definition-level
// message subscriptions in the storage.
func TestLoadFromBytes_MessageStartEvent_IdenticalReloadKeepsExistingSubscription(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	xmlData, err := os.ReadFile("./test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)

	def1, err := engine.LoadFromBytes(t.Context(), xmlData, engine.generateKey())
	require.NoError(t, err)
	require.NotNil(t, def1)

	err = engine.RegisterProcessDefinitionSubscriptions(t.Context(), def1.Key)
	require.NoError(t, err)

	// Exactly one DefinitionMessageSubscription should be registered for the message start event.
	require.Len(t, definitionMessageSubscriptionsForDefinition(store, def1.Key), 1)

	def2, err := engine.LoadFromBytes(t.Context(), xmlData, engine.generateKey())
	require.NoError(t, err)
	require.NotNil(t, def2)
	assert.Equal(t, def1.Key, def2.Key)
	assert.Equal(t, def1.Version, def2.Version)

	// Identical reload must not deploy a new version → no extra subscriptions are created.
	if assert.Len(t, definitionMessageSubscriptionsForDefinition(store, def1.Key), 1,
		"expected identical reload to keep the existing message start event subscription") {
		for _, sub := range store.MessageSubscriptions {
			if _, ok := sub.(*runtime.DefinitionMessageSubscription); ok {
				assert.Equal(t, def1.Key, sub.MessageSubscription().ProcessDefinitionKey)
			}
		}
	}
}

// TestPublishMessage_OnDefinitionSubscription_CreatesActiveProcessInstance mirrors the
// behavior covered by TestTimerStartEvent (e2e) but at the unit-test level: publishing
// the message that the definition is subscribed to creates exactly one active process
// instance, and the definition subscription transitions into Completed state.
func TestPublishMessage_OnDefinitionSubscription_CreatesActiveProcessInstance(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	// Register a no-op handler so the service-task job stays Active and the instance remains
	// in Active state after creation (allowing the assertion to be deterministic).
	h := engine.NewTaskHandler().
		Type("input-task-for-message-start-event-test").
		Handler(func(job ActivatedJob) {
			// intentionally left blank — the instance must remain Active at the service task
		})
	defer engine.RemoveHandler(h)

	def, err := engine.LoadFromFile(t.Context(), "./test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))

	require.Len(t, definitionMessageSubscriptionsForDefinition(store, def.Key), 1)

	err = engine.PublishMessageByName(t.Context(), "messageStartEventProcessRef", nil, map[string]any{
		"messagePayloadVar": "messagePayloadValue",
	})
	require.NoError(t, err)

	// Exactly one active process instance for this definition must exist.
	activeInstances := 0
	var fired runtime.ProcessInstance
	for _, pi := range store.ProcessInstances {
		piData := pi.ProcessInstance()
		if piData.Definition != nil && piData.Definition.Key == def.Key &&
			piData.GetState() == runtime.ActivityStateActive {
			activeInstances++
			fired = pi
		}
	}
	assert.Equal(t, 1, activeInstances, "publishing the message must create exactly one active process instance")
	if fired != nil {
		assert.Equal(t, "messagePayloadValue", fired.ProcessInstance().VariableHolder.GetLocalVariable("messagePayloadVar"),
			"variables published with the message must be propagated to the new process instance")
	}

	// The definition-level subscription must have been transitioned to Completed.
	for _, sub := range store.MessageSubscriptions {
		if defSub, ok := sub.(*runtime.DefinitionMessageSubscription); ok &&
			defSub.MessageSubscription().ProcessDefinitionKey == def.Key {
			assert.Equal(t, runtime.ActivityStateCompleted, defSub.MessageSubscription().State,
				"definition-level message subscription must be Completed after the message has been published")
		}
	}
}

// TestLoadFromBytes_MessageStartEvent_RegisterIsDeterministic verifies that loading two
// distinct versions of the BPMN (different bpmn process ids → independent definitions)
// and then publishing the message of the second version creates a process instance for
// version 2's definition. This is the message-start-event analog of the timer-start-event
// reload test.
func TestLoadFromBytes_MessageStartEvent_RegisterIsDeterministic(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	bpmnData, err := os.ReadFile("./test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)

	// Build two independent definitions by replacing the message name and bpmn process id
	// (using the same process id would create two versions of the same definition, but only
	// the latest version's message subscription would be reachable via PublishMessageByName).
	v1Bytes := []byte(strings.NewReplacer(
		"messageStartEventProcessRef", "messageStartEventProcessRef-v1",
		"message-start-event-process-1", "message-start-event-process-v1",
	).Replace(string(bpmnData)))

	v2Bytes := []byte(strings.NewReplacer(
		"messageStartEventProcessRef", "messageStartEventProcessRef-v2",
		"message-start-event-process-1", "message-start-event-process-v2",
	).Replace(string(bpmnData)))

	def1, err := engine.LoadFromBytes(t.Context(), v1Bytes, engine.generateKey())
	require.NoError(t, err)
	require.NotNil(t, def1)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def1.Key))

	def2, err := engine.LoadFromBytes(t.Context(), v2Bytes, engine.generateKey())
	require.NoError(t, err)
	require.NotNil(t, def2)
	require.NotEqual(t, def1.Key, def2.Key)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def2.Key))

	require.Len(t, definitionMessageSubscriptionsForDefinition(store, def1.Key), 1)
	require.Len(t, definitionMessageSubscriptionsForDefinition(store, def2.Key), 1)

	h := engine.NewTaskHandler().
		Type("input-task-for-message-start-event-test").
		Handler(func(job ActivatedJob) {
			// keep instance Active
		})
	defer engine.RemoveHandler(h)

	require.NoError(t, engine.PublishMessageByName(t.Context(), "messageStartEventProcessRef-v2", nil, nil))

	// Exactly one active process instance must exist for v2; none for v1.
	activeForV1, activeForV2 := 0, 0
	for _, pi := range store.ProcessInstances {
		piData := pi.ProcessInstance()
		if piData.Definition == nil || piData.GetState() != runtime.ActivityStateActive {
			continue
		}
		switch piData.Definition.Key {
		case def1.Key:
			activeForV1++
		case def2.Key:
			activeForV2++
		}
	}
	assert.Equal(t, 0, activeForV1, "no instance should be created for v1 since v2's message was published")
	assert.Equal(t, 1, activeForV2, "exactly one active instance should exist for v2 after publishing its message")
}

// TestPublishMessage_OnDefinitionSubscription_NotFound_NoOp verifies the early-return behavior of publishMessageOnInstanceCreation
// when the subscription has already been consumed by another publisher (state != Active → FindMessageSubscriptionByKey returns ErrNotFound).
// The call must succeed without creating a new process instance.
func TestPublishMessage_OnDefinitionSubscription_NotFound_NoOp(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	def, err := engine.LoadFromFile(t.Context(), "./test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))

	subs := definitionMessageSubscriptionsForDefinition(store, def.Key)
	require.Len(t, subs, 1)
	original := subs[0]

	// Pre-mark the subscription as Completed so a subsequent publish hits the
	// "already consumed" code path. We do this directly on the in-memory store
	// to bypass the normal CreateInstance flow.
	original.State = runtime.ActivityStateCompleted
	require.NoError(t, store.SaveMessageSubscription(t.Context(), original))

	// Calling publishMessageOnInstanceCreation directly with the cached subscription
	// pointer must succeed without panicking and without creating any process instance.
	err = engine.publishMessageOnInstanceCreation(t.Context(), original, map[string]any{})
	assert.NoError(t, err, "publishing onto an already-consumed definition subscription must be a silent no-op")

	for _, pi := range store.ProcessInstances {
		piData := pi.ProcessInstance()
		assert.Falsef(t, piData.Definition != nil && piData.Definition.Key == def.Key,
			"no process instance should have been created for definition %d", def.Key)
	}
}

// TestRegisterProcessDefinitionSubscriptions_CreatesMessageSubscription verifies that
// RegisterProcessDefinitionSubscriptions creates exactly one DefinitionMessageSubscription
// for a process whose start event is a message start event.
func TestRegisterProcessDefinitionSubscriptions_CreatesMessageSubscription(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	def, err := engine.LoadFromFile(t.Context(), "./test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)

	// Before registration the in-memory store must contain no DefinitionMessageSubscription.
	require.Empty(t, definitionMessageSubscriptionsForDefinition(store, def.Key))

	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))

	subs := definitionMessageSubscriptionsForDefinition(store, def.Key)
	require.Len(t, subs, 1)
	assert.Equal(t, "messageStartEventProcessRef", subs[0].MessageSubscription().Name)
	assert.Equal(t, runtime.ActivityStateActive, subs[0].MessageSubscription().State)
	assert.Equal(t, def.Key, subs[0].MessageSubscription().ProcessDefinitionKey)
}

// definitionMessageSubscriptionsForDefinition returns all DefinitionMessageSubscription
// entries in the in-memory storage for the given process definition key.
func definitionMessageSubscriptionsForDefinition(store *inmemory.Storage, processDefinitionKey int64) []*runtime.DefinitionMessageSubscription {
	var out []*runtime.DefinitionMessageSubscription
	for _, sub := range store.MessageSubscriptions {
		defSub, ok := sub.(*runtime.DefinitionMessageSubscription)
		if !ok {
			continue
		}
		if defSub.MessageSubscription().ProcessDefinitionKey != processDefinitionKey {
			continue
		}
		out = append(out, defSub)
	}
	return out
}
