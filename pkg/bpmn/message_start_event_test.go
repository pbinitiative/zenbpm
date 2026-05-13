package bpmn

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

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

// TestLoadFromBytes_MessageStartEvent_ReloadCreatesExactlyOneSubscription is the
// message-start-event analog of TestLoadFromBytes_TimerStartEvent_ReloadCreatesExactlyOneTimer.
//
// It loads the same BPMN process id twice (versioning the definition v1 → v2) with
// non-identical XML content so that the engine creates a new definition version on
// the second load. After registering the definition subscriptions for both versions
// and publishing the message, it asserts that:
//   - exactly one DefinitionMessageSubscription exists for v2 in the storage
//   - no DefinitionMessageSubscription exists for v1 (the v1 sub was deleted when v2 was deployed)
//   - exactly one active process instance was created for v2
//   - no process instance exists for v1
func TestLoadFromBytes_MessageStartEvent_ReloadCreatesExactlyOneSubscription(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	// Keep service-task jobs Active so the spawned instance stays in Active state for assertions.
	h := engine.NewTaskHandler().
		Type("input-task-for-message-start-event-test").
		Handler(func(job ActivatedJob) {
			// intentionally left blank
		})
	defer engine.RemoveHandler(h)

	bpmnData, err := os.ReadFile("./test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)

	// Helper: produce BPMN bytes whose content is unique on every call (so md5 differs and
	// the engine treats the load as a new version) but with the same bpmn process id and
	// same message name (so PublishMessageByName targets the latest version).
	callCount := 0
	buildBpmn := func() []byte {
		callCount++
		marker := fmt.Sprintf("<!-- version-marker-%d-%s -->", callCount, time.Now().UTC().Format(time.RFC3339Nano))
		// Inject a comment right after the opening definitions tag - this changes the md5
		// without altering the parsed semantics (id, name, message name remain identical).
		return []byte(strings.Replace(string(bpmnData),
			`<bpmn:process id="message-start-event-process-1"`,
			marker+`<bpmn:process id="message-start-event-process-1"`,
			1))
	}

	// First load (v1)
	v1Bytes := buildBpmn()
	def1, err := engine.LoadFromBytes(t.Context(), v1Bytes, engine.generateKey())
	require.NoError(t, err)
	require.NotNil(t, def1)
	assert.Equal(t, int32(1), def1.Version)

	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def1.Key))
	require.Len(t, definitionMessageSubscriptionsForDefinition(store, def1.Key), 1,
		"v1 should have exactly one definition message subscription after registration")

	// Second load (v2) - different content, same process id and same message name.
	time.Sleep(10 * time.Millisecond) // ensure different wall-clock time
	v2Bytes := buildBpmn()
	def2, err := engine.LoadFromBytes(t.Context(), v2Bytes, engine.generateKey())
	require.NoError(t, err)
	require.NotNil(t, def2)
	assert.Equal(t, int32(2), def2.Version)
	require.NotEqual(t, def1.Key, def2.Key)

	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def2.Key))

	assert.Empty(t, definitionMessageSubscriptionsForDefinition(store, def1.Key),
		"expected no definition message subscription for v1 after v2 was deployed")
	v2Subs := definitionMessageSubscriptionsForDefinition(store, def2.Key)
	assert.Len(t, v2Subs, 1, "expected exactly one definition message subscription for v2")

	// Publish the message - it must be routed to v2 (the only Active subscription).
	require.NoError(t, engine.PublishMessageByName(t.Context(), "messageStartEventProcessRef", nil, nil))

	// Assert: exactly one active process instance exists for v2 and none for v1.
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
	assert.Equal(t, 0, activeForV1, "no process instance should be created for v1")
	assert.Equal(t, 1, activeForV2, "exactly one active process instance should exist for v2")

	// Assert: no process instance was ever created for v1 (in any state).
	for _, pi := range store.ProcessInstances {
		piData := pi.ProcessInstance()
		if piData.Definition != nil {
			assert.NotEqual(t, def1.Key, piData.Definition.Key,
				"expected no process instance for v1 process definition")
		}
	}
}

// TestPublishMessageOnDefinitionSubscription_SubscriptionMarkedCompletedBeforeInstanceCreation
// is the message-start-event analog of
// TestCreateStartProcessOnTimerStartEvent_TimerMarkedTriggeredBeforeInstanceCreation.
//
// It verifies that the DefinitionMessageSubscription is persisted as Completed BEFORE
// CreateInstanceWithStartingElements is invoked. If instance creation fails, the
// subscription must NOT remain Active (which would cause the same message to be
// consumed again on the next publish, producing duplicate instances).
func TestPublishMessageOnDefinitionSubscription_SubscriptionMarkedCompletedBeforeInstanceCreation(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))

	// Use a non-existent process definition key so that CreateInstanceWithStartingElements
	// returns an error.
	nonExistentPDKey := engine.generateKey()
	subKey := engine.generateKey()

	defSub := &runtime.DefinitionMessageSubscription{
		MessageSubscriptionData: runtime.MessageSubscriptionData{
			Key:                  subKey,
			ElementId:            "messageStartEvent_orphan",
			ProcessDefinitionKey: nonExistentPDKey,
			Name:                 "orphan-message-name",
			State:                runtime.ActivityStateActive,
			CreatedAt:            time.Now(),
		},
	}
	require.NoError(t, store.SaveMessageSubscription(t.Context(), defSub))

	// publishMessageOnInstanceCreation → CreateInstanceWithStartingElements fails because
	// there is no matching process definition.
	err := engine.publishMessageOnInstanceCreation(t.Context(), defSub, map[string]any{})
	require.Error(t, err, "expected an error because the process definition does not exist")

	persisted, getErr := store.FindMessageSubscriptionByKey(t.Context(), subKey, runtime.ActivityStateCompleted)
	require.NoError(t, getErr,
		"definition message subscription must be Completed even when subsequent instance creation fails")
	assert.Equal(t, runtime.ActivityStateCompleted, persisted.MessageSubscription().State)
}
