package bpmn

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPublishMessageOnInstanceCreation_RenewsDefinitionSubscription verifies that the shared
// start-event instance creation handler marks the consumed DefinitionMessageSubscription as
// Completed AND registers a fresh Active subscription for the same start event so that the
// next published message can spawn another process instance.
func TestPublishMessageOnInstanceCreation_RenewsDefinitionSubscription(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	// Keep service-task jobs Active so the spawned instances remain in Active state.
	h := engine.NewTaskHandler().
		Type("input-task-for-message-start-event-test").
		Handler(func(job ActivatedJob) {})
	defer engine.RemoveHandler(h)

	def, err := engine.LoadFromFile(t.Context(), "./test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))

	subsBefore := definitionMessageSubscriptionsForDefinition(store, def.Key)
	require.Len(t, subsBefore, 1)
	originalKey := subsBefore[0].MessageSubscription().Key

	// First publish: should consume the original sub and create a fresh Active one.
	require.NoError(t, engine.PublishMessageByName(t.Context(), "messageStartEventProcessRef", nil, nil))

	subsAfterFirst := definitionMessageSubscriptionsForDefinition(store, def.Key)
	assert.Len(t, subsAfterFirst, 2, "the consumed sub must remain (Completed) and a fresh Active sub must be created")

	var completed, active *runtime.DefinitionMessageSubscription
	for _, sub := range subsAfterFirst {
		switch sub.MessageSubscription().State {
		case runtime.ActivityStateCompleted:
			completed = sub
		case runtime.ActivityStateActive:
			active = sub
		}
	}
	require.NotNil(t, completed, "expected the original subscription to be Completed")
	require.NotNil(t, active, "expected a renewed Active subscription")
	assert.Equal(t, originalKey, completed.MessageSubscription().Key,
		"the original subscription (by key) must be the one that is Completed")
	assert.NotEqual(t, originalKey, active.MessageSubscription().Key,
		"the renewed subscription must have a fresh key")
	assert.Equal(t, "messageStartEventProcessRef", active.MessageSubscription().Name)

	// Second publish: should consume the renewed sub and trigger a second instance.
	require.NoError(t, engine.PublishMessageByName(t.Context(), "messageStartEventProcessRef", nil, nil))

	activeInstances := 0
	for _, pi := range store.ProcessInstances {
		piData := pi.ProcessInstance()
		if piData.Definition != nil && piData.Definition.Key == def.Key &&
			piData.GetState() == runtime.ActivityStateActive {
			activeInstances++
		}
	}
	assert.Equal(t, 2, activeInstances,
		"the renewed subscription must be consumable by a subsequent publish to spawn a second instance")
}

// TestProcessTimerTriggerOnInstanceCreation_DoesNotRenewTimer verifies that triggering a
// definition-level `timeDate` timer marks the original timer as Triggered and does NOT register
// a fresh Created timer for the same start event. `timeDate` is a one-shot trigger per BPMN
// semantics — renewing it would create a new past-due timer that fires immediately, causing a
// cascade of duplicate process instances.
func TestProcessTimerTriggerOnInstanceCreation_DoesNotRenewTimer(t *testing.T) {
	store := inmemory.NewStorage()
	// Use a long pollTimerDelay so the engine's timer manager does not pick up any timer
	// during this synchronous test. We Stop() the engine immediately after asserting.
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(1*time.Hour))
	engine.Start(t.Context())
	defer engine.Stop()

	bpmnData, err := os.ReadFile("./test-cases/process_definition_start_event/timer-start-event-process.bpmn")
	require.NoError(t, err)
	futureTimeDate := time.Now().Add(100 * 365 * 24 * time.Hour).UTC().Format(time.RFC3339)
	xmlData := []byte(strings.ReplaceAll(string(bpmnData), "2026-03-28T10:00:00Z", futureTimeDate))

	def, err := engine.LoadFromBytes(t.Context(), xmlData, engine.generateKey())
	require.NoError(t, err)

	// Inject a Created timer manually so we can trigger it deterministically without the timer manager.
	timerKey := engine.generateKey()
	timer := runtime.Timer{
		Key:                  timerKey,
		ElementId:            "timerStartEvent_1234",
		ProcessDefinitionKey: def.Key,
		ProcessInstanceKey:   nil,
		Token:                nil,
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            time.Now(),
		DueAt:                time.Now(),
	}
	require.NoError(t, store.SaveTimer(t.Context(), timer))

	h := engine.NewTaskHandler().
		Type("input-task-for-timer-start-event-test").
		Handler(func(job ActivatedJob) {})
	defer engine.RemoveHandler(h)

	_, _, err = engine.TriggerTimer(t.Context(), timer)
	require.NoError(t, err)

	var triggered, created int
	for _, ts := range store.Timers {
		if ts.ProcessDefinitionKey != def.Key {
			continue
		}
		switch ts.TimerState {
		case runtime.TimerStateTriggered:
			triggered++
		case runtime.TimerStateCreated:
			created++
		}
	}
	assert.Equal(t, 1, triggered, "the original timer must be Triggered")
	assert.Equal(t, 0, created, "no fresh Created timer must be registered: timeDate timer-start events are one-shot")

	persisted, err := store.GetTimer(t.Context(), timerKey)
	require.NoError(t, err)
	assert.Equal(t, runtime.TimerStateTriggered, persisted.TimerState,
		"the original timer (looked up by key) must be Triggered")
}

// TestHandleStartEventInstanceCreation_SkipRefreshSkipsRenewal verifies that when the refresh
// callback reports the trigger should be skipped (e.g. the subscription has already been
// consumed by a concurrent publisher), neither markConsumed nor the renewal run.
func TestHandleStartEventInstanceCreation_SkipRefreshSkipsRenewal(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	def, err := engine.LoadFromFile(t.Context(), "./test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))

	subsBefore := definitionMessageSubscriptionsForDefinition(store, def.Key)
	require.Len(t, subsBefore, 1)

	markCalled := false
	err = engine.handleStartEventInstanceCreation(t.Context(), startEventInstanceCreationTrigger{
		triggerKey:           subsBefore[0].MessageSubscription().Key,
		processDefinitionKey: def.Key,
		elementId:            subsBefore[0].MessageSubscription().ElementId,
		refresh:              func(ctx context.Context) (bool, error) { return true, nil },
		markConsumed: func(ctx context.Context, _ storage.Batch) error {
			markCalled = true
			return nil
		},
	})
	require.NoError(t, err)
	assert.False(t, markCalled, "markConsumed must not be invoked when refresh signals skip")

	subsAfter := definitionMessageSubscriptionsForDefinition(store, def.Key)
	assert.Len(t, subsAfter, 1, "no renewal must happen when the trigger is skipped")
	assert.Equal(t, runtime.ActivityStateActive, subsAfter[0].MessageSubscription().State,
		"the original subscription must be untouched when the trigger is skipped")
}
