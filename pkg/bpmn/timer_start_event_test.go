package bpmn

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindDurationValue_NilTimeDuration(t *testing.T) {
	timerDef := bpmn20.TTimerEventDefinition{
		TimeDuration: nil,
	}
	_, err := findDurationValue(timerDef)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timerDef.TimeDuration is nil")
}

func TestFindDurationValue_EmptyTimeDuration(t *testing.T) {
	id := "test-id"
	timerDef := bpmn20.TTimerEventDefinition{
		Id:           &id,
		TimeDuration: &bpmn20.TTimeInfo{XMLText: "   "},
	}
	_, err := findDurationValue(timerDef)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Can't find 'timeDuration' value")
}

func TestFindDurationValue_ValidISO8601(t *testing.T) {
	timerDef := bpmn20.TTimerEventDefinition{
		TimeDuration: &bpmn20.TTimeInfo{XMLText: "PT1S"},
	}
	dur, err := findDurationValue(timerDef)
	assert.NoError(t, err)
	result := dur.Shift(time.Now())
	assert.True(t, result.After(time.Now()))
}

func TestFindStartTime_NilTimeDate(t *testing.T) {
	timerDef := bpmn20.TTimerEventDefinition{
		TimeDate: nil,
	}
	_, err := findStartTime(timerDef)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timerDef.TimeDate is nil")
}

func TestFindStartTime_EmptyTimeDate(t *testing.T) {
	id := "test-id"
	timerDef := bpmn20.TTimerEventDefinition{
		Id:       &id,
		TimeDate: &bpmn20.TTimeInfo{XMLText: "   "},
	}
	_, err := findStartTime(timerDef)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Can't find 'timeDate' value")
}

func TestFindStartTime_ValidDateTime(t *testing.T) {
	id := "test-id"
	timerDef := bpmn20.TTimerEventDefinition{
		Id:       &id,
		TimeDate: &bpmn20.TTimeInfo{XMLText: "2026-06-01T12:00:00Z"},
	}
	result, err := findStartTime(timerDef)
	assert.NoError(t, err)
	assert.Equal(t, 2026, result.Year())
	assert.Equal(t, time.June, result.Month())
	assert.Equal(t, 1, result.Day())
}

func TestTimerEventSubprocessInterrupting_LoadAndParse(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer-event-subprocess-interrupting.bpmn")
	require.NoError(t, err)
	assert.NotNil(t, process)
	assert.Equal(t, "Process_timerEventSubProcessInterrupting", process.BpmnProcessId)
}

func TestTimerEventSubprocessNonInterrupting_LoadAndParse(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer-event-subprocess-non-interrupting.bpmn")
	require.NoError(t, err)
	assert.NotNil(t, process)
}

func TestTimerEventSubprocessNestedInterrupting_LoadAndParse(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer-event-subprocess-nested-interrupting.bpmn")
	require.NoError(t, err)
	assert.NotNil(t, process)
}

func TestTimerStartEventProcess_LoadAndParse(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	defer engine.Stop()

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer-start-event-process.bpmn")
	require.NoError(t, err)
	assert.NotNil(t, process)
}

func TestEngineWithPollTimerDelay(t *testing.T) {
	store := inmemory.NewStorage()
	customDelay := 5 * time.Second
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(customDelay))
	assert.Equal(t, customDelay, engine.pollTimerDelay)
}

func TestEngineWithPollTimerDelayDefault(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	// Without EngineWithPollTimerDelay, pollTimerDelay should be zero (default)
	// and Start() will fall back to env var or 10s default
	assert.Equal(t, time.Duration(0), engine.pollTimerDelay)
}

func TestInMemoryStorage_FindTokenActiveTimerSubscriptions_NilToken(t *testing.T) {
	store := inmemory.NewStorage()

	// Insert a timer with nil Token (timer start event scenario)
	piKey := int64(100)
	store.Timers[1] = runtime.Timer{
		ElementId:            "start-timer",
		Key:                  1,
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: 10,
		ProcessInstanceKey:   &piKey,
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            time.Now(),
		DueAt:                time.Now().Add(time.Hour),
		Token:                nil, // no token for timer start events
	}
	// Insert a timer with a Token
	store.Timers[2] = runtime.Timer{
		ElementId:            "catch-timer",
		Key:                  2,
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: 10,
		ProcessInstanceKey:   &piKey,
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            time.Now(),
		DueAt:                time.Now().Add(time.Hour),
		Token:                &runtime.ExecutionToken{Key: 42},
	}

	// Should not panic and should only return the timer with matching token key
	timers, err := store.FindTokenActiveTimerSubscriptions(t.Context(), 42)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(timers))
	assert.Equal(t, int64(2), timers[0].Key)

	// Should return empty for non-matching token key
	timers, err = store.FindTokenActiveTimerSubscriptions(t.Context(), 999)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(timers))
}

func TestInMemoryStorage_FindProcessInstanceTimers_NilProcessInstanceKey(t *testing.T) {
	store := inmemory.NewStorage()

	// Timer without process instance key (process definition timer)
	store.Timers[1] = runtime.Timer{
		ElementId:            "start-timer",
		Key:                  1,
		ProcessDefinitionKey: 10,
		ProcessInstanceKey:   nil,
		TimerState:           runtime.TimerStateCreated,
	}
	// Timer with process instance key
	piKey := int64(100)
	store.Timers[2] = runtime.Timer{
		ElementId:            "catch-timer",
		Key:                  2,
		ProcessDefinitionKey: 10,
		ProcessInstanceKey:   &piKey,
		TimerState:           runtime.TimerStateCreated,
	}

	timers, err := store.FindProcessInstanceTimers(t.Context(), 100, runtime.TimerStateCreated)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(timers))
	assert.Equal(t, int64(2), timers[0].Key)

	// Should not find the process definition timer
	timers, err = store.FindProcessInstanceTimers(t.Context(), 999, runtime.TimerStateCreated)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(timers))
}

func TestLoadFromBytes_TimerStartEvent_IdenticalReloadKeepsExistingTimer(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(200*time.Millisecond))
	engine.Start(t.Context())
	defer engine.Stop()

	bpmnData, err := os.ReadFile("./test-cases/timer-start-event-process.bpmn")
	require.NoError(t, err)

	xmlData := []byte(strings.ReplaceAll(
		string(bpmnData),
		"2026-03-28T10:00:00Z",
		time.Now().Add(1*time.Hour).UTC().Format(time.RFC3339),
	))

	def1, err := engine.LoadFromBytes(t.Context(), xmlData, engine.generateKey())
	require.NoError(t, err)
	require.NotNil(t, def1)

	err = engine.RegisterForPotentialTimerStartEvents(t.Context(), def1.Key)
	require.NoError(t, err)
	require.Len(t, store.Timers, 1)

	def2, err := engine.LoadFromBytes(t.Context(), xmlData, engine.generateKey())
	require.NoError(t, err)
	require.NotNil(t, def2)
	assert.Equal(t, def1.Key, def2.Key)
	assert.Equal(t, def1.Version, def2.Version)

	if assert.Len(t, store.Timers, 1, "expected identical reload to keep the existing timer start event timer") {
		for _, timer := range store.Timers {
			assert.Equal(t, def1.Key, timer.ProcessDefinitionKey)
		}
	}
}

// TestLoadFromBytes_TimerStartEvent_ReloadCreatesExactlyOneTimer verifies that loading a process
// definition with a timer start event twice (with timeDate in future) results in exactly
// one timer in the DB for the latest process definition, and that this only timer fires to create an
// active process instance.
func TestLoadFromBytes_TimerStartEvent_ReloadCreatesExactlyOneTimer(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(200*time.Millisecond))
	engine.Start(t.Context())
	defer engine.Stop()

	bpmnData, err := os.ReadFile("./test-cases/timer-start-event-process.bpmn")
	require.NoError(t, err)

	originalTimeDate := "2026-03-28T10:00:00Z"

	// Helper: build BPMN bytes with timeDate = now+1s
	// Uses extra milliseconds offset to ensure unique content on each call.
	callCount := 0
	buildBpmn := func() []byte {
		callCount++
		// RFC3339Nano gives nanosecond precision, guaranteeing unique content across calls.
		newTimeDate := time.Now().Add(1*time.Second + time.Duration(callCount)*time.Millisecond).UTC().Format(time.RFC3339Nano)
		return []byte(strings.ReplaceAll(string(bpmnData), originalTimeDate, newTimeDate))
	}

	// First load
	v1Bytes := buildBpmn()
	def1, err := engine.LoadFromBytes(t.Context(), v1Bytes, engine.generateKey())
	require.NoError(t, err)
	require.NotNil(t, def1)
	assert.Equal(t, int32(1), def1.Version)

	err = engine.RegisterForPotentialTimerStartEvents(t.Context(), def1.Key)
	require.NoError(t, err)

	// Second load (different content due to nanosecond-precision timestamp)
	time.Sleep(10 * time.Millisecond) // ensure different wall-clock time
	v2Bytes := buildBpmn()
	def2, err := engine.LoadFromBytes(t.Context(), v2Bytes, engine.generateKey())
	require.NoError(t, err)
	require.NotNil(t, def2)
	assert.Equal(t, int32(2), def2.Version)

	err = engine.RegisterForPotentialTimerStartEvents(t.Context(), def2.Key)
	require.NoError(t, err)

	// Wait for the timer to fire (timeDate = now+1s, wait 2s)
	time.Sleep(2 * time.Second)

	// Assert: exactly 1 timer exists in DB for the v2 process definition
	// and no timer exists for the v1 process definition
	var timersForV2 []runtime.Timer
	for _, timer := range store.Timers {
		assert.NotEqual(t, def1.Key, timer.ProcessDefinitionKey, "expected no timer for v1 process definition")
		if timer.ProcessDefinitionKey == def2.Key {
			timersForV2 = append(timersForV2, timer)
		}
	}
	assert.Equal(t, 1, len(timersForV2), "expected exactly 1 timer for v2 process definition")

	// Assert: one process instance for the v2 definition is Active
	activeInstances := 0
	for _, pi := range store.ProcessInstances {
		piData := pi.ProcessInstance()
		if piData.Definition != nil && piData.Definition.Key == def2.Key &&
			piData.GetState() == runtime.ActivityStateActive {
			activeInstances++
		}
	}
	assert.Equal(t, 1, activeInstances, "expected exactly 1 active process instance for v2 process definition")

	// Assert: no process instance exists for the v1 process definition
	for _, pi := range store.ProcessInstances {
		piData := pi.ProcessInstance()
		if piData.Definition != nil {
			assert.NotEqual(t, def1.Key, piData.Definition.Key, "expected no process instance for v1 process definition")
		}
	}
}

// TestCreateStartProcessOnTimerStartEvent_TimerMarkedTriggeredBeforeInstanceCreation
// verifies that the timer is persisted as Triggered BEFORE
// CreateInstanceWithStartingElements is called. If instance creation fails the
// timer must not stay in Created state (which would cause duplicate instances on
// the next poll cycle).
func TestCreateStartProcessOnTimerStartEvent_TimerMarkedTriggeredBeforeInstanceCreation(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))

	// Use a non-existent process definition key so that CreateInstanceWithStartingElements returns an error.
	nonExistentPDKey := engine.generateKey()
	timerKey := engine.generateKey()

	timer := runtime.Timer{
		Key:                  timerKey,
		ElementId:            "start-timer",
		ProcessDefinitionKey: nonExistentPDKey,
		ProcessInstanceKey:   nil,
		Token:                nil,
		TimerState:           runtime.TimerStateCreated,
	}
	require.NoError(t, store.SaveTimer(t.Context(), timer))

	// TriggerTimer → createStartProcessOnTimerStartEvent. Instance creation fails
	// because there is no matching process definition.
	_, _, err := engine.TriggerTimer(t.Context(), timer)
	require.Error(t, err, "expected an error because the process definition does not exist")

	// Despite the error the timer must have been marked Triggered so that
	// the timer manager does not fire it again.
	persisted, getErr := store.GetTimer(t.Context(), timerKey)
	require.NoError(t, getErr)
	assert.Equal(t, runtime.TimerStateTriggered, persisted.TimerState,
		"timer must be Triggered even when subsequent instance creation fails")
}
