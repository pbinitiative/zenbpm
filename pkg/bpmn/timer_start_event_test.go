package bpmn

import (
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
