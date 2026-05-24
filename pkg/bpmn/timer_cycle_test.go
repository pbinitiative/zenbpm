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

func TestParseCycle_InfiniteRepetitions(t *testing.T) {
	spec, err := parseCycle("R/PT10S")
	require.NoError(t, err)
	assert.Equal(t, -1, spec.repetitions)
	assert.False(t, spec.hasStart)
	assert.Equal(t, 10, spec.period.TS)
}

func TestParseCycle_FiniteRepetitions(t *testing.T) {
	spec, err := parseCycle("R3/PT1H")
	require.NoError(t, err)
	assert.Equal(t, 3, spec.repetitions)
	assert.False(t, spec.hasStart)
	assert.Equal(t, 1, spec.period.TH)
}

func TestParseCycle_WithStartDate(t *testing.T) {
	spec, err := parseCycle("R5/2020-01-01T00:00:00Z/P1D")
	require.NoError(t, err)
	assert.Equal(t, 5, spec.repetitions)
	assert.True(t, spec.hasStart)
	assert.Equal(t, 2020, spec.start.Year())
	assert.Equal(t, 1, spec.period.D)
}

func TestParseCycle_WithDurationAndEndDate(t *testing.T) {
	spec, err := parseCycle("R3/P1D/2020-01-03T00:00:00Z")
	require.NoError(t, err)
	assert.Equal(t, 3, spec.repetitions)
	assert.True(t, spec.hasStart)
	assert.Equal(t, time.Date(2020, time.January, 2, 0, 0, 0, 0, time.UTC), spec.start)
	assert.Equal(t, 1, spec.period.D)
}

func TestParseCycle_WithStartAndEndDate(t *testing.T) {
	spec, err := parseCycle("R3/2020-01-01T00:00:00Z/2020-01-01T00:10:00Z")
	require.NoError(t, err)
	assert.Equal(t, 3, spec.repetitions)
	assert.True(t, spec.hasStart)
	assert.True(t, spec.hasFixedPeriod)
	assert.Equal(t, 10*time.Minute, spec.fixedPeriod)
}

func TestParseCycle_CronExpression(t *testing.T) {
	spec, err := parseCycle("0 0 9 ? * MON-FRI *")
	require.NoError(t, err)
	assert.Equal(t, cycleKindCron, spec.kind)
	next := spec.firstCycleDueAt(time.Date(2026, time.May, 11, 8, 0, 0, 0, time.UTC))
	assert.Equal(t, time.Date(2026, time.May, 11, 9, 0, 0, 0, time.UTC), next)
}

func TestParseCycle_CamundaSixFieldCronExpression(t *testing.T) {
	spec, err := parseCycle("0 0 9 ? * MON-FRI")
	require.NoError(t, err)
	assert.Equal(t, cycleKindCron, spec.kind)
	next := spec.firstCycleDueAt(time.Date(2026, time.May, 11, 8, 0, 0, 0, time.UTC))
	assert.Equal(t, time.Date(2026, time.May, 11, 9, 0, 0, 0, time.UTC), next)
}

func TestParseCycle_Errors(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{"empty", ""},
		{"missing R", "PT10S"},
		{"invalid repetitions", "Rabc/PT10S"},
		{"negative repetitions", "R-1/PT10S"},
		{"missing period", "R3/"},
		{"invalid period", "R3/notADuration"},
		{"too many parts", "R3/2020-01-01T00:00:00Z/P1D/extra"},
		{"zero period", "R3/PT0S"},
		{"start after end", "R3/2020-01-01T00:10:00Z/2020-01-01T00:00:00Z"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseCycle(c.expr)
			assert.Error(t, err)
		})
	}
}

func TestFindCycleValue_NilTimeCycle(t *testing.T) {
	timerDef := bpmn20.TTimerEventDefinition{TimeCycle: nil}
	_, err := findCycleValue(timerDef)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timerDef.TimeCycle is nil")
}

func TestFindCycleValue_EmptyTimeCycle(t *testing.T) {
	id := "test-id"
	timerDef := bpmn20.TTimerEventDefinition{
		Id:        &id,
		TimeCycle: &bpmn20.TTimeInfo{XMLText: "   "},
	}
	_, err := findCycleValue(timerDef)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Can't find 'timeCycle' value")
}

func TestFindCycleValue_Valid(t *testing.T) {
	timerDef := bpmn20.TTimerEventDefinition{
		TimeCycle: &bpmn20.TTimeInfo{XMLText: "R3/PT10S"},
	}
	spec, err := findCycleValue(timerDef)
	require.NoError(t, err)
	assert.Equal(t, 3, spec.repetitions)
	assert.Equal(t, 10, spec.period.TS)
}

func TestCycleSpec_FirstCycleDueAt_WithoutStart(t *testing.T) {
	spec, err := parseCycle("R3/PT10S")
	require.NoError(t, err)
	now := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	due := spec.firstCycleDueAt(now)
	assert.Equal(t, now.Add(10*time.Second), due)
}

func TestCycleSpec_FirstCycleDueAt_WithStart(t *testing.T) {
	spec, err := parseCycle("R3/2030-06-01T12:00:00Z/PT10S")
	require.NoError(t, err)
	due := spec.firstCycleDueAt(time.Now())
	assert.Equal(t, 2030, due.Year())
	assert.Equal(t, time.June, due.Month())
}

func TestCycleSpec_ExpiredFiniteStartCycleHasNoFutureDueDate(t *testing.T) {
	spec, err := parseCycle("R3/2020-01-01T00:00:00Z/P1D")
	require.NoError(t, err)
	due, ok := spec.nextDueAt(time.Date(2026, time.May, 11, 0, 0, 0, 0, time.UTC), 0, time.Time{})
	assert.False(t, ok)
	assert.True(t, due.IsZero())
}

func TestCycleSpec_AnchoredNextDueSkipsMissedOccurrences(t *testing.T) {
	spec, err := parseCycle("R5/2026-05-11T10:00:00Z/PT1H")
	require.NoError(t, err)
	due, ok := spec.nextDueAt(time.Date(2026, time.May, 11, 12, 30, 0, 0, time.UTC), 1, time.Time{})
	require.True(t, ok)
	assert.Equal(t, time.Date(2026, time.May, 11, 13, 0, 0, 0, time.UTC), due)
}

func TestCycleSpec_NextCycleDueAt(t *testing.T) {
	spec, err := parseCycle("R/PT5S")
	require.NoError(t, err)
	now := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	due := spec.nextCycleDueAt(now)
	assert.Equal(t, now.Add(5*time.Second), due)
}

func TestIsZeroDuration(t *testing.T) {
	d, err := parseCycle("R/PT1S")
	require.NoError(t, err)
	assert.False(t, isZeroDuration(d.period))
}

func TestExtractTimerEventDefinition(t *testing.T) {
	td := bpmn20.TTimerEventDefinition{
		TimeDuration: &bpmn20.TTimeInfo{XMLText: "PT1S"},
	}
	defs := []bpmn20.EventDefinition{td}
	res := extractTimerEventDefinition(defs)
	require.NotNil(t, res)
	assert.NotNil(t, res.TimeDuration)

	assert.Nil(t, extractTimerEventDefinition([]bpmn20.EventDefinition{}))
}

func TestIsInterruptingTimerElement_StartEvent(t *testing.T) {
	def := &runtime.ProcessDefinition{}
	def.Definitions.Process.StartEvents = []bpmn20.TStartEvent{
		{TEvent: bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{TFlowElement: bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "start-1"}}}}},
	}
	timer := runtime.Timer{ElementId: "start-1", ProcessInstanceKey: nil}
	assert.False(t, isInterruptingTimerElement(def, timer))
}

func TestIsInterruptingTimerElement_BoundaryCancelActivity(t *testing.T) {
	def := &runtime.ProcessDefinition{}
	def.Definitions.Process.BoundaryEvent = []bpmn20.TBoundaryEvent{
		{TEvent: bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{TFlowElement: bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "boundary-1"}}}}, CancellActivity: true},
	}
	piKey := int64(1)
	timer := runtime.Timer{ElementId: "boundary-1", ProcessInstanceKey: &piKey}
	assert.True(t, isInterruptingTimerElement(def, timer))
}

func TestIsInterruptingTimerElement_BoundaryNonCancelActivity(t *testing.T) {
	def := &runtime.ProcessDefinition{}
	def.Definitions.Process.BoundaryEvent = []bpmn20.TBoundaryEvent{
		{TEvent: bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{TFlowElement: bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "boundary-1"}}}}, CancellActivity: false},
	}
	piKey := int64(1)
	timer := runtime.Timer{ElementId: "boundary-1", ProcessInstanceKey: &piKey}
	assert.False(t, isInterruptingTimerElement(def, timer))
}

func TestIsInterruptingTimerElement_IntermediateCatchAlwaysInterrupting(t *testing.T) {
	def := &runtime.ProcessDefinition{}
	def.Definitions.Process.IntermediateCatchEvent = []bpmn20.TIntermediateCatchEvent{
		{TEvent: bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{TFlowElement: bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "catch-1"}}}}},
	}
	piKey := int64(1)
	timer := runtime.Timer{ElementId: "catch-1", ProcessInstanceKey: &piKey}
	assert.True(t, isInterruptingTimerElement(def, timer))
}

func TestIsInterruptingTimerElement_NilDefinition(t *testing.T) {
	timer := runtime.Timer{ElementId: "x"}
	assert.False(t, isInterruptingTimerElement(nil, timer))
}

func TestFindTimerEventDefinition_StartEvent(t *testing.T) {
	def := &runtime.ProcessDefinition{}
	td := bpmn20.TTimerEventDefinition{TimeCycle: &bpmn20.TTimeInfo{XMLText: "R3/PT5S"}}
	def.Definitions.Process.StartEvents = []bpmn20.TStartEvent{
		{
			TEvent:           bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{TFlowElement: bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "start-1"}}}},
			EventDefinitions: []bpmn20.EventDefinition{td},
		},
	}
	res := findTimerEventDefinition(def, "start-1")
	require.NotNil(t, res)
	assert.NotNil(t, res.TimeCycle)
}

func TestFindTimerEventDefinition_NotFound(t *testing.T) {
	def := &runtime.ProcessDefinition{}
	assert.Nil(t, findTimerEventDefinition(def, "missing"))
	assert.Nil(t, findTimerEventDefinition(nil, "missing"))
}

func TestFindTimerEventDefinition_BoundaryEvent(t *testing.T) {
	def := &runtime.ProcessDefinition{}
	td := bpmn20.TTimerEventDefinition{TimeDuration: &bpmn20.TTimeInfo{XMLText: "PT5S"}}
	def.Definitions.Process.BoundaryEvent = []bpmn20.TBoundaryEvent{
		{
			TEvent:          bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{TFlowElement: bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "b-1"}}}},
			EventDefinition: td,
		},
	}
	res := findTimerEventDefinition(def, "b-1")
	require.NotNil(t, res)
	assert.NotNil(t, res.TimeDuration)
}

// TestBpmnParse_TimeCycle verifies that the timeCycle element is correctly parsed from a BPMN XML file.
func TestBpmnParse_TimeCycle(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer_event_subprocess/timer-event-subprocess-non-interrupting-cycle.bpmn")
	require.NoError(t, err)
	require.NotNil(t, process)

	def, err := store.FindProcessDefinitionByKey(t.Context(), process.Key)
	require.NoError(t, err)

	subProcess, startEvent := def.Definitions.Process.GetSubprocessAndStartEventById("eventSubprocessCycleStart")
	require.NotNil(t, subProcess)
	require.NotNil(t, startEvent)
	assert.False(t, startEvent.IsInterrupting, "expected non-interrupting start event")

	td := extractTimerEventDefinition(startEvent.EventDefinitions)
	require.NotNil(t, td)
	require.NotNil(t, td.TimeCycle)
	assert.Equal(t, "R3/PT1S", td.TimeCycle.XMLText)

	spec, err := findCycleValue(*td)
	require.NoError(t, err)
	assert.Equal(t, 3, spec.repetitions)
	assert.Equal(t, 1, spec.period.TS)
}

// TestCreateCycleStartTimer_DoesNotRecreateExhaustedDefinitionCycle
func TestCreateCycleStartTimer_DoesNotRecreateExhaustedDefinitionCycle(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	processDefinitionKey := int64(202)
	elementId := "timer-start-cycle"
	firstDue := time.Now().Add(-5 * time.Second)
	for i := 0; i < 3; i++ {
		require.NoError(t, store.SaveTimer(t.Context(), runtime.Timer{
			ElementId:            elementId,
			Key:                  int64(i + 1),
			ProcessDefinitionKey: processDefinitionKey,
			TimerState:           runtime.TimerStateTriggered,
			DueAt:                firstDue.Add(time.Duration(i) * time.Second),
		}))
	}

	timer, err := engine.createCycleStartTimer(t.Context(), processDefinitionKey, nil, elementId, bpmn20.TTimerEventDefinition{
		TimeCycle: &bpmn20.TTimeInfo{XMLText: "R3/PT1S"},
	}, nil)
	require.NoError(t, err)
	assert.Nil(t, timer)
}

// TestCreateCycleStartTimer_InFlightTriggeredKey_RearmsNext guards the definition-level
// timeCycle renewal path. When the just-consumed timer is still persisted as Created (its
// Triggered transition lives in an open, not-yet-flushed batch), createCycleStartTimer must
// still arm the next cycle iteration when the caller passes the in-flight timer key. This
// is the regression that broke the engine for R[n]/PT cycles on definition-level start events.
func TestCreateCycleStartTimer_InFlightTriggeredKey_RearmsNext(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	processDefinitionKey := int64(303)
	elementId := "timer-start-cycle-renew"

	// Simulate the world after the very first cycle iteration fired and is being consumed
	// in an open batch: the persisted state is still "Created" — exactly what cycleTimerStats
	// would otherwise see when called from the renewal flow inside the same batch.
	inFlightKey := int64(101)
	require.NoError(t, store.SaveTimer(t.Context(), runtime.Timer{
		ElementId:            elementId,
		Key:                  inFlightKey,
		ProcessDefinitionKey: processDefinitionKey,
		TimerState:           runtime.TimerStateCreated,
		DueAt:                time.Now().Add(-100 * time.Millisecond),
	}))

	// Without the in-flight hint, renewal incorrectly sees createdExists==true and bails out.
	noHint, err := engine.createCycleStartTimer(t.Context(), processDefinitionKey, nil, elementId, bpmn20.TTimerEventDefinition{
		TimeCycle: &bpmn20.TTimeInfo{XMLText: "R2/PT1S"},
	}, nil)
	require.NoError(t, err)
	require.Nil(t, noHint, "without inFlightTriggeredTimerKey the renewal would not be armed (regression baseline)")

	// With the in-flight hint, the just-consumed timer is treated as Triggered, so the next
	// (and final) iteration of R2/PT1S must be armed.
	withHint, err := engine.createCycleStartTimer(t.Context(), processDefinitionKey, nil, elementId, bpmn20.TTimerEventDefinition{
		TimeCycle: &bpmn20.TTimeInfo{XMLText: "R2/PT1S"},
	}, &inFlightKey)
	require.NoError(t, err)
	require.NotNil(t, withHint, "with inFlightTriggeredTimerKey the second cycle iteration must be armed")
	assert.True(t, withHint.DueAt.After(time.Now()), "renewed timer must be due in the future, got %v", withHint.DueAt)
}

// TestCreateCycleStartTimer_InFlightTriggeredKey_ExhaustsCycle verifies that the in-flight
// hint is correctly counted against the cycle's repetition budget: when R2 has already
// produced one Triggered timer in persistence and the in-flight one would be the second,
// no further timer is armed.
func TestCreateCycleStartTimer_InFlightTriggeredKey_ExhaustsCycle(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	processDefinitionKey := int64(304)
	elementId := "timer-start-cycle-exhaust"
	now := time.Now()

	// Already-Triggered first iteration.
	require.NoError(t, store.SaveTimer(t.Context(), runtime.Timer{
		ElementId:            elementId,
		Key:                  201,
		ProcessDefinitionKey: processDefinitionKey,
		TimerState:           runtime.TimerStateTriggered,
		DueAt:                now.Add(-2 * time.Second),
	}))
	// In-flight (still persisted as Created) second iteration being consumed in the batch.
	inFlightKey := int64(202)
	require.NoError(t, store.SaveTimer(t.Context(), runtime.Timer{
		ElementId:            elementId,
		Key:                  inFlightKey,
		ProcessDefinitionKey: processDefinitionKey,
		TimerState:           runtime.TimerStateCreated,
		DueAt:                now.Add(-time.Second),
	}))

	timer, err := engine.createCycleStartTimer(t.Context(), processDefinitionKey, nil, elementId, bpmn20.TTimerEventDefinition{
		TimeCycle: &bpmn20.TTimeInfo{XMLText: "R2/PT1S"},
	}, &inFlightKey)
	require.NoError(t, err)
	assert.Nil(t, timer, "R2 cycle should be exhausted after the in-flight iteration is counted (Triggered: 1, in-flight: 1)")
}

// TestBuildNextCycleTimer_NoRearmAfterAllRepetitions verifies that once the triggered
// count reaches the configured repetitions, no further timer is built.
func TestBuildNextCycleTimer_NoRearmAfterAllRepetitions(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	piKey := int64(100)
	processDefinitionKey := int64(200)
	elementId := "evt-start-1"
	firstDue := time.Now().Add(-3 * time.Second)
	require.NoError(t, store.SaveTimer(t.Context(), runtime.Timer{
		ElementId:            elementId,
		Key:                  1,
		ProcessDefinitionKey: processDefinitionKey,
		ProcessInstanceKey:   &piKey,
		TimerState:           runtime.TimerStateTriggered,
		DueAt:                firstDue,
	}))

	// Build a definition with a non-interrupting event subprocess timer-start cycle.
	def := &runtime.ProcessDefinition{}
	def.Key = processDefinitionKey
	def.Definitions.Process.SubProcess = []bpmn20.TSubProcess{
		{
			TActivity: bpmn20.TActivity{
				TFlowNode: bpmn20.TFlowNode{
					TFlowElement: bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "sub-1"}},
				},
			},
			TProcess: bpmn20.TProcess{
				TFlowElementsContainer: bpmn20.TFlowElementsContainer{
					StartEvents: []bpmn20.TStartEvent{
						{
							TEvent:           bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{TFlowElement: bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "evt-start-1"}}}},
							IsInterrupting:   false,
							EventDefinitions: []bpmn20.EventDefinition{bpmn20.TTimerEventDefinition{TimeCycle: &bpmn20.TTimeInfo{XMLText: "R2/PT1S"}}},
						},
					},
				},
			},
		},
	}
	// Sanity: parser sees R2.
	td := findTimerEventDefinition(def, "evt-start-1")
	require.NotNil(t, td)
	require.NotNil(t, td.TimeCycle)
	spec, err := findCycleValue(*td)
	require.NoError(t, err)
	assert.Equal(t, 2, spec.repetitions)

	next, err := engine.buildNextCycleTimer(t.Context(), def, runtime.Timer{
		ElementId:            elementId,
		Key:                  2,
		ProcessDefinitionKey: processDefinitionKey,
		ProcessInstanceKey:   &piKey,
		TimerState:           runtime.TimerStateTriggered,
		DueAt:                firstDue.Add(time.Second),
	})
	require.NoError(t, err)
	assert.Nil(t, next)
}

func TestBuildNextCycleTimer_BoundaryFiniteCycleNoRearmAfterAllRepetitions(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	piKey := int64(101)
	processDefinitionKey := int64(201)
	elementId := "boundary-cycle"
	firstDue := time.Now().Add(-3 * time.Second)
	require.NoError(t, store.SaveTimer(t.Context(), runtime.Timer{
		ElementId:            elementId,
		Key:                  1,
		ProcessDefinitionKey: processDefinitionKey,
		ProcessInstanceKey:   &piKey,
		TimerState:           runtime.TimerStateTriggered,
		DueAt:                firstDue,
	}))

	def := &runtime.ProcessDefinition{Key: processDefinitionKey}
	def.Definitions.Process.BoundaryEvent = []bpmn20.TBoundaryEvent{
		{
			TEvent:          bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{TFlowElement: bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: elementId}}}},
			CancellActivity: false,
			EventDefinition: bpmn20.TTimerEventDefinition{TimeCycle: &bpmn20.TTimeInfo{XMLText: "R2/PT1S"}},
		},
	}

	next, err := engine.buildNextCycleTimer(t.Context(), def, runtime.Timer{
		ElementId:            elementId,
		Key:                  2,
		ProcessDefinitionKey: processDefinitionKey,
		ProcessInstanceKey:   &piKey,
		TimerState:           runtime.TimerStateTriggered,
		DueAt:                firstDue.Add(time.Second),
	})
	require.NoError(t, err)
	assert.Nil(t, next)
}

// TestTimerEventSubprocessNonInterruptingCycle_FiresExpectedNumberOfTimes verifies that a
// non-interrupting event subprocess timer-start with timeCycle = R3/PT1S fires exactly 3 times,
// producing 3 Triggered timers and (after the cycle completes) no leftover Created timer.
func TestTimerEventSubprocessNonInterruptingCycle_FiresExpectedNumberOfTimes(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(100*time.Millisecond))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	// Hold the main service task open so the parent stays active while the cycle fires.
	h := engine.NewTaskHandler().Type("cycle-event-subprocess-task").Handler(func(job ActivatedJob) {
		// intentionally do not complete the job
	})
	defer engine.RemoveHandler(h)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer_event_subprocess/timer-event-subprocess-non-interrupting-cycle.bpmn")
	require.NoError(t, err)
	require.NotNil(t, process)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	piKey := instance.ProcessInstance().Key

	// 1s timer fires (PT1S × R3) ≈ 3s. Allow some headroom.
	const cycleElementId = "eventSubprocessCycleStart"
	var triggered []runtime.Timer
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		all, err := store.FindProcessInstanceTimers(t.Context(), piKey, runtime.TimerStateTriggered)
		if !assert.NoError(collect, err) {
			return
		}
		filtered := make([]runtime.Timer, 0, len(all))
		for _, tt := range all {
			if tt.ElementId == cycleElementId {
				filtered = append(filtered, tt)
			}
		}
		triggered = filtered
		assert.Equal(collect, 3, len(triggered),
			"expected R3 cycle to produce exactly 3 triggered timers")
	}, 6*time.Second, 100*time.Millisecond,
		"R3/PT1S cycle should produce 3 triggered timers")

	// Assert no extra cycle iteration is ever scheduled after R3 is exhausted.
	require.Never(t, func() bool {
		created, err := store.FindProcessInstanceTimers(t.Context(), piKey, runtime.TimerStateCreated)
		if err != nil {
			return false
		}
		for _, c := range created {
			if c.ElementId == cycleElementId {
				return true
			}
		}
		return false
	}, 1500*time.Millisecond, 100*time.Millisecond,
		"no further Created cycle timer should be scheduled after R3 is exhausted")
}
