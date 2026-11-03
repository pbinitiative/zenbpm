package bpmn

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuspendedProcessStartsNoInstanceThroughTheApi(t *testing.T) {
	engine, store := startEngineWithStore(t)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	suspendProcess(t, engine, process.BpmnProcessId, true)

	startCalls := map[string]func() (runtime.ProcessInstance, error){
		"by key": func() (runtime.ProcessInstance, error) {
			return engine.CreateInstanceByKey(t.Context(), process.Key, nil)
		},
		"by id": func() (runtime.ProcessInstance, error) {
			return engine.CreateInstanceById(t.Context(), process.BpmnProcessId, nil)
		},
		"on elements": func() (runtime.ProcessInstance, error) {
			return engine.CreateInstanceWithStartingElements(t.Context(), process.Key, []string{"id"}, nil, nil)
		},
	}
	for name, start := range startCalls {
		t.Run(name, func(t *testing.T) {
			instance, err := start()
			require.Error(t, err)
			assert.Nil(t, instance)
			var suspendedErr *ProcessSuspendedError
			require.ErrorAs(t, err, &suspendedErr)
			assert.Equal(t, process.BpmnProcessId, suspendedErr.BpmnProcessId)
		})
	}
	assert.Empty(t, instancesOfDefinition(store, process.Key), "a suspended process must not leave an instance behind")

	suspendProcess(t, engine, process.BpmnProcessId, false)
	for name, start := range startCalls {
		t.Run("resumed "+name, func(t *testing.T) {
			instance, err := start()
			require.NoError(t, err)
			assert.NotNil(t, instance)
		})
	}
}

func TestSuspensionAppliesToEveryVersionOfOneProcessOnly(t *testing.T) {
	engine, _ := startEngineWithStore(t)

	processId := fmt.Sprintf("suspended-versions-%d", time.Now().UnixNano())
	otherProcessId := processId + "-other"
	_, err := engine.LoadFromBytes(t.Context(), []byte(callActivityVersionChildBPMN(processId, "task-v1")), engine.generateKey())
	require.NoError(t, err)
	other, err := engine.LoadFromBytes(t.Context(), []byte(callActivityVersionChildBPMN(otherProcessId, "task-other")), engine.generateKey())
	require.NoError(t, err)

	suspendProcess(t, engine, processId, true)

	// a version deployed while the process is suspended is suspended as well
	v2, err := engine.LoadFromBytes(t.Context(), []byte(callActivityVersionChildBPMN(processId, "task-v2")), engine.generateKey())
	require.NoError(t, err)
	require.EqualValues(t, 2, v2.Version)
	_, err = engine.CreateInstanceByKey(t.Context(), v2.Key, nil)
	var suspendedErr *ProcessSuspendedError
	require.ErrorAs(t, err, &suspendedErr)

	// other processes are not affected
	_, err = engine.CreateInstanceByKey(t.Context(), other.Key, nil)
	require.NoError(t, err)

	suspended, err := engine.IsProcessSuspended(t.Context(), processId)
	require.NoError(t, err)
	assert.True(t, suspended)
	suspended, err = engine.IsProcessSuspended(t.Context(), otherProcessId)
	require.NoError(t, err)
	assert.False(t, suspended)
}

func TestEveryChangeOfTheSuspensionHasAHigherVersionAndAnOlderOneIsNotApplied(t *testing.T) {
	engine, _ := startEngineWithStore(t)
	const processId = "versioned-process"
	isSuspended := func() bool {
		suspended, err := engine.IsProcessSuspended(t.Context(), processId)
		require.NoError(t, err)
		return suspended
	}

	first := suspendProcess(t, engine, processId, true)
	second := suspendProcess(t, engine, processId, true)
	third := suspendProcess(t, engine, processId, false)
	assert.Less(t, first, second)
	assert.Less(t, second, third)
	assert.False(t, isSuspended())

	// a change which another partition made under an older version does not undo the newer one
	held, err := engine.ApplyProcessSuspension(t.Context(), processId, true, second)
	require.NoError(t, err)
	assert.Equal(t, storage.ProcessSuspension{Suspended: false, Version: third}, held)
	assert.False(t, isSuspended())
	// a newer one does
	held, err = engine.ApplyProcessSuspension(t.Context(), processId, true, third+1)
	require.NoError(t, err)
	assert.Equal(t, storage.ProcessSuspension{Suspended: true, Version: third + 1}, held)
	assert.True(t, isSuspended())

	// the next change made here continues after the newest version it was told about
	assert.Equal(t, third+2, suspendProcess(t, engine, processId, false))
}

func TestAChangeWhichWasNeverTakenAsAVersionIsNotApplied(t *testing.T) {
	engine, _ := startEngineWithStore(t)

	for _, version := range []int64{0, -1} {
		_, err := engine.ApplyProcessSuspension(t.Context(), "never-taken", true, version)
		require.ErrorContains(t, err, "version")
	}
	suspended, err := engine.IsProcessSuspended(t.Context(), "never-taken")
	require.NoError(t, err)
	assert.False(t, suspended)
}

func TestSuspendedProcessDoesNotAffectRunningInstances(t *testing.T) {
	engine, store := startEngineWithStore(t)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, map[string]any{"variable_name": "oldVal"})
	require.NoError(t, err)
	job := waitForPendingJobInStore(t, store, instance.ProcessInstance().Key)

	suspendProcess(t, engine, process.BpmnProcessId, true)

	require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, map[string]any{"variable_name": "newVal"}))
	require.Eventually(t, func() bool {
		persisted, findErr := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		return findErr == nil && persisted.ProcessInstance().State == runtime.ActivityStateCompleted
	}, 2*time.Second, 20*time.Millisecond, "an instance which was running when the process was suspended must run to its end")
}

func TestSuspendedProcessTimerStartEventConsumesTheTimerAndStartsNoInstance(t *testing.T) {
	// a long poll delay keeps the timer manager away: the test fires the timers itself
	store := inmemory.NewStorage()
	var logs syncBuffer
	logger := hclog.New(&hclog.LoggerOptions{Output: &logs, Level: hclog.Warn})
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(time.Hour), EngineWithLogger(logger))
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)

	bpmnData, err := os.ReadFile("./test-cases/process_definition_start_event/timer-start-event-process.bpmn")
	require.NoError(t, err)
	futureTimeDate := time.Now().Add(100 * 365 * 24 * time.Hour).UTC().Format(time.RFC3339)
	xmlData := []byte(strings.ReplaceAll(string(bpmnData), "2026-03-28T10:00:00Z", futureTimeDate))
	def, err := engine.LoadFromBytes(t.Context(), xmlData, engine.generateKey())
	require.NoError(t, err)

	h := engine.NewTaskHandler().Type("input-task-for-timer-start-event-test").Handler(func(ActivatedJob) {})
	t.Cleanup(func() { engine.RemoveHandler(h) })

	fire := func() runtime.Timer {
		timer := runtime.Timer{
			Key:                  engine.generateKey(),
			ElementId:            "timerStartEvent_1234",
			ProcessDefinitionKey: def.Key,
			TimerState:           runtime.TimerStateCreated,
			CreatedAt:            time.Now(),
			DueAt:                time.Now(),
		}
		require.NoError(t, store.SaveTimer(t.Context(), timer))
		_, _, triggerErr := engine.TriggerTimer(t.Context(), timer)
		require.NoError(t, triggerErr)
		persisted, getErr := store.GetTimer(t.Context(), timer.Key)
		require.NoError(t, getErr)
		return persisted
	}

	suspendProcess(t, &engine, def.BpmnProcessId, true)
	timer := fire()
	assert.Equal(t, runtime.TimerStateTriggered, timer.TimerState, "the timer must be consumed, or it would fire again and again")
	created, err := store.FindProcessDefinitionTimers(t.Context(), def.Key, runtime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, created, "a timer which fires once is not made up for after the process is resumed")
	assert.Contains(t, logs.String(), "one-shot start event trigger dropped", "whoever suspended the process can find out that the start was dropped")
	assert.Empty(t, instancesOfDefinition(store, def.Key), "a suspended process must not start an instance from its timer start event")

	suspendProcess(t, &engine, def.BpmnProcessId, false)
	fire()
	assert.Len(t, instancesOfDefinition(store, def.Key), 1, "a resumed process starts instances from its timer start event again")
}

func TestSuspendedProcessCycleTimerStartEventCarriesOnWithTheNextRepetition(t *testing.T) {
	store := inmemory.NewStorage()
	var logs syncBuffer
	logger := hclog.New(&hclog.LoggerOptions{Output: &logs, Level: hclog.Info})
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(time.Hour), EngineWithLogger(logger))
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)

	bpmnData, err := os.ReadFile("./test-cases/process_definition_start_event/timer-cycle-start-event-simple.bpmn")
	require.NoError(t, err)
	xmlData := []byte(strings.ReplaceAll(string(bpmnData), "__TIMER_CYCLE_PLACEHOLDER__", "R3/PT1H"))
	def, err := engine.LoadFromBytes(t.Context(), xmlData, engine.generateKey())
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))
	timers, err := store.FindProcessDefinitionTimers(t.Context(), def.Key, runtime.TimerStateCreated)
	require.NoError(t, err)
	require.Len(t, timers, 1, "the cycle start event registers one timer")
	suspendProcess(t, &engine, def.BpmnProcessId, true)

	_, _, err = engine.TriggerTimer(t.Context(), timers[0])
	require.NoError(t, err)

	assert.Empty(t, instancesOfDefinition(store, def.Key), "a suspended process must not start an instance from its timer start event")
	created, err := store.FindProcessDefinitionTimers(t.Context(), def.Key, runtime.TimerStateCreated)
	require.NoError(t, err)
	assert.Len(t, created, 1, "the cycle must carry on with its next repetition")
	assert.NotEqual(t, timers[0].Key, created[0].Key)
	assert.NotContains(t, logs.String(), "start event trigger", "a cycle can fire every second: a skipped firing is logged at debug level only")
}

func TestARunningInstanceWhichCallsItsOwnSuspendedProcessGetsAnIncident(t *testing.T) {
	engine, store := startEngineWithStore(t)

	processId := fmt.Sprintf("suspended-recursion-%d", time.Now().UnixNano())
	recursive, err := engine.LoadFromBytes(t.Context(), []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0">
  <bpmn:process id="%s" isExecutable="true">
    <bpmn:startEvent id="start"><bpmn:outgoing>to-wait</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="wait">
      <bpmn:extensionElements><zenbpm:taskDefinition type="suspended-recursion-wait" /></bpmn:extensionElements>
      <bpmn:incoming>to-wait</bpmn:incoming><bpmn:outgoing>to-call</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:callActivity id="call-itself">
      <bpmn:extensionElements><zenbpm:calledElement processId="%s" /></bpmn:extensionElements>
      <bpmn:incoming>to-call</bpmn:incoming><bpmn:outgoing>to-end</bpmn:outgoing>
    </bpmn:callActivity>
    <bpmn:endEvent id="end"><bpmn:incoming>to-end</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="to-wait" sourceRef="start" targetRef="wait" />
    <bpmn:sequenceFlow id="to-call" sourceRef="wait" targetRef="call-itself" />
    <bpmn:sequenceFlow id="to-end" sourceRef="call-itself" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>`, processId, processId)), engine.generateKey())
	require.NoError(t, err)
	running, err := engine.CreateInstanceByKey(t.Context(), recursive.Key, nil)
	require.NoError(t, err)
	job := waitForPendingJobInStore(t, store, running.ProcessInstance().Key)

	suspendProcess(t, engine, processId, true)
	// the running instance carries on until it reaches the call of its own, suspended, process
	require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

	require.Eventually(t, func() bool {
		incidents, findErr := store.FindIncidentsByProcessInstanceKey(t.Context(), running.ProcessInstance().Key)
		return findErr == nil && len(incidents) == 1 && incidents[0].ElementId == "call-itself" && strings.Contains(incidents[0].Message, "suspended")
	}, 2*time.Second, 20*time.Millisecond, "the call into the suspended process is an incident of the instance which makes it")
	assert.Len(t, instancesOfDefinition(store, recursive.Key), 1, "no second instance of the suspended process was started")
}

func TestSuspendedProcessUsesUpTheRepetitionsOfALimitedCycle(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(time.Hour))
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)

	bpmnData, err := os.ReadFile("./test-cases/process_definition_start_event/timer-cycle-start-event-simple.bpmn")
	require.NoError(t, err)
	xmlData := []byte(strings.ReplaceAll(string(bpmnData), "__TIMER_CYCLE_PLACEHOLDER__", "R1/PT1H"))
	def, err := engine.LoadFromBytes(t.Context(), xmlData, engine.generateKey())
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))
	timers, err := store.FindProcessDefinitionTimers(t.Context(), def.Key, runtime.TimerStateCreated)
	require.NoError(t, err)
	require.Len(t, timers, 1)
	suspendProcess(t, &engine, def.BpmnProcessId, true)

	_, _, err = engine.TriggerTimer(t.Context(), timers[0])
	require.NoError(t, err)

	assert.Empty(t, instancesOfDefinition(store, def.Key))
	created, err := store.FindProcessDefinitionTimers(t.Context(), def.Key, runtime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, created, "the only repetition was used up by the firing which was skipped, and nothing is made up on resume")
}

func TestSuspendedProcessRefusesTheMessageOfItsMessageStartEventAndKeepsTheSubscription(t *testing.T) {
	engine, store := startEngineWithStore(t)

	def, err := engine.LoadFromFile(t.Context(), "./test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))
	h := engine.NewTaskHandler().Type("input-task-for-message-start-event-test").Handler(func(ActivatedJob) {})
	t.Cleanup(func() { engine.RemoveHandler(h) })

	suspendProcess(t, engine, def.BpmnProcessId, true)
	err = engine.PublishMessageByName(t.Context(), "messageStartEventProcessRef", nil, nil)
	var suspendedErr *ProcessSuspendedError
	require.ErrorAs(t, err, &suspendedErr)
	assert.Empty(t, instancesOfDefinition(store, def.Key))
	subscriptions := definitionMessageSubscriptionsForDefinition(store, def.Key)
	require.Len(t, subscriptions, 1)
	assert.Equal(t, runtime.ActivityStateActive, subscriptions[0].State, "the refused message must leave the subscription active")

	suspendProcess(t, engine, def.BpmnProcessId, false)
	require.NoError(t, engine.PublishMessageByName(t.Context(), "messageStartEventProcessRef", nil, nil))
	assert.Len(t, instancesOfDefinition(store, def.Key), 1, "the message starts an instance once the process is resumed")
}

func TestSuspendedProcessRefusesTheMessageOfItsInstantiatingReceiveTaskAndKeepsTheSubscription(t *testing.T) {
	engine, store := startEngineWithStore(t)

	def, err := engine.LoadFromFile(t.Context(), instantiatingReceiveTaskBpmn)
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))
	h := engine.NewTaskHandler().Type("input-task-for-receive-task-test").Handler(func(ActivatedJob) {})
	t.Cleanup(func() { engine.RemoveHandler(h) })

	subscriptionKey := definitionMessageSubscriptionsForDefinition(store, def.Key)[0].Key

	suspendProcess(t, engine, def.BpmnProcessId, true)
	err = engine.PublishMessageByName(t.Context(), instantiatingReceiveMessageName, nil, nil)
	var suspendedErr *ProcessSuspendedError
	require.ErrorAs(t, err, &suspendedErr)
	assert.Empty(t, instancesOfDefinition(store, def.Key))
	subscriptions := definitionMessageSubscriptionsForDefinition(store, def.Key)
	require.Len(t, subscriptions, 1, "the refused message must not leave a second subscription behind")
	assert.Equal(t, subscriptionKey, subscriptions[0].Key, "the subscription is refused before it is consumed, not consumed and armed again")
	assert.Equal(t, runtime.ActivityStateActive, subscriptions[0].State)

	suspendProcess(t, engine, def.BpmnProcessId, false)
	require.NoError(t, engine.PublishMessageByName(t.Context(), instantiatingReceiveMessageName, nil, nil))
	assert.Len(t, instancesOfDefinition(store, def.Key), 1, "the message starts an instance once the process is resumed")
}

func TestCallActivityOfSuspendedProcessCreatesIncidentThatResolvesAfterResume(t *testing.T) {
	engine, store := startEngineWithStore(t)

	suffix := time.Now().UnixNano()
	childProcessId := fmt.Sprintf("suspended-call-activity-child-%d", suffix)
	parentProcessId := fmt.Sprintf("suspended-call-activity-parent-%d", suffix)
	child, err := engine.LoadFromBytes(t.Context(), []byte(callActivityVersionChildBPMN(childProcessId, "child-task")), engine.generateKey())
	require.NoError(t, err)
	parent, err := engine.LoadFromBytes(t.Context(), []byte(callActivityVersionParentBPMN(parentProcessId, childProcessId, "")), engine.generateKey())
	require.NoError(t, err)
	suspendProcess(t, engine, childProcessId, true)

	parentInstance, err := engine.CreateInstanceByKey(t.Context(), parent.Key, nil)
	require.ErrorContains(t, err, "suspended")
	// the one who starts the caller is not told that the process which was asked for is suspended: it is not, and
	// its instance exists, with an incident
	var suspendedErr *ProcessSuspendedError
	require.False(t, errors.As(err, &suspendedErr))
	require.NotNil(t, parentInstance, "the calling instance exists: it is the call that failed")

	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), parentInstance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	assert.Equal(t, "call-activity", incidents[0].ElementId)
	assert.Contains(t, incidents[0].Message, "suspended")
	assert.Empty(t, instancesOfDefinition(store, child.Key), "a suspended process must not be started by a call activity")

	suspendProcess(t, engine, childProcessId, false)
	require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))
	require.Eventually(t, func() bool {
		return len(instancesOfDefinition(store, child.Key)) == 1
	}, 2*time.Second, 20*time.Millisecond, "resolving the incident after resume must call the process")
}

// startEngineWithStore starts an engine on its own storage, so that the tests can inspect what it persisted.
func startEngineWithStore(t *testing.T) (*Engine, *inmemory.Storage) {
	t.Helper()
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	return &engine, store
}

func suspendProcess(t *testing.T, engine *Engine, bpmnProcessId string, suspended bool) int64 {
	t.Helper()
	version, err := engine.SetProcessSuspended(t.Context(), bpmnProcessId, suspended)
	require.NoError(t, err)
	return version
}

func instancesOfDefinition(store *inmemory.Storage, processDefinitionKey int64) []runtime.ProcessInstance {
	var instances []runtime.ProcessInstance
	for _, instance := range store.ProcessInstancesSnapshot() {
		definition := instance.ProcessInstance().Definition
		if definition != nil && definition.Key == processDefinitionKey {
			instances = append(instances, instance)
		}
	}
	return instances
}

// failingSuspensionReadStorage cannot tell whether one process is suspended.
type failingSuspensionReadStorage struct {
	*inmemory.Storage
	// failFor is the id of the process whose suspension cannot be read, set by the test while the engine runs.
	failFor atomic.Value
}

func (f *failingSuspensionReadStorage) IsProcessSuspended(ctx context.Context, bpmnProcessId string) (bool, error) {
	if failFor, _ := f.failFor.Load().(string); bpmnProcessId == failFor {
		return false, errors.New("storage is gone")
	}
	return f.Storage.IsProcessSuspended(ctx, bpmnProcessId)
}

// TestAFailedReadOfTheSuspensionNeverStartsAnInstance shows that when the engine cannot tell whether a process is
// suspended it refuses to start it, and consumes nothing, on every route.
func TestAFailedReadOfTheSuspensionNeverStartsAnInstance(t *testing.T) {
	suffix := time.Now().UnixNano()
	childProcessId := fmt.Sprintf("failing-read-child-%d", suffix)
	parentProcessId := fmt.Sprintf("failing-read-parent-%d", suffix)

	store := inmemory.NewStorage()
	failing := &failingSuspensionReadStorage{Storage: store}
	engine := NewEngine(EngineWithStorage(failing), EngineWithPollTimerDelay(time.Hour))
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)

	child, err := engine.LoadFromBytes(t.Context(), []byte(callActivityVersionChildBPMN(childProcessId, "child-task")), engine.generateKey())
	require.NoError(t, err)
	parent, err := engine.LoadFromBytes(t.Context(), []byte(callActivityVersionParentBPMN(parentProcessId, childProcessId, "")), engine.generateKey())
	require.NoError(t, err)
	message, err := engine.LoadFromFile(t.Context(), "./test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), message.Key))
	receiveTask, err := engine.LoadFromFile(t.Context(), instantiatingReceiveTaskBpmn)
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), receiveTask.Key))
	timerBpmn, err := os.ReadFile("./test-cases/process_definition_start_event/timer-start-event-process.bpmn")
	require.NoError(t, err)
	futureTimeDate := time.Now().Add(100 * 365 * 24 * time.Hour).UTC().Format(time.RFC3339)
	timerDef, err := engine.LoadFromBytes(t.Context(), []byte(strings.ReplaceAll(string(timerBpmn), "2026-03-28T10:00:00Z", futureTimeDate)), engine.generateKey())
	require.NoError(t, err)

	t.Run("the api", func(t *testing.T) {
		failing.failFor.Store(childProcessId)
		instance, err := engine.CreateInstanceByKey(t.Context(), child.Key, nil)
		require.ErrorContains(t, err, "storage is gone")
		assert.Nil(t, instance)
		assert.Empty(t, instancesOfDefinition(store, child.Key))
	})

	t.Run("a call activity", func(t *testing.T) {
		failing.failFor.Store(childProcessId)
		parentInstance, err := engine.CreateInstanceByKey(t.Context(), parent.Key, nil)
		require.ErrorContains(t, err, "storage is gone")
		require.NotNil(t, parentInstance, "the caller exists, with an incident")
		incidents, findErr := store.FindIncidentsByProcessInstanceKey(t.Context(), parentInstance.ProcessInstance().Key)
		require.NoError(t, findErr)
		assert.Len(t, incidents, 1)
		assert.Empty(t, instancesOfDefinition(store, child.Key), "the called process was not started")
	})

	t.Run("a message start event", func(t *testing.T) {
		failing.failFor.Store(message.BpmnProcessId)
		subscriptionsBefore := definitionMessageSubscriptionsForDefinition(store, message.Key)
		require.Len(t, subscriptionsBefore, 1)

		err := engine.PublishMessageByName(t.Context(), "messageStartEventProcessRef", nil, nil)

		require.ErrorContains(t, err, "storage is gone")
		assert.Empty(t, instancesOfDefinition(store, message.Key))
		subscriptions := definitionMessageSubscriptionsForDefinition(store, message.Key)
		require.Len(t, subscriptions, 1)
		assert.Equal(t, subscriptionsBefore[0].Key, subscriptions[0].Key, "the subscription was not consumed")
		assert.Equal(t, runtime.ActivityStateActive, subscriptions[0].State)
	})

	t.Run("a start on elements", func(t *testing.T) {
		failing.failFor.Store(childProcessId)
		instance, err := engine.CreateInstanceWithStartingElements(t.Context(), child.Key, []string{"child-task"}, nil, nil)
		require.ErrorContains(t, err, "storage is gone")
		assert.Nil(t, instance)
		assert.Empty(t, instancesOfDefinition(store, child.Key))
	})

	t.Run("an instantiating receive task", func(t *testing.T) {
		failing.failFor.Store(receiveTask.BpmnProcessId)
		subscriptionsBefore := definitionMessageSubscriptionsForDefinition(store, receiveTask.Key)
		require.Len(t, subscriptionsBefore, 1)

		err := engine.PublishMessageByName(t.Context(), instantiatingReceiveMessageName, nil, nil)

		require.ErrorContains(t, err, "storage is gone")
		assert.Empty(t, instancesOfDefinition(store, receiveTask.Key))
		subscriptions := definitionMessageSubscriptionsForDefinition(store, receiveTask.Key)
		require.Len(t, subscriptions, 1)
		assert.Equal(t, subscriptionsBefore[0].Key, subscriptions[0].Key, "the subscription was not consumed")
		assert.Equal(t, runtime.ActivityStateActive, subscriptions[0].State)
	})

	t.Run("a timer start event", func(t *testing.T) {
		failing.failFor.Store(timerDef.BpmnProcessId)
		timer := runtime.Timer{
			Key:                  engine.generateKey(),
			ElementId:            "timerStartEvent_1234",
			ProcessDefinitionKey: timerDef.Key,
			TimerState:           runtime.TimerStateCreated,
			CreatedAt:            time.Now(),
			DueAt:                time.Now(),
		}
		require.NoError(t, store.SaveTimer(t.Context(), timer))

		_, _, err := engine.TriggerTimer(t.Context(), timer)

		require.ErrorContains(t, err, "storage is gone")
		assert.Empty(t, instancesOfDefinition(store, timerDef.Key))
		persisted, getErr := store.GetTimer(t.Context(), timer.Key)
		require.NoError(t, getErr)
		assert.Equal(t, runtime.TimerStateCreated, persisted.TimerState, "the timer was not consumed, so it is tried again")
	})
}
