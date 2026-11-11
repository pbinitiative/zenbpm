package bpmn

import (
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestATaskRunsItsStartListenersItsJobAndItsEndListenersInTheOrderOfTheModel(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key

	beforeFirst := requireSinglePendingJob(t, store, instanceKey)
	require.Equal(t, "before-first", beforeFirst.Type, "the first start listener comes before everything else of the task")
	assert.Equal(t, runtime.JobKindExecutionListener, beforeFirst.Kind)
	assert.Equal(t, "start", beforeFirst.ListenerEventType)
	assert.Equal(t, "listened-task", beforeFirst.ElementId)
	assert.Equal(t, "SERVICE_TASK", beforeFirst.ElementType, "a listener's job carries the type of its element")
	assert.Equal(t, map[string]any{"given": "g", "taskInput": "g"}, beforeFirst.InputVariables, "a start listener sees the results of the input mappings")
	elementInstanceKey := beforeFirst.ElementInstanceKey
	assert.Equal(t, runtime.ElementPhaseActivating, scopeOf(t, store, elementInstanceKey).Phase)
	history := historyOf(t, engine, elementInstanceKey)
	assert.Equal(t, map[string]any{"given": "g"}, history.InputVariables, "the history row shows what the task was entered with")
	assert.Nil(t, history.CompletedAt)
	assert.Empty(t, openErrorSubscriptions(t, store, beforeFirst.Token.Key), "a boundary event does not watch the start listeners")

	require.NoError(t, engine.JobCompleteByKey(t.Context(), beforeFirst.Key, map[string]any{"fromBefore": "b1", "shared": "first"}))
	beforeSecond := waitForPendingJobOfType(t, engine, instanceKey, "before-second")
	assert.Equal(t, map[string]any{"given": "g", "taskInput": "g", "fromBefore": "b1", "shared": "first"}, beforeSecond.InputVariables,
		"a later start listener sees what an earlier one was completed with")
	assert.Equal(t, elementInstanceKey, beforeSecond.ElementInstanceKey)

	require.NoError(t, engine.JobCompleteByKey(t.Context(), beforeSecond.Key, map[string]any{"shared": "second"}))
	work := waitForPendingJobOfType(t, engine, instanceKey, "listened-work")
	assert.Equal(t, runtime.JobKindElement, work.Kind)
	assert.Empty(t, work.ListenerEventType)
	assert.Equal(t, elementInstanceKey, work.ElementInstanceKey)
	assert.Equal(t, map[string]any{"given": "g", "taskInput": "g", "fromBefore": "b1", "shared": "second"}, work.InputVariables,
		"the task's own job sees what its start listeners were completed with")
	assert.Equal(t, runtime.ElementPhaseActive, scopeOf(t, store, elementInstanceKey).Phase)
	assert.Len(t, openErrorSubscriptions(t, store, work.Token.Key), 1, "the boundary event watches the task's own work")
	assert.Equal(t, map[string]any{"given": "g"}, variablesOf(t, engine, store, instance), "a start listener's variables stay in the task")
	assert.Len(t, pendingJobs(t, engine, instanceKey), 1)

	require.NoError(t, engine.JobCompleteByKey(t.Context(), work.Key, map[string]any{"result": "r", "unmapped": "u"}))
	afterFirst := waitForPendingJobOfType(t, engine, instanceKey, "after-first")
	assert.Equal(t, "end", afterFirst.ListenerEventType)
	assert.Equal(t, elementInstanceKey, afterFirst.ElementInstanceKey)
	assert.Equal(t, map[string]any{
		"given": "g", "taskResult": "r", "mappedFromBefore": "b1", // the instance, after the output mappings
		"taskInput": "g", "fromBefore": "b1", "shared": "second", // the task's own
		"result": "r", "unmapped": "u", // what the task's job was completed with
	}, afterFirst.InputVariables)
	assert.Equal(t, map[string]any{"given": "g", "taskResult": "r", "mappedFromBefore": "b1"}, variablesOf(t, engine, store, instance),
		"a start listener's variable reaches the instance only where an output mapping of the task names it")
	history = historyOf(t, engine, elementInstanceKey)
	assert.Equal(t, map[string]any{"taskResult": "r", "mappedFromBefore": "b1"}, history.OutputVariables)
	assert.Nil(t, history.CompletedAt, "the task stays active while its end listeners run")
	assert.Empty(t, openErrorSubscriptions(t, store, work.Token.Key), "a boundary event does not watch the end listeners")
	scope := scopeOf(t, store, elementInstanceKey)
	assert.Equal(t, runtime.ElementPhaseCompleting, scope.Phase)
	assert.Zero(t, scope.ListenerIndex)

	require.NoError(t, engine.JobCompleteByKey(t.Context(), afterFirst.Key, map[string]any{"fromAfter": "a1"}))
	afterSecond := waitForPendingJobOfType(t, engine, instanceKey, "after-second")
	assert.Equal(t, "a1", afterSecond.InputVariables["fromAfter"], "a later end listener sees what an earlier one was completed with")
	assert.NotContains(t, variablesOf(t, engine, store, instance), "fromAfter", "an end listener's variables never reach the instance")

	require.NoError(t, engine.JobCompleteByKey(t.Context(), afterSecond.Key, map[string]any{"fromLast": "dropped"}))
	following := waitForPendingJobOfType(t, engine, instanceKey, "following-work")
	assert.Equal(t, map[string]any{"given": "g", "taskResult": "r", "mappedFromBefore": "b1"}, following.InputVariables,
		"the variables of a task end with the task")
	assert.NotEqual(t, elementInstanceKey, following.ElementInstanceKey)
	assert.NotNil(t, historyOf(t, engine, elementInstanceKey).CompletedAt)
	_, err = store.FindElementScope(t.Context(), elementInstanceKey)
	assert.ErrorIs(t, err, storage.ErrNotFound, "the scope is deleted when the token leaves the task")

	count, err := engine.persistence.GetFlowNodeCount(t.Context(), instanceKey)
	require.NoError(t, err)
	assert.Equal(t, int64(8), count, "the start event, the following task, and the listened task once for each of its 2+2 listeners, its job and its leaving")
}

func TestEveryElementWhoseWorkIsOneJobRunsItsListeners(t *testing.T) {
	fixture, err := os.ReadFile(executionListenerFixtures + "job-element-listeners.bpmn")
	require.NoError(t, err)
	cases := []struct {
		name        string
		openingTag  string
		closingTag  string
		elementType string
	}{
		{name: "a service task", openingTag: `<bpmn:serviceTask `, closingTag: `</bpmn:serviceTask>`, elementType: "SERVICE_TASK"},
		{name: "a send task", openingTag: `<bpmn:sendTask `, closingTag: `</bpmn:sendTask>`, elementType: "SERVICE_TASK"},
		{name: "a user task", openingTag: `<bpmn:userTask `, closingTag: `</bpmn:userTask>`, elementType: "USER_TASK"},
		{name: "a business rule task with a task definition", openingTag: `<bpmn:businessRuleTask `, closingTag: `</bpmn:businessRuleTask>`, elementType: "BUSINESS_RULE_TASK"},
		{name: "an event throwing a message", openingTag: `<bpmn:intermediateThrowEvent `,
			closingTag: `<bpmn:messageEventDefinition id="thrown-message" /></bpmn:intermediateThrowEvent>`, elementType: "INTERMEDIATE_THROW_EVENT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine, store := newListenerTestEngine(t)
			model := strings.NewReplacer(`<bpmn:serviceTask `, tc.openingTag, `</bpmn:serviceTask>`, tc.closingTag).Replace(string(fixture))
			definition, err := engine.LoadFromBytes(t.Context(), []byte(model), engine.generateKey())
			require.NoError(t, err)
			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
			require.NoError(t, err)
			instanceKey := instance.ProcessInstance().Key

			before := requireSinglePendingJob(t, store, instanceKey)
			require.Equal(t, "element-before", before.Type)
			assert.Equal(t, tc.elementType, before.ElementType)
			assert.Equal(t, "start", before.ListenerEventType)
			require.NoError(t, engine.JobCompleteByKey(t.Context(), before.Key, map[string]any{"fromBefore": true}))

			work := waitForPendingJobOfType(t, engine, instanceKey, "element-work")
			assert.Equal(t, runtime.JobKindElement, work.Kind)
			assert.Equal(t, tc.elementType, work.ElementType)
			assert.Equal(t, true, work.InputVariables["fromBefore"])
			require.NoError(t, engine.JobCompleteByKey(t.Context(), work.Key, nil))

			after := waitForPendingJobOfType(t, engine, instanceKey, "element-after")
			assert.Equal(t, "end", after.ListenerEventType)
			assert.Equal(t, before.ElementInstanceKey, after.ElementInstanceKey)
			require.NoError(t, engine.JobCompleteByKey(t.Context(), after.Key, nil))

			waitForInstanceState(t, engine, store, instance, runtime.ActivityStateCompleted)
			assert.Empty(t, variablesOf(t, engine, store, instance))
		})
	}
}

func TestATaskWithStartListenersOnlyLeavesWhenItsJobIsCompleted(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerModel(t, engine, "job-element-listeners.bpmn", `<zeebe:executionListener eventType="end" type="element-after" />`, ``)
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)

	before := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), before.Key, nil))
	work := waitForPendingJobOfType(t, engine, instance.ProcessInstance().Key, "element-work")
	require.NoError(t, engine.JobCompleteByKey(t.Context(), work.Key, nil))

	waitForInstanceState(t, engine, store, instance, runtime.ActivityStateCompleted)
	assert.NotNil(t, historyOf(t, engine, before.ElementInstanceKey).CompletedAt)
	_, err = store.FindElementScope(t.Context(), before.ElementInstanceKey)
	assert.ErrorIs(t, err, storage.ErrNotFound)
}

func TestATaskWithEndListenersOnlyCreatesItsJobAtOnce(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerModel(t, engine, "job-element-listeners.bpmn", `<zeebe:executionListener eventType="start" type="element-before" />`, ``)
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)

	work := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
	require.Equal(t, "element-work", work.Type)
	assert.Equal(t, runtime.ElementPhaseActive, scopeOf(t, store, work.ElementInstanceKey).Phase)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), work.Key, map[string]any{"result": 1}))
	after := waitForPendingJobOfType(t, engine, instance.ProcessInstance().Key, "element-after")
	assert.EqualValues(t, 1, after.InputVariables["result"], "the end listener sees what the task's job was completed with")
	require.NoError(t, engine.JobCompleteByKey(t.Context(), after.Key, nil))

	waitForInstanceState(t, engine, store, instance, runtime.ActivityStateCompleted)
}

func TestAnEngineHandlerDoesTheWorkOfATaskBetweenItsListeners(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	var handled atomic.Int32
	engine.NewTaskHandler().Type("listened-work").Handler(func(job ActivatedJob) {
		handled.Add(1)
		job.SetOutputVariable("result", job.Variable("fromBefore"))
		job.Complete()
	})
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key

	beforeFirst := requireSinglePendingJob(t, store, instanceKey)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), beforeFirst.Key, map[string]any{"fromBefore": "b1"}))
	beforeSecond := waitForPendingJobOfType(t, engine, instanceKey, "before-second")
	assert.Zero(t, handled.Load(), "the handler of the task takes no listener's job")
	require.NoError(t, engine.JobCompleteByKey(t.Context(), beforeSecond.Key, nil))

	afterFirst := waitForPendingJobOfType(t, engine, instanceKey, "after-first")
	assert.Equal(t, int32(1), handled.Load())
	assert.Equal(t, "b1", afterFirst.InputVariables["result"], "the end listener sees what the handler completed the task with")
	assert.Equal(t, "b1", variablesOf(t, engine, store, instance)["taskResult"])
	history := historyOf(t, engine, beforeFirst.ElementInstanceKey)
	assert.Nil(t, history.CompletedAt, "the task stays active while its end listeners run")
	assert.Equal(t, "b1", history.OutputVariables["taskResult"])
}

func TestResolvingAFailureOfTheTasksOwnWorkRunsNoCompletedStartListenerAgain(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	var fails atomic.Bool
	fails.Store(true)
	var seenInput atomic.Value
	engine.NewTaskHandler().Type("listened-work").Handler(func(job ActivatedJob) {
		if fails.Load() {
			job.Fail("not yet")
			return
		}
		seenInput.Store(job.Variable("taskInput"))
		job.Complete()
	})
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	beforeFirst := requireSinglePendingJob(t, store, instanceKey)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), beforeFirst.Key, map[string]any{"fromBefore": "b1"}))
	beforeSecond := waitForPendingJobOfType(t, engine, instanceKey, "before-second")

	require.NoError(t, engine.JobCompleteByKey(t.Context(), beforeSecond.Key, nil),
		"the completion of the listener's job is done; the failure behind it is an incident")
	waitForInstanceState(t, engine, store, instance, runtime.ActivityStateFailed)
	incident := onlyOpenIncident(t, engine, instanceKey)
	assert.Equal(t, "listened-task", incident.ElementId)
	assert.Empty(t, pendingJobs(t, engine, instanceKey))

	fails.Store(false)
	_, _, err = engine.ModifyInstance(t.Context(), instanceKey, nil, nil, map[string]any{"given": "corrected"})
	require.NoError(t, err)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))

	afterFirst := waitForPendingJobOfType(t, engine, instanceKey, "after-first")
	assert.Equal(t, "corrected", seenInput.Load(), "resolving the incident evaluates the input mappings again")
	assert.Equal(t, "b1", afterFirst.InputVariables["fromBefore"], "what the start listeners were completed with is kept")
	assert.Equal(t, beforeFirst.ElementInstanceKey, afterFirst.ElementInstanceKey)
	for _, listenerType := range []string{"before-first", "before-second"} {
		assert.Equal(t, 1, jobsOfType(t, store, instanceKey, listenerType), "the start listener %s ran once", listenerType)
	}
}

func TestResolvingAnIncidentOfAListenersJobKeepsWhatEarlierListenersAdded(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	beforeFirst := requireSinglePendingJob(t, store, instanceKey)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), beforeFirst.Key, map[string]any{"fromBefore": "b1"}))
	beforeSecond := waitForPendingJobOfType(t, engine, instanceKey, "before-second")

	require.NoError(t, engine.JobFailByKey(t.Context(), beforeSecond.Key, "listener failed", nil, nil, nil, nil, nil))
	require.Equal(t, runtime.ActivityStateFailed, reloadJob(t, store, beforeSecond.Key).State, "a listener without retries raises an incident at its first failure")
	incident := onlyOpenIncident(t, engine, instanceKey)
	assert.Equal(t, "listened-task", incident.ElementId, "the incident stands on the task")
	_, _, err = engine.ModifyInstance(t.Context(), instanceKey, nil, nil, map[string]any{"given": "corrected"})
	require.NoError(t, err)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))

	resolved := requireSinglePendingJob(t, store, instanceKey)
	assert.Equal(t, beforeSecond.Key, resolved.Key, "the same job is handed out again")
	assert.Equal(t, map[string]any{"given": "corrected", "taskInput": "corrected", "fromBefore": "b1"}, resolved.InputVariables)
	assert.Equal(t, map[string]any{"given": "g"}, historyOf(t, engine, beforeSecond.ElementInstanceKey).InputVariables,
		"the history row of the task is not the listener's to rewrite")
	assert.Equal(t, 1, jobsOfType(t, store, instanceKey, "before-first"), "the completed listener is not run again")
}

func TestResolvingAnIncidentOfTheTasksJobKeepsWhatItsStartListenersAdded(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	work := completeStartListeners(t, engine, store, instanceKey, map[string]any{"fromBefore": "b1"})

	require.NoError(t, engine.JobFailByKey(t.Context(), work.Key, "first", nil, nil, nil, nil, nil))
	require.NoError(t, engine.JobFailByKey(t.Context(), work.Key, "second", nil, nil, nil, nil, nil))
	incident := onlyOpenIncident(t, engine, instanceKey)
	_, _, err = engine.ModifyInstance(t.Context(), instanceKey, nil, nil, map[string]any{"given": "corrected"})
	require.NoError(t, err)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))

	resolved := requireSinglePendingJob(t, store, instanceKey)
	assert.Equal(t, work.Key, resolved.Key)
	assert.Equal(t, map[string]any{"given": "corrected", "taskInput": "corrected", "fromBefore": "b1"}, resolved.InputVariables)
	assert.Equal(t, map[string]any{"given": "corrected"}, historyOf(t, engine, work.ElementInstanceKey).InputVariables,
		"the history row shows the variables the input mappings were evaluated from again")
}

func TestABpmnErrorOfTheTasksJobLeavesByTheBoundaryEventAndOfAListenersJobIsRefused(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	errorCode := "LISTENED_TASK_FAILED"

	beforeFirst := requireSinglePendingJob(t, store, instanceKey)
	err = engine.JobFailByKey(t.Context(), beforeFirst.Key, "thrown by a listener", &errorCode, nil, nil, nil, nil)
	require.ErrorIs(t, err, ErrInvalidJobRequest)
	assert.Contains(t, err.Error(), "a listener job cannot throw a BPMN error")

	work := completeStartListeners(t, engine, store, instanceKey, nil)
	require.NoError(t, engine.JobFailByKey(t.Context(), work.Key, "thrown by the task", &errorCode, nil, nil, nil, nil))

	recovering := waitForPendingJobOfType(t, engine, instanceKey, "recovering-work")
	assert.Equal(t, "recovering-task", recovering.ElementId)
	assert.Equal(t, 0, jobsOfType(t, store, instanceKey, "after-first"), "an end listener does not run for a task which is left by a boundary event")
	assert.NotNil(t, historyOf(t, engine, work.ElementInstanceKey).CompletedAt)
	assert.Equal(t, runtime.ElementPhaseActive, scopeOf(t, store, work.ElementInstanceKey).Phase,
		"the scope of a task which was not left by its outgoing flow stays until the history of the instance is cleaned up")
}

func TestStartingATokenAtATaskRunsItsStartListeners(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	first := requireSinglePendingJob(t, store, instanceKey)

	_, _, err = engine.ModifyInstance(t.Context(), instanceKey, nil, []string{"listened-task"}, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return len(pendingJobs(t, engine, instanceKey)) == 2 }, listenerTestTimeout, listenerTestPoll)
	for _, job := range pendingJobs(t, engine, instanceKey) {
		assert.Equal(t, "before-first", job.Type, "a token started at the task enters it as every token does")
	}
	second := pendingJobs(t, engine, instanceKey)
	assert.NotEqual(t, second[0].ElementInstanceKey, second[1].ElementInstanceKey)
	assert.Contains(t, []int64{second[0].Key, second[1].Key}, first.Key)
}

func TestCancellingAnInstanceTerminatesTheJobOfWhicheverPhaseItsTaskIsIn(t *testing.T) {
	for _, phase := range []string{"before-first", "listened-work", "after-first"} {
		t.Run(phase, func(t *testing.T) {
			engine, store := newListenerTestEngine(t)
			definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
			require.NoError(t, err)
			instanceKey := instance.ProcessInstance().Key
			waiting := requireSinglePendingJob(t, store, instanceKey)
			if phase != "before-first" {
				waiting = completeStartListeners(t, engine, store, instanceKey, map[string]any{"fromBefore": "b1"})
			}
			if phase == "after-first" {
				require.NoError(t, engine.JobCompleteByKey(t.Context(), waiting.Key, map[string]any{"result": "r"}))
				waiting = waitForPendingJobOfType(t, engine, instanceKey, "after-first")
			}
			require.Equal(t, phase, waiting.Type)

			require.NoError(t, engine.CancelInstanceByKey(t.Context(), instanceKey))

			assert.Equal(t, runtime.ActivityStateTerminated, reloadJob(t, store, waiting.Key).State)
			assert.Equal(t, runtime.ActivityStateTerminated, readInstance(t, engine, store, instance).State)
			require.ErrorIs(t, engine.JobCompleteByKey(t.Context(), waiting.Key, nil), ErrJobInTerminalState)
		})
	}
}

func TestAJobCreatedBeforeItsTaskRanListenersCompletesAsBefore(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	work := completeStartListeners(t, engine, store, instanceKey, map[string]any{"fromBefore": "b1"})
	// as a job which an engine created that ignored the listeners of the task
	require.NoError(t, store.DeleteElementScope(t.Context(), work.ElementInstanceKey))

	require.NoError(t, engine.JobCompleteByKey(t.Context(), work.Key, map[string]any{"result": "r"}))

	following := waitForPendingJobOfType(t, engine, instanceKey, "following-work")
	assert.Equal(t, "r", following.InputVariables["taskResult"])
	assert.Equal(t, 0, jobsOfType(t, store, instanceKey, "after-first"), "the task is left as it was entered: without listeners")
	assert.NotNil(t, historyOf(t, engine, work.ElementInstanceKey).CompletedAt)
}

func TestListenersRunOnATaskOfASubProcessAndOfACalledProcess(t *testing.T) {
	t.Run("in a sub-process", func(t *testing.T) {
		engine, store := newListenerTestEngine(t)
		definition := loadListenerFixture(t, engine, "sub-process-task-listeners.bpmn")
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
		require.NoError(t, err)

		runJobsInOrder(t, engine, "inner-before", "inner-work", "inner-after")

		waitForInstanceState(t, engine, store, instance, runtime.ActivityStateCompleted)
	})
	t.Run("in a called process", func(t *testing.T) {
		engine, store := newListenerTestEngine(t)
		loadListenerFixture(t, engine, "job-element-listeners.bpmn")
		caller := loadListenerFixture(t, engine, "job-element-listeners-caller.bpmn")
		instance, err := engine.CreateInstanceByKey(t.Context(), caller.Key, nil)
		require.NoError(t, err)

		runJobsInOrder(t, engine, "element-before", "element-work", "element-after")

		waitForInstanceState(t, engine, store, instance, runtime.ActivityStateCompleted)
	})
}

func TestAStoredTaskListenerADeploymentWouldRefuseRaisesAnIncidentOnTheTask(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	fixture, err := os.ReadFile(executionListenerFixtures + "job-element-listeners.bpmn")
	require.NoError(t, err)
	model := strings.Replace(string(fixture), `type="element-before"`, `type="=jobType"`, 1)
	_, err = engine.LoadFromBytes(t.Context(), []byte(model), engine.generateKey())
	require.ErrorContains(t, err, `its type "=jobType" is an expression`, "a deployment refuses it")
	// stored definitions are imported without the checks of a deployment
	definition, err := engine.ImportProcessDefinition(t.Context(), []byte(model), engine.generateKey(), 1, false)
	require.NoError(t, err)

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.Error(t, err)
	require.NotNil(t, instance)

	incident := onlyOpenIncident(t, engine, instance.ProcessInstance().Key)
	assert.Equal(t, "listened-element", incident.ElementId)
	assert.Contains(t, incident.Message, `invalid execution listener eventType="start" on service task id="listened-element": its type "=jobType" is an expression`)
	assert.Contains(t, incident.Message, "deploy a corrected version of the process")
	assert.Empty(t, pendingJobs(t, engine, instance.ProcessInstance().Key), "neither a listener's job nor the task's is created")
	assert.Equal(t, runtime.ActivityStateFailed, readInstance(t, engine, store, instance).State)
}

func TestAStoredListenerWhereTheEngineRunsNoneStaysIgnored(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	fixture, err := os.ReadFile(executionListenerFixtures + "job-element-listeners.bpmn")
	require.NoError(t, err)
	listeners := `<bpmn:extensionElements><zeebe:executionListeners><zeebe:executionListener eventType="start" type="never-run" /></zeebe:executionListeners></bpmn:extensionElements>`
	model := strings.Replace(string(fixture), `<bpmn:endEvent id="job-element-end">`, `<bpmn:endEvent id="job-element-end">`+listeners, 1)
	_, err = engine.LoadFromBytes(t.Context(), []byte(model), engine.generateKey())
	require.ErrorContains(t, err, `execution listener eventType="start" on end event id="job-element-end" is not supported`, "a deployment refuses it")
	definition, err := engine.ImportProcessDefinition(t.Context(), []byte(model), engine.generateKey(), 1, false)
	require.NoError(t, err, "a stored definition keeps loading")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)

	runJobsInOrder(t, engine, "element-before", "element-work", "element-after")

	waitForInstanceState(t, engine, store, instance, runtime.ActivityStateCompleted)
	assert.Equal(t, 0, jobsOfType(t, store, instance.ProcessInstance().Key, "never-run"))
}

const (
	listenerTestTimeout = 5 * time.Second
	listenerTestPoll    = 20 * time.Millisecond
)

// loadListenerModel deploys the fixture with one piece of it replaced.
func loadListenerModel(t *testing.T, engine *Engine, file string, old string, replacement string) *runtime.ProcessDefinition {
	t.Helper()
	fixture, err := os.ReadFile(executionListenerFixtures + file)
	require.NoError(t, err)
	require.Contains(t, string(fixture), old)
	definition, err := engine.LoadFromBytes(t.Context(), []byte(strings.Replace(string(fixture), old, replacement, 1)), engine.generateKey())
	require.NoError(t, err)
	return definition
}

// completeStartListeners completes the two start listeners of the task of
// task-listeners.bpmn, the first one with the given variables, and answers the
// task's own job.
func completeStartListeners(t *testing.T, engine *Engine, store *inmemory.Storage, instanceKey int64, variables map[string]any) runtime.Job {
	t.Helper()
	beforeFirst := requireSinglePendingJob(t, store, instanceKey)
	require.Equal(t, "before-first", beforeFirst.Type)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), beforeFirst.Key, variables))
	beforeSecond := waitForPendingJobOfType(t, engine, instanceKey, "before-second")
	require.NoError(t, engine.JobCompleteByKey(t.Context(), beforeSecond.Key, nil))
	return waitForPendingJobOfType(t, engine, instanceKey, "listened-work")
}

// runJobsInOrder waits for the one active job of each type in turn, wherever
// its instance is, and completes it.
func runJobsInOrder(t *testing.T, engine *Engine, jobTypes ...string) {
	t.Helper()
	for _, jobType := range jobTypes {
		var jobs []runtime.Job
		require.Eventually(t, func() bool {
			var err error
			jobs, err = engine.persistence.FindActiveJobsByType(t.Context(), jobType)
			return err == nil && len(jobs) == 1
		}, listenerTestTimeout, listenerTestPoll, "the job of type %s", jobType)
		require.NoError(t, engine.JobCompleteByKey(t.Context(), jobs[0].Key, nil))
	}
}

func scopeOf(t *testing.T, store *inmemory.Storage, elementInstanceKey int64) runtime.ElementScope {
	t.Helper()
	scope, err := store.FindElementScope(t.Context(), elementInstanceKey)
	require.NoError(t, err)
	return scope
}

func historyOf(t *testing.T, engine *Engine, elementInstanceKey int64) runtime.FlowElementInstance {
	t.Helper()
	history, err := engine.persistence.GetFlowElementInstanceByKey(t.Context(), elementInstanceKey)
	require.NoError(t, err)
	return history
}

func variablesOf(t *testing.T, engine *Engine, store *inmemory.Storage, instance runtime.ProcessInstance) map[string]any {
	t.Helper()
	data := readInstance(t, engine, store, instance)
	return data.VariableHolder.LocalVariables()
}

func openErrorSubscriptions(t *testing.T, store *inmemory.Storage, tokenKey int64) []runtime.ErrorSubscription {
	t.Helper()
	subscriptions, err := store.FindTokenErrorSubscriptions(t.Context(), tokenKey, runtime.ErrorStateCreated)
	require.NoError(t, err)
	return subscriptions
}

// jobsOfType counts the jobs of the type the instance ever created.
func jobsOfType(t *testing.T, store *inmemory.Storage, instanceKey int64, jobType string) int {
	t.Helper()
	count := 0
	for _, job := range store.Copy().Jobs {
		if job.ProcessInstanceKey == instanceKey && job.Type == jobType {
			count++
		}
	}
	return count
}
