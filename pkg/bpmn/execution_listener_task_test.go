package bpmn

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/script"
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
	taskHeaders := map[string]string{"channel": "post"}
	assert.Equal(t, taskHeaders, beforeFirst.Headers, "a listener's job carries the static headers of its element")
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
	assert.Equal(t, taskHeaders, work.Headers)
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
	assert.Equal(t, taskHeaders, afterFirst.Headers)
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

	require.NoError(t, engine.JobCompleteByKey(t.Context(), afterFirst.Key, map[string]any{"fromAfter": "a1", "shared": "after"}))
	afterSecond := waitForPendingJobOfType(t, engine, instanceKey, "after-second")
	assert.Equal(t, "a1", afterSecond.InputVariables["fromAfter"], "a later end listener sees what an earlier one was completed with")
	assert.Equal(t, "after", afterSecond.InputVariables["shared"], "over a variable of the task with the same name")
	assert.Equal(t, map[string]any{"given": "g", "taskResult": "r", "mappedFromBefore": "b1", "fromAfter": "a1", "shared": "after"},
		variablesOf(t, engine, store, instance), "what an end listener is completed with reaches the instance, named by no output mapping")
	history = historyOf(t, engine, elementInstanceKey)
	assert.Equal(t, map[string]any{"taskResult": "r", "mappedFromBefore": "b1", "fromAfter": "a1", "shared": "after"}, history.OutputVariables,
		"the history shows what an end listener wrote beside what the output mappings wrote")
	assert.Nil(t, history.CompletedAt)

	require.NoError(t, engine.JobCompleteByKey(t.Context(), afterSecond.Key, map[string]any{"fromLast": "a2", "taskResult": "replaced"}))
	following := waitForPendingJobOfType(t, engine, instanceKey, "following-work")
	afterTheTask := map[string]any{"given": "g", "taskResult": "replaced", "mappedFromBefore": "b1", "fromAfter": "a1", "shared": "after", "fromLast": "a2"}
	assert.Equal(t, afterTheTask, following.InputVariables,
		"what the start listeners and the task's job were completed with ends with the task, what the end listeners were completed with stays")
	assert.Equal(t, afterTheTask, variablesOf(t, engine, store, instance), "a later end listener replaces a variable of the instance")
	assert.NotEqual(t, elementInstanceKey, following.ElementInstanceKey)
	history = historyOf(t, engine, elementInstanceKey)
	assert.NotNil(t, history.CompletedAt)
	assert.Equal(t, map[string]any{"taskResult": "replaced", "mappedFromBefore": "b1", "fromAfter": "a1", "shared": "after", "fromLast": "a2"}, history.OutputVariables)
	_, err = store.FindElementScope(t.Context(), elementInstanceKey)
	assert.ErrorIs(t, err, storage.ErrNotFound, "the scope is deleted when the token leaves the task")

	count, err := engine.persistence.GetFlowNodeCount(t.Context(), instanceKey)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count, "the start event, the listened task and the following task: an element counts once, whatever its listeners")
}

func TestEveryElementWhoseWorkIsOneJobRunsItsListeners(t *testing.T) {
	fixture, err := os.ReadFile(executionListenerFixtures + "job-element-listeners.bpmn")
	require.NoError(t, err)
	for _, tc := range elementsWhoseWorkIsOneJob {
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
			assert.Empty(t, variablesOf(t, engine, store, instance), "neither the start listener nor the element's job wrote to the instance")
			require.NoError(t, engine.JobCompleteByKey(t.Context(), after.Key, map[string]any{"fromAfter": true}))

			waitForInstanceState(t, engine, store, instance, runtime.ActivityStateCompleted)
			assert.Equal(t, map[string]any{"fromAfter": true}, variablesOf(t, engine, store, instance), "the end listener did")
		})
	}
}

func TestTheListenersOfATaskDoNotSpendTheFlowNodeBudgetOfItsInstance(t *testing.T) {
	// the start event, the listened task and the task behind it
	engine, store := newListenerTestEngine(t, EngineWithMaxProcessInstanceFlowNodeCount(3))
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key

	runJobsInOrder(t, engine, "before-first", "before-second", "listened-work", "after-first", "after-second")

	waitForPendingJobOfType(t, engine, instanceKey, "following-work")
	incidents, err := engine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), instanceKey)
	require.NoError(t, err)
	assert.Empty(t, incidents, "a task with four listeners counts as one flow node, as a task without any")
	assert.Equal(t, runtime.ActivityStateActive, readInstance(t, engine, store, instance).State)
}

func TestAnElementWithoutListenersRunsTheSameLifecycleWithoutAScopeInTheStorage(t *testing.T) {
	fixture, err := os.ReadFile(executionListenerFixtures + "job-element-listeners.bpmn")
	require.NoError(t, err)
	withoutListeners := strings.NewReplacer(
		`<zeebe:executionListener eventType="start" type="element-before" />`, ``,
		`<zeebe:executionListener eventType="end" type="element-after" />`, ``,
	).Replace(string(fixture))
	require.NotContains(t, withoutListeners, "<zeebe:executionListener ")
	for _, tc := range elementsWhoseWorkIsOneJob {
		for _, handledInTheEngine := range []bool{false, true} {
			name := tc.name + ", its job left to a worker"
			if handledInTheEngine {
				name = tc.name + ", its job completed by a handler in the engine"
			}
			t.Run(name, func(t *testing.T) {
				store := &lifecycleCostCountingStorage{Storage: inmemory.NewStorage()}
				engine := NewEngine(EngineWithStorage(store))
				require.NoError(t, engine.Start(t.Context()))
				t.Cleanup(engine.Stop)
				if handledInTheEngine {
					engine.NewTaskHandler().Id("listened-element").Handler(func(job ActivatedJob) { job.Complete() })
				}
				model := strings.NewReplacer(`<bpmn:serviceTask `, tc.openingTag, `</bpmn:serviceTask>`, tc.closingTag).Replace(withoutListeners)
				definition, err := engine.LoadFromBytes(t.Context(), []byte(model), engine.generateKey())
				require.NoError(t, err)

				instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
				require.NoError(t, err)

				if !handledInTheEngine {
					work := requireSinglePendingJob(t, store.Storage, instance.ProcessInstance().Key)
					require.Equal(t, "element-work", work.Type)
					assert.Empty(t, store.Copy().ElementScopes, "nothing is stored for the element beside its job while the token waits in it")
					require.NoError(t, engine.JobCompleteByKey(t.Context(), work.Key, map[string]any{"result": "r"}))
					assert.Equal(t, int32(1), store.historyUpdatesOf(work.ElementInstanceKey), "the completion of the job writes the outputs and the end of the history row at once")
					history := historyOf(t, &engine, work.ElementInstanceKey)
					assert.NotNil(t, history.CompletedAt)
				}
				waitForInstanceState(t, &engine, store.Storage, instance, runtime.ActivityStateCompleted)
				assert.Zero(t, store.reads.Load(), "neither the start event nor the element asks the storage for a scope")
				assert.Zero(t, store.deletes.Load(), "and neither has one deleted when it is left")
				assert.Empty(t, store.Copy().ElementScopes)
				for _, history := range store.Copy().FlowElementInstance {
					assert.NotNil(t, history.CompletedAt, "the history row of %s is completed", history.ElementId)
				}
			})
		}
	}
}

func TestAJobHandedOutByActivateJobsTellsWhichListenerItBelongsTo(t *testing.T) {
	engine, _ := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "job-element-listeners.bpmn")
	_, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)

	for _, expected := range []struct{ jobType, listenerEventType string }{
		{jobType: "element-before", listenerEventType: "start"},
		{jobType: "element-work", listenerEventType: ""},
		{jobType: "element-after", listenerEventType: "end"},
	} {
		var activated []ActivatedJob
		require.Eventually(t, func() bool {
			activated, err = engine.ActivateJobs(t.Context(), expected.jobType)
			return err == nil && len(activated) == 1
		}, 5*time.Second, 10*time.Millisecond, "the job of type %s is handed out", expected.jobType)
		assert.Equal(t, "listened-element", activated[0].ElementId(), "a listener's job carries the id of its element")
		assert.Equal(t, expected.listenerEventType, activated[0].ListenerEventType(), "the job of type %s", expected.jobType)
		require.NoError(t, engine.JobCompleteByKey(t.Context(), activated[0].Key(), nil))
	}
}

func TestOnlyAnElementWithListenersHasItsScopeDeletedWhenItsTokenIsTakenOutOfIt(t *testing.T) {
	listeners := `<zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="element-before" />
          <zeebe:executionListener eventType="end" type="element-after" />
        </zeebe:executionListeners>`
	takenOut := map[string]func(t *testing.T, engine *Engine, waiting runtime.Job){
		"by the cancellation of its instance": func(t *testing.T, engine *Engine, waiting runtime.Job) {
			require.NoError(t, engine.CancelInstanceByKey(t.Context(), waiting.ProcessInstanceKey))
		},
		"by a modification which terminates the token": func(t *testing.T, engine *Engine, waiting runtime.Job) {
			_, _, err := engine.ModifyInstance(t.Context(), waiting.ProcessInstanceKey, []int64{waiting.ElementInstanceKey}, nil, nil)
			require.NoError(t, err)
		},
	}
	for name, takeTokenOut := range takenOut {
		for _, withListeners := range []bool{false, true} {
			scopesDeleted, waitingFor := int32(0), "element-work"
			testName := "an element without listeners, " + name
			if withListeners {
				scopesDeleted, waitingFor = 1, "element-before"
				testName = "an element with listeners, " + name
			}
			t.Run(testName, func(t *testing.T) {
				store := &lifecycleCostCountingStorage{Storage: inmemory.NewStorage()}
				engine := NewEngine(EngineWithStorage(store))
				require.NoError(t, engine.Start(t.Context()))
				t.Cleanup(engine.Stop)
				replacement := listeners
				if !withListeners {
					replacement = ``
				}
				definition := loadListenerModel(t, &engine, "job-element-listeners.bpmn", listeners, replacement)
				instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
				require.NoError(t, err)
				waiting := requireSinglePendingJob(t, store.Storage, instance.ProcessInstance().Key)
				require.Equal(t, waitingFor, waiting.Type)
				require.Len(t, store.Copy().ElementScopes, int(scopesDeleted), "only an element with listeners keeps a scope while its token waits")

				takeTokenOut(t, &engine, waiting)

				assert.Equal(t, runtime.ActivityStateTerminated, reloadJob(t, store.Storage, waiting.Key).State)
				assert.NotNil(t, historyOf(t, &engine, waiting.ElementInstanceKey).CompletedAt)
				assert.Equal(t, scopesDeleted, store.deletes.Load(), "an element which keeps no scope costs no write for one")
				assert.Empty(t, store.Copy().ElementScopes)
			})
		}
	}
}

func TestATokenOfAnotherInstanceOrAtAnUnknownElementMayKeepAScope(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerModel(t, engine, "job-element-listeners.bpmn", `<zeebe:executionListener eventType="start" type="element-before" />`, ``)
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	listened := requireSinglePendingJob(t, store, instance.ProcessInstance().Key).Token
	require.Equal(t, "listened-element", listened.ElementId)
	atElementWithoutListeners, ofAnotherInstance, atAnUnknownElement := listened, listened, listened
	atElementWithoutListeners.ElementId = "job-element-end"
	ofAnotherInstance.ElementId, ofAnotherInstance.ProcessInstanceKey = "job-element-end", listened.ProcessInstanceKey+1
	atAnUnknownElement.ElementId = "removed-from-the-model"

	assert.True(t, mayKeepScope(instance, listened), "its element carries a listener the engine runs")
	assert.False(t, mayKeepScope(instance, atElementWithoutListeners))
	assert.True(t, mayKeepScope(instance, ofAnotherInstance), "the process of another instance says nothing about the token's element")
	assert.True(t, mayKeepScope(instance, atAnUnknownElement))
}

func TestAnElementWhoseInputMappingsFailNamesItselfInTheIncidentWithListenersAndWithout(t *testing.T) {
	fixture, err := os.ReadFile(executionListenerFixtures + "job-element-listeners.bpmn")
	require.NoError(t, err)
	taskDefinition := `<zenbpm:taskDefinition type="element-work" />`
	// the flag is no flag the function knows, so the mapping fails when it is evaluated
	failingMapping := taskDefinition + `<zenbpm:ioMapping><zenbpm:input source="=matches(&#34;a&#34;, &#34;a&#34;, flag)" target="matched" /></zenbpm:ioMapping>`
	withListeners := strings.Replace(string(fixture), taskDefinition, failingMapping, 1)
	require.NotEqual(t, string(fixture), withListeners)
	withoutListeners := strings.NewReplacer(
		`<zeebe:executionListener eventType="start" type="element-before" />`, ``,
		`<zeebe:executionListener eventType="end" type="element-after" />`, ``,
	).Replace(withListeners)
	for name, model := range map[string]string{"with listeners": withListeners, "without listeners": withoutListeners} {
		t.Run(name, func(t *testing.T) {
			engine, _ := newListenerTestEngine(t)
			definition, err := engine.LoadFromBytes(t.Context(), []byte(model), engine.generateKey())
			require.NoError(t, err)

			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"flag": "x"})
			require.Error(t, err)
			require.NotNil(t, instance)

			incident := onlyOpenIncident(t, engine, instance.ProcessInstance().Key)
			assert.Equal(t, "listened-element", incident.ElementId)
			assert.Contains(t, incident.Message, "failed to process SERVICE_TASK ")
			assert.Contains(t, incident.Message, ": failed to evaluate input variables: ")
			assert.Empty(t, pendingJobs(t, engine, instance.ProcessInstance().Key), "no job of the element is created, a listener's neither")
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

	require.NoError(t, engine.JobCompleteByKey(t.Context(), afterFirst.Key, nil))
	afterSecond := waitForPendingJobOfType(t, engine, instanceKey, "after-second")
	require.NoError(t, engine.JobCompleteByKey(t.Context(), afterSecond.Key, nil))
	waitForPendingJobOfType(t, engine, instanceKey, "following-work")
	count, err := engine.persistence.GetFlowNodeCount(t.Context(), instanceKey)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count, "the start event, the listened task and the following task, as with a job left to a worker")
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
	assert.Equal(t, map[string]any{"given": "corrected"}, historyOf(t, engine, beforeFirst.ElementInstanceKey).InputVariables,
		"the history row shows the variables the task's job got its inputs from")
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
	assert.Equal(t, map[string]any{"given": "corrected"}, historyOf(t, engine, beforeSecond.ElementInstanceKey).InputVariables,
		"the history row shows the variables the task's job will get its inputs from")
	assert.Equal(t, 1, jobsOfType(t, store, instanceKey, "before-first"), "the completed listener is not run again")
}

func TestResolvingAnIncidentOfAnEndListenersJobLeavesTheHistoryOfTheTasksInputsAlone(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	work := completeStartListeners(t, engine, store, instanceKey, nil)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), work.Key, map[string]any{"result": "r"}))
	afterFirst := waitForPendingJobOfType(t, engine, instanceKey, "after-first")

	require.NoError(t, engine.JobFailByKey(t.Context(), afterFirst.Key, "listener failed", nil, nil, nil, nil, nil))
	incident := onlyOpenIncident(t, engine, instanceKey)
	_, _, err = engine.ModifyInstance(t.Context(), instanceKey, nil, nil, map[string]any{"given": "corrected"})
	require.NoError(t, err)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))

	resolved := requireSinglePendingJob(t, store, instanceKey)
	assert.Equal(t, afterFirst.Key, resolved.Key)
	assert.Equal(t, "corrected", resolved.InputVariables["taskInput"], "the end listener sees the input mappings evaluated again")
	assert.Equal(t, map[string]any{"given": "g"}, historyOf(t, engine, work.ElementInstanceKey).InputVariables,
		"the history row keeps the variables the task's job got its inputs from: that job is done")
}

func TestResolvingAnIncidentOfATaskWithListenersIsRefusedWhileItsInputMappingsDoNotEvaluate(t *testing.T) {
	engine, store, incident := listenedTaskWhoseOwnWorkFailed(t, inmemory.NewStorage())
	feel := &failingFeelRuntime{FeelRuntime: engine.feelRuntime}
	engine.feelRuntime = feel

	feel.fails.Store(true)
	err := engine.ResolveIncident(t.Context(), incident.Key)

	require.ErrorIs(t, err, ErrIncidentNotResolvable, "resolving would only raise the incident again")
	assert.ErrorContains(t, err, "failed to evaluate the input mappings of SERVICE_TASK listened-task")
	assert.ErrorContains(t, err, "correct the variables they read and resolve the incident again")
	assert.Equal(t, incident.Key, onlyOpenIncident(t, engine, incident.ProcessInstanceKey).Key, "the incident stays open")

	feel.fails.Store(false)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
	waitForPendingJobOfType(t, engine, incident.ProcessInstanceKey, "after-first")
	assert.Equal(t, 1, jobsOfType(t, store, incident.ProcessInstanceKey, "before-first"))
}

func TestAStorageFailureWhileResolvingAnIncidentOfATaskWithListenersIsNotARefusal(t *testing.T) {
	failing := &failingScopeReadStorage{Storage: inmemory.NewStorage()}
	engine, _, incident := listenedTaskWhoseOwnWorkFailed(t, failing)

	failing.failsFindElementScope.Store(true)
	err := engine.ResolveIncident(t.Context(), incident.Key)

	require.ErrorIs(t, err, errFindElementScopeFailure)
	assert.NotErrorIs(t, err, ErrIncidentNotResolvable, "nothing about the incident has to change: the resolution may be repeated as it is")
	assert.Equal(t, incident.Key, onlyOpenIncident(t, engine, incident.ProcessInstanceKey).Key, "the incident stays open")

	failing.failsFindElementScope.Store(false)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
	waitForPendingJobOfType(t, engine, incident.ProcessInstanceKey, "after-first")
}

func TestResolvingAnIncidentOfAListenersJobIsRefusedWhileTheInputMappingsOfItsTaskDoNotEvaluate(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	work := completeStartListeners(t, engine, store, instanceKey, nil)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), work.Key, nil))
	afterFirst := waitForPendingJobOfType(t, engine, instanceKey, "after-first")
	require.NoError(t, engine.JobFailByKey(t.Context(), afterFirst.Key, "listener failed", nil, nil, nil, nil, nil))
	incident := onlyOpenIncident(t, engine, instanceKey)
	feel := &failingFeelRuntime{FeelRuntime: engine.feelRuntime}
	engine.feelRuntime = feel

	feel.fails.Store(true)
	err = engine.ResolveIncident(t.Context(), incident.Key)

	require.ErrorIs(t, err, ErrIncidentNotResolvable, "the listener's job would be handed out without the variables of its task")
	assert.ErrorContains(t, err, "failed to evaluate the input mappings of SERVICE_TASK listened-task")
	assert.ErrorContains(t, err, "correct the variables they read and resolve the incident again")
	assert.Equal(t, incident.Key, onlyOpenIncident(t, engine, instanceKey).Key, "the incident stays open")
	assert.Equal(t, runtime.ActivityStateFailed, reloadJob(t, store, afterFirst.Key).State, "the job is not handed out")

	feel.fails.Store(false)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
	assert.Equal(t, afterFirst.Key, requireSinglePendingJob(t, store, instanceKey).Key, "the same job is handed out again")
	assert.Equal(t, runtime.ActivityStateActive, reloadJob(t, store, afterFirst.Key).State)
}

func TestAStorageFailureWhileResolvingAnIncidentOfAListenersJobIsNotARefusal(t *testing.T) {
	failing := &failingScopeReadStorage{Storage: inmemory.NewStorage()}
	engine, instanceKey, beforeFirst := listenedTaskAtItsFirstStartListener(t, failing)
	require.NoError(t, engine.JobFailByKey(t.Context(), beforeFirst.Key, "listener failed", nil, nil, new(int32(0)), nil, nil))
	incident := onlyOpenIncident(t, engine, instanceKey)

	failing.failsFindElementScope.Store(true)
	err := engine.ResolveIncident(t.Context(), incident.Key)

	require.ErrorIs(t, err, errFindElementScopeFailure)
	assert.NotErrorIs(t, err, ErrIncidentNotResolvable, "nothing about the incident has to change: the resolution may be repeated as it is")
	assert.Equal(t, incident.Key, onlyOpenIncident(t, engine, instanceKey).Key, "the incident stays open")

	failing.failsFindElementScope.Store(false)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
	assert.Equal(t, beforeFirst.Key, requireSinglePendingJob(t, failing, instanceKey).Key)
}

func TestAScopeWhichCannotBeReadAfterAListenersCompletionIsCommittedFailsTheCompletion(t *testing.T) {
	failing := &failingScopeReadStorage{Storage: inmemory.NewStorage()}
	engine, instanceKey, beforeFirst := listenedTaskAtItsFirstStartListener(t, failing)

	// the completion reads the scope and commits; the run of the instance
	// which follows reads it again
	failing.readsUntilOneFails.Store(2)
	err := engine.JobCompleteByKey(t.Context(), beforeFirst.Key, nil)

	require.ErrorIs(t, err, errFindElementScopeFailure, "the completer is told that the instance did not go on")
	assert.Equal(t, runtime.ActivityStateCompleted, reloadJob(t, failing.Storage, beforeFirst.Key).State, "the completion itself is committed")
	incident := onlyOpenIncident(t, engine, instanceKey)
	assert.Equal(t, "listened-task", incident.ElementId)
	assert.Empty(t, pendingJobs(t, engine, instanceKey), "the next listener's job was not created")

	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
	waitForPendingJobOfType(t, engine, instanceKey, "before-second")
	assert.Equal(t, 1, jobsOfType(t, failing.Storage, instanceKey, "before-first"), "the completed listener is not run again")
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

func TestConcurrentCompletionsOfAListenersJobMoveItsElementOnOnce(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	beforeFirst := requireSinglePendingJob(t, store, instanceKey)

	const completions = 8
	start := make(chan struct{})
	results := make(chan error, completions)
	for range completions {
		go func() {
			<-start
			results <- engine.JobCompleteByKey(context.WithoutCancel(t.Context()), beforeFirst.Key, map[string]any{"fromBefore": "b1"})
		}()
	}
	close(start)
	for range completions {
		require.NoError(t, <-results, "a repeated completion is answered as the first one")
	}

	beforeSecond := waitForPendingJobOfType(t, engine, instanceKey, "before-second")
	assert.Equal(t, "b1", beforeSecond.InputVariables["fromBefore"])
	assert.Len(t, pendingJobs(t, engine, instanceKey), 1)
	assert.Equal(t, 1, jobsOfType(t, store, instanceKey, "before-second"), "the next listener's job is created once")
	scope := scopeOf(t, store, beforeFirst.ElementInstanceKey)
	assert.Equal(t, runtime.ElementPhaseActivating, scope.Phase)
	assert.Equal(t, 1, scope.ListenerIndex, "the scope moved on by one listener")
	assert.Equal(t, map[string]any{"given": "g"}, variablesOf(t, engine, store, instance))
}

func TestAListenerCompletedBeforeARestartIsNotRunAgain(t *testing.T) {
	store := &failingInstanceSaveStorage{Storage: inmemory.NewStorage()}
	engine := NewEngine(EngineWithStorage(store))
	engine.reconciliationInterval = time.Hour
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	definition := loadListenerFixture(t, &engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	beforeFirst := requireSinglePendingJob(t, store, instanceKey)

	// the completion is committed, and the engine goes down before it moves the token on
	store.failsSaveProcessInstance.Store(true)
	err = engine.JobCompleteByKey(t.Context(), beforeFirst.Key, map[string]any{"fromBefore": "b1"})
	require.ErrorIs(t, err, errSaveProcessInstanceFailure)
	require.Equal(t, runtime.ActivityStateCompleted, reloadJob(t, store.Storage, beforeFirst.Key).State)
	require.Empty(t, pendingJobs(t, &engine, instanceKey), "nothing followed the completion")
	store.failsSaveProcessInstance.Store(false)

	require.NoError(t, engine.reconcileRunningTokensAtStartup(t.Context()), "what a start of the engine does with a token left running")

	beforeSecond := requireSinglePendingJob(t, store, instanceKey)
	assert.Equal(t, "before-second", beforeSecond.Type, "the element goes on behind the listener which was completed")
	assert.Equal(t, "b1", beforeSecond.InputVariables["fromBefore"], "what the completed listener added was stored with its completion")
	assert.Equal(t, beforeFirst.ElementInstanceKey, beforeSecond.ElementInstanceKey)
	assert.Equal(t, 1, jobsOfType(t, store.Storage, instanceKey, "before-first"), "the completed listener is not run again")
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
	assert.Empty(t, store.Copy().ElementScopes, "the scope of a task ends with it, however the token left")
}

func TestTheScopeOfATaskEndsWhenItsTokenIsTakenOutOfTheTask(t *testing.T) {
	errorBoundary := `<bpmn:errorEventDefinition id="listened-task-error-definition" errorRef="listened-task-failed" />`
	closing := `</bpmn:definitions>`
	cases := []struct {
		name string
		// the fixture, and what is replaced in it: an old string, then its replacement
		fixture      string
		replaced     []string
		completed    []string
		waitingFor   string
		takeTokenOut func(t *testing.T, engine *Engine, instanceKey int64, waiting runtime.Job)
	}{
		{
			name: "by an interrupting message boundary event", fixture: "task-listeners.bpmn",
			replaced: []string{
				errorBoundary, `<bpmn:messageEventDefinition id="listened-task-message-definition" messageRef="listened-task-interrupt" />`,
				closing, `<bpmn:message id="listened-task-interrupt" name="listened-task-interrupt"><bpmn:extensionElements>` +
					`<zenbpm:subscription correlationKey="=given" /></bpmn:extensionElements></bpmn:message>` + closing,
			},
			completed: []string{"before-first", "before-second"}, waitingFor: "listened-work",
			takeTokenOut: func(t *testing.T, engine *Engine, instanceKey int64, _ runtime.Job) {
				require.NoError(t, engine.PublishMessageByName(t.Context(), "listened-task-interrupt", new("g"), nil))
				waitForPendingJobOfType(t, engine, instanceKey, "recovering-work")
			},
		},
		{
			name: "by an interrupting timer boundary event", fixture: "task-listeners.bpmn",
			replaced: []string{errorBoundary,
				`<bpmn:timerEventDefinition id="listened-task-timer-definition"><bpmn:timeDuration>PT1H</bpmn:timeDuration></bpmn:timerEventDefinition>`},
			completed: []string{"before-first", "before-second"}, waitingFor: "listened-work",
			takeTokenOut: func(t *testing.T, engine *Engine, instanceKey int64, _ runtime.Job) {
				timers, err := engine.persistence.FindProcessInstanceTimers(t.Context(), instanceKey, runtime.TimerStateCreated)
				require.NoError(t, err)
				require.Len(t, timers, 1)
				instance, tokens, err := engine.TriggerTimer(t.Context(), timers[0])
				require.NoError(t, err)
				require.NoError(t, engine.RunProcessInstance(t.Context(), *instance, tokens))
				waitForPendingJobOfType(t, engine, instanceKey, "recovering-work")
			},
		},
		{
			name: "by a modification which terminates the token", fixture: "task-listeners.bpmn", waitingFor: "before-first",
			takeTokenOut: func(t *testing.T, engine *Engine, instanceKey int64, waiting runtime.Job) {
				_, _, err := engine.ModifyInstance(t.Context(), instanceKey, []int64{waiting.ElementInstanceKey}, []string{"recovering-task"}, nil)
				require.NoError(t, err)
				waitForPendingJobOfType(t, engine, instanceKey, "recovering-work")
			},
		},
		{
			name: "by the cancellation of the process around its sub-process", fixture: "sub-process-task-listeners.bpmn",
			completed: []string{"inner-before"}, waitingFor: "inner-work",
			takeTokenOut: func(t *testing.T, engine *Engine, instanceKey int64, _ runtime.Job) {
				require.NoError(t, engine.CancelInstanceByKey(t.Context(), instanceKey))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine, store := newListenerTestEngine(t)
			fixture, err := os.ReadFile(executionListenerFixtures + tc.fixture)
			require.NoError(t, err)
			for i := 0; i < len(tc.replaced); i += 2 {
				require.Contains(t, string(fixture), tc.replaced[i])
			}
			definition, err := engine.LoadFromBytes(t.Context(), []byte(strings.NewReplacer(tc.replaced...).Replace(string(fixture))), engine.generateKey())
			require.NoError(t, err)
			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
			require.NoError(t, err)
			runJobsInOrder(t, engine, tc.completed...)
			waiting := waitForActiveJobOfType(t, engine, tc.waitingFor)
			require.NotEmpty(t, store.Copy().ElementScopes, "the task keeps a scope while the token waits in it")

			tc.takeTokenOut(t, engine, instance.ProcessInstance().Key, waiting)

			assert.Equal(t, runtime.ActivityStateTerminated, reloadJob(t, store, waiting.Key).State)
			assert.Empty(t, store.Copy().ElementScopes, "the scope ends with the element instance: nothing goes on from it")
			assert.NotNil(t, historyOf(t, engine, waiting.ElementInstanceKey).CompletedAt)
		})
	}
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
			assert.Empty(t, store.Copy().ElementScopes, "the scope of the task ends with the instance")
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
		scope := `<bpmn:subProcess id="listened-scope" name="listened-scope">`
		definition := loadListenerModel(t, engine, "sub-process-task-listeners.bpmn", scope, scope+
			`<bpmn:extensionElements><zeebe:ioMapping><zeebe:output source="=fromInnerAfter" target="mappedByTheScope" /></zeebe:ioMapping></bpmn:extensionElements>`)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
		require.NoError(t, err)

		runJobsInOrder(t, engine, "inner-before", "inner-work")
		innerAfter := waitForActiveJobOfType(t, engine, "inner-after")
		require.NoError(t, engine.JobCompleteByKey(t.Context(), innerAfter.Key, map[string]any{"fromInnerAfter": "i"}))

		waitForInstanceState(t, engine, store, instance, runtime.ActivityStateCompleted)
		subProcess := readInstanceByKey(t, engine, store, innerAfter.ProcessInstanceKey)
		assert.Equal(t, "i", subProcess.VariableHolder.LocalVariables()["fromInnerAfter"],
			"an end listener writes to the instance its element runs in: the sub-process")
		assert.Equal(t, map[string]any{"given": "g", "mappedByTheScope": "i"}, variablesOf(t, engine, store, instance),
			"from where it reaches the process as every variable of the sub-process does, by an output mapping")
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

	require.Error(t, engine.ResolveIncident(t.Context(), incident.Key), "the listener is still what it was")
	raisedAgain := onlyOpenIncident(t, engine, instance.ProcessInstance().Key)
	assert.NotEqual(t, incident.Key, raisedAgain.Key)
	assert.Contains(t, raisedAgain.Message, `its type "=jobType" is an expression`)
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
		require.NoError(t, engine.JobCompleteByKey(t.Context(), waitForActiveJobOfType(t, engine, jobType).Key, nil))
	}
}

// waitForActiveJobOfType waits for the one active job of the type, wherever
// its instance is.
func waitForActiveJobOfType(t *testing.T, engine *Engine, jobType string) runtime.Job {
	t.Helper()
	var jobs []runtime.Job
	require.Eventually(t, func() bool {
		var err error
		jobs, err = engine.persistence.FindActiveJobsByType(t.Context(), jobType)
		return err == nil && len(jobs) == 1
	}, listenerTestTimeout, listenerTestPoll, "the job of type %s", jobType)
	return jobs[0]
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

// listenedTaskWhoseOwnWorkFailed runs the task of task-listeners.bpmn until
// its start listeners are completed and the handler of its own job failed,
// and answers the incident of that failure: one of the token, which no job
// waits behind, on a task which keeps a scope.
func listenedTaskWhoseOwnWorkFailed(t *testing.T, store storage.Storage) (*Engine, *inmemory.Storage, runtime.Incident) {
	t.Helper()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	inMemory, ok := store.(*inmemory.Storage)
	if failing, wraps := store.(*failingScopeReadStorage); wraps {
		inMemory, ok = failing.Storage, true
	}
	require.True(t, ok, "the storage of the test is in memory")

	var fails atomic.Bool
	fails.Store(true)
	engine.NewTaskHandler().Type("listened-work").Handler(func(job ActivatedJob) {
		if fails.CompareAndSwap(true, false) {
			job.Fail("not yet")
			return
		}
		job.Complete()
	})
	definition := loadListenerFixture(t, &engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	beforeFirst := requireSinglePendingJob(t, store, instanceKey)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), beforeFirst.Key, nil))
	beforeSecond := waitForPendingJobOfType(t, &engine, instanceKey, "before-second")
	require.NoError(t, engine.JobCompleteByKey(t.Context(), beforeSecond.Key, nil))
	waitForInstanceState(t, &engine, inMemory, instance, runtime.ActivityStateFailed)
	return &engine, inMemory, onlyOpenIncident(t, &engine, instanceKey)
}

// listenedTaskAtItsFirstStartListener starts an instance of task-listeners.bpmn
// on the given storage and answers the job of the task's first start listener.
func listenedTaskAtItsFirstStartListener(t *testing.T, store storage.Storage) (*Engine, int64, runtime.Job) {
	t.Helper()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	definition := loadListenerFixture(t, &engine, "task-listeners.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"given": "g"})
	require.NoError(t, err)
	instanceKey := instance.ProcessInstance().Key
	beforeFirst := requireSinglePendingJob(t, store, instanceKey)
	require.Equal(t, "before-first", beforeFirst.Type)
	return &engine, instanceKey, beforeFirst
}

// elementsWhoseWorkIsOneJob are the elements the service task of the fixture
// job-element-listeners.bpmn is turned into, by its opening and closing tag.
var elementsWhoseWorkIsOneJob = []struct {
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

// lifecycleCostCountingStorage counts what the lifecycle of an element costs
// in the storage: how often the scope of an element instance is asked for, how
// often the delete of one is put into a batch, and how often a batch updates
// the history row of an element instance, by its key.
type lifecycleCostCountingStorage struct {
	*inmemory.Storage
	reads          atomic.Int32
	deletes        atomic.Int32
	historyUpdates sync.Map
}

func (s *lifecycleCostCountingStorage) FindElementScope(ctx context.Context, elementInstanceKey int64) (runtime.ElementScope, error) {
	s.reads.Add(1)
	return s.Storage.FindElementScope(ctx, elementInstanceKey)
}

func (s *lifecycleCostCountingStorage) NewBatch() storage.Batch {
	return &lifecycleCostCountingBatch{Batch: s.Storage.NewBatch(), costs: s}
}

func (s *lifecycleCostCountingStorage) historyUpdatesOf(elementInstanceKey int64) int32 {
	updates, _ := s.historyUpdates.LoadOrStore(elementInstanceKey, new(atomic.Int32))
	return updates.(*atomic.Int32).Load()
}

func (s *lifecycleCostCountingStorage) countHistoryUpdate(elementInstanceKey int64) {
	updates, _ := s.historyUpdates.LoadOrStore(elementInstanceKey, new(atomic.Int32))
	updates.(*atomic.Int32).Add(1)
}

type lifecycleCostCountingBatch struct {
	storage.Batch
	costs *lifecycleCostCountingStorage
}

func (b *lifecycleCostCountingBatch) DeleteElementScope(ctx context.Context, elementInstanceKey int64) error {
	b.costs.deletes.Add(1)
	return b.Batch.DeleteElementScope(ctx, elementInstanceKey)
}

func (b *lifecycleCostCountingBatch) UpdateOutputFlowElementInstance(ctx context.Context, history runtime.FlowElementInstance) error {
	b.costs.countHistoryUpdate(history.Key)
	return b.Batch.UpdateOutputFlowElementInstance(ctx, history)
}

func (b *lifecycleCostCountingBatch) CompleteFlowElementInstance(ctx context.Context, elementInstanceKey int64, completedAt time.Time) error {
	b.costs.countHistoryUpdate(elementInstanceKey)
	return b.Batch.CompleteFlowElementInstance(ctx, elementInstanceKey, completedAt)
}

var errFindElementScopeFailure = errors.New("storage failure: find element scope")

// failingScopeReadStorage fails to read the scope of an element instance on
// demand: every read while failsFindElementScope is set, and the one read
// which readsUntilOneFails counts down to, 2 for the second read from now.
type failingScopeReadStorage struct {
	*inmemory.Storage
	failsFindElementScope atomic.Bool
	readsUntilOneFails    atomic.Int32
}

func (s *failingScopeReadStorage) FindElementScope(ctx context.Context, elementInstanceKey int64) (runtime.ElementScope, error) {
	if s.readsUntilOneFails.Add(-1) == 0 || s.failsFindElementScope.Load() {
		return runtime.ElementScope{}, errFindElementScopeFailure
	}
	return s.Storage.FindElementScope(ctx, elementInstanceKey)
}

var errSaveProcessInstanceFailure = errors.New("storage failure: save process instance")

// failingInstanceSaveStorage fails to save a process instance outside a batch
// on demand, which is what the run of an instance does first: a completion
// is so committed without anything following it.
type failingInstanceSaveStorage struct {
	*inmemory.Storage
	failsSaveProcessInstance atomic.Bool
}

func (s *failingInstanceSaveStorage) SaveProcessInstance(ctx context.Context, instance runtime.ProcessInstance) error {
	if s.failsSaveProcessInstance.Load() {
		return errSaveProcessInstanceFailure
	}
	return s.Storage.SaveProcessInstance(ctx, instance)
}

// failingFeelRuntime fails every evaluation on demand.
type failingFeelRuntime struct {
	script.FeelRuntime
	fails atomic.Bool
}

func (feel *failingFeelRuntime) Evaluate(expression string, variableContext map[string]any) (any, error) {
	if feel.fails.Load() {
		return nil, errors.New("injected evaluation failure")
	}
	return feel.FeelRuntime.Evaluate(expression, variableContext)
}
