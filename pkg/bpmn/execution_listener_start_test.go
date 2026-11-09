package bpmn

import (
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const executionListenerFixtures = "./test-cases/execution_listeners/"

func TestStartListenerHoldsTheTokenUntilItsJobIsCompleted(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"input": "given"})
	require.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateActive, readInstance(t, engine, store, instance).State)
	job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
	assert.Equal(t, "start-listener-job", job.Type)
	assert.Equal(t, string(bpmn20.ElementTypeStartEvent), job.ElementType)
	assert.Equal(t, "start-listener-start", job.ElementId)
	assert.Equal(t, map[string]any{"input": "given", "mappedAtStart": "mapped"}, job.InputVariables,
		"the job carries the instance's variables, the outputs of the start event included")
	token, err := engine.persistence.GetTokenByKey(t.Context(), job.Token.Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.TokenStateWaiting, token.State)

	history, err := engine.persistence.GetFlowElementInstanceByKey(t.Context(), job.ElementInstanceKey)
	require.NoError(t, err)
	assert.Nil(t, history.CompletedAt, "the start event reads active while its listener's job waits")
	assert.Equal(t, "mapped", history.OutputVariables["mappedAtStart"], "the outputs of the start event are written before the job waits")
	assert.Nil(t, history.InputVariables, "the start event's history row copies no variables: the job carries them")

	require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

	next := waitForPendingJobOfType(t, engine, instance.ProcessInstance().Key, "start-listener-after")
	assert.Equal(t, "start-listener-task", next.ElementId)
	history, err = engine.persistence.GetFlowElementInstanceByKey(t.Context(), job.ElementInstanceKey)
	require.NoError(t, err)
	assert.NotNil(t, history.CompletedAt)
	assert.Equal(t, "mapped", history.OutputVariables["mappedAtStart"], "the completion keeps the outputs the start event wrote")
	completed, err := engine.persistence.FindJobByJobKey(t.Context(), job.Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, completed.State)
}

func TestStartListenerJobCompletionChangesNoVariableButPatchedOnesStay(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)

	_, _, err = engine.ModifyInstance(t.Context(), instance.ProcessInstance().Key, nil, nil, map[string]any{"patched": "before completion"})
	require.NoError(t, err)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, map[string]any{"completionVariable": "dropped"}))

	waitForPendingJobOfType(t, engine, instance.ProcessInstance().Key, "start-listener-after")
	read := readInstance(t, engine, store, instance)
	variables := read.VariableHolder.LocalVariables()
	assert.Equal(t, "before completion", variables["patched"])
	assert.NotContains(t, variables, "completionVariable")
}

func TestStartListenerJobCompletedTwiceAnswersSuccess(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)

	require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))
	require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

	waitForPendingJobOfType(t, engine, instance.ProcessInstance().Key, "start-listener-after")
	assert.Len(t, pendingJobs(t, engine, instance.ProcessInstance().Key), 1, "the second completion runs nothing twice")
}

func TestStartListenerJobFailureCountsDownRetriesAndRaisesIncidentOnStartEvent(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
	require.Equal(t, int32(2), job.Retries, "the retries are the listener's literal")

	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "first", nil, nil, nil, nil, nil))
	job = reloadJob(t, store, job.Key)
	assert.Equal(t, runtime.ActivityStateActive, job.State)
	assert.Equal(t, int32(1), job.Retries)

	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "second", nil, nil, nil, nil, nil))
	job = reloadJob(t, store, job.Key)
	assert.Equal(t, runtime.ActivityStateFailed, job.State)
	incident := onlyOpenIncident(t, engine, instance.ProcessInstance().Key)
	assert.Equal(t, "start-listener-start", incident.ElementId, "the incident stands on the start event")

	_, _, err = engine.ModifyInstance(t.Context(), instance.ProcessInstance().Key, nil, nil, map[string]any{"fixed": true})
	require.NoError(t, err)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))

	resolved := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
	assert.Equal(t, job.Key, resolved.Key, "the same job is handed out again")
	assert.Equal(t, runtime.ActivityStateActive, resolved.State)
	assert.Equal(t, true, resolved.InputVariables["fixed"], "the job's variables are rebuilt from the instance")
	assert.Equal(t, runtime.ActivityStateActive, readInstance(t, engine, store, instance).State)
}

func TestStartListenerJobRefusesBpmnError(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)

	err = engine.JobFailByKey(t.Context(), job.Key, "business error", new("some-code"), nil, nil, nil, nil)

	require.ErrorIs(t, err, ErrInvalidJobRequest)
	assert.Contains(t, err.Error(), "a listener job cannot throw a BPMN error")
	unchanged := reloadJob(t, store, job.Key)
	assert.Equal(t, runtime.ActivityStateActive, unchanged.State)
	assert.Equal(t, job.Retries, unchanged.Retries)
}

func TestFailureWithAnErrorCodeOfAnEndedListenerJobIsAnsweredAsForEveryEndedJob(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
	require.NoError(t, engine.CancelInstanceByKey(t.Context(), instance.ProcessInstance().Key))

	err = engine.JobFailByKey(t.Context(), job.Key, "business error", new("some-code"), nil, nil, nil, nil)

	require.ErrorIs(t, err, ErrJobInTerminalState, "a worker's lock of the ended job is released as for every ended job")
}

func TestRepeatedFailureWithAnErrorCodeOfARecordedListenerDeliveryAnswersSuccess(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
	token := deliver(t, store, job.Key)
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "first", nil, nil, nil, nil, &token))

	err = engine.JobFailByKey(t.Context(), job.Key, "repeated", new("some-code"), nil, nil, nil, &token)

	require.NoError(t, err, "the failure of the delivery is recorded already")
	assert.Equal(t, int32(1), reloadJob(t, store, job.Key).Retries, "the repeated failure spends nothing")
}

func TestModifyInstanceAtStartEventRunsTheListenerAgain(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	first := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)

	_, _, err = engine.ModifyInstance(t.Context(), instance.ProcessInstance().Key, nil, []string{"start-listener-start"}, nil)
	require.NoError(t, err)

	jobs := pendingJobs(t, engine, instance.ProcessInstance().Key)
	require.Len(t, jobs, 2)
	for _, job := range jobs {
		assert.Equal(t, "start-listener-job", job.Type)
	}
	assert.NotEqual(t, jobs[0].Key, jobs[1].Key)
	assert.Contains(t, []int64{jobs[0].Key, jobs[1].Key}, first.Key)
}

func TestInterruptingEventSubProcessTerminatesTheWaitingStartListenerJob(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener-event-subprocess.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"correlationKey": "esp-1"})
	require.NoError(t, err)
	job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)

	require.NoError(t, engine.PublishMessageByName(t.Context(), "start-listener-esp-message", new("esp-1"), nil))

	waitForInstanceState(t, engine, store, instance, runtime.ActivityStateCompleted)
	assert.Equal(t, runtime.ActivityStateTerminated, reloadJob(t, store, job.Key).State)
}

func TestCancelWhileStartListenerWaitsTerminatesItsJob(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)

	require.NoError(t, engine.CancelInstanceByKey(t.Context(), instance.ProcessInstance().Key))

	assert.Equal(t, runtime.ActivityStateTerminated, readInstance(t, engine, store, instance).State)
	assert.Equal(t, runtime.ActivityStateTerminated, reloadJob(t, store, job.Key).State)
	err = engine.JobCompleteByKey(t.Context(), job.Key, nil)
	require.ErrorIs(t, err, ErrJobInTerminalState)
}

func TestMessageStartEventWithListenerWaitsBehindItsJob(t *testing.T) {
	engine, _ := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener-message.bpmn")
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), definition.Key))

	// the called process is not deployed: nothing behind the start event runs inside the publish
	require.NoError(t, engine.PublishMessageByName(t.Context(), "start-listener-message", nil, map[string]any{"orderId": "order-1"}))

	jobs, err := engine.persistence.FindActiveJobsByType(t.Context(), "start-listener-message-job")
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	job := jobs[0]
	assert.Equal(t, "order-1", job.InputVariables["orderFromMessage"])
	history, err := engine.persistence.GetFlowElementInstanceByKey(t.Context(), job.ElementInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, "order-1", history.OutputVariables["orderFromMessage"], "the payload mapping shows in the history before the completion")

	require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

	history, err = engine.persistence.GetFlowElementInstanceByKey(t.Context(), job.ElementInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, "order-1", history.OutputVariables["orderFromMessage"], "the payload mapping shows in the history after the completion")
	require.Eventually(t, func() bool {
		incidents, err := engine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), job.ProcessInstanceKey)
		return err == nil && len(incidents) > 0
	}, 5*time.Second, 20*time.Millisecond, "the missing called process surfaces as an incident of the run after the completion")
}

func TestCalledProcessWaitsAtItsStartListener(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	loadListenerFixture(t, engine, "start-listener-called.bpmn")
	caller := loadListenerFixture(t, engine, "start-listener-caller.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), caller.Key, nil)
	require.NoError(t, err)

	job := waitForActiveJob(t, store, "start-listener-called-job")
	assert.NotEqual(t, instance.ProcessInstance().Key, job.ProcessInstanceKey, "the child holds its own job")
	assert.Equal(t, runtime.ActivityStateActive, readInstance(t, engine, store, instance).State)

	require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

	waitForInstanceState(t, engine, store, instance, runtime.ActivityStateCompleted)
}

func TestTimerStartEventWaitsBehindItsListener(t *testing.T) {
	cases := []struct {
		name      string
		timer     string
		instances int
	}{
		{name: "date", timer: `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">` + time.Now().Add(time.Second).UTC().Format(time.RFC3339Nano) + `</bpmn:timeDate>`, instances: 1},
		{name: "cycle", timer: `<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">R2/PT1S</bpmn:timeCycle>`, instances: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine, store := newListenerTestEngine(t, EngineWithPollTimerDelay(100*time.Millisecond))
			fixture, err := os.ReadFile(executionListenerFixtures + "start-listener-timer.bpmn")
			require.NoError(t, err)
			model := strings.Replace(string(fixture), `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">2026-01-01T00:00:00Z</bpmn:timeDate>`, tc.timer, 1)
			definition, err := engine.LoadFromBytes(t.Context(), []byte(model), engine.generateKey())
			require.NoError(t, err)
			require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), definition.Key))

			var jobs []runtime.Job
			require.Eventually(t, func() bool {
				jobs, err = engine.persistence.FindActiveJobsByType(t.Context(), "start-listener-timer-job")
				return err == nil && len(jobs) == tc.instances
			}, 10*time.Second, 50*time.Millisecond, "every instance the timer creates waits behind a job of its own")
			instances := map[int64]bool{}
			for _, job := range jobs {
				instances[job.ProcessInstanceKey] = true
				assert.Equal(t, runtime.ActivityStateActive, readInstanceByKey(t, engine, store, job.ProcessInstanceKey).State)
				token, err := engine.persistence.GetTokenByKey(t.Context(), job.Token.Key)
				require.NoError(t, err)
				assert.Equal(t, runtime.TokenStateWaiting, token.State, "the token of a timer start waits at its start event")
				history, err := engine.persistence.GetFlowElementInstanceByKey(t.Context(), job.ElementInstanceKey)
				require.NoError(t, err)
				assert.Nil(t, history.CompletedAt, "the timer's start event reads active while its listener's job waits")
			}
			assert.Len(t, instances, tc.instances)

			for _, job := range jobs {
				require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))
				waitForInstanceStateByKey(t, engine, store, job.ProcessInstanceKey, runtime.ActivityStateCompleted)
			}
		})
	}
}

func TestEveryStartEventATokenReachesWaitsBehindItsOwnJob(t *testing.T) {
	engine, _ := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "start-listener-entries.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)

	jobsByElement := map[string]string{}
	for _, job := range pendingJobs(t, engine, instance.ProcessInstance().Key) {
		jobsByElement[job.ElementId] = job.Type
	}
	assert.Equal(t, map[string]string{"entries-first-start": "entries-first-job", "entries-second-start": "entries-second-job"}, jobsByElement,
		"each none start event holds its token behind a job of its own, and nothing behind them runs")
}

func TestEnteringAProcessElsewhereRunsNoListener(t *testing.T) {
	t.Run("an instance started at another element, through a multi-instance activity", func(t *testing.T) {
		engine, _ := newListenerTestEngine(t)
		definition := loadListenerFixture(t, engine, "start-listener-entries.bpmn")

		_, err := engine.CreateInstanceWithStartingElements(t.Context(), definition.Key, []string{"entries-task"}, nil, nil)
		require.NoError(t, err)

		assert.Equal(t, map[string]int{"entries-first-job": 0, "entries-second-job": 0, "entries-work": 2}, waitForEntriesJobs(t, engine),
			"no start event listener ran, neither for the instance nor for an iteration")
	})
	t.Run("an instance started by an instantiating receive task", func(t *testing.T) {
		engine, _ := newListenerTestEngine(t)
		definition := loadListenerFixture(t, engine, "start-listener-entries.bpmn")
		require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), definition.Key))

		require.NoError(t, engine.PublishMessageByName(t.Context(), "start-listener-entries-message", nil, nil))

		assert.Equal(t, map[string]int{"entries-first-job": 0, "entries-second-job": 0, "entries-work": 2}, waitForEntriesJobs(t, engine),
			"the instance starts at the receive task, and no start event listener ran")
	})
}

func TestListenerJobsAreNeverHandedToAnEngineHandler(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	definition := loadListenerFixture(t, engine, "listener-handler.bpmn")
	var calls atomic.Int32
	count := func(job ActivatedJob) {
		calls.Add(1)
		job.Complete()
	}
	startHandler := engine.NewTaskHandler().Id("listener-handler-start").Handler(count)
	defer engine.RemoveHandler(startHandler)

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	startJob := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
	assert.Equal(t, "listener-handler-start-job", startJob.Type)
	assert.Equal(t, runtime.ActivityStateActive, startJob.State)
	assert.Equal(t, int32(0), calls.Load(), "no handler of the engine was called for a listener")

	require.NoError(t, engine.JobCompleteByKey(t.Context(), startJob.Key, nil))
	waitForPendingJobOfType(t, engine, instance.ProcessInstance().Key, "listener-handler-work")
}

func TestStoredListenerWithoutLiteralsRaisesAnIncident(t *testing.T) {
	cases := []struct {
		name         string
		listener     string
		errorMessage string
	}{
		{name: "an expression as the type", listener: `type="=jobType"`, errorMessage: `its type "=jobType" is an expression`},
		{name: "an expression as the retries", listener: `type="start-listener-job" retries="=missingVar"`, errorMessage: `its retries "=missingVar" is an expression`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine, store := newListenerTestEngine(t)
			fixture, err := os.ReadFile(executionListenerFixtures + "start-listener.bpmn")
			require.NoError(t, err)
			model := strings.Replace(string(fixture), `type="start-listener-job" retries="2"`, tc.listener, 1)
			// stored definitions are imported without the checks of a deployment
			definition, err := engine.ImportProcessDefinition(t.Context(), []byte(model), engine.generateKey(), 1, false)
			require.NoError(t, err)

			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"jobType": "computed"})
			require.Error(t, err)
			require.NotNil(t, instance)

			incident := onlyOpenIncident(t, engine, instance.ProcessInstance().Key)
			assert.Equal(t, "start-listener-start", incident.ElementId)
			assert.Contains(t, incident.Message, tc.errorMessage)
			assert.Empty(t, pendingJobs(t, engine, instance.ProcessInstance().Key), "no job is created from an expression")
			assert.Equal(t, runtime.ActivityStateFailed, readInstance(t, engine, store, instance).State)
		})
	}
}

func TestDeploymentRefusesAListenerTheEngineDoesNotRunButAnImportKeepsIt(t *testing.T) {
	engine, store := newListenerTestEngine(t)
	fixture, err := os.ReadFile(executionListenerFixtures + "start-listener.bpmn")
	require.NoError(t, err)
	model := []byte(strings.Replace(string(fixture), `eventType="end" type="start-listener-job"`, `eventType="start" type="start-listener-job"`, 1))

	_, err = engine.LoadFromBytes(t.Context(), model, engine.generateKey())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `execution listener eventType="start" on start event id="start-listener-start" is not supported: a start event takes one listener with eventType="end"`)

	imported, err := engine.ImportProcessDefinition(t.Context(), model, engine.generateKey(), 1, false)
	require.NoError(t, err, "a stored definition keeps loading")
	instance, err := engine.CreateInstanceByKey(t.Context(), imported.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, "start-listener-after", requireSinglePendingJob(t, store, instance.ProcessInstance().Key).Type,
		"a listener of an event type the engine does not run is not run")
}

func TestImportLogsADefinitionWhoseStartEventListenerRuns(t *testing.T) {
	var log syncBuffer
	engine, _ := newListenerTestEngine(t, EngineWithLogger(hclog.New(&hclog.LoggerOptions{Output: &log, Level: hclog.Info})))
	fixture, err := os.ReadFile(executionListenerFixtures + "start-listener.bpmn")
	require.NoError(t, err)
	logged := func() int {
		return strings.Count(log.String(), "imported process definition carries start event listeners")
	}

	key := engine.generateKey()
	_, err = engine.ImportProcessDefinition(t.Context(), fixture, key, 1, false)
	require.NoError(t, err)
	assert.Equal(t, 1, logged())

	_, err = engine.ImportProcessDefinition(t.Context(), fixture, key, 1, false)
	require.NoError(t, err)
	assert.Equal(t, 1, logged(), "a repeated import logs nothing")

	withoutListeners := strings.Replace(string(fixture), `<zeebe:executionListener eventType="end" type="start-listener-job" retries="2" retryBackoff="PT0S" />`, "", 1)
	_, err = engine.ImportProcessDefinition(t.Context(), []byte(withoutListeners), key, 1, false)
	require.ErrorIs(t, err, storage.ErrUniqueConstraint)
	assert.Equal(t, 1, logged(), "a refused import logs nothing")

	_, err = engine.ImportProcessDefinition(t.Context(), []byte(withoutListeners), engine.generateKey(), 2, false)
	require.NoError(t, err)
	assert.Equal(t, 1, logged(), "a definition without start event listeners logs nothing")

	notRun := strings.Replace(string(fixture), `eventType="end" type="start-listener-job"`, `eventType="start" type="start-listener-job"`, 1)
	_, err = engine.ImportProcessDefinition(t.Context(), []byte(notRun), engine.generateKey(), 3, false)
	require.NoError(t, err)
	assert.Equal(t, 1, logged(), "a definition whose listeners are not run logs nothing")
}

func newListenerTestEngine(t *testing.T, options ...EngineOption) (*Engine, *inmemory.Storage) {
	t.Helper()
	store := inmemory.NewStorage()
	engine := NewEngine(append([]EngineOption{EngineWithStorage(store)}, options...)...)
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	return &engine, store
}

func loadListenerFixture(t *testing.T, engine *Engine, file string) *runtime.ProcessDefinition {
	t.Helper()
	definition, err := engine.LoadFromFile(t.Context(), executionListenerFixtures+file)
	require.NoError(t, err)
	return definition
}

// readInstance reads a copy of the instance's data under the engine's lock of
// the instance (see processInstanceSnapshot): the in-memory storage hands out
// the very object the engine writes, so a read without the lock, or of its
// variables after the lock is released, races with an engine goroutine still
// running the instance.
func readInstance(t *testing.T, engine *Engine, store *inmemory.Storage, instance runtime.ProcessInstance) runtime.ProcessInstanceData {
	t.Helper()
	return readInstanceByKey(t, engine, store, instance.ProcessInstance().Key)
}

func readInstanceByKey(t *testing.T, engine *Engine, store *inmemory.Storage, key int64) runtime.ProcessInstanceData {
	t.Helper()
	snapshot, err := processInstanceSnapshot(t, engine, store, key)
	require.NoError(t, err)
	return *snapshot.ProcessInstance()
}

// waitForInstanceState waits until the instance reaches the state, reading it
// as readInstance does.
func waitForInstanceState(t *testing.T, engine *Engine, store *inmemory.Storage, instance runtime.ProcessInstance, state runtime.ActivityState) {
	t.Helper()
	waitForInstanceStateByKey(t, engine, store, instance.ProcessInstance().Key, state)
}

func waitForInstanceStateByKey(t *testing.T, engine *Engine, store *inmemory.Storage, key int64, state runtime.ActivityState) {
	t.Helper()
	require.Eventually(t, func() bool {
		snapshot, err := processInstanceSnapshot(t, engine, store, key)
		return err == nil && snapshot.ProcessInstance().State == state
	}, 5*time.Second, 20*time.Millisecond, "process instance %d should reach state %s", key, state)
}

// waitForEntriesJobs waits until the engine of a test of
// start-listener-entries.bpmn holds the two iteration jobs of its
// multi-instance task, and answers how many jobs of each type are active then.
func waitForEntriesJobs(t *testing.T, engine *Engine) map[string]int {
	t.Helper()
	counts := map[string]int{}
	require.Eventually(t, func() bool {
		for _, jobType := range []string{"entries-first-job", "entries-second-job", "entries-work"} {
			jobs, err := engine.persistence.FindActiveJobsByType(t.Context(), jobType)
			if err != nil {
				return false
			}
			counts[jobType] = len(jobs)
		}
		return counts["entries-work"] == 2
	}, 5*time.Second, 20*time.Millisecond, "the two iterations of entries-task wait")
	return counts
}

func pendingJobs(t *testing.T, engine *Engine, instanceKey int64) []runtime.Job {
	t.Helper()
	jobs, err := engine.persistence.FindPendingProcessInstanceJobs(t.Context(), instanceKey)
	require.NoError(t, err)
	return jobs
}

func waitForPendingJobOfType(t *testing.T, engine *Engine, instanceKey int64, jobType string) runtime.Job {
	t.Helper()
	var found runtime.Job
	require.Eventually(t, func() bool {
		jobs, err := engine.persistence.FindPendingProcessInstanceJobs(t.Context(), instanceKey)
		if err != nil {
			return false
		}
		for _, job := range jobs {
			if job.Type == jobType {
				found = job
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond, "a job of type %s of instance %d waits", jobType, instanceKey)
	return found
}

func onlyOpenIncident(t *testing.T, engine *Engine, instanceKey int64) runtime.Incident {
	t.Helper()
	incidents, err := engine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), instanceKey)
	require.NoError(t, err)
	var open []runtime.Incident
	for _, incident := range incidents {
		if incident.ResolvedAt == nil {
			open = append(open, incident)
		}
	}
	require.Len(t, open, 1, "exactly one incident of instance %d is open", instanceKey)
	return open[0]
}
