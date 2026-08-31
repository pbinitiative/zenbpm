package bpmn

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEngineUsesDefaultMaxProcessInstanceElementExecutionCount(t *testing.T) {
	engine := NewEngine()
	t.Cleanup(engine.contextCancel)

	assert.Equal(t, DefaultMaxProcessInstanceElementExecutionCount, engine.maxProcessInstanceElementExecutionCount)
}

// TestLoopingProcessInstanceStopsAtMaxProcessInstanceElementExecutionCountAndCreatesIncident verifies that a
// sequence-flow loop without a reachable exit condition is stopped by the element execution
// guard: the engine fails the token, fails the instance, and raises an incident instead of
// looping forever.
func TestLoopingProcessInstanceStopsAtMaxProcessInstanceElementExecutionCountAndCreatesIncident(t *testing.T) {
	const maxExecutionCount = int64(6)
	engine, store := startEngineWithMaxProcessInstanceElementExecutionCount(t, maxExecutionCount)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/simple-flow-loop.bpmn")
	require.NoError(t, err)

	var handlerInvocations atomic.Int64
	handler := engine.NewTaskHandler().Type("loopJobType").Handler(func(job ActivatedJob) {
		handlerInvocations.Add(1)
		job.SetOutputVariable("done", false) // never allows the loop to exit
		job.Complete()
	})
	defer engine.RemoveHandler(handler)

	// job handlers run inline, so the guard error of the endlessly looping instance
	// surfaces through the creation call itself; the incident is recorded regardless
	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, map[string]any{"done": false})
	require.ErrorIs(t, err, ErrMaxProcessInstanceElementExecutionCountExceeded)
	require.NotNil(t, instance)

	incident := waitForElementExecutionCountIncident(t, engine, store, 10*time.Second)
	assert.Contains(t, incident.Message, fmt.Sprintf("maximum allowed process instance element execution count of %d", maxExecutionCount))
	assert.Equal(t, instance.ProcessInstance().Key, incident.ProcessInstanceKey)

	// the guard must fail the offending token and the process instance
	require.NotZero(t, incident.Token.Key, "element execution count incidents must be bound to the failing token")
	failedToken, err := store.GetTokenByKey(t.Context(), incident.Token.Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.TokenStateFailed, failedToken.State)
	waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateFailed)

	// the loop must have stopped: handler invocations are bounded by the configured limit
	assert.LessOrEqual(t, handlerInvocations.Load(), maxExecutionCount,
		"the job handler must not keep being invoked after the guard tripped")
}

// TestLoopingProcessCompletesUnderMaxProcessInstanceElementExecutionCount verifies that a legitimate,
// bounded loop that stays under the configured limit completes without incidents.
func TestLoopingProcessCompletesUnderMaxProcessInstanceElementExecutionCount(t *testing.T) {
	const maxExecutionCount = int64(50)
	engine, store := startEngineWithMaxProcessInstanceElementExecutionCount(t, maxExecutionCount)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/simple-flow-loop.bpmn")
	require.NoError(t, err)

	var handlerInvocations atomic.Int64
	handler := engine.NewTaskHandler().Type("loopJobType").Handler(func(job ActivatedJob) {
		if handlerInvocations.Add(1) >= 3 {
			job.SetOutputVariable("done", true)
		}
		job.Complete()
	})
	defer engine.RemoveHandler(handler)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, map[string]any{"done": false})
	require.NoError(t, err)

	waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateCompleted)
	assert.Equal(t, int64(3), handlerInvocations.Load())

	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Empty(t, incidents)
}

// TestNonPositiveMaxProcessInstanceElementExecutionCountDisablesLoopGuard verifies that limits <= 0 disable
// the element execution guard entirely: a loop iterating more often than any small limit
// completes without incidents.
func TestNonPositiveMaxProcessInstanceElementExecutionCountDisablesLoopGuard(t *testing.T) {
	for _, maxExecutionCount := range []int64{0, -1} {
		t.Run(fmt.Sprintf("limit_%d", maxExecutionCount), func(t *testing.T) {
			engine, store := startEngineWithMaxProcessInstanceElementExecutionCount(t, maxExecutionCount)

			process, err := engine.LoadFromFile(t.Context(), "./test-cases/simple-flow-loop.bpmn")
			require.NoError(t, err)

			var handlerInvocations atomic.Int64
			handler := engine.NewTaskHandler().Type("loopJobType").Handler(func(job ActivatedJob) {
				if handlerInvocations.Add(1) >= 8 {
					job.SetOutputVariable("done", true)
				}
				job.Complete()
			})
			defer engine.RemoveHandler(handler)

			instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, map[string]any{"done": false})
			require.NoError(t, err)

			waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateCompleted)
			incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
			require.NoError(t, err)
			assert.Empty(t, incidents)

			// disabled guard must not persist any counter
			assert.Zero(t, executionCountForInstance(store, instance.ProcessInstance().Key))
		})
	}
}

func TestProcessInstanceElementExecutionCountSurvivesEngineRecreation(t *testing.T) {
	store := inmemory.NewStorage()
	instance := &runtime.DefaultProcessInstance{ProcessInstanceData: runtime.ProcessInstanceData{Key: store.GenerateId()}}
	token := runtime.ExecutionToken{
		Key:                store.GenerateId(),
		ElementInstanceKey: store.GenerateId(),
		ElementId:          "loop-element",
		ProcessInstanceKey: instance.ProcessInstance().Key,
		State:              runtime.TokenStateRunning,
	}

	firstEngine := NewEngine(EngineWithStorage(store), EngineWithMaxProcessInstanceElementExecutionCount(1))
	t.Cleanup(firstEngine.contextCancel)
	firstBatch, err := firstEngine.NewEngineBatchClean()
	require.NoError(t, err)
	require.NoError(t, firstEngine.validateAndIncrementElementExecutionCount(
		t.Context(), &firstBatch, instance, token, &elementExecutionRunCount{},
	))
	require.NoError(t, firstBatch.Flush(t.Context()))

	secondEngine := NewEngine(EngineWithStorage(store), EngineWithMaxProcessInstanceElementExecutionCount(1))
	t.Cleanup(secondEngine.contextCancel)
	secondBatch, err := secondEngine.NewEngineBatchClean()
	require.NoError(t, err)
	err = secondEngine.validateAndIncrementElementExecutionCount(
		t.Context(), &secondBatch, instance, token, &elementExecutionRunCount{},
	)
	require.ErrorIs(t, err, ErrMaxProcessInstanceElementExecutionCountExceeded)
	secondBatch.Clear(t.Context())

	count, err := store.GetElementExecutionCount(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "the rejected retry must not be persisted")
}

// TestResolvingProcessInstanceElementExecutionCountIncidentResetsCounterAndAllowsCompletion verifies that
// resolution grants a fresh execution budget by resetting the instance-wide counter, so the
// corrected loop can continue and complete.
func TestResolvingProcessInstanceElementExecutionCountIncidentResetsCounterAndAllowsCompletion(t *testing.T) {
	const maxExecutionCount = int64(6)
	engine, store := startEngineWithMaxProcessInstanceElementExecutionCount(t, maxExecutionCount)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/simple-flow-loop.bpmn")
	require.NoError(t, err)

	var exitAllowed atomic.Bool
	handler := engine.NewTaskHandler().Type("loopJobType").Handler(func(job ActivatedJob) {
		job.SetOutputVariable("done", exitAllowed.Load())
		job.Complete()
	})
	defer engine.RemoveHandler(handler)

	// job handlers run inline, so the guard error of the endlessly looping instance
	// surfaces through the creation call itself; the incident is recorded regardless
	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, map[string]any{"done": false})
	require.ErrorIs(t, err, ErrMaxProcessInstanceElementExecutionCountExceeded)
	require.NotNil(t, instance)

	incident := waitForElementExecutionCountIncident(t, engine, store, 10*time.Second)
	waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateFailed)
	assert.Equal(t, runtime.IncidentTypeMaxProcessInstanceElementExecutionCountExceeded, incident.Type)
	assert.Equal(t, maxExecutionCount, executionCountForInstance(store, instance.ProcessInstance().Key))

	// operator intervention: let the loop exit and resolve the incident
	exitAllowed.Store(true)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))

	// resolution reset the counter, so the corrected traversal fits into a fresh budget:
	// the instance completes and the final counter reflects only post-resolution executions
	waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateCompleted)
	countAfterResolution := executionCountForInstance(store, instance.ProcessInstance().Key)
	assert.Positive(t, countAfterResolution)
	assert.LessOrEqual(t, countAfterResolution, maxExecutionCount)
}

// TestParallelTokensCreateOneProcessInstanceElementExecutionCountIncidentAndResumeTogether verifies that a
// process-wide budget breach stops the current run after the first incident. Runnable sibling
// tokens remain persisted and are resumed together after the single incident resets the counter.
func TestParallelTokensCreateOneProcessInstanceElementExecutionCountIncidentAndResumeTogether(t *testing.T) {
	const maxExecutionCount = int64(4)
	engine, store := startEngineWithMaxProcessInstanceElementExecutionCount(t, maxExecutionCount)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/parallel-gateway-flow.bpmn")
	require.NoError(t, err)

	aHandler := engine.NewTaskHandler().Id("id-a-1").Handler(func(job ActivatedJob) {
		job.Complete()
	})
	defer engine.RemoveHandler(aHandler)
	var firstBranchExecutions atomic.Int64
	b1Handler := engine.NewTaskHandler().Id("id-b-1").Handler(func(job ActivatedJob) {
		firstBranchExecutions.Add(1)
		job.Complete()
	})
	defer engine.RemoveHandler(b1Handler)
	var secondBranchExecutions atomic.Int64
	b2Handler := engine.NewTaskHandler().Id("id-b-2").Handler(func(job ActivatedJob) {
		secondBranchExecutions.Add(1)
		job.Complete()
	})
	defer engine.RemoveHandler(b2Handler)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.ErrorIs(t, err, ErrMaxProcessInstanceElementExecutionCountExceeded)
	require.NotNil(t, instance)

	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1, "one exhausted process-instance budget must create one incident")
	incident := incidents[0]
	assert.Equal(t, runtime.IncidentTypeMaxProcessInstanceElementExecutionCountExceeded, incident.Type)

	activeTokens, err := store.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Condition(t, func() bool {
		for _, token := range activeTokens {
			if token.Key != incident.Token.Key && token.State == runtime.TokenStateRunning {
				return true
			}
		}
		return false
	}, "a parallel sibling must remain runnable while the incident is unresolved")

	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
	waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateCompleted)
	assert.Equal(t, int64(1), firstBranchExecutions.Load())
	assert.Equal(t, int64(1), secondBranchExecutions.Load())

	incidents, err = store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1, "resuming sibling tokens must not create another budget incident")
	assert.NotNil(t, incidents[0].ResolvedAt)
	assert.LessOrEqual(t, executionCountForInstance(store, instance.ProcessInstance().Key), maxExecutionCount)
}

func TestEngineRestartDoesNotResumeParallelTokensBlockedByElementExecutionCountIncident(t *testing.T) {
	const maxExecutionCount = int64(4)
	store := inmemory.NewStorage()
	firstEngine := NewEngine(
		EngineWithStorage(store),
		EngineWithMaxProcessInstanceElementExecutionCount(maxExecutionCount),
	)
	t.Cleanup(firstEngine.Stop)
	require.NoError(t, firstEngine.Start(t.Context()))
	registerParallelCompletionHandlers(t, &firstEngine)

	process, err := firstEngine.LoadFromFile(t.Context(), "./test-cases/parallel-gateway-flow.bpmn")
	require.NoError(t, err)
	instance, err := firstEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.ErrorIs(t, err, ErrMaxProcessInstanceElementExecutionCountExceeded)
	require.NotNil(t, instance)

	processInstanceKey := instance.ProcessInstance().Key
	incidentsBeforeRestart, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidentsBeforeRestart, 1)
	incident := incidentsBeforeRestart[0]
	runningTokenKeysBeforeRestart := runningTokenKeysForInstance(t, store, processInstanceKey)
	require.NotEmpty(t, runningTokenKeysBeforeRestart, "a runnable sibling must be preserved for incident resolution")
	assert.Equal(t, maxExecutionCount, executionCountForInstance(store, processInstanceKey))
	waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateFailed)
	firstEngine.Stop()

	secondEngine := NewEngine(
		EngineWithStorage(store),
		EngineWithMaxProcessInstanceElementExecutionCount(maxExecutionCount),
	)
	t.Cleanup(secondEngine.Stop)
	registerParallelCompletionHandlers(t, &secondEngine)
	require.NoError(t, secondEngine.Start(t.Context()))

	incidentsAfterRestart, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidentsAfterRestart, 1, "startup recovery must not create another incident")
	assert.Equal(t, incident.Key, incidentsAfterRestart[0].Key)
	assert.Nil(t, incidentsAfterRestart[0].ResolvedAt)
	assert.ElementsMatch(t, runningTokenKeysBeforeRestart, runningTokenKeysForInstance(t, store, processInstanceKey))
	assert.Equal(t, maxExecutionCount, executionCountForInstance(store, processInstanceKey))
	restartedInstance, err := store.FindProcessInstanceByKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, restartedInstance.ProcessInstance().State)

	require.NoError(t, secondEngine.ResolveIncident(t.Context(), incident.Key))
	waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateCompleted)
	incidentsAfterResolution, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidentsAfterResolution, 1)
	assert.NotNil(t, incidentsAfterResolution[0].ResolvedAt)
}

// startEngineWithMaxProcessInstanceElementExecutionCount starts a dedicated engine backed by a fresh in-memory
// storage with the given maximum element execution count. The engine is stopped on test cleanup.
func startEngineWithMaxProcessInstanceElementExecutionCount(t *testing.T, maxExecutionCount int64, extraOptions ...EngineOption) (*Engine, *inmemory.Storage) {
	t.Helper()
	store := inmemory.NewStorage()
	options := append([]EngineOption{
		EngineWithStorage(store),
		EngineWithMaxProcessInstanceElementExecutionCount(maxExecutionCount),
	}, extraOptions...)
	engine := NewEngine(options...)
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	return &engine, store
}

// waitForElementExecutionCountIncident waits for and returns the first unresolved incident in the
// given store whose message reports a breach of the maximum element execution count.
func waitForElementExecutionCountIncident(t *testing.T, engine *Engine, store *inmemory.Storage, timeout time.Duration) runtime.Incident {
	t.Helper()
	var incident runtime.Incident
	require.Eventually(t, func() bool {
		for _, pi := range processInstancesSnapshot(t, engine, store) {
			incidents, findErr := store.FindIncidentsByProcessInstanceKey(t.Context(), pi.ProcessInstance().Key)
			if findErr != nil {
				continue
			}
			for _, candidate := range incidents {
				if candidate.ResolvedAt == nil && strings.Contains(candidate.Message, "maximum allowed process instance element execution count") {
					incident = candidate
					return true
				}
			}
		}
		return false
	}, timeout, 50*time.Millisecond, "expected an element execution count incident to be created")
	return incident
}

// executionCountForInstance returns a snapshot of the persisted total element execution counter
// of the given process instance.
func executionCountForInstance(store *inmemory.Storage, processInstanceKey int64) int64 {
	return store.Copy().ElementExecutionCounters[processInstanceKey]
}

func registerParallelCompletionHandlers(t *testing.T, engine *Engine) {
	t.Helper()
	for _, elementID := range []string{"id-a-1", "id-b-1", "id-b-2"} {
		handler := engine.NewTaskHandler().Id(elementID).Handler(func(job ActivatedJob) {
			job.Complete()
		})
		t.Cleanup(func() { engine.RemoveHandler(handler) })
	}
}

func runningTokenKeysForInstance(t *testing.T, store *inmemory.Storage, processInstanceKey int64) []int64 {
	t.Helper()
	tokens, err := store.GetActiveTokensForProcessInstance(t.Context(), processInstanceKey)
	require.NoError(t, err)
	keys := make([]int64, 0, len(tokens))
	for _, token := range tokens {
		if token.State == runtime.TokenStateRunning {
			keys = append(keys, token.Key)
		}
	}
	return keys
}
