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

func TestNewEngineUsesDefaultMaxProcessInstanceFlowNodeCount(t *testing.T) {
	engine := NewEngine()
	t.Cleanup(engine.contextCancel)

	assert.Equal(t, DefaultMaxProcessInstanceFlowNodeCount, engine.maxProcessInstanceFlowNodeCount)
}

// TestLoopingProcessInstanceStopsAtMaxProcessInstanceFlowNodeCountAndCreatesIncident verifies that a
// sequence-flow loop without a reachable exit condition is stopped by the flow node count
// guard: the engine fails the token, fails the instance, and raises an incident instead of
// looping forever.
func TestLoopingProcessInstanceStopsAtMaxProcessInstanceFlowNodeCountAndCreatesIncident(t *testing.T) {
	const maxFlowNodeCount = int64(6)
	engine, store := startEngineWithMaxProcessInstanceFlowNodeCount(t, maxFlowNodeCount)

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
	require.ErrorIs(t, err, ErrMaxProcessInstanceFlowNodeCountExceeded)
	require.NotNil(t, instance)

	incident := waitForFlowNodeCountIncident(t, engine, store, 10*time.Second)
	assert.Contains(t, incident.Message, fmt.Sprintf("maximum allowed process instance flow node count of %d", maxFlowNodeCount))
	assert.Equal(t, instance.ProcessInstance().Key, incident.ProcessInstanceKey)

	// the guard must fail the offending token and the process instance
	require.NotZero(t, incident.Token.Key, "flow node count incidents must be bound to the failing token")
	failedToken, err := store.GetTokenByKey(t.Context(), incident.Token.Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.TokenStateFailed, failedToken.State)
	waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateFailed)

	// the loop must have stopped: handler invocations are bounded by the configured limit
	assert.LessOrEqual(t, handlerInvocations.Load(), maxFlowNodeCount,
		"the job handler must not keep being invoked after the guard tripped")
}

// TestLoopingProcessCompletesUnderMaxProcessInstanceFlowNodeCount verifies that a legitimate,
// bounded loop that stays under the configured limit completes without incidents.
func TestLoopingProcessCompletesUnderMaxProcessInstanceFlowNodeCount(t *testing.T) {
	const maxFlowNodeCount = int64(50)
	engine, store := startEngineWithMaxProcessInstanceFlowNodeCount(t, maxFlowNodeCount)

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

// TestNonPositiveMaxProcessInstanceFlowNodeCountDisablesLoopGuard verifies that limits <= 0 disable
// the flow node count guard entirely: a loop iterating more often than any small limit
// completes without incidents.
func TestNonPositiveMaxProcessInstanceFlowNodeCountDisablesLoopGuard(t *testing.T) {
	for _, maxFlowNodeCount := range []int64{0, -1} {
		t.Run(fmt.Sprintf("limit_%d", maxFlowNodeCount), func(t *testing.T) {
			engine, store := startEngineWithMaxProcessInstanceFlowNodeCount(t, maxFlowNodeCount)

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
			assert.Zero(t, flowNodeCountForInstance(store, instance.ProcessInstance().Key))
		})
	}
}

func TestProcessInstanceFlowNodeCountSurvivesEngineRecreation(t *testing.T) {
	store := inmemory.NewStorage()
	instance := &runtime.DefaultProcessInstance{ProcessInstanceData: runtime.ProcessInstanceData{Key: store.GenerateId()}}
	require.NoError(t, store.SaveProcessInstance(t.Context(), instance))
	token := runtime.ExecutionToken{
		Key:                store.GenerateId(),
		ElementInstanceKey: store.GenerateId(),
		ElementId:          "loop-element",
		ProcessInstanceKey: instance.ProcessInstance().Key,
		State:              runtime.TokenStateRunning,
	}

	firstEngine := NewEngine(EngineWithStorage(store), EngineWithMaxProcessInstanceFlowNodeCount(1))
	t.Cleanup(firstEngine.contextCancel)
	firstBatch, err := firstEngine.NewEngineBatchClean()
	require.NoError(t, err)
	require.NoError(t, firstEngine.validateAndIncrementFlowNodeCount(
		t.Context(), &firstBatch, instance, token, &flowNodeRunCount{},
	))
	require.NoError(t, firstBatch.Flush(t.Context()))

	secondEngine := NewEngine(EngineWithStorage(store), EngineWithMaxProcessInstanceFlowNodeCount(1))
	t.Cleanup(secondEngine.contextCancel)
	require.NoError(t, store.RefreshProcessInstance(t.Context(), instance))
	secondBatch, err := secondEngine.NewEngineBatchClean()
	require.NoError(t, err)
	err = secondEngine.validateAndIncrementFlowNodeCount(
		t.Context(), &secondBatch, instance, token, &flowNodeRunCount{},
	)
	require.ErrorIs(t, err, ErrMaxProcessInstanceFlowNodeCountExceeded)
	secondBatch.Clear(t.Context())

	count, err := store.GetFlowNodeCount(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "the rejected retry must not be persisted")
}

// TestResolvingProcessInstanceFlowNodeCountIncidentResetsCounterAndAllowsCompletion verifies that
// resolution grants a fresh execution budget by resetting the instance-wide counter, so the
// corrected loop can continue and complete.
func TestResolvingProcessInstanceFlowNodeCountIncidentResetsCounterAndAllowsCompletion(t *testing.T) {
	const maxFlowNodeCount = int64(6)
	engine, store := startEngineWithMaxProcessInstanceFlowNodeCount(t, maxFlowNodeCount)

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
	require.ErrorIs(t, err, ErrMaxProcessInstanceFlowNodeCountExceeded)
	require.NotNil(t, instance)

	incident := waitForFlowNodeCountIncident(t, engine, store, 10*time.Second)
	waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateFailed)
	assert.Equal(t, runtime.IncidentTypeMaxProcessInstanceFlowNodeCountExceeded, incident.Type)
	assert.Equal(t, maxFlowNodeCount, flowNodeCountForInstance(store, instance.ProcessInstance().Key))

	// operator intervention: let the loop exit and resolve the incident
	exitAllowed.Store(true)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))

	// resolution reset the counter, so the corrected traversal fits into a fresh budget:
	// the instance completes and the final counter reflects only post-resolution executions
	waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateCompleted)
	countAfterResolution := flowNodeCountForInstance(store, instance.ProcessInstance().Key)
	assert.Positive(t, countAfterResolution)
	assert.LessOrEqual(t, countAfterResolution, maxFlowNodeCount)
}

func TestResolvingFlowNodeCountIncidentReturnsSuccessWhenLoopCreatesReplacementIncident(t *testing.T) {
	const maxFlowNodeCount = int64(6)
	engine, store := startEngineWithMaxProcessInstanceFlowNodeCount(t, maxFlowNodeCount)
	process, err := engine.LoadFromFile(t.Context(), "./test-cases/simple-flow-loop.bpmn")
	require.NoError(t, err)
	handler := engine.NewTaskHandler().Type("loopJobType").Handler(func(job ActivatedJob) {
		job.SetOutputVariable("done", false)
		job.Complete()
	})
	defer engine.RemoveHandler(handler)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, map[string]any{"done": false})
	require.ErrorIs(t, err, ErrMaxProcessInstanceFlowNodeCountExceeded)
	require.NotNil(t, instance)
	original := waitForFlowNodeCountIncident(t, engine, store, 10*time.Second)

	require.NoError(t, engine.ResolveIncident(t.Context(), original.Key))
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, incidents, 2)
	var resolved, unresolved int
	for _, incident := range incidents {
		if incident.ResolvedAt == nil {
			unresolved++
		} else {
			resolved++
		}
	}
	assert.Equal(t, 1, resolved)
	assert.Equal(t, 1, unresolved)
	assert.Equal(t, maxFlowNodeCount, flowNodeCountForInstance(store, instance.ProcessInstance().Key))
	waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateFailed)
}

// TestParallelTokensCreateOneProcessInstanceFlowNodeCountIncidentAndResumeTogether verifies that a
// process-wide budget breach stops the current run after the first incident. Runnable sibling
// tokens remain persisted and are resumed together after the single incident resets the counter.
func TestParallelTokensCreateOneProcessInstanceFlowNodeCountIncidentAndResumeTogether(t *testing.T) {
	const maxFlowNodeCount = int64(4)
	engine, store := startEngineWithMaxProcessInstanceFlowNodeCount(t, maxFlowNodeCount)

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
	require.ErrorIs(t, err, ErrMaxProcessInstanceFlowNodeCountExceeded)
	require.NotNil(t, instance)

	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1, "one exhausted process-instance budget must create one incident")
	incident := incidents[0]
	assert.Equal(t, runtime.IncidentTypeMaxProcessInstanceFlowNodeCountExceeded, incident.Type)

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
	assert.LessOrEqual(t, flowNodeCountForInstance(store, instance.ProcessInstance().Key), maxFlowNodeCount)
}

func TestEngineRestartDoesNotResumeParallelTokensBlockedByFlowNodeCountIncident(t *testing.T) {
	const maxFlowNodeCount = int64(4)
	store := inmemory.NewStorage()
	firstEngine := NewEngine(
		EngineWithStorage(store),
		EngineWithMaxProcessInstanceFlowNodeCount(maxFlowNodeCount),
	)
	t.Cleanup(firstEngine.Stop)
	require.NoError(t, firstEngine.Start(t.Context()))
	registerParallelCompletionHandlers(t, &firstEngine)

	process, err := firstEngine.LoadFromFile(t.Context(), "./test-cases/parallel-gateway-flow.bpmn")
	require.NoError(t, err)
	instance, err := firstEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.ErrorIs(t, err, ErrMaxProcessInstanceFlowNodeCountExceeded)
	require.NotNil(t, instance)

	processInstanceKey := instance.ProcessInstance().Key
	incidentsBeforeRestart, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidentsBeforeRestart, 1)
	incident := incidentsBeforeRestart[0]
	runningTokenKeysBeforeRestart := runningTokenKeysForInstance(t, store, processInstanceKey)
	require.NotEmpty(t, runningTokenKeysBeforeRestart, "a runnable sibling must be preserved for incident resolution")
	assert.Equal(t, maxFlowNodeCount, flowNodeCountForInstance(store, processInstanceKey))
	waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateFailed)
	firstEngine.Stop()

	secondEngine := NewEngine(
		EngineWithStorage(store),
		EngineWithMaxProcessInstanceFlowNodeCount(maxFlowNodeCount),
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
	assert.Equal(t, maxFlowNodeCount, flowNodeCountForInstance(store, processInstanceKey))
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

// TestResolvingRegularIncidentResumesPersistedRunningSiblingTokensAfterRestart is a regression test
// for parallel tokens stranded after a restart: startup recovery skips Failed instances, so incident
// resolution must reschedule persisted Running sibling tokens for EVERY token-bound incident type,
// not only for flow-node-count incidents. It reproduces the crash-recovery state of a
// parallel instance that failed on a regular (unspecified) incident before its sibling token was
// drained, restarts on top of it, resolves the incident, and expects all branches to complete.
func TestResolvingRegularIncidentResumesPersistedRunningSiblingTokensAfterRestart(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.Stop)
	registerParallelCompletionHandlers(t, &engine)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/parallel-gateway-flow.bpmn")
	require.NoError(t, err)

	// crash-recovery state: instance Failed, incident token Failed, sibling token still Running
	instance := &runtime.DefaultProcessInstance{ProcessInstanceData: runtime.ProcessInstanceData{
		Definition:     process,
		Key:            store.GenerateId(),
		State:          runtime.ActivityStateFailed,
		CreatedAt:      time.Now(),
		VariableHolder: runtime.NewVariableHolder(nil, nil),
	}}
	require.NoError(t, store.SaveProcessInstance(t.Context(), instance))
	processInstanceKey := instance.ProcessInstance().Key
	failedToken := runtime.ExecutionToken{
		Key:                store.GenerateId(),
		ElementInstanceKey: store.GenerateId(),
		ElementId:          "id-b-1",
		ProcessInstanceKey: processInstanceKey,
		State:              runtime.TokenStateFailed,
	}
	runningSibling := runtime.ExecutionToken{
		Key:                store.GenerateId(),
		ElementInstanceKey: store.GenerateId(),
		ElementId:          "id-b-2",
		ProcessInstanceKey: processInstanceKey,
		State:              runtime.TokenStateRunning,
	}
	require.NoError(t, store.SaveToken(t.Context(), failedToken))
	require.NoError(t, store.SaveToken(t.Context(), runningSibling))
	incident := runtime.Incident{
		Key:                store.GenerateId(),
		ElementInstanceKey: failedToken.ElementInstanceKey,
		ElementId:          failedToken.ElementId,
		ProcessInstanceKey: processInstanceKey,
		Type:               runtime.IncidentTypeUnspecified,
		Message:            "regular incident recorded before the sibling token was processed",
		CreatedAt:          time.Now(),
		Token:              failedToken,
	}
	require.NoError(t, store.SaveIncident(t.Context(), incident))

	// startup recovery must skip the failed instance and keep the sibling token persisted
	require.NoError(t, engine.Start(t.Context()))
	restartedInstance, err := store.FindProcessInstanceByKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Equal(t, runtime.ActivityStateFailed, restartedInstance.ProcessInstance().State)
	require.ElementsMatch(t, []int64{runningSibling.Key}, runningTokenKeysForInstance(t, store, processInstanceKey))

	// resolving the regular incident must also resume the persisted Running sibling,
	// otherwise the parallel branch stays stranded and the instance never completes
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
	waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateCompleted)
}

// startEngineWithMaxProcessInstanceFlowNodeCount starts a dedicated engine backed by a fresh in-memory
// storage with the given maximum flow node count. The engine is stopped on test cleanup.
func startEngineWithMaxProcessInstanceFlowNodeCount(t *testing.T, maxFlowNodeCount int64, extraOptions ...EngineOption) (*Engine, *inmemory.Storage) {
	t.Helper()
	store := inmemory.NewStorage()
	options := append([]EngineOption{
		EngineWithStorage(store),
		EngineWithMaxProcessInstanceFlowNodeCount(maxFlowNodeCount),
	}, extraOptions...)
	engine := NewEngine(options...)
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	return &engine, store
}

// waitForFlowNodeCountIncident waits for and returns the first unresolved incident in the
// given store whose message reports a breach of the maximum flow node count.
func waitForFlowNodeCountIncident(t *testing.T, engine *Engine, store *inmemory.Storage, timeout time.Duration) runtime.Incident {
	t.Helper()
	var incident runtime.Incident
	require.Eventually(t, func() bool {
		for _, pi := range processInstancesSnapshot(t, engine, store) {
			incidents, findErr := store.FindIncidentsByProcessInstanceKey(t.Context(), pi.ProcessInstance().Key)
			if findErr != nil {
				continue
			}
			for _, candidate := range incidents {
				if candidate.ResolvedAt == nil && strings.Contains(candidate.Message, "maximum allowed process instance flow node count") {
					incident = candidate
					return true
				}
			}
		}
		return false
	}, timeout, 50*time.Millisecond, "expected a flow node count incident to be created")
	return incident
}

// flowNodeCountForInstance returns a snapshot of the persisted total flow node counter
// of the given process instance.
func flowNodeCountForInstance(store *inmemory.Storage, processInstanceKey int64) int64 {
	return store.Copy().FlowNodeCounts[processInstanceKey]
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
