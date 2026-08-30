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

func TestNewEngineUsesDefaultMaxElementExecutionCount(t *testing.T) {
	engine := NewEngine()
	t.Cleanup(engine.contextCancel)

	assert.Equal(t, DefaultMaxElementExecutionCount, engine.maxElementExecutionCount)
}

// TestLoopingProcessInstanceStopsAtMaxElementExecutionCountAndCreatesIncident verifies that a
// sequence-flow loop without a reachable exit condition is stopped by the element execution
// guard: the engine fails the token, fails the instance, and raises an incident instead of
// looping forever.
func TestLoopingProcessInstanceStopsAtMaxElementExecutionCountAndCreatesIncident(t *testing.T) {
	const maxExecutionCount = int64(6)
	engine, store := startEngineWithMaxElementExecutionCount(t, maxExecutionCount)

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
	require.ErrorIs(t, err, ErrMaxElementExecutionCountExceeded)
	require.NotNil(t, instance)

	incident := waitForElementExecutionCountIncident(t, engine, store, 10*time.Second)
	assert.Contains(t, incident.Message, fmt.Sprintf("maximum allowed element execution count of %d", maxExecutionCount))
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

// TestLoopingProcessCompletesUnderMaxElementExecutionCount verifies that a legitimate,
// bounded loop that stays under the configured limit completes without incidents.
func TestLoopingProcessCompletesUnderMaxElementExecutionCount(t *testing.T) {
	const maxExecutionCount = int64(50)
	engine, store := startEngineWithMaxElementExecutionCount(t, maxExecutionCount)

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

// TestNonPositiveMaxElementExecutionCountDisablesLoopGuard verifies that limits <= 0 disable
// the element execution guard entirely: a loop iterating more often than any small limit
// completes without incidents.
func TestNonPositiveMaxElementExecutionCountDisablesLoopGuard(t *testing.T) {
	for _, maxExecutionCount := range []int64{0, -1} {
		t.Run(fmt.Sprintf("limit_%d", maxExecutionCount), func(t *testing.T) {
			engine, store := startEngineWithMaxElementExecutionCount(t, maxExecutionCount)

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

			// disabled guard must not persist any counters
			assert.Empty(t, elementExecutionCountersForInstance(store, instance.ProcessInstance().Key))
		})
	}
}

func TestElementExecutionCountSurvivesEngineRecreation(t *testing.T) {
	store := inmemory.NewStorage()
	instance := &runtime.DefaultProcessInstance{ProcessInstanceData: runtime.ProcessInstanceData{Key: store.GenerateId()}}
	token := runtime.ExecutionToken{
		Key:                store.GenerateId(),
		ElementInstanceKey: store.GenerateId(),
		ElementId:          "loop-element",
		ProcessInstanceKey: instance.ProcessInstance().Key,
		State:              runtime.TokenStateRunning,
	}

	firstEngine := NewEngine(EngineWithStorage(store), EngineWithMaxElementExecutionCount(1))
	t.Cleanup(firstEngine.contextCancel)
	firstBatch, err := firstEngine.NewEngineBatchClean()
	require.NoError(t, err)
	require.NoError(t, firstEngine.validateAndIncrementElementExecutionCount(
		t.Context(), &firstBatch, instance, token, make(map[string]int64),
	))
	require.NoError(t, firstBatch.Flush(t.Context()))

	secondEngine := NewEngine(EngineWithStorage(store), EngineWithMaxElementExecutionCount(1))
	t.Cleanup(secondEngine.contextCancel)
	secondBatch, err := secondEngine.NewEngineBatchClean()
	require.NoError(t, err)
	err = secondEngine.validateAndIncrementElementExecutionCount(
		t.Context(), &secondBatch, instance, token, make(map[string]int64),
	)
	require.ErrorIs(t, err, ErrMaxElementExecutionCountExceeded)
	secondBatch.Clear(t.Context())

	count, err := store.GetElementExecutionCount(t.Context(), instance.ProcessInstance().Key, token.ElementId)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "the rejected retry must not be persisted")
}

// TestResolvingElementExecutionCountIncidentAllowsSingleTraversalWithoutReset verifies that
// resolution permits one corrected traversal without resetting cumulative execution history.
func TestResolvingElementExecutionCountIncidentAllowsSingleTraversalWithoutReset(t *testing.T) {
	const maxExecutionCount = int64(6)
	engine, store := startEngineWithMaxElementExecutionCount(t, maxExecutionCount)

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
	require.ErrorIs(t, err, ErrMaxElementExecutionCountExceeded)
	require.NotNil(t, instance)

	incident := waitForElementExecutionCountIncident(t, engine, store, 10*time.Second)
	waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateFailed)
	assert.Equal(t, runtime.IncidentTypeMaxElementExecutionCountExceeded, incident.Type)
	countersBeforeResolution := elementExecutionCountersForInstance(store, instance.ProcessInstance().Key)
	assert.Equal(t, maxExecutionCount, countersBeforeResolution[incident.ElementId])

	const unrelatedElementID = "unrelated-element"
	require.NoError(t, store.IncrementElementExecutionCount(t.Context(), instance.ProcessInstance().Key, unrelatedElementID))
	require.NoError(t, store.IncrementElementExecutionCount(t.Context(), instance.ProcessInstance().Key, unrelatedElementID))

	// operator intervention: let the loop exit and resolve the incident
	exitAllowed.Store(true)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))

	// Traversed elements consume their one allowance and reach the original limit again. The
	// artificial counter demonstrates that history was decremented once, not reset.
	waitForProcessInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateCompleted)
	countersAfterResolution := elementExecutionCountersForInstance(store, instance.ProcessInstance().Key)
	assert.Equal(t, maxExecutionCount, countersAfterResolution[incident.ElementId])
	assert.Equal(t, int64(1), countersAfterResolution[unrelatedElementID])
}

// startEngineWithMaxElementExecutionCount starts a dedicated engine backed by a fresh in-memory
// storage with the given maximum element execution count. The engine is stopped on test cleanup.
func startEngineWithMaxElementExecutionCount(t *testing.T, maxExecutionCount int64, extraOptions ...EngineOption) (*Engine, *inmemory.Storage) {
	t.Helper()
	store := inmemory.NewStorage()
	options := append([]EngineOption{
		EngineWithStorage(store),
		EngineWithMaxElementExecutionCount(maxExecutionCount),
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
				if candidate.ResolvedAt == nil && strings.Contains(candidate.Message, "maximum allowed element execution count") {
					incident = candidate
					return true
				}
			}
		}
		return false
	}, timeout, 50*time.Millisecond, "expected an element execution count incident to be created")
	return incident
}

// elementExecutionCountersForInstance returns a snapshot of the persisted element execution
// counters of the given process instance.
func elementExecutionCountersForInstance(store *inmemory.Storage, processInstanceKey int64) map[string]int64 {
	counters := make(map[string]int64)
	for key, count := range store.Copy().ElementExecutionCounters {
		if key.ProcessInstanceKey == processInstanceKey {
			counters[key.ElementID] = count
		}
	}
	return counters
}
