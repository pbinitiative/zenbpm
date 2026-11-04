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

func TestManualTaskProcessCanBeDeployed(t *testing.T) {
	engine, _ := newManualTaskTestEngine(t)

	definition := deployManualTaskProcess(t, engine, "manual-task.bpmn")

	assert.Equal(t, "manual-task-process", definition.BpmnProcessId)
	manualTask := definition.Definitions.Process.GetFlowNodeById("manual-task")
	require.NotNil(t, manualTask)
	assert.Equal(t, bpmn20.ElementTypeManualTask, manualTask.GetType())
}

func TestManualTaskCompletesWithoutJobAndProcessContinues(t *testing.T) {
	engine, store := newManualTaskTestEngine(t)
	definition := deployManualTaskProcess(t, engine, "manual-task.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	processInstanceKey := instance.ProcessInstance().Key

	manualTask := requireFlowElementInstance(t, engine, processInstanceKey, "manual-task")

	// User tasks are jobs as well, so no job of the manual task in any state proves
	// it created neither. The token continues to the service task, so its job is
	// the only one the token has.
	tokenJobs, err := engine.persistence.GetJobsInStateByTokenKey(t.Context(), manualTask.ExecutionTokenKey, allActivityStates())
	require.NoError(t, err)
	require.Len(t, tokenJobs, 1, "only the service task after the manual task creates a job")
	assert.Equal(t, "service-task", tokenJobs[0].ElementId)

	jobs, err := engine.persistence.FindPendingProcessInstanceJobs(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	assert.Equal(t, string(bpmn20.ElementTypeManualTask), manualTask.ElementType)
	assert.False(t, manualTask.CreatedAt.IsZero(), "the manual task is activated")
	assert.NotNil(t, manualTask.CompletedAt, "the manual task completes without external interaction")
	outgoingFlow := requireFlowElementInstance(t, engine, processInstanceKey, "flow-manual-task-to-service-task")
	assert.NotNil(t, outgoingFlow.CompletedAt, "the outgoing sequence flow is taken")

	require.NoError(t, engine.JobCompleteByKey(t.Context(), jobs[0].Key, nil))

	waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateCompleted)
}

func TestManualTaskAppliesInputAndOutputMappings(t *testing.T) {
	engine, store := newManualTaskTestEngine(t)
	definition := deployManualTaskProcess(t, engine, "manual-task-io-mapping.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"customer": "Alice"})
	require.NoError(t, err)
	processInstanceKey := instance.ProcessInstance().Key

	waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateCompleted)
	storedInstance, err := store.FindProcessInstanceByKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	processVariables := storedInstance.ProcessInstance().VariableHolder.LocalVariables()
	assert.Equal(t, "signed by Alice", processVariables["signature"])
	assert.NotContains(t, processVariables, "signatory", "an input mapping stays in the manual task's local scope")

	manualTask := requireFlowElementInstance(t, engine, processInstanceKey, "manual-task")
	assert.Equal(t, "Alice", manualTask.InputVariables["customer"])
	assert.Equal(t, map[string]any{"signature": "signed by Alice"}, manualTask.OutputVariables)
}

func TestMultiInstanceManualTaskCompletesEveryIteration(t *testing.T) {
	testCases := []struct {
		name string
		file string
		// Only sequential iterations keep the order of the input collection.
		ordered bool
	}{
		{name: "sequential", file: "manual-task-multi-instance-sequential.bpmn", ordered: true},
		{name: "parallel", file: "manual-task-multi-instance-parallel.bpmn", ordered: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			engine, store := newManualTaskTestEngine(t)
			definition := deployManualTaskProcess(t, engine, testCase.file)

			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{
				"contracts": []any{"first", "second", "third"},
			})
			require.NoError(t, err)
			processInstanceKey := instance.ProcessInstance().Key

			waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateCompleted)
			storedInstance, err := store.FindProcessInstanceByKey(t.Context(), processInstanceKey)
			require.NoError(t, err)
			expected := []any{"first-signed", "second-signed", "third-signed"}
			actual := storedInstance.ProcessInstance().VariableHolder.GetLocalVariable("signedContracts")
			if testCase.ordered {
				assert.Equal(t, expected, actual, "every iteration contributes its output element in collection order")
			} else {
				assert.ElementsMatch(t, expected, actual, "every iteration contributes its output element")
			}

			manualTask := requireFlowElementInstance(t, engine, processInstanceKey, "manual-task")
			assert.NotNil(t, manualTask.CompletedAt)

			iterations := requireMultiInstanceIterations(t, engine, store, processInstanceKey, "manual-task")
			require.Len(t, iterations, 3, "every element of the collection runs one iteration")
			for _, iteration := range iterations {
				assert.NotNil(t, iteration.CompletedAt, "iteration %d is completed in history", iteration.Key)
			}
		})
	}
}

func TestMessageBoundaryEventInterruptsWaitingMultiInstanceManualTask(t *testing.T) {
	engine, store := newManualTaskTestEngine(t)
	definition := deployManualTaskProcess(t, engine, "manual-task-multi-instance-message-boundary.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{
		"contracts": []any{"first", "second"},
	})
	require.NoError(t, err)
	processInstanceKey := instance.ProcessInstance().Key

	require.Eventually(t, func() bool {
		tokens, err := store.GetAllTokensForProcessInstance(t.Context(), processInstanceKey)
		if err != nil || len(tokens) != 1 || tokens[0].State != runtime.TokenStateWaiting {
			return false
		}
		children, err := store.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
		return err == nil && len(children) == 1 && children[0].ProcessInstance().State == runtime.ActivityStateFailed
	}, 5*time.Second, 20*time.Millisecond, "the parent token waits at the manual task after its iterations fail")

	correlationKey := "manual-task-boundary"
	require.NoError(t, engine.PublishMessageByName(t.Context(), "manual-task-boundary-message", &correlationKey, nil))

	waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateCompleted)
	boundaryEvent := requireFlowElementInstance(t, engine, processInstanceKey, "boundary-message")
	assert.NotNil(t, boundaryEvent.CompletedAt)
	endEvent := requireFlowElementInstance(t, engine, processInstanceKey, "end-after-message")
	assert.NotNil(t, endEvent.CompletedAt, "the process continues along the boundary event's outgoing flow")
}

func allActivityStates() []runtime.ActivityState {
	states := make([]runtime.ActivityState, 0, runtime.ActivityStateWithdrawn)
	for state := runtime.ActivityStateActive; state <= runtime.ActivityStateWithdrawn; state++ {
		states = append(states, state)
	}
	return states
}

func requireMultiInstanceIterations(t *testing.T, engine *Engine, store *inmemory.Storage, processInstanceKey int64, elementID string) []runtime.FlowElementInstance {
	t.Helper()
	tokens, err := store.GetAllTokensForProcessInstance(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, tokens, 1)
	children, err := store.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	require.NoError(t, err)
	require.Len(t, children, 1, "the iterations run in one multi-instance child instance")

	flowElementInstances, err := engine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), children[0].ProcessInstance().Key, true)
	require.NoError(t, err)
	iterations := make([]runtime.FlowElementInstance, 0, len(flowElementInstances))
	for _, flowElementInstance := range flowElementInstances {
		if flowElementInstance.ElementId == elementID {
			iterations = append(iterations, flowElementInstance)
		}
	}
	return iterations
}

func TestManualTaskInSubProcessCompletes(t *testing.T) {
	engine, store := newManualTaskTestEngine(t)
	definition := deployManualTaskProcess(t, engine, "manual-task-in-sub-process.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	processInstanceKey := instance.ProcessInstance().Key

	waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateCompleted)
	tokens, err := store.GetAllTokensForProcessInstance(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, tokens, 1)
	subProcesses, err := store.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	require.NoError(t, err)
	require.Len(t, subProcesses, 1)

	manualTask := requireFlowElementInstance(t, engine, subProcesses[0].ProcessInstance().Key, "manual-task")
	assert.NotNil(t, manualTask.CompletedAt)
}

// newManualTaskTestEngine returns an engine with its own storage, so a test sees
// only the jobs and history of its own process instances.
func newManualTaskTestEngine(t *testing.T) (*Engine, *inmemory.Storage) {
	t.Helper()
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.Stop)
	return &engine, store
}

func deployManualTaskProcess(t *testing.T, engine *Engine, fileName string) *runtime.ProcessDefinition {
	t.Helper()
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/manual_task/"+fileName)
	require.NoError(t, err)
	return definition
}

func requireFlowElementInstance(t *testing.T, engine *Engine, processInstanceKey int64, elementID string) runtime.FlowElementInstance {
	t.Helper()
	flowElementInstances, err := engine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), processInstanceKey, true)
	require.NoError(t, err)
	for _, flowElementInstance := range flowElementInstances {
		if flowElementInstance.ElementId == elementID {
			return flowElementInstance
		}
	}
	require.Failf(t, "flow element instance not found", "element %q is not in the history of process instance %d", elementID, processInstanceKey)
	return runtime.FlowElementInstance{}
}
