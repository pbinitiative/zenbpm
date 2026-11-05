package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManualTaskEdgeCases(t *testing.T) {
	t.Run("empty sequential collection completes without running an iteration", func(t *testing.T) {
		engine, store := newManualTaskTestEngine(t)
		definition := deployManualTaskProcess(t, engine, "manual-task-multi-instance-sequential.bpmn")

		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{
			"contracts": []any{},
		})
		require.NoError(t, err)

		requireEmptyManualTaskCollectionCompleted(t, engine, store, instance.ProcessInstance().Key)
	})

	t.Run("empty parallel collection completes without running an iteration", func(t *testing.T) {
		engine, store := newManualTaskTestEngine(t)
		definition := deployManualTaskProcess(t, engine, "manual-task-multi-instance-parallel.bpmn")

		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{
			"contracts": []any{},
		})
		require.NoError(t, err)

		requireEmptyManualTaskCollectionCompleted(t, engine, store, instance.ProcessInstance().Key)
	})

	t.Run("invalid input expression creates an incident without propagating outputs", func(t *testing.T) {
		engine, store := newManualTaskTestEngine(t)
		definition := deployManualTaskProcess(t, engine, "manual-task-io-mapping.bpmn")
		manualTask, ok := definition.Definitions.Process.GetFlowNodeById("manual-task").(*bpmn20.TManualTask)
		require.True(t, ok)
		// Invalid FEEL syntax deliberately fails before the output mapping can run.
		manualTask.Input = []extensions.TIoMapping{{Source: `=customer: "invalid"`, Target: "signatory"}}
		require.NoError(t, store.SaveProcessDefinition(t.Context(), *definition))

		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"customer": "Alice"})
		require.ErrorContains(t, err, "failed to evaluate input mappings of manual task manual-task")
		require.NotNil(t, instance)

		requireManualTaskMappingIncident(t, engine, store, instance.ProcessInstance().Key, "input mappings")
	})

	t.Run("invalid second output expression does not propagate the first mapped output", func(t *testing.T) {
		engine, store := newManualTaskTestEngine(t)
		definition := deployManualTaskProcess(t, engine, "manual-task-io-mapping.bpmn")
		manualTask, ok := definition.Definitions.Process.GetFlowNodeById("manual-task").(*bpmn20.TManualTask)
		require.True(t, ok)
		manualTask.Output = []extensions.TIoMapping{
			{Source: `="signed by " + signatory`, Target: "signature"},
			// Fail after evaluating the first output to exercise all-or-nothing propagation.
			{Source: `=signatory: "invalid"`, Target: "secondSignature"},
		}
		require.NoError(t, store.SaveProcessDefinition(t.Context(), *definition))

		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"customer": "Alice"})
		require.ErrorContains(t, err, "failed to evaluate output mappings of manual task manual-task")
		require.NotNil(t, instance)

		requireManualTaskMappingIncident(t, engine, store, instance.ProcessInstance().Key, "output mappings")
	})
}

func requireEmptyManualTaskCollectionCompleted(t *testing.T, engine *Engine, store *inmemory.Storage, instanceKey int64) {
	t.Helper()
	stored, err := store.FindProcessInstanceByKey(t.Context(), instanceKey)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, stored.ProcessInstance().State)
	assert.Equal(t, map[string]any{"contracts": []any{}, "signedContracts": []any{}}, stored.ProcessInstance().VariableHolder.LocalVariables())

	manualTask := requireFlowElementInstance(t, engine, instanceKey, "manual-task")
	assert.Equal(t, string(bpmn20.ElementTypeManualTask), manualTask.ElementType)
	assert.NotNil(t, manualTask.CompletedAt, "the parent activity history must complete even without a child instance")
	assert.Equal(t, map[string]any{"signedContracts": []any{}}, manualTask.OutputVariables)
	end := requireFlowElementInstance(t, engine, instanceKey, "end")
	assert.NotNil(t, end.CompletedAt)

	tokens, err := store.GetAllTokensForProcessInstance(t.Context(), instanceKey)
	require.NoError(t, err)
	require.Len(t, tokens, 1)
	assert.Equal(t, runtime.TokenStateCompleted, tokens[0].State)
	children, err := store.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	require.NoError(t, err)
	assert.Empty(t, children, "an empty input collection must not create a child or any iterations")
	jobs, err := store.GetJobsInStateByTokenKey(t.Context(), tokens[0].Key, allActivityStates())
	require.NoError(t, err)
	assert.Empty(t, jobs)
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instanceKey)
	require.NoError(t, err)
	assert.Empty(t, incidents)
}

func requireManualTaskMappingIncident(t *testing.T, engine *Engine, store *inmemory.Storage, instanceKey int64, mappingStage string) {
	t.Helper()
	stored, err := store.FindProcessInstanceByKey(t.Context(), instanceKey)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, stored.ProcessInstance().State)
	assert.Equal(t, map[string]any{"customer": "Alice"}, stored.ProcessInstance().VariableHolder.LocalVariables(), "neither mapped locals nor partial outputs may leak to process variables")

	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instanceKey)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	assert.Equal(t, "manual-task", incidents[0].ElementId)
	assert.Contains(t, incidents[0].Message, "failed to evaluate "+mappingStage)
	assert.Nil(t, incidents[0].ResolvedAt)

	tokens, err := store.GetAllTokensForProcessInstance(t.Context(), instanceKey)
	require.NoError(t, err)
	require.Len(t, tokens, 1)
	assert.Equal(t, "manual-task", tokens[0].ElementId)
	assert.Equal(t, runtime.TokenStateFailed, tokens[0].State)
	jobs, err := store.GetJobsInStateByTokenKey(t.Context(), tokens[0].Key, allActivityStates())
	require.NoError(t, err)
	assert.Empty(t, jobs)

	history, err := engine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instanceKey, true)
	require.NoError(t, err)
	for _, step := range history {
		assert.NotEqual(t, "end", step.ElementId, "failed mappings must not continue along the normal flow")
		if step.ElementId == "manual-task" {
			assert.Nil(t, step.CompletedAt)
			assert.Empty(t, step.OutputVariables, "partial output mappings must not appear in history")
		}
	}
}
