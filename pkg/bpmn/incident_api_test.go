package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExclusiveGatewayWithExpressionsNoOutgoingCreatesIncident(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-with-condition.bpmn")
	assert.NoError(t, err)
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": 0,
	}

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.Error(t, err)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(incidents))
	assert.Empty(t, incidents[0].ResolvedAt)
	assert.Equal(t, runtime.TokenStateFailed, incidents[0].Token.State)

	instanceRes, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, instanceRes.ProcessInstance().State)

}

func TestExclusiveGatewayWithExpressionsNoOutgoingResolvesIncident(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-with-condition.bpmn")
	assert.NoError(t, err)
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": 0,
	}

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.Error(t, err)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(incidents))

	// now fix the variable
	pi := store.ProcessInstances[instance.ProcessInstance().Key]
	pi.ProcessInstance().VariableHolder.SetLocalVariable("price", 50)

	err = bpmnEngine.ResolveIncident(t.Context(), incidents[0].Key)
	assert.NoError(t, err)

	instanceRes, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instanceRes.ProcessInstance().State)

	incident, err := bpmnEngine.persistence.FindIncidentByKey(t.Context(), incidents[0].Key)
	assert.NoError(t, err)
	assert.NotEmpty(t, incident.ResolvedAt)

}

func TestResolveIncidentReturnsErrNotFound(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	var nonExistingKey int64 = -1
	err := bpmnEngine.ResolveIncident(t.Context(), nonExistingKey)

	assert.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrNotFound)
}

func TestResolveIncidentWithMissingTokenReturnsErrNotFound(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-with-condition.bpmn")
	require.NoError(t, err)
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{"price": 0})
	require.Error(t, err)
	require.NotNil(t, instance)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	require.NotZero(t, incidents[0].Token.Key)

	delete(store.ExecutionTokens, incidents[0].Token.Key)

	err = bpmnEngine.ResolveIncident(t.Context(), incidents[0].Key)
	assert.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrNotFound)

	incident, err := bpmnEngine.persistence.FindIncidentByKey(t.Context(), incidents[0].Key)
	require.NoError(t, err)
	assert.Nil(t, incident.ResolvedAt)
}

func TestExclusiveGatewayMissingVariableObserve(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-missing-variable-default.bpmn")
	assert.NoError(t, err)
	variables := map[string]interface{}{
		"b": 2,
	}

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.Error(t, err)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(incidents))

	for _, incident := range incidents {
		assert.Contains(t, []string{"incident_1", "incident_2"}, incident.ElementId)
	}

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, true)
	assert.NoError(t, err)
	for _, fe := range flowElements {
		if fe.ElementId == "default_end" {
			assert.Equal(t, runtime.ActivityStateCompleted, runtime.ActivityStateCompleted)
		}
	}
}
