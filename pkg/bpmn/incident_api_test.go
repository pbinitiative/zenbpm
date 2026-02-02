package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestExclusiveGatewayWithExpressionsNoOutgoingCreatesIncident(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/exclusive-gateway-with-condition.bpmn")
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": 0,
	}

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.Error(t, err)

	// then
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
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/exclusive-gateway-with-condition.bpmn")
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": 0,
	}

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.Error(t, err)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(incidents))

	// now fix the variable
	pi := store.ProcessInstances[instance.ProcessInstance().Key]
	pi.ProcessInstance().VariableHolder.SetLocalVariable("price", 50)

	// then
	err = bpmnEngine.ResolveIncident(t.Context(), incidents[0].Key)
	assert.NoError(t, err)

	instanceRes, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instanceRes.ProcessInstance().State)

	incident, err := bpmnEngine.persistence.FindIncidentByKey(t.Context(), incidents[0].Key)
	assert.NoError(t, err)
	assert.NotEmpty(t, incident.ResolvedAt)

}
