package bpmn

import (
	"testing"
	"time"

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

func TestResolveJobIncidentReevaluatesInputMappingsWithCurrentProcessVariables(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/incident-job-input-refresh.bpmn")
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{
		"sourceValue": "original",
		"staleValue":  "remove-before-retry",
	})
	require.NoError(t, err)

	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, "original", jobs[0].InputVariables["workerValue"])

	require.NoError(t, engine.JobFailByKey(t.Context(), jobs[0].Key, "expected incident", nil, nil))
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)

	_, err = engine.DeleteInstanceVariable(t.Context(), instance.ProcessInstance().Key, "staleValue")
	require.NoError(t, err)
	_, _, err = engine.ModifyInstance(t.Context(), instance.ProcessInstance().Key, nil, nil, map[string]interface{}{
		"sourceValue": "corrected",
	})
	require.NoError(t, err)
	require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))

	flowElementInstance, err := store.GetFlowElementInstanceByKey(t.Context(), jobs[0].ElementInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"sourceValue": "corrected",
	}, flowElementInstance.InputVariables)

	storedJob, err := store.FindJobByJobKey(t.Context(), jobs[0].Key)
	require.NoError(t, err)
	assert.Equal(t, jobs[0].Key, storedJob.Key)
	assert.Equal(t, map[string]interface{}{
		"sourceValue": "corrected",
		"workerValue": "corrected",
	}, storedJob.InputVariables)

	activatedJobs, err := engine.ActivateJobs(t.Context(), "incident-retry-job")
	require.NoError(t, err)
	require.Len(t, activatedJobs, 1)
	assert.Equal(t, jobs[0].Key, activatedJobs[0].Key())
	assert.Equal(t, "corrected", activatedJobs[0].Variable("workerValue"))

	require.NoError(t, engine.JobCompleteByKey(t.Context(), activatedJobs[0].Key(), nil))
	completedJob, err := store.FindJobByJobKey(t.Context(), activatedJobs[0].Key())
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, completedJob.State)
	resolvedIncident, err := store.FindIncidentByKey(t.Context(), incidents[0].Key)
	require.NoError(t, err)
	assert.NotNil(t, resolvedIncident.ResolvedAt)
	completedInstance, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, completedInstance.ProcessInstance().State)
}

func TestResolveMultiInstanceJobIncidentPreservesIterationVariable(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/multi_instance_service_task.bpmn")
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{
		"testInputCollection": []string{"first"},
		"currentProcessValue": "original",
	})
	require.NoError(t, err)

	var jobs []runtime.Job
	require.Eventually(t, func() bool {
		jobs, err = store.FindActiveJobsByType(t.Context(), "TestType")
		return err == nil && len(jobs) == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, "first", jobs[0].InputVariables["testElementInput"])

	require.NoError(t, engine.JobFailByKey(t.Context(), jobs[0].Key, "expected incident", nil, nil))
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), jobs[0].ProcessInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidents, 1)

	_, _, err = engine.ModifyInstance(t.Context(), jobs[0].ProcessInstanceKey, nil, nil, map[string]interface{}{
		"currentProcessValue": "corrected",
	})
	require.NoError(t, err)
	require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))

	flowElementInstance, err := store.GetFlowElementInstanceByKey(t.Context(), jobs[0].ElementInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, "corrected", flowElementInstance.InputVariables["currentProcessValue"])
	assert.Equal(t, "first", flowElementInstance.InputVariables["item"])
	assert.NotContains(t, flowElementInstance.InputVariables, "testElementInput")

	activatedJobs, err := engine.ActivateJobs(t.Context(), "TestType")
	require.NoError(t, err)
	require.Len(t, activatedJobs, 1)
	assert.Equal(t, "first", activatedJobs[0].Variable("testElementInput"))
	assert.Equal(t, "corrected", activatedJobs[0].Variable("currentProcessValue"))

	require.NoError(t, engine.JobCompleteByKey(t.Context(), activatedJobs[0].Key(), map[string]interface{}{
		"testJobOutput": "first-completed",
	}))
	require.Eventually(t, func() bool {
		completedRoot, rootErr := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		completedIteration, iterationErr := store.FindProcessInstanceByKey(t.Context(), jobs[0].ProcessInstanceKey)
		return rootErr == nil && iterationErr == nil &&
			completedRoot.ProcessInstance().State == runtime.ActivityStateCompleted &&
			completedIteration.ProcessInstance().State == runtime.ActivityStateCompleted
	}, time.Second, 10*time.Millisecond)
	completedJob, err := store.FindJobByJobKey(t.Context(), activatedJobs[0].Key())
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, completedJob.State)
	resolvedIncident, err := store.FindIncidentByKey(t.Context(), incidents[0].Key)
	require.NoError(t, err)
	assert.NotNil(t, resolvedIncident.ResolvedAt)
}

func TestResolveMultiInstanceJobIncidentReevaluatesMappingFromOriginalIterationVariable(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/incident-multi-instance-mapped-loop-variable.bpmn")
	require.NoError(t, err)
	_, err = engine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{
		"items": []string{"first"},
	})
	require.NoError(t, err)

	var jobs []runtime.Job
	require.Eventually(t, func() bool {
		jobs, err = store.FindActiveJobsByType(t.Context(), "mapped-loop-variable-retry-job")
		return err == nil && len(jobs) == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, "first-mapped", jobs[0].InputVariables["item"])

	require.NoError(t, engine.JobFailByKey(t.Context(), jobs[0].Key, "expected incident", nil, nil))
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), jobs[0].ProcessInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))

	activatedJobs, err := engine.ActivateJobs(t.Context(), "mapped-loop-variable-retry-job")
	require.NoError(t, err)
	require.Len(t, activatedJobs, 1)
	assert.Equal(t, "first-mapped", activatedJobs[0].Variable("item"))
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
