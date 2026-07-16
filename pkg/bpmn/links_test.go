package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func TestLinkEventsAreThrownAndCaughtAndFlowContinued(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple-link-events.bpmn")
	assert.NoError(t, err)
	h := bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	// then
	assert.Nil(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
	assert.Equal(t, "Task-A,Task-B", cp.CallPath)

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, true)
	assert.NoError(t, err)
	byID := make(map[string]runtime.FlowElementInstance, len(flowElements))
	for _, flowElement := range flowElements {
		byID[flowElement.ElementId] = flowElement
	}
	for _, elementID := range []string{"Link-A-Throw", "Link-A-Catch", "Link-B-Throw", "Link-B-Catch"} {
		flowElement, found := byID[elementID]
		assert.True(t, found, "link event %s should be in history", elementID)
		assert.NotNil(t, flowElement.CompletedAt, "link event %s should have CompletedAt after execution passes it", elementID)
	}
}

func TestMissingIntermediateLinkCatchEventStopsEngineWithError(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple-link-event-broken.bpmn")
	assert.NoError(t, err)
	h := bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.Error(t, err)

	// then
	assert.ErrorContains(t, err, "failed to find link")
	assert.Equal(t, runtime.ActivityStateFailed, instance.ProcessInstance().State)
	assert.Equal(t, "", cp.CallPath)

	instanceDb, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, instanceDb.ProcessInstance().State)
}

func TestMissingIntermediateLinkVariablesMapped(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple-link-event-output-variables.bpmn")
	assert.NoError(t, err)
	h := bpmnEngine.NewTaskHandler().Type("task").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	assert.Nil(t, err)
	assert.Equal(t, instance.ProcessInstance().State, runtime.ActivityStateCompleted)
	assert.Equal(t, "Task", cp.CallPath)

	// then
	assert.NotNil(t, instance.ProcessInstance().GetVariable("throw"))
	assert.Equal(t, "throw", instance.ProcessInstance().GetVariable("throw").(string))
	// then
	assert.NotNil(t, instance.ProcessInstance().GetVariable("catch"))
	assert.Equal(t, "catch", instance.ProcessInstance().GetVariable("catch").(string))

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, true)
	assert.NoError(t, err)
	byID := make(map[string]runtime.FlowElementInstance, len(flowElements))
	for _, flowElement := range flowElements {
		byID[flowElement.ElementId] = flowElement
	}
	assert.Equal(t, map[string]any{"throw": "throw"}, byID["Link-Throw"].OutputVariables)
	assert.Equal(t, map[string]any{"catch": "catch"}, byID["Link-Catch"].OutputVariables)
}
