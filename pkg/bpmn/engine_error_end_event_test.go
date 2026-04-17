package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubProcessEndErrorWithMatchingBoundaryIsCaught(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_end_event/sub_process/simple_sub_process_task_with_error_end_event_and_matching_error_boundary.bpmn")
	require.NoError(t, err)

	processInstance, zerr := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.Nil(t, zerr)

	assert.Eventually(t, func() bool {
		current, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), processInstance.ProcessInstance().Key)
		return err == nil && current.ProcessInstance().State == runtime.ActivityStateCompleted
	}, time.Second, 25*time.Millisecond)

	processInstance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, processInstance.ProcessInstance().State)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Len(t, incidents, 0)

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Contains(t, tokenElementIDs(tokens), "handled_end")
	assert.NotContains(t, tokenElementIDs(tokens), "Event_should_not_happen")

	subProcess := findSubProcessInstance(processInstance.ProcessInstance().Key)
	require.NotNil(t, subProcess)
	assert.Equal(t, runtime.ActivityStateTerminated, subProcess.ProcessInstance().State)
}

func TestSubProcessEndErrorWithCatchAllBoundaryIsCaught(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_end_event/sub_process/simple_sub_process_task_with_error_end_event_and_catch_all_error_boundary.bpmn")
	require.NoError(t, err)

	processInstance, zerr := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.Nil(t, zerr)

	assert.Eventually(t, func() bool {
		current, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), processInstance.ProcessInstance().Key)
		return err == nil && current.ProcessInstance().State == runtime.ActivityStateCompleted
	}, time.Second, 25*time.Millisecond)

	processInstance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, processInstance.ProcessInstance().State)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Len(t, incidents, 0)

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Contains(t, tokenElementIDs(tokens), "handled_end")
	assert.NotContains(t, tokenElementIDs(tokens), "Event_should_not_happen")

	subProcess := findSubProcessInstance(processInstance.ProcessInstance().Key)
	require.NotNil(t, subProcess)
	assert.Equal(t, runtime.ActivityStateTerminated, subProcess.ProcessInstance().State)
}

func TestSubProcessEndErrorWithoutMatchingBoundaryCreatesIncident(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_end_event/sub_process/simple_sub_process_task_with_error_end_event_and_nonmatching_error_boundary.bpmn")
	require.NoError(t, err)

	processInstance, zerr := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.Nil(t, zerr)

	assert.Eventually(t, func() bool {
		current, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), processInstance.ProcessInstance().Key)
		return err == nil && current.ProcessInstance().State == runtime.ActivityStateFailed
	}, time.Second, 25*time.Millisecond)

	processInstance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, processInstance.ProcessInstance().State)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	assert.Equal(t, "Subprocess", incidents[0].ElementId)

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.NotContains(t, tokenElementIDs(tokens), "handled_end")
	assert.NotContains(t, tokenElementIDs(tokens), "Event_should_not_happen")

	subProcess := findSubProcessInstance(processInstance.ProcessInstance().Key)
	require.NotNil(t, subProcess)
	assert.Equal(t, runtime.ActivityStateTerminated, subProcess.ProcessInstance().State)
}

func TestNestedCallActivityEndErrorBubblesToOuterBoundaryMatchingErrorCode(t *testing.T) {
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_end_event/call_activity/call_activity_nested_with_error_end_event_leaf.bpmn")
	require.NoError(t, err)
	_, err = bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_end_event/call_activity/call_activity_nested_with_error_end_event_middle.bpmn")
	require.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_end_event/call_activity/call_activity_nested_with_error_end_event_root_matching.bpmn")
	require.NoError(t, err)

	processInstance, zerr := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.Nil(t, zerr)

	assert.Eventually(t, func() bool {
		current, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), processInstance.ProcessInstance().Key)
		return err == nil && current.ProcessInstance().State == runtime.ActivityStateCompleted
	}, 3*time.Second, 25*time.Millisecond)

	processInstance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, processInstance.ProcessInstance().State)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Len(t, incidents, 0)

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), processInstance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Contains(t, tokenElementIDs(tokens), "root-handled-end")
	assert.NotContains(t, tokenElementIDs(tokens), "root-should-not-happen-end")

	middleInstance := findProcessInstanceByProcessIdAndParent(processInstance.ProcessInstance().Key, "nested_call_activity_error_end_event_middle")
	require.NotNil(t, middleInstance)
	assert.Equal(t, runtime.ActivityStateTerminated, middleInstance.ProcessInstance().State)

	leafInstance := findProcessInstanceByProcessIdAndParent(middleInstance.ProcessInstance().Key, "nested_call_activity_error_end_event_leaf")
	require.NotNil(t, leafInstance)
	assert.Equal(t, runtime.ActivityStateTerminated, leafInstance.ProcessInstance().State)
}

func findSubProcessInstance(parentProcessInstanceKey int64) runtime.ProcessInstance {
	for _, processInstance := range engineStorage.ProcessInstances {
		subProcess, ok := processInstance.(*runtime.SubProcessInstance)
		if !ok {
			continue
		}
		if subProcess.ParentProcessExecutionToken.ProcessInstanceKey == parentProcessInstanceKey {
			return processInstance
		}
	}
	return nil
}

func findProcessInstanceByProcessIdAndParent(parentProcessInstanceKey int64, processID string) runtime.ProcessInstance {
	for _, processInstance := range engineStorage.ProcessInstances {
		parentKey := processInstance.GetParentProcessInstanceKey()
		if parentKey == nil || *parentKey != parentProcessInstanceKey {
			continue
		}
		if processInstance.ProcessInstance().Definition.BpmnProcessId == processID {
			return processInstance
		}
	}
	return nil
}
