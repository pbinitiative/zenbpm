package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobFailWithMatchingErrorBoundaryIsCaught(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile("./test-cases/service_task_with_error_boundary_event.bpmn")
	require.NoError(t, err)

	pi, zerr := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.Nil(t, zerr)

	job := findJobForProcessInstance(pi.ProcessInstance().Key, "service-task-error-boundary")
	require.NotZero(t, job.Key, "expected to find service-task-error-boundary job created for process instance")

	errorCode := "42"
	err = bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected boundary error", &errorCode, nil)
	assert.NoError(t, err)

	pi, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, pi.ProcessInstance().State)

	job, err = bpmnEngine.persistence.FindJobByJobKey(t.Context(), job.Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, job.State)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), pi.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Len(t, incidents, 0)

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), pi.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Contains(t, tokenElementIDs(tokens), "handled-end")
	assert.NotContains(t, tokenElementIDs(tokens), "should-not-happen-end")
}

func TestJobFailWithMismatchingErrorBoundaryCreatesIncident(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile("./test-cases/service_task_with_error_boundary_event.bpmn")
	require.NoError(t, err)

	pi, zerr := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.Nil(t, zerr)

	job := findJobForProcessInstance(pi.ProcessInstance().Key, "service-task-error-boundary")
	require.NotZero(t, job.Key, "expected to find service-task-error-boundary job created for process instance")

	errorCode := "99"
	err = bpmnEngine.JobFailByKey(t.Context(), job.Key, "expected incident", &errorCode, nil)
	assert.NoError(t, err)

	pi, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, pi.ProcessInstance().State)

	job, err = bpmnEngine.persistence.FindJobByJobKey(t.Context(), job.Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, job.State)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), pi.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Len(t, incidents, 1)

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), pi.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotContains(t, tokenElementIDs(tokens), "handled-end")
	assert.NotContains(t, tokenElementIDs(tokens), "should-not-happen-end")
}

func TestJobFailWithoutErrorCodeCreatesIncident(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile("./test-cases/service_task_with_error_boundary_event.bpmn")
	require.NoError(t, err)

	pi, zerr := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.Nil(t, zerr)

	job := findJobForProcessInstance(pi.ProcessInstance().Key, "service-task-error-boundary")
	require.NotZero(t, job.Key, "expected to find service-task-error-boundary job created for process instance")

	err = bpmnEngine.JobFailByKey(t.Context(), job.Key, "missing error code", nil, nil)
	assert.NoError(t, err)

	pi, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, pi.ProcessInstance().State)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), pi.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Len(t, incidents, 1)
}

func findJobForProcessInstance(processInstanceKey int64, elementID string) runtime.Job {
	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == processInstanceKey && job.ElementId == elementID {
			return job
		}
	}
	return runtime.Job{}
}

func tokenElementIDs(tokens []runtime.ExecutionToken) []string {
	ids := make([]string, 0, len(tokens))
	for _, token := range tokens {
		ids = append(ids, token.ElementId)
	}
	return ids
}
