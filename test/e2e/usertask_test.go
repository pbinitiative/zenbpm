package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestRestApiUsertask(t *testing.T) {
	var instance public.ProcessInstance
	var definition zenclient.ProcessDefinitionSimple
	err := deployDefinition(t, "usertask-assignee-mapping.bpmn")
	assert.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == "usertask-assignee-mapping-process" {
			definition = def
			break
		}
	}
	instance, err = createProcessInstance(t, definition.Key, map[string]any{
		"assignee": "foo",
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)

	jobs, err := getJobs(t, zenclient.GetJobsParams{
		ProcessInstanceKey: &instance.Key,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, jobs.Partitions)
	assert.NotEmpty(t, jobs.Partitions[0].Items)

	for _, job := range jobs.Partitions[0].Items {
		switch job.ElementId {
		case "user-task-dynamic":
			assert.Equal(t, "foo", ptr.Deref(job.Assignee, ""))
		case "user-task-static":
			assert.Equal(t, "static", ptr.Deref(job.Assignee, ""))
		}
	}

}
