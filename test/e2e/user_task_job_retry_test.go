package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestUserTaskJobSpendsRetriesBeforeIncident(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/user_task/user_task_retries.bpmn", nil)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	assert.Equal(t, new(int32(2)), job.Retries)

	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("form service down")})
	assert.Equal(t, new(int32(1)), getJob(t, job.Key).Retries)
	assertProcessInstanceIncidentsLength(t, instance.Key, 0)

	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("form service down")})
	waitForProcessInstanceJobByElementId(t, instance.Key, "retried-task", public.JobStateFailed)
	assertProcessInstanceIncidentsLength(t, instance.Key, 1)
}
