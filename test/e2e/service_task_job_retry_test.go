package e2e

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServiceTaskRetriesBeforeIncident(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	require.Equal(t, new(int32(3)), job.Retries, "the job starts with the retries of its task definition")

	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("payment service unavailable")})
	assert.Equal(t, new(int32(2)), getJob(t, job.Key).Retries)
	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("payment service unavailable")})
	afterSecond := getJob(t, job.Key)
	assert.Equal(t, zenclient.JobStateActive, afterSecond.State)
	assert.Equal(t, new(int32(1)), afterSecond.Retries)
	assert.Equal(t, new(int32(2)), afterSecond.Attempts)
	assertProcessInstanceIncidentsLength(t, instance.Key, 0)

	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("payment service down for good")})

	failed := waitForProcessInstanceJobByElementId(t, instance.Key, "retried-task", public.JobStateFailed)
	assert.Equal(t, new(int32(0)), failed.Retries)
	incidents, err := getProcessInstanceIncidents(t, instance.Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	assert.Equal(t, &job.Key, incidents[0].JobKey, "the incident leads to its job")
	assert.Equal(t, fmt.Sprintf("payment service down for good (job %d: 3 attempts, retries exhausted)", job.Key), incidents[0].Message)
}

// TestAFailureNamingItsAttemptIsRecordedOnce shows a failure over REST which
// names its attempt spends it once: repeated after a timeout, or arriving late
// for an attempt which failed already, it is answered like the first and
// changes nothing, and an attempt never handed out is refused.
func TestAFailureNamingItsAttemptIsRecordedOnce(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	firstAttempt := zenclient.FailJobJSONRequestBody{Message: new("payment service unavailable"), Attempt: new(int32(1)), RetryBackoff: new("PT0S")}

	failJobWithRetryRequest(t, job.Key, firstAttempt)
	failJobWithRetryRequest(t, job.Key, firstAttempt)

	afterRepeat := getJob(t, job.Key)
	assert.Equal(t, new(int32(1)), afterRepeat.Attempts)
	assert.Equal(t, new(int32(2)), afterRepeat.Retries)

	response, err := app.restClient.FailJobWithResponse(t.Context(), job.Key, zenclient.FailJobJSONRequestBody{Attempt: new(int32(5))})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, response.StatusCode(), "body: %s", string(response.Body))
	require.NotNil(t, response.JSON400)
	assert.Contains(t, response.JSON400.Message, "attempt 5")
	assert.Equal(t, new(int32(1)), getJob(t, job.Key).Attempts)
	assertProcessInstanceIncidentsLength(t, instance.Key, 0)
}

func TestServiceTaskFailOverRestWithRetries(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")

	before := time.Now()
	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{
		Message:      new("java.net.ConnectException: refused"),
		Retries:      new(int32(5)),
		RetryBackoff: new("PT1H"),
	})

	inBackoff := getJob(t, job.Key)
	assert.Equal(t, zenclient.JobStateActive, inBackoff.State, "a job waiting out its backoff stays active")
	assert.Equal(t, new(int32(5)), inBackoff.Retries)
	assert.Equal(t, new("java.net.ConnectException: refused"), inBackoff.LastFailureMessage)
	require.NotNil(t, inBackoff.RetryAt)
	assert.WithinRange(t, *inBackoff.RetryAt, before.Add(time.Hour).Add(-time.Second), time.Now().Add(time.Hour).Add(time.Second))

	t.Run("negative retries are refused", func(t *testing.T) {
		response, err := app.restClient.FailJobWithResponse(t.Context(), job.Key, zenclient.FailJobJSONRequestBody{Retries: new(int32(-1))})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, response.StatusCode(), "body: %s", string(response.Body))
	})
	t.Run("a backoff which is no ISO-8601 duration is refused", func(t *testing.T) {
		response, err := app.restClient.FailJobWithResponse(t.Context(), job.Key, zenclient.FailJobJSONRequestBody{RetryBackoff: new("10 seconds")})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, response.StatusCode(), "body: %s", string(response.Body))
	})
	assert.Equal(t, new(int32(5)), getJob(t, job.Key).Retries, "a refused failure spends nothing")
}

func TestResolvedIncidentRestoresRetries(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("down"), Retries: new(int32(0))})
	waitForProcessInstanceJobByElementId(t, instance.Key, "retried-task", public.JobStateFailed)
	incidents, err := getProcessInstanceIncidents(t, instance.Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)

	resolveIncident(t, incidents[0].Key)

	resolved := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	assert.Equal(t, new(int32(3)), resolved.Retries)
	assert.Equal(t, new(int32(0)), resolved.Attempts)
	assert.Nil(t, resolved.RetryAt)
}

// TestResolvingAnIncidentWhoseRetriesNoLongerEvaluateAnswersTheWayOut shows a
// resolution which cannot evaluate the retries of the job it would leave
// waiting is refused with 409 and changes nothing, that its message names the
// retries endpoint, and that the resolution succeeds once the retries are set.
func TestResolvingAnIncidentWhoseRetriesNoLongerEvaluateAnswersTheWayOut(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/service_task/service_task_retries_expression.bpmn", map[string]any{"attemptsAllowed": 0})
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("down")})
	waitForProcessInstanceJobByElementId(t, instance.Key, "retried-task", public.JobStateFailed)
	incidents, err := getProcessInstanceIncidents(t, instance.Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	updated, err := app.restClient.UpdateProcessInstanceVariablesWithResponse(t.Context(), instance.Key, zenclient.UpdateProcessInstanceVariablesJSONRequestBody{
		Variables: map[string]any{"attemptsAllowed": "many"},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, updated.StatusCode(), "body: %s", string(updated.Body))

	refused, err := app.restClient.ResolveIncidentWithResponse(t.Context(), incidents[0].Key)

	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, refused.StatusCode(), "body: %s", string(refused.Body))
	require.NotNil(t, refused.JSON409)
	assert.Contains(t, refused.JSON409.Message, fmt.Sprintf("POST /v1/jobs/%d/retries", job.Key))
	assert.Equal(t, zenclient.JobStateFailed, getJob(t, job.Key).State, "the refused resolution changes nothing")
	stillOpen, err := getProcessInstanceIncidents(t, instance.Key)
	require.NoError(t, err)
	require.Len(t, stillOpen, 1)
	assert.Nil(t, stillOpen[0].ResolvedAt)

	retries, err := app.restClient.UpdateJobRetriesWithResponse(t.Context(), job.Key, zenclient.UpdateJobRetriesJSONRequestBody{Retries: 2})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, retries.StatusCode(), "body: %s", string(retries.Body))
	resolveIncident(t, incidents[0].Key)

	assert.Equal(t, new(int32(2)), waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task").Retries)
}

func TestFailWithErrorCodeStillRoutesTheBoundaryEvent(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/service_task/service_task_retries_error_boundary.bpmn", nil)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")

	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{ErrorCode: new("PAYMENT_DECLINED"), Message: new("declined")})

	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	handled := getJob(t, job.Key)
	assert.Equal(t, new(int32(3)), handled.Retries, "a BPMN error spends no retry")
	assert.Nil(t, handled.LastFailureMessage)
	assertProcessInstanceIncidentsLength(t, instance.Key, 0)
}

func TestJobFailuresAreListed(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("first"), RetryBackoff: new("PT1M")})
	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("second")})

	response, err := app.restClient.GetJobFailuresWithResponse(t.Context(), job.Key, &zenclient.GetJobFailuresParams{})

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode(), "body: %s", string(response.Body))
	page := response.JSON200
	require.Len(t, page.Items, 2)
	assert.Equal(t, 2, page.TotalCount)
	assert.Equal(t, int32(2), page.Items[0].Attempt, "newest first")
	assert.Equal(t, "second", page.Items[0].Message)
	assert.Nil(t, page.Items[0].RetryAt)
	assert.Equal(t, int32(1), page.Items[1].Attempt)
	assert.Equal(t, "first", page.Items[1].Message)
	require.NotNil(t, page.Items[1].RetryAt)
	assert.WithinDuration(t, page.Items[1].FailedAt.Add(time.Minute), *page.Items[1].RetryAt, time.Second)
	for _, failure := range page.Items {
		assert.Equal(t, job.Key, failure.JobKey)
		assert.Equal(t, instance.Key, failure.ProcessInstanceKey)
		assert.Nil(t, failure.IncidentKey)
	}

	t.Run("a page beyond the last still reports the total", func(t *testing.T) {
		response, err := app.restClient.GetJobFailuresWithResponse(t.Context(), job.Key, &zenclient.GetJobFailuresParams{
			Page: new(int32(3)),
			Size: new(int32(1)),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode(), "body: %s", string(response.Body))
		assert.Empty(t, response.JSON200.Items)
		assert.Equal(t, 2, response.JSON200.TotalCount)
	})
}

func TestUpdateRetriesMakesAJobInBackoffDeliverable(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("down"), RetryBackoff: new("PT1H")})
	require.NotNil(t, getJob(t, job.Key).RetryAt)

	response, err := app.restClient.UpdateJobRetriesWithResponse(t.Context(), job.Key, zenclient.UpdateJobRetriesJSONRequestBody{Retries: 4})

	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode(), "body: %s", string(response.Body))
	updated := getJob(t, job.Key)
	assert.Equal(t, new(int32(4)), updated.Retries)
	assert.Nil(t, updated.RetryAt, "without retryAt the job is deliverable at once")
}

func TestUpdateRetriesThenResolveKeepsTheUpdatedCount(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("down"), Retries: new(int32(0))})
	waitForProcessInstanceJobByElementId(t, instance.Key, "retried-task", public.JobStateFailed)

	response, err := app.restClient.UpdateJobRetriesWithResponse(t.Context(), job.Key, zenclient.UpdateJobRetriesJSONRequestBody{Retries: 7})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode(), "body: %s", string(response.Body))
	incidents, err := getProcessInstanceIncidents(t, instance.Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	require.Nil(t, incidents[0].ResolvedAt, "updating the retries leaves the incident to the operator")

	resolveIncident(t, incidents[0].Key)

	resolved := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	assert.Equal(t, new(int32(7)), resolved.Retries)
}

func TestUpdateRetriesWithADeadlineThenResolveKeepsTheDeadline(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	failJobWithRetryRequest(t, job.Key, zenclient.FailJobJSONRequestBody{Message: new("down"), Retries: new(int32(0))})
	waitForProcessInstanceJobByElementId(t, instance.Key, "retried-task", public.JobStateFailed)
	retryAt := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	response, err := app.restClient.UpdateJobRetriesWithResponse(t.Context(), job.Key, zenclient.UpdateJobRetriesJSONRequestBody{Retries: 2, RetryAt: &retryAt})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode(), "body: %s", string(response.Body))
	incidents, err := getProcessInstanceIncidents(t, instance.Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)

	resolveIncident(t, incidents[0].Key)

	resolved := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	assert.Equal(t, new(int32(2)), resolved.Retries)
	require.NotNil(t, resolved.RetryAt, "the operator's deadline survives the resolution")
	assert.True(t, retryAt.Equal(*resolved.RetryAt), "expected %s, got %s", retryAt, *resolved.RetryAt)
}

func TestJobEndpointsAnswerNotFoundForAKeyNamingNoJob(t *testing.T) {
	keys := map[string]int64{
		"no job in an existing partition":            int64(1) << int64(zenflake.StepBits),
		"partition 0, reserved for global resources": 42,
		"a partition the cluster does not have":      int64(1000) << int64(zenflake.StepBits),
	}
	for name, key := range keys {
		t.Run(name, func(t *testing.T) {
			failures, err := app.restClient.GetJobFailuresWithResponse(t.Context(), key, &zenclient.GetJobFailuresParams{})
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, failures.StatusCode(), "failures body: %s", string(failures.Body))

			update, err := app.restClient.UpdateJobRetriesWithResponse(t.Context(), key, zenclient.UpdateJobRetriesJSONRequestBody{Retries: 2})
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, update.StatusCode(), "update body: %s", string(update.Body))

			fail, err := app.restClient.FailJobWithResponse(t.Context(), key, zenclient.FailJobJSONRequestBody{})
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, fail.StatusCode(), "fail body: %s", string(fail.Body))

			complete, err := app.restClient.CompleteJobWithResponse(t.Context(), key, zenclient.CompleteJobJSONRequestBody{})
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, complete.StatusCode(), "complete body: %s", string(complete.Body))
		})
	}
}

func TestUpdateRetriesAnswersWhatItCannotDo(t *testing.T) {
	instance, _ := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")

	t.Run("below one is a bad request", func(t *testing.T) {
		response, err := app.restClient.UpdateJobRetriesWithResponse(t.Context(), job.Key, zenclient.UpdateJobRetriesJSONRequestBody{Retries: 0})
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, response.StatusCode(), "body: %s", string(response.Body))
	})
	t.Run("above jobs.maxRetries is a bad request", func(t *testing.T) {
		response, err := app.restClient.UpdateJobRetriesWithResponse(t.Context(), job.Key, zenclient.UpdateJobRetriesJSONRequestBody{Retries: 101})
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, response.StatusCode(), "body: %s", string(response.Body))
	})
	t.Run("a completed job is a conflict", func(t *testing.T) {
		require.NoError(t, completeJob(t, job.Key, nil))
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)

		response, err := app.restClient.UpdateJobRetriesWithResponse(t.Context(), job.Key, zenclient.UpdateJobRetriesJSONRequestBody{Retries: 2})
		require.NoError(t, err)
		assert.Equal(t, http.StatusConflict, response.StatusCode(), "body: %s", string(response.Body))
	})
}

// startRetryFixture deploys a fixture whose job type is charge-card under a
// process id and a job type of its own, so that no other test's worker picks
// its jobs, and starts an instance of it. It returns the instance and the job type.
func startRetryFixture(t *testing.T, fixture string, variables map[string]any) (zenclient.ProcessInstance, string) {
	t.Helper()
	content, err := os.ReadFile(filepath.Clean(fixture))
	require.NoError(t, err)
	unique := fmt.Sprintf("%s-%d", strings.TrimSuffix(filepath.Base(fixture), ".bpmn"), time.Now().UnixNano())
	processID, found := getStringInBetweenTwoString(string(content), "bpmn:process id=\"", "\"")
	require.True(t, found, "fixture %s names no process id", fixture)
	model := strings.ReplaceAll(string(content), `"`+processID+`"`, `"`+unique+`"`)
	jobType := unique + "-job"
	model = strings.ReplaceAll(model, `type="charge-card"`, `type="`+jobType+`"`)

	deployed := deployProcessDefinitionContent(t, []byte(model))
	require.NotNil(t, deployed.JSON201, "deployment answered %s", deployed.Status())
	instance, err := createProcessInstance(t, &deployed.JSON201.ProcessDefinitionKey, variables)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, instance.Key)
	})
	return instance, jobType
}

func failJobWithRetryRequest(t testing.TB, jobKey int64, body zenclient.FailJobJSONRequestBody) {
	t.Helper()
	response, err := app.restClient.FailJobWithResponse(t.Context(), jobKey, body)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode(), "unexpected fail job response: %s body: %s", response.Status(), string(response.Body))
}

func getJob(t testing.TB, jobKey int64) zenclient.Job {
	t.Helper()
	response, err := app.restClient.GetJobWithResponse(t.Context(), jobKey)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode(), "body: %s", string(response.Body))
	return *response.JSON200
}
