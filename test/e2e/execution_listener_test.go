package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestStartListenerJobsReachWorkersOverTheJobStream(t *testing.T) {
	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	worker := zenclient.NewGrpc(conn)
	register := func(jobType string, handle func(job *proto.WaitingJob)) {
		_, err := worker.RegisterWorker(t.Context(), jobType, func(_ context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			handle(job)
			return nil, nil
		}, jobType)
		require.NoError(t, err)
	}
	process := deployListenerProcess(t)
	var startJobs, workJobs atomic.Int32
	register(process.startJobType, func(job *proto.WaitingJob) {
		assert.Equal(t, "end", job.GetListenerEventType())
		assert.Equal(t, "START_EVENT", job.GetElementType())
		startJobs.Add(1)
	})
	register(process.workJobType, func(job *proto.WaitingJob) {
		assert.Nil(t, job.ListenerEventType, "an ordinary job carries no listener event type")
		workJobs.Add(1)
	})

	instance, err := createProcessInstance(t, &process.definitionKey, nil)
	require.NoError(t, err)

	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	assert.Equal(t, int32(1), startJobs.Load())
	assert.Equal(t, int32(1), workJobs.Load())
}

func TestStartListenerHoldsAnInstanceCreatedOverRest(t *testing.T) {
	process := deployListenerProcess(t)

	instance, err := createProcessInstance(t, &process.definitionKey, map[string]any{"order": "o-2"})
	require.NoError(t, err)
	t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

	assert.Equal(t, zenclient.ProcessInstanceStateActive, instance.State)
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "listener-start")
	assert.Equal(t, process.startJobType, job.Type)
	assert.Equal(t, "START_EVENT", job.ElementType)
	require.NotNil(t, job.ListenerEventType)
	assert.Equal(t, public.JobListenerEventTypeEnd, *job.ListenerEventType)
	assert.Equal(t, "o-2", job.InputVariables["order"])

	refused, err := app.restClient.FailJobWithResponse(t.Context(), job.Key, zenclient.FailJobJSONRequestBody{ErrorCode: new("listener-error")})
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, refused.StatusCode(), "a listener job cannot throw a BPMN error: %s", string(refused.Body))

	response, err := app.restClient.CompleteJobWithResponse(t.Context(), job.Key, zenclient.CompleteJobJSONRequestBody{})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, response.StatusCode(), string(response.Body))

	work := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "listener-work")
	assert.Nil(t, work.ListenerEventType, "absent for the job of a task")
}

func TestATaskRunsItsListenersAroundItsJobOverRest(t *testing.T) {
	task := deployListenerTask(t)
	instance, err := createProcessInstance(t, &task.definitionKey, map[string]any{"order": "o-3"})
	require.NoError(t, err)
	t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

	before := waitForActiveJobOfType(t, instance.Key, task.beforeJobType)
	assert.Equal(t, "listened-task", before.ElementId)
	assert.Equal(t, "SERVICE_TASK", before.ElementType, "a listener's job carries the type of its element")
	require.NotNil(t, before.ListenerEventType)
	assert.Equal(t, public.JobListenerEventTypeStart, *before.ListenerEventType)
	assert.Equal(t, "o-3", before.InputVariables["order"])
	taskHeaders := &map[string]string{"carrier": "post"}
	assert.Equal(t, taskHeaders, before.TaskHeaders, "a listener's job carries the static headers of its element")
	completeListenerTaskJob(t, before.Key, map[string]any{"reserved": "r-1"})

	work := waitForActiveJobOfType(t, instance.Key, task.workJobType)
	assert.Nil(t, work.ListenerEventType, "absent for the task's own job")
	assert.Equal(t, before.ElementInstanceKey, work.ElementInstanceKey, "the token stayed in the task")
	assert.Equal(t, "r-1", work.InputVariables["reserved"], "the task's job sees what its start listener was completed with")
	assert.Equal(t, taskHeaders, work.TaskHeaders)
	completeListenerTaskJob(t, work.Key, map[string]any{"shipped": true})

	after := waitForActiveJobOfType(t, instance.Key, task.afterJobType)
	require.NotNil(t, after.ListenerEventType)
	assert.Equal(t, public.JobListenerEventTypeEnd, *after.ListenerEventType)
	assert.Equal(t, taskHeaders, after.TaskHeaders)
	assert.Equal(t, "r-1", after.InputVariables["reservedBefore"], "the output mapping wrote the start listener's variable to the instance")
	assert.Equal(t, true, after.InputVariables["shipped"], "the end listener sees what the task's job was completed with")
	listened := getFlowElementByElementId(t, instance.Key, "listened-task")
	assert.Nil(t, listened.CompletedAt, "the task reads active while its end listener runs")
	completeListenerTaskJob(t, after.Key, map[string]any{"notified": true})

	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	completed, err := getProcessInstance(t, instance.Key)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"order": "o-3", "reservedBefore": "r-1"}, completed.Variables,
		"what the jobs of the task were completed with reaches the instance through its output mappings only")
}

func TestTheListenersOfATaskReachWorkersOverTheJobStream(t *testing.T) {
	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	worker := zenclient.NewGrpc(conn)
	task := deployListenerTask(t)
	var order atomic.Value
	order.Store("")
	register := func(jobType string, listenerEventType string) {
		_, err := worker.RegisterWorker(t.Context(), jobType, func(_ context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			assert.Equal(t, listenerEventType, job.GetListenerEventType())
			assert.Equal(t, "SERVICE_TASK", job.GetElementType())
			assert.Equal(t, map[string]string{"carrier": "post"}, job.GetHeaders(), "every job of the task carries its static headers")
			order.Store(order.Load().(string) + " " + listenerEventType + "/" + job.GetElementId())
			return map[string]any{"reserved": "r-2"}, nil
		}, jobType)
		require.NoError(t, err)
	}
	register(task.beforeJobType, "start")
	register(task.workJobType, "")
	register(task.afterJobType, "end")

	instance, err := createProcessInstance(t, &task.definitionKey, nil)
	require.NoError(t, err)

	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	assert.Equal(t, " start/listened-task /listened-task end/listened-task", order.Load())
}

func TestMessageStartListenerAnswersThePublisherBeforeItsJobIsCompleted(t *testing.T) {
	suffix := fmt.Sprintf("%d", rand.Int63())
	messageName := "listener-start-message-" + suffix
	jobType := "listener-message-start-job-" + suffix
	deployListenerFixture(t, "listener-message-start.bpmn", map[string]string{
		`id="listener-message-start"`:       `id="listener-message-start-` + suffix + `"`,
		`name="listener-start-message"`:     `name="` + messageName + `"`,
		`type="listener-message-start-job"`: `type="` + jobType + `"`,
	})

	response, err := app.restClient.PublishMessageWithResponse(t.Context(), zenclient.PublishMessageJSONRequestBody{
		MessageName: messageName,
		Variables:   &map[string]any{"orderId": "order-7"},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, response.StatusCode(), string(response.Body))

	job := waitForActiveJobByType(t, jobType)
	require.NotNil(t, job.ListenerEventType)
	assert.Equal(t, zenclient.JobListenerEventTypeEnd, *job.ListenerEventType)
	assert.Equal(t, "order-7", job.InputVariables["orderFromMessage"])
	instance, err := getProcessInstance(t, job.ProcessInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, zenclient.ProcessInstanceStateActive, instance.State)
	waiting := getFlowElementByElementId(t, job.ProcessInstanceKey, "listener-message-start-event")
	assert.Nil(t, waiting.CompletedAt, "the start event stays active while its listener's job waits")
	assert.Equal(t, "order-7", waiting.OutputVariables["orderFromMessage"], "the history shows the start event's outputs while the job waits")

	completion, err := app.restClient.CompleteJobWithResponse(t.Context(), job.Key, zenclient.CompleteJobJSONRequestBody{})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, completion.StatusCode(), string(completion.Body))
	waitForProcessInstanceState(t, job.ProcessInstanceKey, zenclient.ProcessInstanceStateCompleted)
	completed := getFlowElementByElementId(t, job.ProcessInstanceKey, "listener-message-start-event")
	assert.NotNil(t, completed.CompletedAt)
	assert.Equal(t, "order-7", completed.OutputVariables["orderFromMessage"], "completing the job keeps the start event's outputs")
}

func TestDeploymentRefusesAListenerTheEngineDoesNotRun(t *testing.T) {
	model, err := readE2ETestDataBPMN("execution_listener/listener-refused.bpmn")
	require.NoError(t, err)
	model = []byte(strings.ReplaceAll(string(model), `id="listener-refused"`, fmt.Sprintf(`id="listener-refused-%d"`, rand.Int63())))

	response, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), "application/octet-stream", strings.NewReader(string(model)))

	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, response.StatusCode(), string(response.Body))
	assert.Contains(t, string(response.Body), `execution listener eventType=\"start\" on end event id=\"listener-refused-end\" is not supported`)
}

func TestDeploymentRefusesAStartListenerOutsideItsContainer(t *testing.T) {
	model, err := readE2ETestDataBPMN("execution_listener/listener-process.bpmn")
	require.NoError(t, err)
	text := strings.ReplaceAll(string(model), `id="listener-process"`, fmt.Sprintf(`id="listener-misplaced-%d"`, rand.Int63()))
	text = strings.Replace(text, "<zeebe:executionListeners>", "", 1)
	text = strings.Replace(text, "</zeebe:executionListeners>", "", 1)

	response, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), "application/octet-stream", strings.NewReader(text))

	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, response.StatusCode(), string(response.Body))
	assert.Contains(t, string(response.Body), `execution listener eventType=\"end\" on start event id=\"listener-start\" is not supported where it sits`)
}

// listenerProcess is a deployment of listener-process.bpmn under a process id
// and job types of its own.
type listenerProcess struct {
	definitionKey int64
	processID     string
	startJobType  string
	workJobType   string
}

func deployListenerProcess(t *testing.T) listenerProcess {
	t.Helper()
	suffix := fmt.Sprintf("%d", rand.Int63())
	process := listenerProcess{
		processID:    "listener-process-" + suffix,
		startJobType: "listener-start-job-" + suffix,
		workJobType:  "listener-work-job-" + suffix,
	}
	process.definitionKey = deployListenerFixture(t, "listener-process.bpmn", map[string]string{
		`id="listener-process"`:     `id="` + process.processID + `"`,
		`type="listener-start-job"`: `type="` + process.startJobType + `"`,
		`type="listener-work-job"`:  `type="` + process.workJobType + `"`,
	})
	return process
}

// listenerTask is a deployment of listener-task.bpmn under a process id and
// job types of its own.
type listenerTask struct {
	definitionKey int64
	beforeJobType string
	workJobType   string
	afterJobType  string
}

func deployListenerTask(t *testing.T) listenerTask {
	t.Helper()
	suffix := fmt.Sprintf("%d", rand.Int63())
	task := listenerTask{
		beforeJobType: "listener-task-before-" + suffix,
		workJobType:   "listener-task-work-" + suffix,
		afterJobType:  "listener-task-after-" + suffix,
	}
	task.definitionKey = deployListenerFixture(t, "listener-task.bpmn", map[string]string{
		`id="listener-task"`:          `id="listener-task-` + suffix + `"`,
		`type="listener-task-before"`: `type="` + task.beforeJobType + `"`,
		`type="listener-task-work"`:   `type="` + task.workJobType + `"`,
		`type="listener-task-after"`:  `type="` + task.afterJobType + `"`,
	})
	return task
}

// waitForActiveJobOfType waits for the active job of the given type of the
// instance: the jobs of one element follow each other on the same element id.
func waitForActiveJobOfType(t *testing.T, processInstanceKey int64, jobType string) public.Job {
	t.Helper()
	var found public.Job
	require.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if err != nil {
			return false
		}
		for _, job := range jobs {
			if job.Type == jobType && job.State == public.JobStateActive {
				found = job
				return true
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "process instance %d should expose an active job of type %s", processInstanceKey, jobType)
	return found
}

func completeListenerTaskJob(t *testing.T, jobKey int64, variables map[string]any) {
	t.Helper()
	response, err := app.restClient.CompleteJobWithResponse(t.Context(), jobKey, zenclient.CompleteJobJSONRequestBody{Variables: &variables})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, response.StatusCode(), string(response.Body))
}

func deployListenerFixture(t *testing.T, file string, replacements map[string]string) int64 {
	t.Helper()
	model, err := readE2ETestDataBPMN("execution_listener/" + file)
	require.NoError(t, err)
	text := string(model)
	for old, replacement := range replacements {
		text = strings.ReplaceAll(text, old, replacement)
	}
	response := deployProcessDefinitionContent(t, []byte(text))
	definition := public.CreateProcessDefinition201JSONResponse{}
	require.NoError(t, json.Unmarshal(response.Body, &definition))
	return definition.ProcessDefinitionKey
}
