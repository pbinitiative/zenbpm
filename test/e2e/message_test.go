package e2e

import (
	"encoding/json"
	"fmt"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TODO: Test with multiple partitions/nodes
func TestRestApiMessage(t *testing.T) {
	cleanProcessInstances(t)

	var instance public.ProcessInstance
	var definition zenclient.ProcessDefinitionSimple
	err := deployDefinition(t, "message-intermediate-catch-event.bpmn")
	assert.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == "message-intermediate-catch-event" {
			definition = def
			break
		}
	}
	instance, err = createProcessInstance(t, definition.Key, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)

	failedInstance, err := createProcessInstance(t, definition.Key, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
	assert.Equal(t, public.ProcessInstanceStateFailed, failedInstance.State)

	t.Run("publish message standard process", func(t *testing.T) {
		err := publishMessage(t, "globalMsgRef", "correlation-key-one", &map[string]any{
			"test-var": "test",
		})
		assert.NoError(t, err)
		processInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.NotEmpty(t, processInstance.Variables)
		assert.NotEmpty(t, processInstance.Variables["test-var"])
		assert.Equal(t, "test", processInstance.Variables["test-var"])
		assert.Equal(t, float64(123), processInstance.Variables["testVar"])

		err = publishMessage(t, "globalMsgRef", "correlation-key-one", &map[string]any{
			"test-var": "test",
		})
		assert.Error(t, err)

		_, err = createProcessInstance(t, definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
	})
	t.Run("publish message multiInstance process", func(t *testing.T) {
		multiInstanceDefinition, err := deployGetUniqueDefinition(t, "multi_instance_service_task.bpmn")
		assert.NoError(t, err)

		instance, err = createProcessInstance(t, multiInstanceDefinition.Key, map[string]any{
			"testInputCollection": []string{"test1", "test2", "test3"},
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)

		err = publishMessage(t, "boundary message", "1234", &map[string]any{
			"test-var": "test",
		})
		assert.NoError(t, err)
		processInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.NotEmpty(t, processInstance.Variables)
		assert.NotEmpty(t, processInstance.Variables["test-var"])
		assert.Equal(t, "test", processInstance.Variables["test-var"])

		err = publishMessage(t, "boundary message", "1234", &map[string]any{
			"test-var": "test",
		})
		assert.Error(t, err)

		_, err = createProcessInstance(t, definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
	})

	t.Run("publish message call activity", func(t *testing.T) {
		callActivityDefinition, err := deployGetUniqueDefinition(t, "call-activity-simple.bpmn")
		assert.NoError(t, err)
		err = deployDefinition(t, "simple_task.bpmn")
		assert.NoError(t, err)

		instance, err = createProcessInstance(t, callActivityDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)

		err = publishMessage(t, "Message_2ffbhei", "testMessage", &map[string]any{
			"test-var": "test",
		})
		assert.NoError(t, err)
		processInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.NotEmpty(t, processInstance.Variables)
		assert.NotEmpty(t, processInstance.Variables["test-var"])
		assert.Equal(t, "test", processInstance.Variables["test-var"])
		assert.Equal(t, float64(123), processInstance.Variables["testVar"])

		err = publishMessage(t, "Message_2ffbhei", "testMessage", &map[string]any{
			"test-var": "test",
		})
		assert.Error(t, err)

		_, err = createProcessInstance(t, definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
	})

	t.Run("publish message subprocess", func(t *testing.T) {
		subprocessDefinition, err := deployGetUniqueDefinition(t, "simple_sub_process_task.bpmn")
		assert.NoError(t, err)

		instance, err = createProcessInstance(t, subprocessDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)

		err = publishMessage(t, "Message_1tfendh", "testMessage", &map[string]any{
			"test-var": "test",
		})
		assert.NoError(t, err)
		processInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.NotEmpty(t, processInstance.Variables)
		assert.NotEmpty(t, processInstance.Variables["test-var"])
		assert.Equal(t, "test", processInstance.Variables["test-var"])
		assert.Equal(t, float64(123), processInstance.Variables["testVar"])

		err = publishMessage(t, "Message_1tfendh", "testMessage", &map[string]any{
			"test-var": "test",
		})
		assert.Error(t, err)

		_, err = createProcessInstance(t, definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
	})

	t.Run("publish message - in loop test", func(t *testing.T) {
		def, err := deployGetUniqueDefinition(t, "message-boundary-task-loop.bpmn")
		assert.NoError(t, err)

		instance, err := createProcessInstance(t, def.Key, map[string]any{
			"testInputCollection": []string{"test1", "test2", "test3"},
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)

		err = publishMessage(t, "return", "approvalId", &map[string]any{
			"test-var": "test",
		})
		assert.NoError(t, err)

		jobs, err := getJobs(t, zenclient.GetJobsParams{
			State:              ptr.To(zenclient.JobStateActive),
			ProcessInstanceKey: ptr.To(instance.Key),
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobs.Partitions))
		assert.Equal(t, 1, len(jobs.Partitions[0].Items))

		response, err := app.restClient.CompleteJobWithResponse(t.Context(), jobs.Partitions[0].Items[0].Key, zenclient.CompleteJobJSONRequestBody{})
		if err != nil {
			return
		}
		assert.Equal(t, 201, response.StatusCode())

		jobs, err = getJobs(t, zenclient.GetJobsParams{
			State:              ptr.To(zenclient.JobStateActive),
			ProcessInstanceKey: ptr.To(instance.Key),
		})
		assert.NoError(t, err)

		err = publishMessage(t, "return", "approvalId", &map[string]any{
			"test-var": "test",
		})
		assert.NoError(t, err)

		jobs, err = getJobs(t, zenclient.GetJobsParams{
			State:              ptr.To(zenclient.JobStateActive),
			ProcessInstanceKey: ptr.To(instance.Key),
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobs.Partitions))
		assert.Equal(t, 1, len(jobs.Partitions[0].Items))
	})
}

func publishMessage(t testing.TB, name string, correlationKey string, vars *map[string]any) error {
	respBody, status, _, err := app.NewRequest(t).
		WithPath("/v1/messages").
		WithMethod("POST").
		WithBody(public.PublishMessageJSONBody{
			CorrelationKey: correlationKey,
			MessageName:    name,
			Variables:      vars,
		}).
		Do()
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	if status != 201 {
		unmarshalledResp := public.PublishMessage502JSONResponse{}
		err = json.Unmarshal(respBody, &unmarshalledResp)
		return fmt.Errorf("failed to publish message expected status 201 got %d:%s", status, unmarshalledResp.Message)
	}
	return nil
}
