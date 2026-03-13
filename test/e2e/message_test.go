package e2e

import (
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

// TODO: Test with multiple partitions/nodes
func TestRestApiMessage(t *testing.T) {
	cleanProcessInstances(t)

	var instance public.ProcessInstance
	var definition zenclient.ProcessDefinitionSimple
	_, err := deployDefinition(t, "message-intermediate-catch-event.bpmn")
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
		_, err = deployDefinition(t, "simple_task.bpmn")
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

func TestPublishMessageNotFound(t *testing.T) {
	t.Run("publish message with non-existing correlationKey returns NOT_FOUND", func(t *testing.T) {
		response, err := publishMessageWithResponse(t, "nonExistingMessage", "non-existing-correlation-key", nil)
		assert.NoError(t, err)
		assert.Equal(t, 404, response.StatusCode())
		assert.Nil(t, response.JSON502)
		assert.NotNil(t, response.JSON404)
		assert.Equal(t, "NOT_FOUND", response.JSON404.Code)
	})

	t.Run("publish message with non-existing messageName returns NOT_FOUND", func(t *testing.T) {
		_, err := deployDefinition(t, "message-intermediate-catch-event.bpmn")
		assert.NoError(t, err)
		definitions, err := listProcessDefinitions(t)
		assert.NoError(t, err)
		var definitionKey int64
		for _, def := range definitions {
			if def.BpmnProcessId == "message-intermediate-catch-event" {
				definitionKey = def.Key
				break
			}
		}
		_, err = createProcessInstance(t, definitionKey, map[string]any{"testVar": 123})
		assert.NoError(t, err)

		response, err := publishMessageWithResponse(t, "nonExistingMessageName", "correlation-key-one", nil)
		assert.NoError(t, err)
		assert.Equal(t, 404, response.StatusCode())
		assert.Nil(t, response.JSON502)
		assert.NotNil(t, response.JSON404)
		assert.Equal(t, "NOT_FOUND", response.JSON404.Code)
	})
}

func publishMessage(t testing.TB, name string, correlationKey string, vars *map[string]any) error {
	response, err := publishMessageWithResponse(t, name, correlationKey, vars)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	if response.StatusCode() != 201 {
		if response.JSON404 != nil {
			return fmt.Errorf("failed to publish message, expected status 201 got %d: %s", response.StatusCode(), response.JSON404.Message)
		}
		return fmt.Errorf("failed to publish message, expected status 201 got %d", response.StatusCode())
	}
	return nil
}

func publishMessageWithResponse(t testing.TB, name string, correlationKey string, vars *map[string]any) (*zenclient.PublishMessageResponse, error) {
	return app.restClient.PublishMessageWithResponse(t.Context(), zenclient.PublishMessageJSONRequestBody{
		CorrelationKey: correlationKey,
		MessageName:    name,
		Variables:      vars,
	})
}
