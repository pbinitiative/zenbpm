package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	messageEndEventBasicPath      = "testdata/message_end_event/message_end_event.bpmn"
	messageEndEventPayloadPath    = "testdata/message_end_event/message_end_event_payload.bpmn"
	messageEndEventParallelPath   = "testdata/message_end_event/message_end_event_parallel.bpmn"
	messageEndEventSubprocessPath = "testdata/message_end_event/message_end_event_subprocess.bpmn"

	messageEndEventElementID      = "message_end_event"
	messageEndEventPublicationJob = "orders.completed"
)

func createMessageEndEventProcessInstance(
	t testing.TB,
	definitionKey int64,
	businessKey string,
	variables map[string]any,
) zenclient.ProcessInstance {
	t.Helper()

	response, err := app.restClient.CreateProcessInstanceWithResponse(t.Context(), zenclient.CreateProcessInstanceJSONRequestBody{
		BusinessKey:          &businessKey,
		ProcessDefinitionKey: &definitionKey,
		Variables:            &variables,
	})
	require.NoError(t, err)
	require.Equal(t, 201, response.StatusCode())
	require.NotNil(t, response.JSON201)
	return *response.JSON201
}

func messageEndEventPayloadVariables(businessKey string, messageID string, orderID string, customerName string, occurredAt string) map[string]any {
	return map[string]any{
		"businessKey":  businessKey,
		"messageId":    messageID,
		"orderId":      orderID,
		"amount":       42.5,
		"approved":     true,
		"customerName": customerName,
		"occurredAt":   occurredAt,
	}
}

func uniqueMessageEndEventBusinessKey(prefix string) string {
	return fmt.Sprintf("message-end-event-%s-%d", prefix, time.Now().UnixNano())
}

func assertExactlyOneMessageEndEventJob(t testing.TB, processInstanceKey int64, elementID string) {
	t.Helper()

	var requestErr error
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if !assert.NoError(collect, err) {
			return
		}

		count := 0
		for _, job := range jobs {
			if job.ElementId == elementID {
				count++
			}
		}
		assert.Equal(collect, 1, count)
	}, 5*time.Second, 50*time.Millisecond)

	require.Never(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if err != nil {
			requestErr = err
			return false
		}
		requestErr = nil

		count := 0
		for _, job := range jobs {
			if job.ElementId == elementID {
				count++
			}
		}
		return count > 1
	}, 750*time.Millisecond, 50*time.Millisecond, "message end event must not create a duplicate publication job")
	require.NoError(t, requestErr)
}

func assertProcessInstanceHasNoActiveJobs(t testing.TB, processInstanceKey int64) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if !assert.NoError(collect, err) {
			return
		}

		for _, job := range jobs {
			assert.NotEqual(collect, public.JobStateActive, job.State)
		}
	}, 5*time.Second, 50*time.Millisecond, "completed process instance must not expose active jobs")
}
