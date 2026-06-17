package e2e

import (
	"bytes"
	"fmt"
	"mime/multipart"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	eventBasedGatewayMessageTimerPath          = "testdata/gateway_event_based/event_based_gateway_message_timer.bpmn"
	eventBasedGatewayConditionalPath           = "testdata/gateway_event_based/event_based_gateway_conditional_flow.bpmn"
	eventBasedGatewayMessageName               = "event-gateway-message"
	eventBasedGatewayMessageElementId          = "catch_message"
	eventBasedGatewayTimerElementId            = "catch_timer"
	eventBasedGatewayElementId                 = "event_based_gateway"
	eventBasedGatewayMessageTaskId             = "task_message"
	eventBasedGatewayTimerTaskId               = "task_timer"
	eventBasedGatewayMultiEventPath            = "testdata/gateway_event_based/event_based_gateway_two_messages_timer.bpmn"
	eventBasedGatewayReentryPath               = "testdata/gateway_event_based/event_based_gateway_reentry.bpmn"
	eventBasedGatewayMessageOnlyPath           = "testdata/gateway_event_based/event_based_gateway_two_messages.bpmn"
	eventBasedGatewayMessageAName              = "event-gateway-message-a"
	eventBasedGatewayMessageBName              = "event-gateway-message-b"
	eventBasedGatewayMessageAElementId         = "catch_message_a"
	eventBasedGatewayMessageBElementId         = "catch_message_b"
	eventBasedGatewayMessageATaskID            = "task_message_a"
	eventBasedGatewayMessageBTaskID            = "task_message_b"
	eventBasedGatewayMessageAEndElementId      = "end_message_a"
	eventBasedGatewayMessageBEndElementId      = "end_message_b"
	eventBasedGatewayReentryLoopElementID      = "catch_message_loop"
	eventBasedGatewayReentryLoopTaskID         = "service_task_message_loop"
	eventBasedGatewayReentryLoopEndElementID   = "end_event_loop"
	eventBasedGatewayReentrySingleElementID    = "catch_message_single"
	eventBasedGatewayReentrySingleEndElementID = "end_event_single"
)

func TestEventBasedGatewayFlow(t *testing.T) {
	t.Run("Waits after entering the event based gateway", func(t *testing.T) {
		processInstance, _ := createEventBasedGatewayInstance(t, "PT1H")

		waitForEventBasedGatewayWaitingState(t, processInstance.Key)

		assertProcessInstanceTokenState(t, processInstance.Key, eventBasedGatewayElementId, runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageElementId, zenclient.EventSubscriptionStateActive)
		assertTimerSubscriptionState(t, processInstance.Key, eventBasedGatewayTimerElementId, zenclient.EventSubscriptionStateActive)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId)
	})

	t.Run("Message wins over timer and cancels timer path", func(t *testing.T) {
		processInstance, correlationKey := createEventBasedGatewayInstance(t, "PT1H")

		waitForEventBasedGatewayWaitingState(t, processInstance.Key)

		err := publishMessage(t, eventBasedGatewayMessageName, correlationKey, &map[string]any{
			"winner": "message",
		})
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageElementId, zenclient.EventSubscriptionStateCompleted)
		assertTimerSubscriptionState(t, processInstance.Key, eventBasedGatewayTimerElementId, zenclient.EventSubscriptionStateWithdrawn)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{eventBasedGatewayMessageTaskId},
			[]string{eventBasedGatewayTimerTaskId, "end_timer"})

		completeJobForElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId, nil)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_message", runtime.TokenStateCompleted)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_message"}, []string{"end_timer"})
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_start_to_gateway",
			"event_based_gateway",
			"Flow_message_to_task",
			"task_message",
			"Flow_message_to_end",
			"end_message",
		})
	})

	t.Run("Timer wins over message and later message cannot change the flow", func(t *testing.T) {
		processInstance, correlationKey := createEventBasedGatewayInstance(t, "PT1S")

		waitForEventBasedGatewayWaitingState(t, processInstance.Key)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId, public.JobStateActive)

		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId)
		assertTimerSubscriptionState(t, processInstance.Key, eventBasedGatewayTimerElementId, zenclient.EventSubscriptionStateCompleted)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageElementId, zenclient.EventSubscriptionStateTerminated)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{eventBasedGatewayTimerTaskId},
			[]string{eventBasedGatewayMessageTaskId, "end_message"})

		response, err := publishMessageWithResponse(t, eventBasedGatewayMessageName, correlationKey, &map[string]any{
			"late": true,
		})
		require.NoError(t, err)
		require.Equal(t, 404, response.StatusCode(), "late message should not match the cancelled gateway subscription")

		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{eventBasedGatewayTimerTaskId},
			[]string{eventBasedGatewayMessageTaskId, "end_message"})

		completeJobForElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId, nil)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_timer", runtime.TokenStateCompleted)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_timer"}, []string{"end_message"})
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_start_to_gateway",
			"event_based_gateway",
			"Flow_timer_to_task",
			"task_timer",
			"Flow_timer_to_end",
			"end_timer",
		})
	})

	t.Run("Cancel while waiting cleans up event subscriptions", func(t *testing.T) {
		processInstance, _ := createEventBasedGatewayInstance(t, "PT1H")

		waitForEventBasedGatewayWaitingState(t, processInstance.Key)

		response, err := app.restClient.CancelProcessInstanceWithResponse(t.Context(), processInstance.Key)
		require.NoError(t, err)
		require.Equal(t, 204, response.StatusCode())

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageElementId, zenclient.EventSubscriptionStateTerminated)
		assertTimerSubscriptionState(t, processInstance.Key, eventBasedGatewayTimerElementId, zenclient.EventSubscriptionStateWithdrawn)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId)
	})

	t.Run("Conditional outgoing flow deployment is rejected through the API", func(t *testing.T) {
		response, err := deployE2eTestDataDefinitionRaw(t, eventBasedGatewayConditionalPath)
		require.NoError(t, err)
		require.GreaterOrEqual(t, response.StatusCode(), 400, "event gateway outgoing condition deployment should fail")
		assert.Contains(t, string(response.Body), "eventBasedGateway")
		assert.Contains(t, string(response.Body), "condition")
		assert.Contains(t, string(response.Body), "EventBasedGateway_1")
	})

	t.Run("Message only waits without fallback timer", func(t *testing.T) {
		definitionKey := deployEventBasedGatewayMessageOnlyDefinition(t)
		correlationKey := eventBasedGatewayCorrelationKey(definitionKey)
		processInstance := createEventBasedGatewayInstanceForDefinition(t, definitionKey, correlationKey)

		assertProcessInstanceTokenState(t, processInstance.Key, eventBasedGatewayElementId, runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageAElementId, zenclient.EventSubscriptionStateActive)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageBElementId, zenclient.EventSubscriptionStateActive)
		assertProcessInstanceHasNoTimerSubscriptions(t, processInstance.Key)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageATaskID)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageBTaskID)
	})

	t.Run("Message A wins and cancels message B and timer", func(t *testing.T) {
		processInstance, correlationKey := createEventBasedGatewayMultiEventInstance(t, "PT1H")

		waitForEventBasedGatewayMultiEventWaitingState(t, processInstance.Key)

		err := publishMessage(t, eventBasedGatewayMessageAName, correlationKey, &map[string]any{
			"winner": "message-a",
		})
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageATaskID)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageBTaskID)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageAElementId, zenclient.EventSubscriptionStateCompleted)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageBElementId, zenclient.EventSubscriptionStateTerminated)
		assertTimerSubscriptionState(t, processInstance.Key, eventBasedGatewayTimerElementId, zenclient.EventSubscriptionStateWithdrawn)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{eventBasedGatewayMessageATaskID},
			[]string{eventBasedGatewayMessageBTaskID, eventBasedGatewayTimerTaskId, eventBasedGatewayMessageBEndElementId, "end_timer"})

		completeJobForElementId(t, processInstance.Key, eventBasedGatewayMessageATaskID, nil)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, eventBasedGatewayMessageAEndElementId, runtime.TokenStateCompleted)
	})

	t.Run("Message B wins and cancels message A and timer", func(t *testing.T) {
		processInstance, correlationKey := createEventBasedGatewayMultiEventInstance(t, "PT1H")

		waitForEventBasedGatewayMultiEventWaitingState(t, processInstance.Key)

		err := publishMessage(t, eventBasedGatewayMessageBName, correlationKey, &map[string]any{
			"winner": "message-b",
		})
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageBTaskID)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageATaskID)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageBElementId, zenclient.EventSubscriptionStateCompleted)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageAElementId, zenclient.EventSubscriptionStateTerminated)
		assertTimerSubscriptionState(t, processInstance.Key, eventBasedGatewayTimerElementId, zenclient.EventSubscriptionStateWithdrawn)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{eventBasedGatewayMessageBTaskID},
			[]string{eventBasedGatewayMessageATaskID, eventBasedGatewayTimerTaskId, eventBasedGatewayMessageAEndElementId, "end_timer"})

		completeJobForElementId(t, processInstance.Key, eventBasedGatewayMessageBTaskID, nil)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, eventBasedGatewayMessageBEndElementId, runtime.TokenStateCompleted)
	})

	t.Run("Timer wins and cancels both message paths", func(t *testing.T) {
		processInstance, _ := createEventBasedGatewayMultiEventInstance(t, "PT1S")

		waitForEventBasedGatewayMultiEventWaitingState(t, processInstance.Key)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId, public.JobStateActive)

		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageATaskID)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageBTaskID)
		assertTimerSubscriptionState(t, processInstance.Key, eventBasedGatewayTimerElementId, zenclient.EventSubscriptionStateCompleted)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageAElementId, zenclient.EventSubscriptionStateTerminated)
		assertMessageSubscriptionState(t, processInstance.Key, eventBasedGatewayMessageBElementId, zenclient.EventSubscriptionStateTerminated)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{eventBasedGatewayTimerTaskId},
			[]string{eventBasedGatewayMessageATaskID, eventBasedGatewayMessageBTaskID, eventBasedGatewayMessageAEndElementId, eventBasedGatewayMessageBEndElementId})

		completeJobForElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId, nil)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_timer", runtime.TokenStateCompleted)
	})

	t.Run("Re-entering the gateway creates fresh subscriptions and completes from the second selection", func(t *testing.T) {
		processInstance, correlationKey := createEventBasedGatewayReentryInstance(t)

		waitForEventBasedGatewayReentryWaitingState(t, processInstance.Key)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateActive, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateActive, 1)

		err := publishMessage(t, eventBasedGatewayMessageAName, correlationKey, &map[string]any{
			"winner": "first-message-loop",
		})
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, eventBasedGatewayReentryLoopTaskID)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateTerminated, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateActive, 0)

		completeJobForElementId(t, processInstance.Key, eventBasedGatewayReentryLoopTaskID, map[string]any{
			"loopCount": 1,
		})

		assertProcessInstanceTokenStates(t, processInstance.Key, eventBasedGatewayElementId, runtime.TokenStateWaiting, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateActive, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateTerminated, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateActive, 1)

		err = publishMessage(t, eventBasedGatewayMessageBName, correlationKey, &map[string]any{
			"winner": "second-message-single",
		})
		require.NoError(t, err)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayReentryLoopTaskID)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateTerminated, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateTerminated, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateActive, 0)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, eventBasedGatewayReentrySingleEndElementID, runtime.TokenStateCompleted)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_start_to_gateway",
			"event_based_gateway",
			"Flow_message_a_to_task",
			"service_task_message_loop",
			"Flow_message_a_to_decision",
			"exclusive_gateway_loop_decision",
			"Flow_reenter_to_gateway",
			"event_based_gateway",
			"Flow_message_b_to_task",
			"end_event_single",
		})
	})

	t.Run("Re-entering the gateway can complete through the loop path", func(t *testing.T) {
		processInstance, correlationKey := createEventBasedGatewayReentryInstance(t)

		waitForEventBasedGatewayReentryWaitingState(t, processInstance.Key)

		err := publishMessage(t, eventBasedGatewayMessageAName, correlationKey, &map[string]any{
			"winner": "first-loop-message",
		})
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, eventBasedGatewayReentryLoopTaskID)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateTerminated, 1)

		completeJobForElementId(t, processInstance.Key, eventBasedGatewayReentryLoopTaskID, map[string]any{
			"loopCount": 1,
		})

		waitForEventBasedGatewayReentryWaitingState(t, processInstance.Key)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateActive, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateTerminated, 1)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateActive, 1)

		err = publishMessage(t, eventBasedGatewayMessageAName, correlationKey, &map[string]any{
			"winner": "second-loop-message",
		})
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, eventBasedGatewayReentryLoopTaskID)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateCompleted, 2)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateTerminated, 2)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, processInstance.Key, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateActive, 0)

		completeJobForElementId(t, processInstance.Key, eventBasedGatewayReentryLoopTaskID, map[string]any{
			"loopCount": 2,
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayReentryLoopTaskID)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, eventBasedGatewayReentryLoopEndElementID, runtime.TokenStateCompleted)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_start_to_gateway",
			"event_based_gateway",
			"Flow_message_a_to_task",
			"service_task_message_loop",
			"Flow_message_a_to_decision",
			"exclusive_gateway_loop_decision",
			"Flow_reenter_to_gateway",
			"event_based_gateway",
			"Flow_message_a_to_task",
			"service_task_message_loop",
			"Flow_message_a_to_decision",
			"exclusive_gateway_loop_decision",
			"Flow_message_a_to_end",
			"end_event_loop",
		})
	})

	t.Run("Concurrent messages still select exactly one path", func(t *testing.T) {
		processInstance, correlationKey := createEventBasedGatewayMultiEventInstance(t, "PT1H")

		waitForEventBasedGatewayMultiEventWaitingState(t, processInstance.Key)

		ctx := t.Context()
		start := make(chan struct{})
		var wg sync.WaitGroup
		responses := make([]*zenclient.PublishMessageResponse, 2)
		errs := make([]error, 2)

		for index, messageName := range []string{eventBasedGatewayMessageAName, eventBasedGatewayMessageBName} {
			wg.Add(1)
			go func(index int, messageName string) {
				defer wg.Done()
				<-start
				responses[index], errs[index] = app.restClient.PublishMessageWithResponse(ctx, zenclient.PublishMessageJSONRequestBody{
					CorrelationKey: &correlationKey,
					MessageName:    messageName,
					Variables: &map[string]any{
						"concurrent_message": messageName,
					},
				})
			}(index, messageName)
		}
		close(start)
		wg.Wait()

		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		for _, response := range responses {
			require.NotNil(t, response)
			require.Contains(t, []int{201, 404}, response.StatusCode())
		}

		winnerTaskID := waitForExactlyOneActiveJobAmong(t, processInstance.Key, eventBasedGatewayMessageATaskID, eventBasedGatewayMessageBTaskID)
		loserTaskID := eventBasedGatewayMessageATaskID
		winnerMessageElementId := eventBasedGatewayMessageBElementId
		loserMessageElementId := eventBasedGatewayMessageAElementId
		if winnerTaskID == eventBasedGatewayMessageATaskID {
			loserTaskID = eventBasedGatewayMessageBTaskID
			winnerMessageElementId = eventBasedGatewayMessageAElementId
			loserMessageElementId = eventBasedGatewayMessageBElementId
		}

		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, loserTaskID)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId)
		assertMessageSubscriptionState(t, processInstance.Key, winnerMessageElementId, zenclient.EventSubscriptionStateCompleted)
		assertMessageSubscriptionState(t, processInstance.Key, loserMessageElementId, zenclient.EventSubscriptionStateTerminated)
		assertTimerSubscriptionState(t, processInstance.Key, eventBasedGatewayTimerElementId, zenclient.EventSubscriptionStateWithdrawn)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	})
}

func createEventBasedGatewayInstance(t testing.TB, timerDuration string) (zenclient.ProcessInstance, string) {
	t.Helper()

	definitionKey := deployEventBasedGatewayDefinition(t, timerDuration)
	correlationKey := eventBasedGatewayCorrelationKey(definitionKey)
	processInstance, err := createProcessInstance(t, &definitionKey, map[string]any{
		"correlationKey": correlationKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})

	return processInstance, correlationKey
}

func deployEventBasedGatewayDefinition(t testing.TB, timerDuration string) int64 {
	t.Helper()

	file, err := readE2ETestDataBPMN(eventBasedGatewayMessageTimerPath)
	require.NoError(t, err)

	processID := fmt.Sprintf("event-based-gateway-message-timer-%d", time.Now().UnixNano())
	content := strings.NewReplacer(
		`bpmn:process id="event-based-gateway-message-timer"`,
		fmt.Sprintf(`bpmn:process id="%s"`, processID),
		">PT1S<",
		fmt.Sprintf(">%s<", timerDuration),
	).Replace(string(file))

	response, err := deployDefinitionFromBytes(t, []byte(content), eventBasedGatewayMessageTimerPath)
	require.NoError(t, err)
	require.Equal(t, 201, response.StatusCode())
	require.NotNil(t, response.JSON201)

	return response.JSON201.ProcessDefinitionKey
}

func eventBasedGatewayCorrelationKey(key int64) string {
	return fmt.Sprintf("event-gateway-%d", key)
}

func deployE2eTestDataDefinitionRaw(t testing.TB, filename string) (*zenclient.CreateProcessDefinitionResponse, error) {
	t.Helper()

	file, err := readE2ETestDataBPMN(filename)
	if err != nil {
		return nil, err
	}

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)
	part, err := writer.CreateFormFile("resource", filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}
	if _, err = part.Write(file); err != nil {
		return nil, fmt.Errorf("failed to write file to multipart form: %w", err)
	}
	if err = writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	return app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), writer.FormDataContentType(), &requestBody)
}

func waitForEventBasedGatewayWaitingState(t testing.TB, processInstanceKey int64) {
	t.Helper()

	assertProcessInstanceTokenState(t, processInstanceKey, eventBasedGatewayElementId, runtime.TokenStateWaiting)
	assertMessageSubscriptionState(t, processInstanceKey, eventBasedGatewayMessageElementId, zenclient.EventSubscriptionStateActive)
	assertTimerSubscriptionState(t, processInstanceKey, eventBasedGatewayTimerElementId, zenclient.EventSubscriptionStateActive)
}

func deployEventBasedGatewayMessageOnlyDefinition(t testing.TB) int64 {
	t.Helper()

	file, err := readE2ETestDataBPMN(eventBasedGatewayMessageOnlyPath)
	require.NoError(t, err)

	processID := fmt.Sprintf("event-based-gateway-two-messages-%d", time.Now().UnixNano())
	content := strings.NewReplacer(
		`bpmn:process id="event-based-gateway-two-messages"`,
		fmt.Sprintf(`bpmn:process id="%s"`, processID),
	).Replace(string(file))

	response, err := deployDefinitionFromBytes(t, []byte(content), eventBasedGatewayMessageOnlyPath)
	require.NoError(t, err)
	require.Equal(t, 201, response.StatusCode())
	require.NotNil(t, response.JSON201)

	return response.JSON201.ProcessDefinitionKey
}

func createEventBasedGatewayMultiEventInstance(t testing.TB, timerDuration string) (zenclient.ProcessInstance, string) {
	t.Helper()

	definitionKey := deployEventBasedGatewayMultiEventDefinition(t, timerDuration)
	correlationKey := eventBasedGatewayCorrelationKey(definitionKey)
	processInstance := createEventBasedGatewayInstanceForDefinition(t, definitionKey, correlationKey)

	return processInstance, correlationKey
}

func createEventBasedGatewayReentryInstance(t testing.TB) (zenclient.ProcessInstance, string) {
	t.Helper()

	definitionKey := deployEventBasedGatewayReentryDefinition(t)
	correlationKey := eventBasedGatewayCorrelationKey(definitionKey)
	processInstance := createEventBasedGatewayInstanceForDefinition(t, definitionKey, correlationKey)

	return processInstance, correlationKey
}

func deployEventBasedGatewayMultiEventDefinition(t testing.TB, timerDuration string) int64 {
	t.Helper()

	file, err := readE2ETestDataBPMN(eventBasedGatewayMultiEventPath)
	require.NoError(t, err)

	processID := fmt.Sprintf("event-based-gateway-two-messages-timer-%d", time.Now().UnixNano())
	content := strings.NewReplacer(
		`bpmn:process id="event-based-gateway-two-messages-timer"`,
		fmt.Sprintf(`bpmn:process id="%s"`, processID),
		">PT1S<",
		fmt.Sprintf(">%s<", timerDuration),
	).Replace(string(file))

	response, err := deployDefinitionFromBytes(t, []byte(content), eventBasedGatewayMultiEventPath)
	require.NoError(t, err)
	require.Equal(t, 201, response.StatusCode())
	require.NotNil(t, response.JSON201)

	return response.JSON201.ProcessDefinitionKey
}

func deployEventBasedGatewayReentryDefinition(t testing.TB) int64 {
	t.Helper()

	file, err := readE2ETestDataBPMN(eventBasedGatewayReentryPath)
	require.NoError(t, err)

	processID := fmt.Sprintf("event-based-gateway-reentry-%d", time.Now().UnixNano())
	content := strings.NewReplacer(
		`bpmn:process id="event-based-gateway-reentry"`,
		fmt.Sprintf(`bpmn:process id="%s"`, processID),
	).Replace(string(file))

	response, err := deployDefinitionFromBytes(t, []byte(content), eventBasedGatewayReentryPath)
	require.NoError(t, err)
	require.Equal(t, 201, response.StatusCode())
	require.NotNil(t, response.JSON201)

	return response.JSON201.ProcessDefinitionKey
}

func waitForEventBasedGatewayMultiEventWaitingState(t testing.TB, processInstanceKey int64) {
	t.Helper()

	assertProcessInstanceTokenState(t, processInstanceKey, eventBasedGatewayElementId, runtime.TokenStateWaiting)
	assertMessageSubscriptionState(t, processInstanceKey, eventBasedGatewayMessageAElementId, zenclient.EventSubscriptionStateActive)
	assertMessageSubscriptionState(t, processInstanceKey, eventBasedGatewayMessageBElementId, zenclient.EventSubscriptionStateActive)
	assertTimerSubscriptionState(t, processInstanceKey, eventBasedGatewayTimerElementId, zenclient.EventSubscriptionStateActive)
}

func waitForEventBasedGatewayReentryWaitingState(t testing.TB, processInstanceKey int64) {
	t.Helper()

	assertProcessInstanceTokenState(t, processInstanceKey, eventBasedGatewayElementId, runtime.TokenStateWaiting)
	assertMessageSubscriptionStateCount(t, processInstanceKey, eventBasedGatewayReentryLoopElementID, zenclient.EventSubscriptionStateActive, 1)
	assertMessageSubscriptionStateCount(t, processInstanceKey, eventBasedGatewayReentrySingleElementID, zenclient.EventSubscriptionStateActive, 1)
}
