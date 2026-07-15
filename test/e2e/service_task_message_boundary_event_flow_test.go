package e2e

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestMessageBoundaryEventFlow(t *testing.T) {
	t.Run("interrupting boundary message cancels the activity and completes the boundary path", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployMessageBoundaryDefinition(t, "testdata/service_task/service_task_with_message_boundary_event_interrupting.bpmn", "message-boundary-event-interrupting")
		instance := createMessageBoundaryInstance(t, definitionKey)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service_task")
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive)

		err := publishMessage(t, messageName, correlationKey, &map[string]any{"payload": "interrupting-boundary"})
		require.NoError(t, err)

		waitForProcessInstanceJobByElementId(t, instance.Key, "service_task", public.JobStateTerminated)
		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_boundary")
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 0)
		assertProcessInstanceVariables(t, instance.Key, map[string]any{"payload": "interrupting-boundary"})
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted)

		response, err := publishMessageWithResponse(t, messageName, correlationKey, &map[string]any{"payload": "duplicate-boundary"})
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode())
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_17cg00h",
			"service_task",
			"message_boundary_event",
			"Flow_14sh6sh",
			"end_event_boundary",
		})
	})

	t.Run("message A completes interrupting boundary B and cancels boundary A", func(t *testing.T) {
		definitionKey, messageAName, correlationKeyA, messageBName, correlationKeyB := deployTwoMessageBoundaryDefinition(
			t,
			"testdata/service_task/service_task_with_message_boundary_event_two_interrupting.bpmn",
			"two-message-boundary-event-interrupting",
		)
		instance := createMessageBoundaryInstance(t, definitionKey)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service_task")
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 2)

		err := publishMessage(t, messageAName, correlationKeyA, &map[string]any{"payloadB": "message-a"})
		require.NoError(t, err)

		waitForProcessInstanceJobByElementId(t, instance.Key, "service_task", public.JobStateTerminated)
		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_boundary_b")
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateCompleted, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateTerminated, 2)
		assertProcessInstanceVariables(t, instance.Key, map[string]any{"payloadB": "message-a"})
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary_b", runtime.TokenStateCompleted)
		assertProcessInstanceTokenCount(t, instance.Key, "end_event_boundary_a", 0)
		assertProcessInstanceTokenCount(t, instance.Key, "end_event_main", 0)

		response, err := publishMessageWithResponse(t, messageBName, correlationKeyB, &map[string]any{})
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode())
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_17cg00h",
			"service_task",
			"boundary_message_event_b",
			"Flow_0nzuzj2",
			"end_event_boundary_b",
		})
	})

	t.Run("message B completes interrupting boundary A and cancels boundary B", func(t *testing.T) {
		definitionKey, messageAName, correlationKeyA, messageBName, correlationKeyB := deployTwoMessageBoundaryDefinition(
			t,
			"testdata/service_task/service_task_with_message_boundary_event_two_interrupting.bpmn",
			"two-message-boundary-event-interrupting",
		)
		instance := createMessageBoundaryInstance(t, definitionKey)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service_task")
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 2)

		err := publishMessage(t, messageBName, correlationKeyB, &map[string]any{"payloadA": "message-b"})
		require.NoError(t, err)

		waitForProcessInstanceJobByElementId(t, instance.Key, "service_task", public.JobStateTerminated)
		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_boundary_a")
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateCompleted, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateTerminated, 2)
		assertProcessInstanceVariables(t, instance.Key, map[string]any{"payloadA": "message-b"})
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary_a", runtime.TokenStateCompleted)
		assertProcessInstanceTokenCount(t, instance.Key, "end_event_boundary_b", 0)
		assertProcessInstanceTokenCount(t, instance.Key, "end_event_main", 0)

		response, err := publishMessageWithResponse(t, messageAName, correlationKeyA, &map[string]any{})
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode())
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_17cg00h",
			"service_task",
			"boundary_message_event_a",
			"Flow_14sh6sh",
			"end_event_boundary_a",
		})
	})

	t.Run("message after activity completion is rejected because the subscription is cancelled", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployMessageBoundaryDefinition(t, "testdata/service_task/service_task_with_message_boundary_event_noninterrupting.bpmn", "message-boundary-event-noninterrupting")
		instance := createMessageBoundaryInstance(t, definitionKey)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service_task")
		assertMessageSubscriptionState(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive)

		completeJobForElementId(t, instance.Key, "service_task", nil)

		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_main")
		assertProcessInstanceTokenState(t, instance.Key, "end_event_main", runtime.TokenStateCompleted)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateTerminated, 1)

		response, err := publishMessageWithResponse(t, messageName, correlationKey, &map[string]any{"payload": "late-boundary"})
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode())

		assertProcessInstanceTokenCount(t, instance.Key, "boundary_message_event", 0)
		assertProcessInstanceTokenCount(t, instance.Key, "end_event_boundary", 0)
		assertProcessInstanceVariables(t, instance.Key, map[string]any{})
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_17cg00h",
			"service_task",
			"Flow_1gnes9r",
			"end_event_main",
		})
	})

	t.Run("non-interrupting boundary message can fire repeatedly while the activity remains active", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployMessageBoundaryDefinition(t, "testdata/service_task/service_task_with_message_boundary_event_noninterrupting.bpmn", "message-boundary-event-noninterrupting")
		instance := createMessageBoundaryInstance(t, definitionKey)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service_task")
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 1)

		err := publishMessage(t, messageName, correlationKey, &map[string]any{"payload": "first-boundary"})
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service_task")
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceTokenStates(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 1)
		assertProcessInstanceVariables(t, instance.Key, map[string]any{"payload": "first-boundary"})
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_17cg00h",
			"service_task",
			"boundary_message_event",
			"Flow_14sh6sh",
			"end_event_boundary",
		})

		err = publishMessage(t, messageName, correlationKey, &map[string]any{"payload": "second-boundary"})
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service_task")
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceTokenStates(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted, 2)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateCompleted, 2)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 1)
		assertProcessInstanceVariables(t, instance.Key, map[string]any{"payload": "second-boundary"})
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_17cg00h",
			"service_task",
			"boundary_message_event",
			"Flow_14sh6sh",
			"end_event_boundary",
			"boundary_message_event",
			"Flow_14sh6sh",
			"end_event_boundary",
		})

		completeJobForElementId(t, instance.Key, "service_task", nil)

		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_main")
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateTerminated, 1)
		assertProcessInstanceTokenStates(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted, 2)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_main", runtime.TokenStateCompleted)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_17cg00h",
			"service_task",
			"boundary_message_event",
			"Flow_14sh6sh",
			"end_event_boundary",
			"boundary_message_event",
			"Flow_14sh6sh",
			"end_event_boundary",
			"Flow_1gnes9r",
			"end_event_main",
		})
	})

	t.Run("multiple non-interrupting boundary messages can fire independently while the activity remains active", func(t *testing.T) {
		definitionKey, messageAName, correlationKeyA, messageBName, correlationKeyB := deployTwoMessageBoundaryDefinition(
			t,
			"testdata/service_task/service_task_with_message_boundary_event_two_noninterrupting.bpmn",
			"two-message-boundary-event-noninterrupting",
		)
		instance := createMessageBoundaryInstance(t, definitionKey)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service_task")
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 2)

		// first boundary message (A)
		err := publishMessage(t, messageAName, correlationKeyA, &map[string]any{"payloadB": "message-a"})
		require.NoError(t, err)

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary_b", runtime.TokenStateCompleted)
		assertProcessInstanceTokenCount(t, instance.Key, "end_event_boundary_a", 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 2)
		assertProcessInstanceVariables(t, instance.Key, map[string]any{"payloadB": "message-a"})
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_17cg00h",
			"service_task",
			"boundary_message_event_b",
			"Flow_0nzuzj2",
			"end_event_boundary_b",
		})

		// first boundary message (B)
		err = publishMessage(t, messageBName, correlationKeyB, &map[string]any{"payloadA": "message-b"})
		require.NoError(t, err)

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary_a", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary_b", runtime.TokenStateCompleted)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateCompleted, 2)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 2)
		assertProcessInstanceVariables(t, instance.Key, map[string]any{
			"payloadA": "message-b",
			"payloadB": "message-a",
		})
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_17cg00h",
			"service_task",
			"boundary_message_event_b",
			"Flow_0nzuzj2",
			"end_event_boundary_b",
			"boundary_message_event_a",
			"Flow_14sh6sh",
			"end_event_boundary_a",
		})

		completeJobForElementId(t, instance.Key, "service_task", nil)

		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_main")
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateTerminated, 2)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary_a", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary_b", runtime.TokenStateCompleted)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_17cg00h",
			"service_task",
			"boundary_message_event_b",
			"Flow_0nzuzj2",
			"end_event_boundary_b",
			"boundary_message_event_a",
			"Flow_14sh6sh",
			"end_event_boundary_a",
			"Flow_1gnes9r",
			"end_event_main",
		})
	})
}

func deployMessageBoundaryDefinition(t testing.TB, filename string, baseProcessID string) (int64, string, string) {
	return deployMessageBoundaryDefinitionWithInterrupting(t, filename, baseProcessID, false)
}

func deployInterruptingMessageBoundaryDefinition(t testing.TB, filename string, baseProcessID string) (int64, string, string) {
	return deployMessageBoundaryDefinitionWithInterrupting(t, filename, baseProcessID, true)
}

func deployMessageBoundaryDefinitionWithInterrupting(t testing.TB, filename string, baseProcessID string, interrupting bool) (int64, string, string) {
	t.Helper()

	suffix := messageEventTestSuffix()
	processID := fmt.Sprintf("%s-%d", baseProcessID, suffix)
	messageName := fmt.Sprintf("%s-ref-%d", baseProcessID, suffix)
	correlationKey := fmt.Sprintf("%s-key-%d", baseProcessID, suffix)
	content := string(readBPMNTestCaseFile(t, filename))
	if interrupting {
		content = strings.Replace(content, `cancelActivity="false"`, `cancelActivity="true"`, 1)
	}
	content = strings.NewReplacer(
		fmt.Sprintf(`bpmn:process id="%s"`, baseProcessID), fmt.Sprintf(`bpmn:process id="%s"`, processID),
		`name="message"`, fmt.Sprintf(`name="%s"`, messageName),
		`correlationKey="=&#34;message-boundary-event-interruptingCorrelationKey&#34;"`, fmt.Sprintf(`correlationKey="=&#34;%s&#34;"`, correlationKey),
		`correlationKey="=correlationKey"`, fmt.Sprintf(`correlationKey="=&#34;%s&#34;"`, correlationKey),
	).Replace(content)

	return deployBPMNTestCaseContent(t, filename, []byte(content)), messageName, correlationKey
}

func deployTwoMessageBoundaryDefinition(t testing.TB, filename string, baseProcessID string) (int64, string, string, string, string) {
	t.Helper()

	suffix := messageEventTestSuffix()
	processID := fmt.Sprintf("%s-%d", baseProcessID, suffix)
	messageAName := fmt.Sprintf("%s-a-ref-%d", baseProcessID, suffix)
	messageBName := fmt.Sprintf("%s-b-ref-%d", baseProcessID, suffix)
	correlationKeyA := fmt.Sprintf("%s-a-key-%d", baseProcessID, suffix)
	correlationKeyB := fmt.Sprintf("%s-b-key-%d", baseProcessID, suffix)
	content := strings.NewReplacer(
		fmt.Sprintf(`bpmn:process id="%s"`, baseProcessID), fmt.Sprintf(`bpmn:process id="%s"`, processID),
		`name="messageA"`, fmt.Sprintf(`name="%s"`, messageAName),
		`name="messageB"`, fmt.Sprintf(`name="%s"`, messageBName),
		`correlationKey="=correlationKeyA"`, fmt.Sprintf(`correlationKey="=&#34;%s&#34;"`, correlationKeyA),
		`correlationKey="=correlationKeyB"`, fmt.Sprintf(`correlationKey="=&#34;%s&#34;"`, correlationKeyB),
	).Replace(string(readBPMNTestCaseFile(t, filename)))

	definitionKey := deployBPMNTestCaseContent(t, filename, []byte(content))
	return definitionKey, messageAName, correlationKeyA, messageBName, correlationKeyB
}

func createMessageBoundaryInstance(t testing.TB, definitionKey int64) zenclient.ProcessInstance {
	t.Helper()

	processInstance, err := createProcessInstance(t, &definitionKey, map[string]any{})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})
	return processInstance
}
