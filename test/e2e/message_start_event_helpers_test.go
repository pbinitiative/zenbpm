package e2e

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const (
	messageStartEventFixturePath = "message_event/message-start-event-process.bpmn"
	messageStartEventElementID   = "messageStartEvent_1234"
	messageStartEventTaskID      = "service-task-1"
	messageStartEventEndID       = "Event_0461t71"
)

type messageStartEventTestDefinition struct {
	key         int64
	processID   string
	messageName string
	jobType     string
}

func deployUniqueMessageStartEventDefinition(t testing.TB, prefix string) messageStartEventTestDefinition {
	t.Helper()

	return deployUniqueMessageStartEventDefinitionWithContent(t, prefix, "")
}

func deployUniqueMessageStartEventDefinitionWithContent(t testing.TB, prefix string, startEventExtensionElements string) messageStartEventTestDefinition {
	t.Helper()

	suffix := messageEventTestSuffix()
	definition := messageStartEventTestDefinition{
		processID:   fmt.Sprintf("message-start-event-%s-%d", prefix, suffix),
		messageName: fmt.Sprintf("message-start-event-%s-ref-%d", prefix, suffix),
		jobType:     fmt.Sprintf("message-start-event-%s-job-%d", prefix, suffix),
	}
	definition.key = deployMessageStartDefinitionWithContent(
		t,
		definition.processID,
		definition.messageName,
		definition.jobType,
		startEventExtensionElements,
	)
	return definition
}

func deployMessageStartDefinition(t testing.TB, processID string, messageName string, jobType string) int64 {
	t.Helper()

	return deployMessageStartDefinitionWithContent(t, processID, messageName, jobType, "")
}

func deployMessageStartDefinitionWithContent(
	t testing.TB,
	processID string,
	messageName string,
	jobType string,
	startEventExtensionElements string,
) int64 {
	t.Helper()

	bpmnData, err := readE2ETestDataBPMN(messageStartEventFixturePath)
	require.NoError(t, err)
	content := strings.NewReplacer(
		"message-start-event-process-1", processID,
		"messageStartEventProcessRef", messageName,
		"input-task-for-message-start-event-test", jobType,
	).Replace(string(bpmnData))
	if startEventExtensionElements != "" {
		content = strings.Replace(content, "<bpmn:extensionElements />", startEventExtensionElements, 1)
	}

	return deployBPMNTestCaseContent(t, "message-start-event-process.bpmn", []byte(content))
}

func publishMessageStartEvent(t testing.TB, messageName string, variables map[string]any) {
	t.Helper()

	response, err := app.restClient.PublishMessageWithResponse(t.Context(), zenclient.PublishMessageJSONRequestBody{
		CorrelationKey: nil,
		MessageName:    messageName,
		Variables:      &variables,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, response.StatusCode())
}
