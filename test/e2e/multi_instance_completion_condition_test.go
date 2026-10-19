package e2e

import (
	"bytes"
	"fmt"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const multiInstanceServiceTaskWithCompletionConditionBpmn = "testdata/multi_instance/multi_instance_service_task_with_completion_condition.bpmn"

var completionConditionElement = regexp.MustCompile(`\s*<bpmn:completionCondition[^>]*>[^<]*</bpmn:completionCondition>`)

func TestMultiInstanceCompletionConditionDeployment(t *testing.T) {
	t.Run("CreateProcessDefinition - 400 for multi-instance activity with completion condition", func(t *testing.T) {
		processID, content := uniqueMultiInstanceCompletionConditionDefinition(t)

		resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), "application/octet-stream", bytes.NewReader(content))

		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400, "unexpected response body: %s", string(resp.Body))
		assert.Equal(t, "BAD_REQUEST", resp.JSON400.Code)
		assert.Contains(t, resp.JSON400.Message, "unsupported element configuration")
		assert.Contains(t, resp.JSON400.Message, `serviceTask id="service_task"`)
		assert.Contains(t, resp.JSON400.Message, "completionCondition")
		assert.Empty(t, getProcessDefinitionVersions(t, processID, false), "a refused definition must not be stored")
	})

	t.Run("CreateProcessDefinition - same activity without completion condition is deployed", func(t *testing.T) {
		processID, content := uniqueMultiInstanceCompletionConditionDefinition(t)
		withoutCompletionCondition := completionConditionElement.ReplaceAll(content, nil)
		require.NotEqual(t, content, withoutCompletionCondition, "the fixture must declare a completion condition")

		resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), "application/octet-stream", bytes.NewReader(withoutCompletionCondition))

		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode(), "unexpected response body: %s", string(resp.Body))
		require.Len(t, getProcessDefinitionVersions(t, processID, false), 1)
	})
}

func uniqueMultiInstanceCompletionConditionDefinition(t *testing.T) (string, []byte) {
	t.Helper()

	content, err := readE2ETestDataBPMN(multiInstanceServiceTaskWithCompletionConditionBpmn)
	require.NoError(t, err)
	const fixtureProcessID = "MultiInstance_Service_Task_With_Completion_Condition"
	processID := fmt.Sprintf("%s-%d", fixtureProcessID, time.Now().UnixNano())
	content = bytes.ReplaceAll(content, []byte(`bpmn:process id="`+fixtureProcessID+`"`), []byte(`bpmn:process id="`+processID+`"`))
	return processID, content
}
