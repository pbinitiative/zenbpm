package e2e

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUserTaskDefinitionDeploymentRoundTrip verifies that the configured
// zenbpm:taskDefinition on a User Task survives deployment. It deploys a
// BPMN file containing a custom task type, reads the deployment back via
// the public REST client (GET /process-definitions/{key}), and asserts
// that the taskDefinition element with its type attribute is preserved
// verbatim. This is the explicit evidence for the "persistence through
// import/export and deployment" acceptance criterion of #789.
func TestUserTaskDefinitionDeploymentRoundTrip(t *testing.T) {
	const (
		bpmnFixturePath = "testdata/user_task/user_tasks_with_static_assignment.bpmn"
		bpmnFixtureName = "user_tasks_with_static_assignment.bpmn"
		expectedType    = "approval"
	)

	wd, err := os.Getwd()
	require.NoError(t, err)
	bpmnBytes, err := os.ReadFile(filepath.Join(wd, bpmnFixturePath))
	require.NoError(t, err)
	bpmnString := string(bpmnBytes)
	expectedSnippet := fmt.Sprintf(`<zenbpm:taskDefinition type=%q`, expectedType)
	require.True(t, strings.Contains(bpmnString, expectedSnippet),
		"sanity check: fixture %q must contain %q before deployment", bpmnFixturePath, expectedSnippet)

	deployResp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(
		t.Context(), "application/octet-stream", bytes.NewReader(bpmnBytes),
	)
	require.NoError(t, err)
	require.Lessf(t, deployResp.StatusCode(), http.StatusBadRequest,
		"deploy failed: status=%d body=%s", deployResp.StatusCode(), string(deployResp.Body))

	var processDefinitionKey int64
	switch {
	case deployResp.JSON201 != nil:
		processDefinitionKey = deployResp.JSON201.ProcessDefinitionKey
	case deployResp.JSON200 != nil:
		processDefinitionKey = deployResp.JSON200.ProcessDefinitionKey
	default:
		t.Fatalf("deploy response missing both 200 and 201 JSON bodies: %s", string(deployResp.Body))
	}
	require.NotZero(t, processDefinitionKey)

	getResp, err := app.restClient.GetProcessDefinitionWithResponse(t.Context(), processDefinitionKey)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	require.NotNil(t, getResp.JSON200)
	require.NotNil(t, getResp.JSON200.BpmnData, "expected bpmnData in the GetProcessDefinition response")

	require.True(t, strings.Contains(*getResp.JSON200.BpmnData, expectedSnippet),
		"configured task type must be preserved in the deployed BPMN; got %s",
		*getResp.JSON200.BpmnData)
}
