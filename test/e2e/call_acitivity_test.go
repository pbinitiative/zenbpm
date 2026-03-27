package e2e

import (
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

func TestCallActivity(t *testing.T) {
	var instance zenclient.ProcessInstance
	definition, err := deployGetDefinition(t, "call-activity-with-simple-subprocess.bpmn", "Simple_CallActivity_Process")
	assert.NoError(t, err)

	_, err = deployGetDefinition(t, "simple-simple-sub-process.bpmn", "empty-sub-process")
	assert.NoError(t, err)

	instance, err = createProcessInstance(t, &definition.Key, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)

	assert.Eventually(t, func() bool {
		resp, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(), instance.Key, &zenclient.GetChildProcessInstancesParams{})
		if err != nil || resp.HTTPResponse.StatusCode != http.StatusOK {
			return false
		}
		if len(resp.JSON200.Partitions[0].Items) != 1 {
			return false
		} else {
			return resp.JSON200.Partitions[0].Items[0].State == "completed"
		}
	}, 10*time.Second, 1*time.Second, "job should have failed")
}
