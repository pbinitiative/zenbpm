package e2e

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func messageEventTestSuffix() int64 {
	return time.Now().UnixNano()
}

func readBPMNTestCaseFile(t testing.TB, filepathn string) []byte {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)

	loc := filepath.Join(wd, filepathn)

	content, err := os.ReadFile(loc)
	require.NoError(t, err)
	return content
}

func deployBPMNTestCaseContent(t testing.TB, filename string, content []byte) int64 {
	t.Helper()

	response, err := deployDefinitionFromBytes(t, content, filename)
	require.NoError(t, err)

	if response.JSON201 != nil {
		require.NotZero(t, response.JSON201.ProcessDefinitionKey)
		return response.JSON201.ProcessDefinitionKey
	}
	require.NotNil(t, response.JSON200)
	require.NotZero(t, response.JSON200.ProcessDefinitionKey)
	return response.JSON200.ProcessDefinitionKey
}

func waitForProcessInstancesByBPMNProcessID(t testing.TB, bpmnProcessID string, expectedCount int) []zenclient.ProcessInstancesSimple {
	t.Helper()

	var instances []zenclient.ProcessInstancesSimple
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		instances = processInstancesByBPMNProcessID(t, bpmnProcessID)
		assert.Len(collect, instances, expectedCount)
	}, 10*time.Second, 100*time.Millisecond, "expected %d process instances for BPMN process id %s", expectedCount, bpmnProcessID)

	return instances
}

func processInstancesByBPMNProcessID(t testing.TB, bpmnProcessID string) []zenclient.ProcessInstancesSimple {
	t.Helper()

	size := int32(100)
	response, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
		BpmnProcessId: &bpmnProcessID,
		Size:          &size,
	})
	require.NoError(t, err)
	require.NotNil(t, response.JSON200)

	instances := make([]zenclient.ProcessInstancesSimple, 0, response.JSON200.TotalCount)
	for _, partition := range response.JSON200.Partitions {
		instances = append(instances, partition.Items...)
	}
	return instances
}
