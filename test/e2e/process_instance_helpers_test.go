package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createProcessInstance(t testing.TB, processDefinitionKey *int64, variables map[string]any) (zenclient.ProcessInstance, error) {
	resp, err := app.restClient.CreateProcessInstanceWithResponse(t.Context(), zenclient.CreateProcessInstanceJSONRequestBody{
		BpmnProcessId:        nil,
		BusinessKey:          nil,
		HistoryTimeToLive:    nil,
		ProcessDefinitionKey: processDefinitionKey,
		Variables:            &variables,
	})
	assert.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode())
	assert.NotNil(t, resp.JSON201)
	return *resp.JSON201, nil
}

func getProcessInstance(t testing.TB, key int64) (zenclient.ProcessInstance, error) {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-instances/%d", key)).
		DoOk()
	if err != nil {
		return zenclient.ProcessInstance{}, fmt.Errorf("failed to read process instance: %w", err)
	}
	instance := zenclient.ProcessInstance{}

	err = json.Unmarshal(resp, &instance)
	if err != nil {
		return zenclient.ProcessInstance{}, fmt.Errorf("failed to unmarshal process instance: %w", err)
	}
	return instance, nil
}

func getChildInstances(t testing.TB, key int64) (zenclient.ProcessInstancePage, error) {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-instances?parentProcessInstanceKey=%d&includeChildProcesses=true", key)).
		DoOk()
	if err != nil {
		return zenclient.ProcessInstancePage{}, fmt.Errorf("failed to read process instance: %w", err)
	}
	page := zenclient.ProcessInstancePage{}

	err = json.Unmarshal(resp, &page)
	if err != nil {
		return zenclient.ProcessInstancePage{}, fmt.Errorf("failed to unmarshal process instance: %w", err)
	}
	return page, nil
}

func getProcessInstanceJobs(t testing.TB, key int64) ([]public.Job, error) {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-instances/%d/jobs", key)).
		DoOk()
	if err != nil {
		return nil, fmt.Errorf("failed to read process instance jobs: %w", err)
	}
	jobPage := public.JobPage{}

	err = json.Unmarshal(resp, &jobPage)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal job page: %w", err)
	}
	return jobPage.Items, nil
}

func getProcessInstanceIncidents(t testing.TB, key int64) ([]public.Incident, error) {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-instances/%d/incidents", key)).
		DoOk()
	if err != nil {
		return nil, fmt.Errorf("failed to read process instance incidents: %w", err)
	}
	incidentPage := public.IncidentPage{}

	err = json.Unmarshal(resp, &incidentPage)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal incident page: %w", err)
	}
	return incidentPage.Items, nil
}

func waitForProcessInstanceState(t testing.TB, processInstanceKey int64, expectedState zenclient.ProcessInstanceState) {
	t.Helper()

	assert.Eventually(t, func() bool {
		current, err := getProcessInstance(t, processInstanceKey)
		if err != nil {
			return false
		}
		return current.State == expectedState
	}, 15*time.Second, 100*time.Millisecond, "process instance %d should reach state %s", processInstanceKey, expectedState)
}

func waitForProcessInstanceJobByElementId(t testing.TB, processInstanceKey int64, elementId string) public.Job {
	t.Helper()

	var foundJob public.Job
	require.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if err != nil {
			return false
		}
		for _, job := range jobs {
			if job.ElementId == elementId && job.State == public.JobStateActive {
				foundJob = job
				return true
			}
		}
		return false
	}, 1*time.Second, 100*time.Millisecond, "process instance %d should expose active job for element %s", processInstanceKey, elementId)
	return foundJob
}

func assertProcessInstanceVariables(t testing.TB, processInstanceKey int64, expected map[string]any) {
	t.Helper()

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		instance, err := getProcessInstance(t, processInstanceKey)
		require.NoError(collect, err)
		assert.Equal(collect, expected, instance.Variables)
	}, 5*time.Second, 100*time.Millisecond, "process instance %d variables should match", processInstanceKey)
}

func assertProcessInstanceTokenElements(t testing.TB, processInstanceKey int64, contains []string, notContains []string) {
	t.Helper()

	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		tokens, err := store.GetAllTokensForProcessInstance(t.Context(), processInstanceKey)
		require.NoError(collect, err)

		elementIds := make([]string, 0, len(tokens))
		for _, token := range tokens {
			elementIds = append(elementIds, token.ElementId)
		}

		for _, elementId := range contains {
			assert.Contains(collect, elementIds, elementId)
		}
		for _, elementId := range notContains {
			assert.NotContains(collect, elementIds, elementId)
		}
	}, 5*time.Second, 100*time.Millisecond, "process instance %d should contain %v elements and not contains %v elements", processInstanceKey, contains, notContains)
}

func assertProcessInstanceTokenState(t testing.TB, processInstanceKey int64, elementId string, expectedState bpmnruntime.TokenState) {
	t.Helper()

	require.Eventually(t, func() bool {
		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
		if err != nil {
			return false
		}

		tokens, err := store.GetAllTokensForProcessInstance(t.Context(), processInstanceKey)
		if err != nil {
			return false
		}

		for _, token := range tokens {
			if token.ElementId == elementId && token.State == expectedState {
				return true
			}
		}

		return false
	}, 1*time.Second, 100*time.Millisecond, "process instance %d should contain token for element %s in state %s", processInstanceKey, elementId, expectedState)
}

func assertProcessInstanceHistory(t testing.TB, processInstanceKey int64, expectedHistoryElements []string) {
	t.Helper()

	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
	require.NoError(t, err)

	flowElements, err := store.GetFlowElementInstancesByProcessInstanceKey(t.Context(), processInstanceKey, false)
	require.NoError(t, err)

	elementIds := make([]string, 0, len(flowElements))
	for _, flowElement := range flowElements {
		elementIds = append(elementIds, flowElement.ElementId)
	}

	assert.ElementsMatch(t, expectedHistoryElements, elementIds, fmt.Sprintf("History elements should match, History elements: %v", elementIds))
}

func cleanupOwnedProcessInstance(t testing.TB, processInstanceKey int64) {
	t.Helper()

	response, err := app.restClient.CancelProcessInstanceWithResponse(context.Background(), processInstanceKey)
	assert.NoError(t, err)

	switch response.StatusCode() {
	case http.StatusNoContent, http.StatusConflict:
		return
	default:
		assert.Failf(t, "unexpected cleanup response", "process instance %d cleanup returned %s", processInstanceKey, response.Status())
	}
}
