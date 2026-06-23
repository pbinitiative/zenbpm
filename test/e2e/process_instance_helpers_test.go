package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Deprecated: use deployAndGetUniqueDefinition instead.
func deployGetUniqueDefinition(t *testing.T, filename string) (zenclient.ProcessDefinitionSimple, error) {
	t.Helper()

	uniqueDefinitionName, err := deployUniqueDefinition(t, filename)
	assert.NoError(t, err)

	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)

	var definition zenclient.ProcessDefinitionSimple
	for _, def := range definitions {
		if def.BpmnProcessId == *uniqueDefinitionName {
			definition = def
			break
		}
	}
	return definition, err
}

func deployAndGetUniqueProcessDefinition(t *testing.T, filePath string) zenclient.ProcessDefinitionSimple {
	t.Helper()

	deployedProcessDefinition := deployUniqueProcessDefinition(t, filePath)
	definitions, err := listProcessDefinitions(t)
	require.NoError(t, err)

	var processDefinition zenclient.ProcessDefinitionSimple
	for _, def := range definitions {
		if def.BpmnProcessId == *deployedProcessDefinition {
			processDefinition = def
			break
		}
	}

	return processDefinition
}

func deployProcessDefinitionContent(t testing.TB, filename string, content []byte) *zenclient.CreateProcessDefinitionResponse {
	t.Helper()

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	part, err := writer.CreateFormFile("resource", filename)
	require.NoError(t, err, fmt.Errorf("failed to create form file: %w", err))

	_, err = part.Write(content)
	require.NoError(t, err, fmt.Errorf("failed to write file to multipart form: %w", err))

	err = writer.Close()
	require.NoError(t, err, fmt.Errorf("failed to close multipart writer: %w", err))

	resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), writer.FormDataContentType(), &requestBody)
	require.NoError(t, err, fmt.Errorf("failed to deploy process definition: %w", err))

	isErrorResponse := resp.StatusCode() >= 400
	require.False(t, isErrorResponse, "failed to deploy process definition: %s", string(resp.Body))

	definition := public.CreateProcessDefinition201JSONResponse{}
	err = json.Unmarshal(resp.Body, &definition)
	require.NoError(t, err, fmt.Errorf("failed to unmarshal create definition response: %w", err))

	return resp
}

func deployAndCreateUniqueProcessDefinition(t *testing.T, filePath string, variables map[string]any) zenclient.ProcessInstance {
	t.Helper()

	deployedProcessDefinition := deployAndGetUniqueProcessDefinition(t, filePath)
	processInstance, err := createProcessInstance(t, &deployedProcessDefinition.Key, variables)
	require.NoError(t, err)

	return processInstance
}

func deployProcessDefinitionKey(t *testing.T, filename string, processId string) int64 {
	t.Helper()

	response, err := deployDefinition(t, filename)
	require.NoError(t, err)
	if response.JSON201 != nil {
		require.NotZero(t, response.JSON201.ProcessDefinitionKey)
		return response.JSON201.ProcessDefinitionKey
	}
	if response.JSON200 != nil {
		require.NotZero(t, response.JSON200.ProcessDefinitionKey)
		return response.JSON200.ProcessDefinitionKey
	}

	definition, err := deployGetDefinition(t, filename, processId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key)
	return definition.Key
}

func createProcessInstance(t testing.TB, processDefinitionKey *int64, variables map[string]any) (zenclient.ProcessInstance, error) {
	t.Helper()
	resp, err := app.restClient.CreateProcessInstanceWithResponse(t.Context(), zenclient.CreateProcessInstanceJSONRequestBody{
		BpmnProcessId:        nil,
		BusinessKey:          nil,
		HistoryTimeToLive:    nil,
		ProcessDefinitionKey: processDefinitionKey,
		Variables:            &variables,
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode())
	require.NotNil(t, resp.JSON201)

	return *resp.JSON201, nil
}

func createProcessInstanceWithDefaultVariables(t testing.TB, definitionKey int64) zenclient.ProcessInstance {
	return createProcessInstanceWithVariables(t, definitionKey, map[string]any{
		"variable_name": "test-value",
	})
}

func createProcessInstanceWithVariables(t testing.TB, definitionKey int64, variables map[string]any) zenclient.ProcessInstance {
	t.Helper()

	instance, err := createProcessInstance(t, &definitionKey, variables)
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	return instance
}

func getProcessInstance(t testing.TB, key int64) (zenclient.ProcessInstance, error) {
	t.Helper()

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
	t.Helper()

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

func assertChildProcessInstancesCount(t testing.TB, parentProcessInstanceKey int64, expectedCount int) {
	t.Helper()

	require.Eventually(t, func() bool {
		page, err := getChildInstances(t, parentProcessInstanceKey)

		if err != nil {
			return false
		}

		if len(page.Partitions) == 0 || len(page.Partitions[0].Items) != expectedCount {
			return false
		}

		return true
	}, 1*time.Second, 100*time.Millisecond, "process instance %d should create a child process instances", parentProcessInstanceKey)
}

func waitForChildProcessInstance(t testing.TB, parentProcessInstanceKey int64, childIndex int) zenclient.ProcessInstancesSimple {
	t.Helper()

	var child zenclient.ProcessInstancesSimple
	require.Eventually(t, func() bool {
		page, err := getChildInstances(t, parentProcessInstanceKey)
		if err != nil {
			return false
		}
		if len(page.Partitions) == 0 || len(page.Partitions[0].Items) <= childIndex {
			return false
		}
		child = page.Partitions[0].Items[childIndex]
		return true
	}, 15*time.Second, 100*time.Millisecond, "process instance %d should create a child process instances with index: %d", parentProcessInstanceKey, childIndex)
	return child
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

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		current, err := getProcessInstance(t, processInstanceKey)
		if !assert.NoError(c, err) {
			return
		}
		assert.Equal(c, expectedState, current.State, "process instance %d should reach state %s", processInstanceKey, expectedState)
	}, 15*time.Second, 100*time.Millisecond, "process instance %d should reach state %s", processInstanceKey, expectedState)
}

func waitForTwoProcessInstanceStates(t testing.TB, firstKey int64, firstExpected zenclient.ProcessInstanceState, secondKey int64, secondExpected zenclient.ProcessInstanceState) {
	t.Helper()

	assert.Eventually(t, func() bool {
		first, err := getProcessInstance(t, firstKey)
		if err != nil {
			return false
		}
		second, err := getProcessInstance(t, secondKey)
		if err != nil {
			return false
		}
		return first.State == firstExpected && second.State == secondExpected
	}, 10*time.Second, 100*time.Millisecond, "process instances %d and %d should reach states %s and %s", firstKey, secondKey, firstExpected, secondExpected)
}

func assertProcessInstanceVariables(t testing.TB, processInstanceKey int64, expected map[string]any) {
	t.Helper()

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		instance, err := getProcessInstance(t, processInstanceKey)
		require.NoError(collect, err)
		require.Equal(collect, expected, instance.Variables)
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

func assertProcessInstanceIncidentsLength(t testing.TB, processInstanceKey int64, expectedLen int) {
	t.Helper()

	assert.Eventually(t, func() bool {
		incidents, err := getProcessInstanceIncidents(t, processInstanceKey)
		if err != nil {
			return false
		}
		return len(incidents) == expectedLen
	}, 5*time.Second, 50*time.Millisecond,
		"process instance %d should have %d incidents",
		processInstanceKey, expectedLen)
}

func assertProcessInstanceErrorSubscriptionCount(t testing.TB, processInstanceKey int64, expectedCreatedCount int, expectedCancelledCount int) {
	t.Helper()

	assert.Eventually(t, func() bool {
		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
		if err != nil {
			return false
		}

		createdSubs, err := store.FindProcessInstanceErrorSubscriptions(t.Context(), processInstanceKey, bpmnruntime.ErrorStateCreated)
		if err != nil || len(createdSubs) != expectedCreatedCount {
			return false
		}

		cancelledSubs, err := store.FindProcessInstanceErrorSubscriptions(t.Context(), processInstanceKey, bpmnruntime.ErrorStateCancelled)
		if err != nil || len(cancelledSubs) != expectedCancelledCount {
			return false
		}

		return true
	}, 5*time.Second, 50*time.Millisecond,
		"process instance %d should have %d created and %d cancelled error subscriptions",
		processInstanceKey, expectedCreatedCount, expectedCancelledCount)
}

func assertProcessInstanceErrorSubscriptionsCountIsZero(t testing.TB, processInstanceKey int64) {
	t.Helper()

	assertProcessInstanceErrorSubscriptionCount(t, processInstanceKey, 0, 0)
}

func assertProcessInstanceTokenState(t testing.TB, processInstanceKey int64, elementId string, expectedState bpmnruntime.TokenState) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
		if !assert.NoError(collect, err) {
			return
		}

		tokens, err := store.GetAllTokensForProcessInstance(t.Context(), processInstanceKey)
		if !assert.NoError(collect, err) {
			return
		}

		for _, token := range tokens {
			if token.ElementId == elementId {
				assert.Equal(collect, expectedState, token.State, "process instance %d should contain token for element %s in state %s", processInstanceKey, elementId, expectedState)
				return
			}
		}

		assert.Fail(collect, "Token not found", "process instance %d does not expose token on element %s", processInstanceKey, elementId)
	}, 5*time.Second, 100*time.Millisecond, "process instance %d should contain token for element %s in state %s", processInstanceKey, elementId, expectedState)
}

func assertProcessInstanceTokenStates(t testing.TB, processInstanceKey int64, elementId string, expectedState bpmnruntime.TokenState, expectedCount int) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
		if !assert.NoError(collect, err) {
			return
		}

		tokens, err := store.GetAllTokensForProcessInstance(t.Context(), processInstanceKey)
		if !assert.NoError(collect, err) {
			return
		}

		matchedStates := make([]bpmnruntime.TokenState, 0)
		for _, token := range tokens {
			if token.ElementId == elementId {
				matchedStates = append(matchedStates, token.State)
			}
		}

		assert.Len(collect, matchedStates, expectedCount, "process instance %d should expose %d tokens for element %s", processInstanceKey, expectedCount, elementId)
		for i, state := range matchedStates {
			assert.Equal(collect, expectedState, state, "process instance %d token %d for element %s should be in state %s", processInstanceKey, i, elementId, expectedState)
		}
	}, 5*time.Second, 100*time.Millisecond, "process instance %d should contain %d tokens for element %s in state %s", processInstanceKey, expectedCount, elementId, expectedState)
}

func assertProcessInstanceTokenCount(t testing.TB, processInstanceKey int64, elementId string, expectedCount int) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
		if !assert.NoError(collect, err) {
			return
		}

		tokens, err := store.GetAllTokensForProcessInstance(t.Context(), processInstanceKey)
		if !assert.NoError(collect, err) {
			return
		}

		matchedCount := 0
		for _, token := range tokens {
			if token.ElementId == elementId {
				matchedCount++
			}
		}

		assert.Equal(collect, expectedCount, matchedCount, "process instance %d should expose %d tokens for element %s", processInstanceKey, expectedCount, elementId)
	}, 5*time.Second, 100*time.Millisecond, "process instance %d should contain %d tokens for element %s", processInstanceKey, expectedCount, elementId)
}

func assertProcessInstanceIsCompleted(t testing.TB, processInstanceKey int64, tokenElementId string) {
	t.Helper()

	waitForProcessInstanceState(t, processInstanceKey, zenclient.ProcessInstanceStateCompleted)
	assertProcessInstanceTokenState(t, processInstanceKey, tokenElementId, bpmnruntime.TokenStateCompleted)
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

func assertExactProcessInstanceHistory(t testing.TB, processInstanceKey int64, expectedHistoryElements []string) {
	t.Helper()

	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
	require.NoError(t, err)

	flowElements, err := store.GetFlowElementInstancesByProcessInstanceKey(t.Context(), processInstanceKey, false)
	require.NoError(t, err)

	require.Len(t, flowElements, len(expectedHistoryElements))

	sort.Slice(flowElements, func(i, j int) bool {
		if flowElements[i].CreatedAt.Equal(flowElements[j].CreatedAt) {
			return flowElements[i].Key < flowElements[j].Key
		}
		return flowElements[i].CreatedAt.Before(flowElements[j].CreatedAt)
	})

	elementIds := make([]string, 0, len(flowElements))
	for _, flowElement := range flowElements {
		elementIds = append(elementIds, flowElement.ElementId)
	}

	for i := 0; i < len(expectedHistoryElements); i++ {
		require.Equal(t, expectedHistoryElements[i], elementIds[i], fmt.Sprintf("History elements should match, History elements: %v", elementIds))
	}
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

func requireFirstActiveInstanceWithSingleToken(t testing.TB, processInstances *zenclient.GetProcessInstancesResponse) (zenclient.ProcessInstancesSimple, storage.Storage) {
	t.Helper()
	fetchedProcessInstance := processInstances.JSON200.Partitions[0].Items[0]
	assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedProcessInstance.State)

	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(fetchedProcessInstance.Key))
	require.NoError(t, err)
	tokens, err := store.GetAllTokensForProcessInstance(t.Context(), fetchedProcessInstance.Key)
	require.NoError(t, err)
	assert.Equal(t, 1, len(tokens))

	return fetchedProcessInstance, store
}

func waitForChildProcessInstanceByType(t testing.TB, parentProcessInstanceKey int64, processType zenclient.ProcessInstanceProcessType) zenclient.ProcessInstancesSimple {
	t.Helper()

	var child zenclient.ProcessInstancesSimple
	require.Eventually(t, func() bool {
		page, err := getChildInstances(t, parentProcessInstanceKey)
		if err != nil {
			return false
		}
		if len(page.Partitions) == 0 {
			return false
		}
		for _, item := range page.Partitions[0].Items {
			if item.ProcessType == processType {
				child = item
				return true
			}
		}
		return false
	}, 1*time.Second, 100*time.Millisecond, "process instance %d should create a %s child process instance", parentProcessInstanceKey, processType)
	return child
}

func getFlowElementInstancesByElementId(t testing.TB, processInstanceKey int64, elementId string) []bpmnruntime.FlowElementInstance {
	t.Helper()

	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
	require.NoError(t, err)

	flowElementInstances, err := store.GetFlowElementInstancesByProcessInstanceKey(t.Context(), processInstanceKey, false)
	require.NoError(t, err)

	matchedFlowElementInstances := make([]bpmnruntime.FlowElementInstance, 0)
	for _, flowElementInstance := range flowElementInstances {
		if flowElementInstance.ElementId == elementId {
			matchedFlowElementInstances = append(matchedFlowElementInstances, flowElementInstance)
		}
	}
	sort.Slice(matchedFlowElementInstances, func(i, j int) bool {
		if matchedFlowElementInstances[i].CreatedAt.Equal(matchedFlowElementInstances[j].CreatedAt) {
			return matchedFlowElementInstances[i].Key < matchedFlowElementInstances[j].Key
		}
		return matchedFlowElementInstances[i].CreatedAt.Before(matchedFlowElementInstances[j].CreatedAt)
	})
	return matchedFlowElementInstances
}

func assertFlowElementInputVariablesAt(t testing.TB, processInstanceKey int64, elementId string, iteration int, expected map[string]any) {
	t.Helper()

	instances := getFlowElementInstancesByElementId(t, processInstanceKey, elementId)

	require.Greaterf(t, len(instances), iteration,
		"expected at least %d flow element instance(s) for %s on process instance %d, got %d", iteration+1, elementId, processInstanceKey, len(instances))
	require.Equalf(t, expected, instances[iteration].InputVariables,
		"input variables of iteration %d on element %s mismatch", iteration, elementId)
}
