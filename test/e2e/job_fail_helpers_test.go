package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func deployProcessDefinitionKey(t *testing.T, filename string, processId string) int64 {
	t.Helper()

	definition, err := deployGetDefinition(t, filename, processId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key)
	return definition.Key
}

func createErrorBoundaryProcessInstanceWithDefaultVariables(t testing.TB, definitionKey int64) zenclient.ProcessInstance {
	return createErrorBoundaryProcessInstanceWithVariables(t, definitionKey, map[string]any{
		"variable_name": "test-value",
	})
}

func createErrorBoundaryProcessInstanceWithVariables(t testing.TB, definitionKey int64, variables map[string]any) zenclient.ProcessInstance {
	t.Helper()

	instance, err := createProcessInstance(t, &definitionKey, variables)
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	return instance
}

func waitForProcessInstanceState(t testing.TB, processInstanceKey int64, expectedState zenclient.ProcessInstanceState) {
	t.Helper()

	assert.Eventually(t, func() bool {
		current, err := getProcessInstance(t, processInstanceKey)
		if err != nil {
			return false
		}
		return current.State == expectedState
	}, 10*time.Second, 100*time.Millisecond, "process instance %d should reach state %s", processInstanceKey, expectedState)
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

	instance, err := getProcessInstance(t, processInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, expected, instance.Variables)
}

func callFailJobViaRest(t testing.TB, jobKey int64, errorCode *string) {
	t.Helper()

	body := zenclient.FailJobJSONRequestBody{}
	if errorCode != nil {
		body.ErrorCode = errorCode
		body.Variables = &map[string]any{
			"variable_from_request": "request_variable",
		}
	}

	response, err := app.restClient.FailJobWithResponse(t.Context(), jobKey, body)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode(), "unexpected fail job response: %s body: %s", response.Status(), string(response.Body))
	require.Nil(t, response.JSON400)
	require.Nil(t, response.JSON502)
}

func callFailActiveJobViaGrpc(t testing.TB, job public.Job, message string, errorCode *string) {
	t.Helper()

	require.NotNil(t, errorCode)

	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	clientId := fmt.Sprintf("grpc-error-boundary-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)

	client := proto.NewZenBpmClient(conn)
	streamCtx := metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		zenclient.MetadataClientID: clientId,
	}))
	stream, err := client.JobStream(streamCtx)
	require.NoError(t, err)

	err = stream.Send(&proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Subscription{
			Subscription: &proto.StreamSubscriptionRequest{
				JobType: ptr.To(job.Type),
				Type:    ptr.To(proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE),
			},
		},
	})
	require.NoError(t, err)

	for {
		response, err := stream.Recv()
		require.NoError(t, err)
		if response.Error != nil {
			require.FailNowf(t, "unexpected job stream error", "job %d stream returned error: %s", job.Key, response.Error.GetMessage())
		}
		if response.Job == nil || response.Job.GetKey() != job.Key {
			continue
		}
		assert.Equal(t, job.Type, response.Job.GetType())
		break
	}

	vars, err := json.Marshal(map[string]any{
		"variable_from_request": "request_variable",
	})
	require.NoError(t, err)

	err = stream.Send(&proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Fail{
			Fail: &proto.JobFailRequest{
				Key:       ptr.To(job.Key),
				Message:   ptr.To(fmt.Sprintf("failed to fail job: %s", errors.New(message).Error())),
				ErrorCode: errorCode,
				Variables: vars,
			},
		},
	})
	require.NoError(t, err)
}

func assertProcessInstanceIncidentsLength(t testing.TB, processInstanceKey int64, expectedLen int) {
	t.Helper()

	incidents, err := getProcessInstanceIncidents(t, processInstanceKey)
	assert.NoError(t, err)
	assert.Len(t, incidents, expectedLen)
}

func assertProcessInstanceErrorSubscriptionCount(t testing.TB, processInstanceKey int64, expectedCreatedCount int, expectedCancelledCount int) {
	t.Helper()

	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
	require.NoError(t, err)

	subscriptions, err := store.FindProcessInstanceErrorSubscriptions(t.Context(), processInstanceKey, bpmnruntime.ErrorStateCreated)
	require.NoError(t, err)
	assert.Len(t, subscriptions, expectedCreatedCount)

	subscriptions, err = store.FindProcessInstanceErrorSubscriptions(t.Context(), processInstanceKey, bpmnruntime.ErrorStateCancelled)
	require.NoError(t, err)
	assert.Len(t, subscriptions, expectedCancelledCount)
}

func assertProcessInstanceErrorSubscriptionsCountIsZero(t testing.TB, processInstanceKey int64) {
	t.Helper()

	assertProcessInstanceErrorSubscriptionCount(t, processInstanceKey, 0, 0)
}

func assertProcessInstanceTokenElements(t testing.TB, processInstanceKey int64, contains []string, notContains []string) {
	t.Helper()

	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
	require.NoError(t, err)

	tokens, err := store.GetAllTokensForProcessInstance(t.Context(), processInstanceKey)
	require.NoError(t, err)

	elementIds := make([]string, 0, len(tokens))
	for _, token := range tokens {
		elementIds = append(elementIds, token.ElementId)
	}

	for _, elementId := range contains {
		assert.Contains(t, elementIds, elementId)
	}
	for _, elementId := range notContains {
		assert.NotContains(t, elementIds, elementId)
	}
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
