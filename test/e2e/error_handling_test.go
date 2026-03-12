package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestGrpcJobFailErrorBoundaryHandled(t *testing.T) {

	t.Run("GetDecisionInstances_get_by_DmnResourceDefinitionKey", func(t *testing.T) {
		jobType := fmt.Sprintf("error-boundary-handled-%d", time.Now().UnixNano())
		var instance public.ProcessInstance
		conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		assert.NoError(t, err)
		t.Cleanup(func() {
			_ = conn.Close()
		})
		zenClient := zenclient.NewGrpc(conn)
		t.Cleanup(func() {
			if instance.Key == 0 {
				return
			}
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		definition := deployErrorBoundaryDefinition(t, jobType)
		registerFailingWorker(t, zenClient, "error-boundary-handled-client", jobType, "42", "expected boundary error")

		instance = createErrorBoundaryProcessInstance(t, definition.ProcessDefinitionKey)
		waitForProcessInstanceState(t, instance.Key, public.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLen(t, instance.Key, 0)
		assertProcessHistoryElements(t, instance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})
}

func TestGrpcJobFailErrorBoundaryMissCreatesIncident(t *testing.T) {

	jobType := fmt.Sprintf("error-boundary-incident-%d", time.Now().UnixNano())
	var instance public.ProcessInstance
	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})
	zenClient := zenclient.NewGrpc(conn)
	t.Cleanup(func() {
		if instance.Key == 0 {
			return
		}
		cleanupOwnedProcessInstance(t, instance.Key)
	})

	definition := deployErrorBoundaryDefinition(t, jobType)
	registerFailingWorker(t, zenClient, "error-boundary-incident-client", jobType, "99", "expected incident")

	instance = createErrorBoundaryProcessInstance(t, definition.ProcessDefinitionKey)
	waitForProcessInstanceState(t, instance.Key, public.ProcessInstanceStateFailed)
	assertProcessInstanceIncidentsLen(t, instance.Key, 1)
	assertProcessHistoryElements(t, instance.Key, nil, []string{"handled-end", "should-not-happen-end"})
}

func TestGrpcJobFailWithoutErrorCodeCreatesIncident(t *testing.T) {

	jobType := fmt.Sprintf("error-boundary-no-code-%d", time.Now().UnixNano())
	var instance public.ProcessInstance
	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})
	t.Cleanup(func() {
		if instance.Key == 0 {
			return
		}
		cleanupOwnedProcessInstance(t, instance.Key)
	})

	definition := deployErrorBoundaryDefinition(t, jobType)
	registerFailingWorkerWithoutErrorCode(t, conn, "error-boundary-no-code-client", jobType, "expected incident without code")

	instance = createErrorBoundaryProcessInstance(t, definition.ProcessDefinitionKey)
	waitForProcessInstanceState(t, instance.Key, public.ProcessInstanceStateFailed)
	assertProcessInstanceIncidentsLen(t, instance.Key, 1)
	assertProcessHistoryElements(t, instance.Key, nil, []string{"handled-end", "should-not-happen-end"})
}

func deployErrorBoundaryDefinition(t testing.TB, jobType string) public.CreateProcessDefinition201JSONResponse {
	t.Helper()

	definition, err := deployDefinitionWithJobType(t, "service_task_with_error_boundary_event.bpmn", jobType, map[string]string{
		"TestType": jobType,
	})
	assert.NoError(t, err)
	return definition
}

func registerFailingWorker(t testing.TB, client *zenclient.Grpc, clientID string, jobType string, errorCode string, errMessage string) {
	t.Helper()

	_, err := client.RegisterWorker(t.Context(), clientID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, jobType, job.GetType())
		return nil, &zenclient.WorkerError{
			Err:       errors.New(errMessage),
			ErrorCode: errorCode,
			Variables: nil,
		}
	}, jobType)
	assert.NoError(t, err)
}

func registerFailingWorkerWithoutErrorCode(t testing.TB, conn *grpc.ClientConn, clientID string, jobType string, errMessage string) {
	t.Helper()

	client := proto.NewZenBpmClient(conn)
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.New(map[string]string{
		zenclient.MetadataClientID: clientID,
	}))
	stream, err := client.JobStream(ctx)
	assert.NoError(t, err)

	subscriptionType := proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE
	err = stream.Send(&proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Subscription{
			Subscription: &proto.StreamSubscriptionRequest{
				JobType: &jobType,
				Type:    &subscriptionType,
			},
		},
	})
	assert.NoError(t, err)

	go func() {
		response, recvErr := stream.Recv()
		assert.NoError(t, recvErr)
		if recvErr != nil || response.GetJob() == nil {
			return
		}

		message := fmt.Sprintf("failed to complete job: %s", errMessage)
		err = stream.Send(&proto.JobStreamRequest{
			Request: &proto.JobStreamRequest_Fail{
				Fail: &proto.JobFailRequest{
					Key:       response.GetJob().Key,
					Message:   &message,
					ErrorCode: nil,
					Variables: nil,
				},
			},
		})
		assert.NoError(t, err)
	}()
}

func createErrorBoundaryProcessInstance(t testing.TB, definitionKey int64) public.ProcessInstance {
	t.Helper()

	instance, err := createProcessInstance(t, definitionKey, map[string]any{
		"variable_name": "test-value",
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)
	return instance
}

func waitForProcessInstanceState(t testing.TB, processInstanceKey int64, expectedState public.ProcessInstanceState) {
	t.Helper()

	assert.Eventually(t, func() bool {
		current, err := getProcessInstance(t, processInstanceKey)
		if err != nil {
			return false
		}
		return current.State == expectedState
	}, 10*time.Second, 100*time.Millisecond, "process instance %d should reach state %s", processInstanceKey, expectedState)
}

func assertProcessInstanceIncidentsLen(t testing.TB, processInstanceKey int64, expectedLen int) {
	t.Helper()

	incidents, err := getProcessInstanceIncidents(t, processInstanceKey)
	assert.NoError(t, err)
	assert.Len(t, incidents, expectedLen)
}

func assertProcessHistoryElements(t testing.TB, processInstanceKey int64, contains []string, notContains []string) {
	t.Helper()

	history, err := getProcessInstanceHistory(t, processInstanceKey)
	assert.NoError(t, err)
	for _, elementID := range contains {
		assert.Contains(t, history, elementID)
	}
	for _, elementID := range notContains {
		assert.NotContains(t, history, elementID)
	}
}

func getProcessInstanceHistory(t testing.TB, processInstanceKey int64) ([]string, error) {
	response, err := app.restClient.GetHistoryWithResponse(t.Context(), processInstanceKey, &zenclient.GetHistoryParams{})
	if err != nil {
		return nil, fmt.Errorf("failed to get history for process instance %d: %w", processInstanceKey, err)
	}
	if response.StatusCode() != 200 {
		return nil, fmt.Errorf("failed to get history for process instance %d: %s", processInstanceKey, response.Status())
	}
	if response.JSON200 == nil || response.JSON200.Items == nil {
		return []string{}, nil
	}

	elementIDs := make([]string, 0, len(*response.JSON200.Items))
	for _, item := range *response.JSON200.Items {
		elementIDs = append(elementIDs, item.ElementId)
	}
	return elementIDs, nil
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
