package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type messageEndEventGrpcDelivery struct {
	job       *proto.WaitingJob
	variables map[string]any
	err       error
}

func TestMessageEndEventFlow(t *testing.T) {
	t.Run("Publication job is delivered to the gRPC worker only after the event is reached", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, messageEndEventPayloadPath)
		businessKey := uniqueMessageEndEventBusinessKey("single-publication")
		receivedJob := make(chan messageEndEventGrpcDelivery, 1)

		conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, conn.Close())
		})

		grpcClient := zenclient.NewGrpc(conn)
		worker, err := grpcClient.RegisterWorker(
			t.Context(),
			businessKey,
			func(_ context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
				variables := map[string]any{}
				decodeErr := json.Unmarshal(job.GetInputVariables(), &variables)
				receivedJob <- messageEndEventGrpcDelivery{
					job:       job,
					variables: variables,
					err:       decodeErr,
				}
				return map[string]any{}, nil
			},
			messageEndEventPublicationJob,
		)
		require.NoError(t, err)
		require.NotNil(t, worker)

		instance := createMessageEndEventProcessInstance(t, definitionKey, businessKey, messageEndEventPayloadVariables(
			businessKey,
			"4f0ed402-4281-40f2-b724-4c6e4017b315",
			"order-1001",
			"Žluťoučký kůň 🐎 / 東京",
			"2026-07-16T08:45:30+02:00",
		))
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "prepare_publication")
		assertProcessInstanceHasNoActiveJobByElementId(t, instance.Key, messageEndEventElementID)

		completeJobForElementId(t, instance.Key, "prepare_publication", nil)

		var delivery messageEndEventGrpcDelivery
		select {
		case delivery = <-receivedJob:
		case <-time.After(10 * time.Second):
			t.Fatal("message end event job was not delivered to the gRPC worker")
		}

		require.NoError(t, delivery.err)
		require.NotNil(t, delivery.job)
		require.Equal(t, messageEndEventPublicationJob, delivery.job.GetType())
		require.Equal(t, messageEndEventElementID, delivery.job.GetElementId())
		require.Equal(t, instance.Key, delivery.job.GetInstanceKey())
		require.Equal(t, "order-1001", delivery.variables["orderId"])
		require.Equal(t, businessKey, delivery.variables["correlationId"])

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		completedJob := waitForProcessInstanceJobByElementId(t, instance.Key, messageEndEventElementID, public.JobStateCompleted)
		require.Equal(t, delivery.job.GetKey(), completedJob.Key)
		assertExactlyOneMessageEndEventJob(t, instance.Key, messageEndEventElementID)
		assertProcessInstanceTokenState(t, instance.Key, messageEndEventElementID, runtime.TokenStateCompleted)
	})

	t.Run("Completing the publication job ends the branch and leaves no active activity", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, messageEndEventPayloadPath)
		businessKey := uniqueMessageEndEventBusinessKey("branch-completion")
		instance := createMessageEndEventProcessInstance(t, definitionKey, businessKey, messageEndEventPayloadVariables(
			businessKey,
			"58124e41-504d-4039-8518-44e92e8a41d0",
			"order-branch",
			"Branch Customer",
			"2026-07-16T09:00:00Z",
		))
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		completeJobForElementId(t, instance.Key, "prepare_publication", nil)
		publicationJob := waitForProcessInstanceActiveJobByElementId(t, instance.Key, messageEndEventElementID)
		require.NoError(t, completeJob(t, publicationJob.Key, nil))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, instance.Key, messageEndEventElementID, runtime.TokenStateCompleted)
		assertProcessInstanceHasNoActiveJobs(t, instance.Key)
		completedJob := waitForProcessInstanceJobByElementId(t, instance.Key, messageEndEventElementID, public.JobStateCompleted)
		require.Equal(t, publicationJob.Key, completedJob.Key)
		assertExactlyOneMessageEndEventJob(t, instance.Key, messageEndEventElementID)
	})

	t.Run("Completing a message end event does not terminate another parallel branch", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, messageEndEventParallelPath)
		businessKey := uniqueMessageEndEventBusinessKey("parallel")
		instance := createMessageEndEventProcessInstance(t, definitionKey, businessKey, map[string]any{
			"businessKey": businessKey,
			"messageId":   "7d8ae85b-8b63-4944-b952-d35662a4ad1d",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		publicationJob := waitForProcessInstanceActiveJobByElementId(t, instance.Key, messageEndEventElementID)
		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "parallel_task")
		require.Equal(t, "orders.completed.parallel", publicationJob.Type)
		require.NoError(t, completeJob(t, publicationJob.Key, nil))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, messageEndEventElementID, runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, instance.Key, "parallel_task", runtime.TokenStateWaiting)
		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "parallel_task")

		completeJobForElementId(t, instance.Key, "parallel_task", nil)
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, instance.Key, "parallel_end_event", runtime.TokenStateCompleted)
	})

	t.Run("Message end event inside a subprocess lets the parent continue", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, messageEndEventSubprocessPath)
		businessKey := uniqueMessageEndEventBusinessKey("subprocess")
		instance := createMessageEndEventProcessInstance(t, definitionKey, businessKey, map[string]any{
			"businessKey": businessKey,
			"messageId":   "4880d126-ab74-4899-8f34-8a8bb145306b",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		child := waitForChildProcessInstance(t, instance.Key, 0)
		publicationJob := waitForProcessInstanceActiveJobByElementId(t, child.Key, "subprocess_message_end")
		require.Equal(t, "orders.completed.subprocess", publicationJob.Type)
		assertProcessInstanceTokenState(t, instance.Key, "message_subprocess", runtime.TokenStateWaiting)

		require.NoError(t, completeJob(t, publicationJob.Key, nil))

		waitForProcessInstanceState(t, child.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, child.Key, "subprocess_message_end", runtime.TokenStateCompleted)
		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "parent_followup")
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceHistory(t, instance.Key, []string{
			"parent_start",
			"Flow_parent_start_to_subprocess",
			"message_subprocess",
			"Flow_subprocess_to_followup",
			"parent_followup",
		})

		completeJobForElementId(t, instance.Key, "parent_followup", nil)
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, instance.Key, "parent_end", runtime.TokenStateCompleted)
	})
}
