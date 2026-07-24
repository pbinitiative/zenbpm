package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServiceTaskJobFailIncident(t *testing.T) {

	t.Run("Failing job creates incident when error is uncaught", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		failJobForElementId(t, processInstance.Key, "service_task", nil, nil)

		waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Equal(t, "service_task", incidents[0].ElementId)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
	})

	t.Run("Failing job creates incident when error is uncaught and then resolved", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")

		failJobForElementId(t, processInstance.Key, "service_task", nil, nil)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)

		resolveIncident(t, incidents[0].Key)

		incidentsAfterResolve, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidentsAfterResolve, 1)
		require.NotNil(t, incidentsAfterResolve[0].ResolvedAt)
	})

	t.Run("Resolved job reevaluates input mappings with updated process variables", func(t *testing.T) {
		type jobDelivery struct {
			key       int64
			variables map[string]any
			err       error
		}

		processInstance := deployAndCreateUniqueProcessDefinition(t,
			"testdata/service_task/service_task_job_fail_incident_refresh_input_variables.bpmn",
			map[string]any{"source_value": "original"},
		)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		deliveries := make(chan jobDelivery, 2)
		connection, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, connection.Close())
		})

		grpcClient := zenclient.NewGrpc(connection)
		_, err = grpcClient.RegisterWorker(t.Context(), "incident-input-refresh", func(_ context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			variables := make(map[string]any)
			unmarshalErr := json.Unmarshal(job.GetInputVariables(), &variables)
			deliveries <- jobDelivery{key: job.GetKey(), variables: variables, err: unmarshalErr}
			if unmarshalErr != nil {
				return nil, &zenclient.WorkerError{Err: unmarshalErr}
			}
			if variables["worker_value"] != "corrected" {
				return nil, &zenclient.WorkerError{Err: errors.New("input variables are not corrected")}
			}
			return nil, nil
		}, "incident-input-refresh")
		require.NoError(t, err)

		nextDelivery := func() jobDelivery {
			t.Helper()
			select {
			case delivery := <-deliveries:
				return delivery
			case <-time.After(30 * time.Second):
				t.Fatal("job was not delivered to the worker")
				return jobDelivery{}
			}
		}

		firstDelivery := nextDelivery()
		require.NoError(t, firstDelivery.err)
		require.Equal(t, map[string]any{
			"source_value": "original",
			"worker_value": "original",
		}, firstDelivery.variables)

		waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)
		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Nil(t, incidents[0].ResolvedAt)

		require.NoError(t, updateProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"source_value": "corrected",
		}))
		resolveIncident(t, incidents[0].Key)

		secondDelivery := nextDelivery()
		require.NoError(t, secondDelivery.err)
		require.Equal(t, firstDelivery.key, secondDelivery.key)
		require.Equal(t, map[string]any{
			"source_value": "corrected",
			"worker_value": "corrected",
		}, secondDelivery.variables)
		assertFlowElementInputVariables(t, processInstance.Key, "service_task", map[string]any{
			"source_value": "corrected",
		})

		waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateCompleted)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)

		resolvedIncidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, resolvedIncidents, 1)
		require.NotNil(t, resolvedIncidents[0].ResolvedAt)
	})

	t.Run("Resolved multi-instance job maps the original loop variable only once", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t,
			"testdata/service_task/service_task_multi_instance_mapped_loop_variable_incident.bpmn",
			map[string]any{
				"items":         []string{"first"},
				"current_value": "original",
			},
		)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		require.Equal(t, "first-mapped", job.InputVariables["item"])

		failJob(t, job.Key, nil, nil)
		waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task", public.JobStateFailed)
		incidents, err := getProcessInstanceIncidents(t, multiInstanceProcess.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Nil(t, incidents[0].ResolvedAt)

		require.NoError(t, updateProcessInstanceVariables(t, multiInstanceProcess.Key, map[string]any{
			"current_value": "corrected",
		}))
		resolveIncident(t, incidents[0].Key)
		retryJob := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		require.Equal(t, job.Key, retryJob.Key)
		require.Equal(t, "first-mapped", retryJob.InputVariables["item"])
		assertFlowElementInputVariables(t, multiInstanceProcess.Key, "service_task", map[string]any{
			"items":         []any{"first"},
			"current_value": "corrected",
			"item":          "first",
		})

		require.NoError(t, completeJob(t, retryJob.Key, nil))
		waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task", public.JobStateCompleted)
		waitForTwoProcessInstanceStates(t,
			processInstance.Key, zenclient.ProcessInstanceStateCompleted,
			multiInstanceProcess.Key, zenclient.ProcessInstanceStateCompleted,
		)

		resolvedIncidents, err := getProcessInstanceIncidents(t, multiInstanceProcess.Key)
		require.NoError(t, err)
		require.Len(t, resolvedIncidents, 1)
		require.NotNil(t, resolvedIncidents[0].ResolvedAt)
	})
}
