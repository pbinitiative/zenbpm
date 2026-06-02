package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGrpcJobStream(t *testing.T) {
	var instance zenclient.ProcessInstance
	randomID := fmt.Sprintf("test-process-%d", rand.Int63())
	completeType := fmt.Sprintf("%s-complete", randomID)
	definition, err := deployDefinitionWithJobType(t, "long-task-chain.bpmn", randomID, map[string]string{
		"TestType":         randomID,
		"TestCompleteType": completeType,
	})
	assert.NoError(t, err)

	instances := 10
	startedInstances := make([]int64, 0, instances)
	for range instances {
		instance, err = createProcessInstance(t, &definition.ProcessDefinitionKey, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
		startedInstances = append(startedInstances, instance.Key)
	}

	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	defer conn.Close()

	zenClient := zenclient.NewGrpc(conn)

	// Counters are mutated from worker callback goroutines spawned by the gRPC client,
	// so they must be accessed atomically to avoid data races with the assertions below.
	var count atomic.Int64
	var completed atomic.Int64
	start := time.Now()
	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, randomID, job.GetType())
		count.Add(1)
		return map[string]any{
			"testVar": 456,
		}, nil
	}, randomID)
	assert.NoError(t, err)

	_, err = zenClient.RegisterWorker(t.Context(), completeType, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, completeType, job.GetType())
		completed.Add(1)
		return map[string]any{
			"testVar": 456,
		}, nil
	}, completeType)
	assert.NoError(t, err)

	// Wait until every started instance has completed together with all of its jobs.
	// Checking the instance state (not just the jobs) avoids a false positive in the window between one job completing
	// and the next being created, where all currently persisted jobs would momentarily be completed.
	require.Eventually(t, func() bool {
		for _, instanceKey := range startedInstances {
			inst, err := getProcessInstance(t, instanceKey)
			if err != nil || inst.State != zenclient.ProcessInstanceStateCompleted {
				return false
			}
			jobs, err := getProcessInstanceJobs(t, instanceKey)
			if err != nil {
				return false
			}
			for _, job := range jobs {
				if job.State != public.JobStateCompleted {
					return false
				}
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "all process instances should have all jobs completed")

	fmt.Println(time.Since(start).String())
	assert.Positive(t, count.Load(), "the task worker should have processed at least one job")
	assert.Positive(t, completed.Load(), "the complete worker should have processed at least one job")
}

func TestGrpcJobStreamFailjob(t *testing.T) {
	var instance zenclient.ProcessInstance
	randomID := fmt.Sprintf("test-process-%d", rand.Int63())
	definition, err := deployDefinitionWithJobType(t, "simple_task.bpmn", randomID, map[string]string{
		"TestType": randomID,
	})
	assert.NoError(t, err)
	instance, err = createProcessInstance(t, &definition.ProcessDefinitionKey, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)

	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	defer conn.Close()
	zenClient := zenclient.NewGrpc(conn)

	fmt.Println("registering worker")
	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, randomID, job.GetType())
		return nil, &zenclient.WorkerError{
			Err:       fmt.Errorf("job fail test"),
			ErrorCode: "fail-test",
			Variables: nil,
		}
	}, randomID)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, instance.Key)

		if err != nil {
			return false
		}

		if len(jobs) != 1 {
			return false
		} else {
			return jobs[0].State == public.JobStateFailed
		}
	}, 30*time.Second, 100*time.Millisecond, "job should have failed")

	// now setup for resolve
	conn.Close()

	conn, err = grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)

	defer conn.Close()
	zenClient = zenclient.NewGrpc(conn)

	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, randomID, job.GetType())
		return map[string]any{
			"testVar": 456,
		}, nil
	}, randomID)
	assert.NoError(t, err)

	completeType := fmt.Sprintf("%s-complete", randomID)
	_, err = zenClient.RegisterWorker(t.Context(), completeType, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, completeType, job.Type)
		return map[string]any{
			"testVar": 456,
		}, nil
	}, completeType)
	assert.NoError(t, err)

	// Now testing that we can resolve
	incidents, err := getProcessInstanceIncidents(t, instance.Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)

	resolveIncident(t, incidents[0].Key)

	assert.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, instance.Key)

		if err != nil {
			return false
		}

		if len(jobs) != 1 {
			return false
		}
		return jobs[0].State == public.JobStateCompleted
	}, 30*time.Second, 100*time.Millisecond, "job should have completed")

	assert.Eventually(t, func() bool {
		instance, err = getProcessInstance(t, instance.Key)

		if err != nil {
			return false
		}
		return instance.State == zenclient.ProcessInstanceStateCompleted
	}, 30*time.Second, 100*time.Millisecond, "job should have completed")

}

func deployDefinitionWithJobType(t testing.TB, filename string, processId string, jobTypeMap map[string]string) (public.CreateProcessDefinition201JSONResponse, error) {
	t.Helper()

	result := public.CreateProcessDefinition201JSONResponse{}
	wd, err := os.Getwd()
	if err != nil {
		return result, err
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	loc := path.Join(wd, "pkg", "bpmn", "test-cases", filename)
	file, err := os.ReadFile(loc)
	if err != nil {
		return result, fmt.Errorf("failed to read file: %w", err)
	}
	// this is dumb but works for now
	re, err := regexp.Compile("bpmn:process id=\"\\w*\"")
	assert.NoError(t, err)
	file = re.ReplaceAll(file, fmt.Appendf([]byte{}, "bpmn:process id=\"%s\"", processId))

	for k, v := range jobTypeMap {
		template := `taskDefinition type="%s"`
		oldBytes := &bytes.Buffer{}
		fmt.Fprintf(oldBytes, template, k)
		newBytes := &bytes.Buffer{}
		fmt.Fprintf(newBytes, template, v)
		file = bytes.ReplaceAll(file, oldBytes.Bytes(), newBytes.Bytes())
	}

	// fmt.Println("deployed process", string(file))
	resp, err := app.NewRequest(t).
		WithPath("/v1/process-definitions").
		WithMethod("POST").
		WithMultipartBody(file, filename).
		DoOk()
	if err != nil {
		return result, fmt.Errorf("failed to deploy process definition: %s %w", string(resp), err)
	}
	err = json.Unmarshal(resp, &result)
	if err != nil {
		return result, fmt.Errorf("failed to unmarshal create definition response: %w", err)
	}
	return result, nil
}
