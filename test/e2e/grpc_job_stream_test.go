package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/client"
	"github.com/pbinitiative/zenbpm/pkg/client/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGrpcJobStream(t *testing.T) {
	// t.SkipNow()
	var instance public.ProcessInstance
	randomID := fmt.Sprintf("test-process-%d", rand.Int63())
	definition, err := deployDefinitionWithJobType(t, "simple_task.bpmn", randomID, randomID)
	assert.NoError(t, err)
	instance, err = createProcessInstance(t, definition.ProcessDefinitionKey, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)

	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	fmt.Println("conn", conn.GetState())
	assert.NoError(t, err)
	zenClient := client.NewGrpc(conn)

	fmt.Println("registering worker")
	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, error) {
		assert.Equal(t, randomID, job.Type)
		return map[string]any{
			"testVar": 456,
		}, nil
	}, randomID)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, instance.Key)
		fmt.Println("instance jobs", jobs)
		if err != nil {
			return false
		}
		if len(jobs) != 0 {
			return false
		}
		return true
	}, 10*time.Second, 1*time.Second, "Process instance should have all jobs completed")
}

func TestGrpcJobStreamFailjob(t *testing.T) {
	// t.SkipNow()
	var instance public.ProcessInstance
	randomID := fmt.Sprintf("test-process-%d", rand.Int63())
	definition, err := deployDefinitionWithJobType(t, "simple_task.bpmn", randomID, randomID)
	assert.NoError(t, err)
	instance, err = createProcessInstance(t, definition.ProcessDefinitionKey, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)

	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	fmt.Println("conn", conn.GetState())
	assert.NoError(t, err)
	zenClient := client.NewGrpc(conn)

	fmt.Println("registering worker")
	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, error) {
		assert.Equal(t, randomID, job.Type)
		return map[string]any{
			"testVar": 456,
		}, fmt.Errorf("Testing failed job")
	}, randomID)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, instance.Key)
		fmt.Println("instance jobs", jobs)
		if err != nil {
			return false
		}
		if len(jobs) != 0 {
			return false
		}
		return true
	}, 10*time.Second, 1*time.Second, "Process instance should have all jobs completed")
}

func deployDefinitionWithJobType(t testing.TB, filename string, processId string, jobType string) (public.CreateProcessDefinition200JSONResponse, error) {
	result := public.CreateProcessDefinition200JSONResponse{}
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

	re, err = regexp.Compile("taskDefinition type=\"\\w*\"")
	assert.NoError(t, err)
	file = re.ReplaceAll(file, fmt.Appendf([]byte{}, "taskDefinition type=\"%s\"", jobType))

	resp, err := app.NewRequest(t).
		WithPath("/v1/process-definitions").
		WithMethod("POST").
		WithBody(file).
		WithHeader("Content-Type", "application/xml").
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
