// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

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
	"strconv"
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
	var instance public.ProcessInstance
	randomID := fmt.Sprintf("test-process-%d", rand.Int63())
	completeType := fmt.Sprintf("%s-complete", randomID)
	definition, err := deployDefinitionWithJobType(t, "long-task-chain.bpmn", randomID, map[string]string{
		"TestType":         randomID,
		"TestCompleteType": completeType,
	})
	assert.NoError(t, err)

	instances := 10
	startedInstances := make([]string, 0, instances)
	for range instances {
		instance, err = createProcessInstance(t, definition.ProcessDefinitionKey, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
		startedInstances = append(startedInstances, instance.Key)
	}

	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	defer conn.Close()

	zenClient := client.NewGrpc(conn)

	count := 0
	completed := 0
	start := time.Now()
	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *client.WorkerError) {
		assert.Equal(t, randomID, job.Type)
		count++
		return map[string]any{
			"testVar": 456,
		}, nil
	}, randomID)
	assert.NoError(t, err)

	_, err = zenClient.RegisterWorker(t.Context(), completeType, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *client.WorkerError) {
		assert.Equal(t, completeType, job.Type)
		completed++
		return map[string]any{
			"testVar": 456,
		}, nil
	}, completeType)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, instance.Key)
		if err != nil {
			return false
		}
		if instances != len(startedInstances) && len(startedInstances) != completed {
			return false
		}
		for _, job := range jobs {
			if job.State != public.JobStateCompleted {
				return false
			}
		}
		fmt.Println(time.Since(start).String())
		return true
	}, 10*time.Second, 10*time.Millisecond, "Process instance should have all jobs completed")
}

func TestGrpcJobStreamFailjob(t *testing.T) {
	var instance public.ProcessInstance
	randomID := fmt.Sprintf("test-process-%d", rand.Int63())
	definition, err := deployDefinitionWithJobType(t, "simple_task.bpmn", randomID, map[string]string{
		"TestType": randomID,
	})
	assert.NoError(t, err)
	instance, err = createProcessInstance(t, definition.ProcessDefinitionKey, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)

	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	defer conn.Close()
	zenClient := client.NewGrpc(conn)

	fmt.Println("registering worker")
	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *client.WorkerError) {
		assert.Equal(t, randomID, job.Type)
		return nil, &client.WorkerError{
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
	}, 10*time.Second, 1*time.Second, "job should have failed")

	// now setup for resolve
	conn.Close()

	conn, err = grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)

	defer conn.Close()
	zenClient = client.NewGrpc(conn)

	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *client.WorkerError) {
		assert.Equal(t, randomID, job.Type)
		return map[string]any{
			"testVar": 456,
		}, nil
	}, randomID)
	assert.NoError(t, err)

	// Now testing that we can resolve
	incidents, err := getProcessInstanceIncidents(t, instance.Key)
	assert.NoError(t, err)
	assert.Len(t, incidents, 1)

	key, err := strconv.ParseInt(incidents[0].Key, 10, 64)
	assert.NoError(t, err)

	resolveIncident(t, key)

	assert.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, instance.Key)

		if err != nil {
			return false
		}

		if len(jobs) != 1 {
			return false
		} else {
			return jobs[0].State == public.JobStateCompleted
		}
	}, 10*time.Second, 10*time.Millisecond, "job should have completed")

	instance, err = getProcessInstance(t, instance.Key)
	assert.NoError(t, err)
	assert.Equal(t, instance.State, public.ProcessInstanceStateCompleted)

}

func deployDefinitionWithJobType(t testing.TB, filename string, processId string, jobTypeMap map[string]string) (public.CreateProcessDefinition200JSONResponse, error) {
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
