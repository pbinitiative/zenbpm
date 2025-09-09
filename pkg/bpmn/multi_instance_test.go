// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn

import (
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func Test_multi_instance_service_task(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi-instance-service-task.bpmn")
	assert.NoError(t, err)

	jobCompletionCount := 0
	jobKeysToComplete := make([]int64, 3)

	jobHandler := func(job ActivatedJob) {
		inputElementValue := job.Variable("inputElementName")

		job.SetVariable("out", inputElementValue)
		job.Complete()
		jobKeysToComplete[jobCompletionCount] = job.Key()
		jobCompletionCount++
		fmt.Printf("Job completed for input: %v\n", inputElementValue)
	}

	job1Handler := bpmnEngine.NewTaskHandler().Type("job1").Handler(jobHandler)
	defer bpmnEngine.RemoveHandler(job1Handler)

	pi, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	currentPi, err := bpmnEngine.FindProcessInstance(pi.Key)
	assert.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateCompleted, currentPi.GetState(),
		"Process should be completed")

	assert.Equal(t, 3, jobCompletionCount, "3 jobs should have completed")

}
func Test_multi_instance_service_task_with_error_handling(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi-instance-service-task.bpmn")
	assert.NoError(t, err)
	jobCompletionCount := 0
	jobHandler := func(job ActivatedJob) {
		inputElementValue := job.Variable("inputElementName")

		// Simulate error handling - fail if input is 2
		if fmt.Sprintf("%s", inputElementValue) == "2" {
			job.Fail("Simulated error for input 2")
			return
		}

		job.SetVariable("out", inputElementValue)
		job.Complete()
		jobCompletionCount++
		fmt.Printf("Job completed for input: %v\n", inputElementValue)
	}

	job1Handler := bpmnEngine.NewTaskHandler().Type("job1").Handler(jobHandler)
	defer bpmnEngine.RemoveHandler(job1Handler)

	pi, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	currentPi, err := bpmnEngine.FindProcessInstance(pi.Key)
	assert.NoError(t, err)

	// Process should still be active (not completed) because one job failed
	assert.NotEqual(t, runtime.ActivityStateCompleted, currentPi.GetState(),
		"Process should not complete when a job fails")

	// Only 2 jobs should have completed successfully
	assert.Equal(t, 2, jobCompletionCount, "Only 2 jobs should have completed (input 1 and 3)")

}
