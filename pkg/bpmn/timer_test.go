// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn

import (
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func TestEventBasedGatewaySelectsPathWhereTimerOccurs(t *testing.T) {
	cleanUpMessageSubscriptions()
	engineStorage.Timers = make(map[int64]runtime.Timer)
	cp := CallPath{}

	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-timer-event.bpmn")
	mH := bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(mH)
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	_, _ = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	time.Sleep(2 * time.Second)

	assert.Equal(t, "task-for-timer", cp.CallPath)
	for _, timer := range engineStorage.Timers {
		assert.Equal(t, timer.TimerState, runtime.TimerStateTriggered)
	}
}

func TestInvalidTimerWillStopExecutionAndReturnErr(t *testing.T) {
	cp := CallPath{}

	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-invalid-timer-event.bpmn")
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	assert.Equal(t, runtime.ActivityStateFailed, instance.State)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Can't find 'timeDuration' value for INTERMEDIATE_CATCH_EVENT with id=TimerEventDefinition_0he1igl"))
	assert.Equal(t, "", cp.CallPath)
}

func TestEventBasedGatewaySelectsJustOnePath(t *testing.T) {
	cleanUpMessageSubscriptions()
	cp := CallPath{}

	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-timer-event.bpmn")
	mH := bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(mH)
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	_, _ = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	for _, message := range engineStorage.MessageSubscriptions {
		err := bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
		assert.NoError(t, err)
	}
	time.Sleep((2 * time.Second) + (1 * time.Millisecond))

	assert.True(t, strings.HasPrefix(cp.CallPath, "task-for-message"))
	assert.NotContains(t, cp.CallPath, ",")

	cp.CallPath = ""
	_, _ = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	assert.Eventually(t, func() bool {
		if strings.HasPrefix(cp.CallPath, "task-for-timer") {
			return true
		}
		return false
	}, (5*time.Second)+(1*time.Millisecond), 500*time.Millisecond)

	for _, message := range engineStorage.MessageSubscriptions {
		if message.State != runtime.ActivityStateActive {
			continue
		}
		err := bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
		assert.NoError(t, err)
	}

	assert.True(t, strings.HasPrefix(cp.CallPath, "task-for-timer"))
	assert.NotContains(t, cp.CallPath, ",")
}

func Test_interrupting_boundary_event_timer_catch_triggered(t *testing.T) {
	// 1) After process start the message subscription bound to the boundary event should be created
	//    - process should be active
	//    - message subscription should be active
	// 2) After process message is thrown
	//    - the job and
	//    - any other boundary events subscriptions should be cancelled and
	//    - flow outgoing from the boundary should be taken

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/timer-boundary-event-interrupting.bpmn")
	// when
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// then

	jobs := findActiveJobsForProcessInstance(instance.Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

	timers, err := bpmnEngine.persistence.FindTimersTo(t.Context(), time.Now().Add(2*time.Second))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(timers))

	// when

	time.Sleep(2 * time.Second)
	// then
	timers, err = bpmnEngine.persistence.FindTimersTo(t.Context(), time.Now())
	assert.NoError(t, err)
	assert.Equal(t, 0, len(timers))

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())

	jobs = findActiveJobsForProcessInstance(instance.Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))

}

func findActiveJobsForProcessInstance(processInstanceKey int64, jobType string) []runtime.Job {
	foundServiceJobs := make([]runtime.Job, 0)
	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == processInstanceKey && job.Type == jobType && job.State == runtime.ActivityStateActive {
			foundServiceJobs = append(foundServiceJobs, job)
			break
		}
	}
	return foundServiceJobs
}
