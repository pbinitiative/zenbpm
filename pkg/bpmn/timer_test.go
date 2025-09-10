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

	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"message",
		"message",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)
	time.Sleep((2 * time.Second) + (1 * time.Millisecond))
	assert.Nil(t, err)

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

	msPointer, err = engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"message",
		"message",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.Error(t, err)

	assert.True(t, strings.HasPrefix(cp.CallPath, "task-for-timer"))
	assert.NotContains(t, cp.CallPath, ",")
}
