// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"testing"
)

func cleanUpMessageSubscriptions() {
	engineStorage.MessageSubscriptionPointers = make(map[int64]runtime.MessageSubscriptionPointer)
	engineStorage.MessageSubscriptions = make(map[int64]runtime.MessageSubscription)
}

func Test_creating_a_process_sets_state_to_ACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")

	// when
	pi, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, pi.GetState(),
		"Since the BPMN contains an intermediate catch event, the process instance must be active and can't complete.")
}

func Test_IntermediateCatchEvent_received_message_completes_the_instance(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(t.Context(), "globalMsgRef", "correlation-key-one")
	assert.NoError(t, err)

	// when
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	instance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_IntermediateCatchEvent_a_catch_event_produces_an_active_subscription(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	subscriptions := engineStorage.MessageSubscriptions
	var subscription runtime.MessageSubscription
	for _, sub := range subscriptions {
		if sub.ProcessInstanceKey == pi.Key {
			subscription = sub
			break
		}
	}

	assert.Equal(t, "globalMsgRef", subscription.Name)
	assert.Equal(t, "id-1", subscription.ElementId)
	assert.Equal(t, runtime.ActivityStateActive, subscription.MessageState)
}

func Test_IntermediateCatchEventMultipleInstancesWithSameMessageAndKey(t *testing.T) {
	cleanUpMessageSubscriptions()
	engineStorage.Incidents = make(map[int64]runtime.Incident)

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi1, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{})
	assert.NoError(t, err)
	pi2, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{})
	assert.Error(t, err)

	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(t.Context(), "globalMsgRef", "correlation-key-one")
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.Error(t, err)

	*pi1, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi1.Key)
	assert.NoError(t, err)
	*pi2, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi2.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi1.GetState())
	assert.Equal(t, runtime.ActivityStateFailed, pi2.GetState())

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), pi2.Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(incidents))

	err = bpmnEngine.ResolveIncident(t.Context(), incidents[0].Key)
	assert.NoError(t, err)

	msPointer, err = engineStorage.FindActiveMessageSubscriptionPointer(t.Context(), "globalMsgRef", "correlation-key-one")
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	*pi2, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi2.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi1.GetState())
	assert.Equal(t, runtime.ActivityStateCompleted, pi2.GetState())
}

func Test_Having_IntermediateCatchEvent_and_ServiceTask_in_parallel_the_process_state_is_maintained(t *testing.T) {
	cleanUpMessageSubscriptions()
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event-and-parallel-tasks.bpmn")
	t1H := bpmnEngine.NewTaskHandler().Id("task-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(t1H)
	t2H := bpmnEngine.NewTaskHandler().Id("task-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(t2H)
	instance, _ := bpmnEngine.CreateInstance(t.Context(), process, nil)

	// when
	tokens, err := bpmnEngine.persistence.GetTokensForProcessInstance(t.Context(), instance.Key)
	assert.NoError(t, err)
	err = bpmnEngine.runProcessInstance(t.Context(), instance, tokens)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())

	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"event-1",
		"test",
	)
	assert.NoError(t, err)

	// when
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-2,task-1", cp.CallPath)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_multiple_intermediate_catch_events_possible(t *testing.T) {
	cleanUpMessageSubscriptions()
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events.bpmn")
	h1 := bpmnEngine.NewTaskHandler().Id("task1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h1)
	h2 := bpmnEngine.NewTaskHandler().Id("task2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h2)
	h3 := bpmnEngine.NewTaskHandler().Id("task3").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h3)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-2",
		"key",
	)
	assert.NoError(t, err)

	// when
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task2", cp.CallPath)
	// then still active, since there's an implicit fork
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_merged_COMPLETED(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-merged.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-1",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	msPointer, err = engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-2",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	msPointer, err = engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-3",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)
	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_merged_ACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-merged.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-2",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, instance.GetState(), runtime.ActivityStateActive)
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_parallel_gateway_COMPLETED(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-parallel.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.Nil(t, err)

	// when
	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-1",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	msPointer, err = engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-2",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	msPointer, err = engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-3",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted.String(), instance.State.String())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_parallel_gateway_ACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-parallel.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-2",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_exclusive_gateway_COMPLETED(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-exclusive.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-1",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	msPointer, err = engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-2",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	msPointer, err = engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-3",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_exclusive_gateway_ACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-exclusive.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-event-2",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func Test_publishing_a_random_message_does_no_harm(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// when
	assert.NoError(t, err)
	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"random-message",
		"random-key",
	)
	assert.Error(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.Error(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func TestEventBasedGatewayJustFiresOneEventAndInstanceCOMPLETED(t *testing.T) {
	cleanUpMessageSubscriptions()
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-EventBasedGateway.bpmn")
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)

	// when
	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg-b",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-b", cp.CallPath)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_intermediate_message_catch_event_publishes_variables_into_instance(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-intermediate-message-catch-event.bpmn")
	instance, _ := bpmnEngine.CreateInstance(t.Context(), process, nil)

	// when
	vars := map[string]interface{}{"foo": "bar"}
	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, vars)
	assert.NoError(t, err)

	// then
	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
	assert.Equal(t, "bar", instance.GetVariable("mappedFoo"))
}

func Test_intermediate_message_catch_event_output_mapping_failed(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-intermediate-message-catch-event-broken.bpmn")
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// when
	msPointer, err := engineStorage.FindActiveMessageSubscriptionPointer(
		t.Context(),
		"msg",
		"key",
	)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessage(t.Context(), msPointer, nil)
	assert.Error(t, err)

	// then
	message, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.Key, runtime.ActivityStateFailed)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	assert.Equal(t, instance.GetState(), runtime.ActivityStateFailed)
	assert.Nil(t, instance.GetVariable("mappedFoo"))
	assert.Equal(t, message[0].GetState(), runtime.ActivityStateFailed)
}
