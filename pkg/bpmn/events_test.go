package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func Test_creating_a_process_sets_state_to_READY(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")

	// when
	pi, _ := bpmnEngine.CreateInstance(process, nil)
	// then
	assert.Equal(t, runtime.ActivityStateReady, pi.GetState())
}

func Test_running_a_process_sets_state_to_ACTIVE(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")

	// when
	pi, _ := bpmnEngine.CreateInstance(process, nil)
	procInst, _ := bpmnEngine.RunOrContinueInstance(pi.GetInstanceKey())

	// then
	assert.Equal(t, runtime.ActivityStateActive, procInst.GetState(),
		"Since the BPMN contains an intermediate catch event, the process instance must be active and can't complete.")
}

func Test_IntermediateCatchEvent_received_message_completes_the_instance(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi, _ := bpmnEngine.CreateAndRunInstance(process.Key, nil)

	// when
	err := bpmnEngine.PublishEventForInstance(pi.GetInstanceKey(), "globalMsgRef", nil)
	assert.Nil(t, err)
	pi, err = bpmnEngine.RunOrContinueInstance(pi.GetInstanceKey())
	assert.Nil(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi.GetState())
}

func Test_IntermediateCatchEvent_message_can_be_published_before_running_the_instance(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi, _ := bpmnEngine.CreateInstance(process, nil)

	// when
	bpmnEngine.PublishEventForInstance(pi.GetInstanceKey(), "globalMsgRef", nil)
	pi, _ = bpmnEngine.RunOrContinueInstance(pi.GetInstanceKey())

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi.GetState())
}

func Test_IntermediateCatchEvent_a_catch_event_produces_an_active_subscription(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	subscriptions := engineStorage.MessageSubscriptions
	var subscription runtime.MessageSubscription
	for _, sub := range subscriptions {
		if sub.ProcessInstanceKey == pi.Key {
			subscription = sub
			break
		}
	}

	assert.Equal(t, "event-1", subscription.Name)
	assert.Equal(t, "id-1", subscription.ElementId)
	assert.Equal(t, runtime.ActivityStateActive, subscription.MessageState)
}

func Test_IntermediateCatchEvent_multiple_instances_received_message_completes_the_instance(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi1, _ := bpmnEngine.CreateAndRunInstance(process.Key, map[string]interface{}{})
	pi2, _ := bpmnEngine.CreateAndRunInstance(process.Key, map[string]interface{}{})

	// when
	bpmnEngine.PublishEventForInstance(pi1.GetInstanceKey(), "globalMsgRef", nil)
	bpmnEngine.RunOrContinueInstance(pi1.GetInstanceKey())
	bpmnEngine.RunOrContinueInstance(pi2.GetInstanceKey())

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi1.GetState())
	assert.Equal(t, runtime.ActivityStateActive, pi2.GetState())

	// when
	bpmnEngine.PublishEventForInstance(pi2.GetInstanceKey(), "globalMsgRef", nil)
	bpmnEngine.RunOrContinueInstance(pi1.GetInstanceKey())
	bpmnEngine.RunOrContinueInstance(pi2.GetInstanceKey())

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi1.GetState())
	assert.Equal(t, runtime.ActivityStateCompleted, pi2.GetState())
}

func Test_Having_IntermediateCatchEvent_and_ServiceTask_in_parallel_the_process_state_is_maintained(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event-and-parallel-tasks.bpmn")
	instance, _ := bpmnEngine.CreateInstance(process, nil)
	bpmnEngine.NewTaskHandler().Id("task-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("task-2").Handler(cp.TaskHandler)

	// when
	instance, _ = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())

	// when
	bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "event-1", nil)
	instance, _ = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())

	// then
	assert.Equal(t, "task-2,task-1", cp.CallPath)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_multiple_intermediate_catch_events_possible(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events.bpmn")
	bpmnEngine.NewTaskHandler().Id("task1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("task2").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("task3").Handler(cp.TaskHandler)
	instance, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// when
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-2", nil)
	assert.Nil(t, err)
	_, err = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())
	assert.Nil(t, err)

	// then
	assert.Equal(t, "task2", cp.CallPath)
	// then still active, since there's an implicit fork
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_merged_COMPLETED(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-merged.bpmn")
	instance, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// when
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-1", nil)
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-2", nil)
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-3", nil)
	assert.Nil(t, err)
	instance, _ = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_merged_ACTIVE(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-merged.bpmn")
	instance, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// when
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-2", nil)
	assert.Nil(t, err)
	bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())

	// then
	assert.Equal(t, instance.GetState(), runtime.ActivityStateActive)
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_parallel_gateway_COMPLETED(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-parallel.bpmn")
	instance, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// when
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-1", nil)
	assert.Nil(t, err)
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-2", nil)
	assert.Nil(t, err)
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-3", nil)
	assert.Nil(t, err)
	instance, _ = bpmnEngine.RunOrContinueInstance(instance.Key)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted.String(), instance.State.String())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_parallel_gateway_ACTIVE(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-parallel.bpmn")
	instance, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// when
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-2", nil)
	assert.Nil(t, err)
	bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_exclusive_gateway_COMPLETED(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-exclusive.bpmn")
	instance, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// when
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-1", nil)
	assert.Nil(t, err)
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-2", nil)
	assert.Nil(t, err)
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-3", nil)
	assert.Nil(t, err)
	instance, _ = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_exclusive_gateway_ACTIVE(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-exclusive.bpmn")
	instance, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// when
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-event-2", nil)
	assert.Nil(t, err)
	bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func Test_publishing_a_random_message_does_no_harm(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	instance, err := bpmnEngine.CreateInstance(process, nil)
	assert.Nil(t, err)

	// when
	err = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "random-message", nil)
	assert.Nil(t, err)
	instance, err = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())
	assert.Nil(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func Test_eventBasedGateway_just_fires_one_event_and_instance_COMPLETED(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-EventBasedGateway.bpmn")
	instance, _ := bpmnEngine.CreateInstance(process, nil)
	bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)

	// when
	_ = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg-b", nil)
	instance, err := bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())
	assert.Nil(t, err)

	// then
	assert.Equal(t, "task-b", cp.CallPath)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_intermediate_message_catch_event_publishes_variables_into_instance(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-intermediate-message-catch-event.bpmn")
	instance, _ := bpmnEngine.CreateInstance(process, nil)

	// when
	vars := map[string]interface{}{"foo": "bar"}
	_ = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg", vars)
	instance, _ = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
	assert.Equal(t, "bar", instance.GetVariable("foo"))
	assert.Equal(t, "bar", instance.GetVariable("mappedFoo"))
}

func Test_intermediate_message_catch_event_output_mapping_failed(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-intermediate-message-catch-event-broken.bpmn")
	instance, _ := bpmnEngine.CreateInstance(process, nil)

	// when
	_ = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg", nil)
	instance, _ = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())

	// then
	message, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.Key, runtime.ActivityStateFailed)
	assert.Nil(t, err)

	assert.Equal(t, instance.GetState(), runtime.ActivityStateFailed)
	assert.Nil(t, instance.GetVariable("mappedFoo"))
	assert.Equal(t, message[0].GetState(), runtime.ActivityStateFailed)
}
