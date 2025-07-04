package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func Test_creating_a_process_sets_state_to_ACTIVE(t *testing.T) {
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
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	err = bpmnEngine.PublishMessageForInstance(t.Context(), pi.GetInstanceKey(), "globalMsgRef", nil)
	assert.NoError(t, err)

	instance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_IntermediateCatchEvent_a_catch_event_produces_an_active_subscription(t *testing.T) {
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

func Test_IntermediateCatchEvent_multiple_instances_received_message_completes_the_instance(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi1, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{})
	assert.NoError(t, err)
	pi2, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{})
	assert.NoError(t, err)

	// when
	bpmnEngine.PublishMessageForInstance(t.Context(), pi1.GetInstanceKey(), "globalMsgRef", nil)

	*pi1, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi1.Key)
	assert.NoError(t, err)
	*pi2, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi2.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi1.GetState())
	assert.Equal(t, runtime.ActivityStateActive, pi2.GetState())

	// when
	bpmnEngine.PublishMessageForInstance(t.Context(), pi2.GetInstanceKey(), "globalMsgRef", nil)

	*pi2, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi2.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi1.GetState())
	assert.Equal(t, runtime.ActivityStateCompleted, pi2.GetState())
}

func Test_Having_IntermediateCatchEvent_and_ServiceTask_in_parallel_the_process_state_is_maintained(t *testing.T) {
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

	// when
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "event-1", nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-2,task-1", cp.CallPath)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_multiple_intermediate_catch_events_possible(t *testing.T) {
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

	// when
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-2", nil)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task2", cp.CallPath)
	// then still active, since there's an implicit fork
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_merged_COMPLETED(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-merged.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-1", nil)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-2", nil)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-3", nil)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)
	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_merged_ACTIVE(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-merged.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-2", nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, instance.GetState(), runtime.ActivityStateActive)
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_parallel_gateway_COMPLETED(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-parallel.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.Nil(t, err)

	// when
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-1", nil)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-2", nil)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-3", nil)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted.String(), instance.State.String())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_parallel_gateway_ACTIVE(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-parallel.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-2", nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_exclusive_gateway_COMPLETED(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-exclusive.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-1", nil)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-2", nil)
	assert.NoError(t, err)
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-3", nil)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_multiple_intermediate_catch_events_implicit_fork_and_exclusive_gateway_ACTIVE(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-exclusive.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-event-2", nil)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func Test_publishing_a_random_message_does_no_harm(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// when
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "random-message", nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.GetState())
}

func TestEventBasedGatewayJustFiresOneEventAndInstanceCOMPLETED(t *testing.T) {
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
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg-b", nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-b", cp.CallPath)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
}

func Test_intermediate_message_catch_event_publishes_variables_into_instance(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-intermediate-message-catch-event.bpmn")
	instance, _ := bpmnEngine.CreateInstance(t.Context(), process, nil)

	// when
	vars := map[string]interface{}{"foo": "bar"}
	err := bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg", vars)
	assert.NoError(t, err)

	// then
	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.GetState())
	assert.Equal(t, "bar", instance.GetVariable("mappedFoo"))
}

func Test_intermediate_message_catch_event_output_mapping_failed(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-intermediate-message-catch-event-broken.bpmn")
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// when
	err = bpmnEngine.PublishMessageForInstance(t.Context(), instance.GetInstanceKey(), "msg", nil)
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
