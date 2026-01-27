package bpmn

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func cleanUpMessageSubscriptions() {
	engineStorage.MessageSubscriptions = make(map[int64]runtime.MessageSubscription)
}

func TestCreatingAProcessSetsStateToACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")

	// when
	pi, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, pi.ProcessInstance().GetState(),
		"Since the BPMN contains an intermediate catch event, the process instance must be active and can't complete.")
}

func TestIntermediateCatchEventReceivedMessageCompletesTheInstance(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "globalMsgRef" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	// then
	instance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestIntermediateCatchEventACatchEventProducesAnActiveSubscription(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	subscriptions := engineStorage.MessageSubscriptions
	var subscription runtime.MessageSubscription
	for _, sub := range subscriptions {
		if sub.ProcessInstanceKey == pi.ProcessInstance().Key {
			subscription = sub
			break
		}
	}

	assert.Equal(t, "globalMsgRef", subscription.Name)
	assert.Equal(t, "id-1", subscription.ElementId)
	assert.Equal(t, runtime.ActivityStateActive, subscription.State)
}

func TestIntermediateCatchEventMultipleInstancesWithSameMessageAndKey(t *testing.T) {
	cleanUpMessageSubscriptions()
	engineStorage.Incidents = make(map[int64]runtime.Incident)

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	pi1, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{})
	assert.NoError(t, err)
	pi2, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{})
	assert.Error(t, err)

	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "globalMsgRef" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.Error(t, err)
		}
	}

	pi1, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi1.ProcessInstance().Key)
	assert.NoError(t, err)
	pi2, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi2.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi1.ProcessInstance().GetState())
	assert.Equal(t, runtime.ActivityStateFailed, pi2.ProcessInstance().GetState())

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), pi2.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(incidents))

	err = bpmnEngine.ResolveIncident(t.Context(), incidents[0].Key)
	assert.NoError(t, err)

	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "globalMsgRef" && message.State == runtime.ActivityStateActive {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	pi2, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi2.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi1.ProcessInstance().GetState())
	assert.Equal(t, runtime.ActivityStateCompleted, pi2.ProcessInstance().GetState())
}

func TestHavingIntermediateCatchEventAndServiceTaskInParallelTheProcessStateIsMaintained(t *testing.T) {
	cleanUpMessageSubscriptions()
	cp := CallPath{}

	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event-and-parallel-tasks.bpmn")
	t1H := bpmnEngine.NewTaskHandler().Id("task-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(t1H)
	t2H := bpmnEngine.NewTaskHandler().Id("task-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(t2H)
	instance, _ := bpmnEngine.CreateInstance(t.Context(), process, nil)

	tokens, err := bpmnEngine.persistence.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	err = bpmnEngine.RunProcessInstance(t.Context(), instance, tokens)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().GetState())

	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "event-1" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	assert.Equal(t, "task-2,task-1", cp.CallPath)

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestMultipleIntermediateCatchEventsPossible(t *testing.T) {
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

	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg-event-2" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task2", cp.CallPath)
	// then still active, since there's an implicit fork
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().GetState())
}

func TestMultipleIntermediateCatchEventsImplicitForkAndMergedCOMPLETED(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-merged.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg-event-1" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg-event-2" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg-event-3" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestMultipleIntermediateCatchEventsImplicitForkAndMergedACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-merged.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg-event-2" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	// then
	assert.Equal(t, instance.ProcessInstance().GetState(), runtime.ActivityStateActive)
}

func TestMultipleIntermediateCatchEventsImplicitForkAndParallelGatewayCOMPLETED(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-parallel.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.Nil(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.State != runtime.ActivityStateActive {
			continue
		}
		err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
		assert.NoError(t, err)
	}

	for _, message := range engineStorage.MessageSubscriptions {
		if message.State != runtime.ActivityStateActive {
			continue
		}
		err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
		assert.NoError(t, err)
	}

	for _, message := range engineStorage.MessageSubscriptions {
		if message.State != runtime.ActivityStateActive {
			continue
		}
		err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
		assert.NoError(t, err)
	}

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted.String(), instance.ProcessInstance().State.String())
}

func TestMultipleIntermediateCatchEventsImplicitForkAndParallelGatewayACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-parallel.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
		assert.NoError(t, err)
	}

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestMultipleIntermediateCatchEventsImplicitForkAndExclusiveGatewayCOMPLETED(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-exclusive.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg-event-1" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}
	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg-event-2" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg-event-3" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestMultipleIntermediateCatchEventsImplicitForkAndExclusiveGatewayACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-multiple-intermediate-catch-events-exclusive.bpmn")
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg-event-2" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().GetState())
}

func TestPublishingARandomMessageDoesNoHarm(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-catch-event.bpmn")
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "random-message" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().GetState())
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
	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg-b" {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	// then
	assert.Equal(t, "task-b", cp.CallPath)

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestIntermediateMessageCatchEventPublishesVariablesIntoInstance(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-intermediate-message-catch-event.bpmn")
	instance, _ := bpmnEngine.CreateInstance(t.Context(), process, nil)

	// when
	var err error
	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg" {
			vars := map[string]interface{}{"foo": "bar"}
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, vars)
			assert.NoError(t, err)
		}
	}

	// then
	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
	assert.Equal(t, "bar", instance.ProcessInstance().GetVariable("mappedFoo"))
}

func TestIntermediateMessageCatchEventOutputMappingReturnsEmpty(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-intermediate-message-catch-event-broken.bpmn")
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg" && message.State == runtime.ActivityStateActive {
			err = bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	// then
	message, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateCompleted)
	assert.NoError(t, err)

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.Equal(t, instance.ProcessInstance().GetState(), runtime.ActivityStateCompleted)
	assert.Nil(t, instance.ProcessInstance().GetVariable("mappedFoo"))
	assert.Equal(t, message[0].GetState(), runtime.ActivityStateCompleted)
}

func TestInterruptingBoundaryEventMessageCatchTriggered(t *testing.T) {
	// 1) After process start the message subscription bound to the boundary event should be created
	//    - process should be active
	//    - message subscription should be active
	// 2) After process message is thrown
	//    - the job and
	//    - any other boundary events subscriptions should be cancelled and
	//    - flow outgoing from the boundary should be taken

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-boundary-event-interrupting.bpmn")
	variableContext := make(map[string]interface{}, 1)
	randomCorellationKey := rand.Int63()
	variableContext["correlationKey"] = fmt.Sprint(randomCorellationKey)
	// when
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, variableContext)
	assert.NoError(t, err)

	// then
	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subscriptions))

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

	// when
	variables := map[string]interface{}{"payload": "message payload"}
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", fmt.Sprint(randomCorellationKey), variables)
	assert.NoError(t, err)

	// then
	subscriptions, err = bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())

	jobs = findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))

}

func TestNoninterruptingBoundaryEventMessageCatchTriggered(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-boundary-event-noninterrupting.bpmn")
	variableContext := make(map[string]interface{}, 1)
	randomCorellationKey := rand.Int63()
	variableContext["correlationKey"] = fmt.Sprint(randomCorellationKey)
	// when
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, variableContext)
	assert.NoError(t, err)

	// then
	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subscriptions))

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

	// when
	variables := map[string]interface{}{"payload": "message payload"}
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", fmt.Sprint(randomCorellationKey), variables)
	assert.NoError(t, err)

	// then
	subscriptions, err = bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subscriptions))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().GetState())

	jobs = findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

}

func TestBoundaryEventActivityCompleteCancelsSubscriptions(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-boundary-event-noninterrupting.bpmn")
	variableContext := make(map[string]interface{}, 1)
	randomCorellationKey := rand.Int63()
	variableContext["correlationKey"] = fmt.Sprint(randomCorellationKey)
	// when
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, variableContext)
	assert.NoError(t, err)

	// then
	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subscriptions))

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

	// when
	err = bpmnEngine.JobCompleteByKey(t.Context(), jobs[0].Key, jobs[0].Variables)
	assert.NoError(t, err)

	// then
	subscriptions, err = bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	jobs = findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())

}
